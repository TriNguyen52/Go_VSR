package vsr_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

func mustMessage(t *testing.T, m vsr.Message) []byte {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// These schedules follow section 9 of the companion research. Each trigger is
// a protocol predicate; each case checks safety through the existing checker,
// heals, finishes pending work, and compares a preserved boundary trace.
func TestResearchFailureScenarios(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T) *retryCluster
	}{
		{"1-primary-crash-lost-reply", func(t *testing.T) *retryCluster { return crashBeforeReply(t, true) }},
		{"1-primary-crash-before-ack", func(t *testing.T) *retryCluster { return crashBeforeReply(t, false) }},
		{"2-isolated-primary-delayed-traffic", isolatedPrimary},
		{"3-duplicates-and-gap", func(t *testing.T) *retryCluster {
			// Handler fixtures cover stale ACKs and distinct view-report senders;
			// the recorded network schedule forces duplicate traffic and a gap.
			TestDuplicateRequestsAndPrepares(t)
			TestViewSelectionAndFencing(t)
			lost := lossScenario(t, vsr.Reply)
			gap := gapScenario(t, "")
			gap.trace.WriteString(lost.trace.String() + lost.runner.Trace())
			return gap
		}},
		{"4-obsolete-recovery", obsoleteRecovery},
		{"5-lagging-new-primary-lost-view-change", laggingNewPrimary},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := test.run(t)
			trace := []byte(c.trace.String() + c.runner.Trace())
			path := filepath.Join("testdata", test.name+".trace.gz")
			if os.Getenv("VSR_M9_UPDATE_TRACES") == "1" {
				if err := os.MkdirAll("testdata", 0755); err != nil {
					t.Fatal(err)
				}
				var buffer bytes.Buffer
				writer := gzip.NewWriter(&buffer)
				if _, err := writer.Write(trace); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, buffer.Bytes(), 0644); err != nil {
					t.Fatal(err)
				}
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			want, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(trace, want) {
				t.Fatal("regression boundary trace diverged; inspect the schedule before updating fixtures")
			}
			t.Logf("healed, requests drained; trace sha256=%x", sha256.Sum256(trace))
		})
	}
}

func healScenario(t *testing.T, c *retryCluster, operations uint64) {
	t.Helper()
	c.drop, c.deliveries = nil, nil
	for id := 0; id < 3; id++ {
		if c.stopped[id] {
			c.restart(t, id)
		}
	}
	until(t, c, func() bool {
		if c.client.Pending() {
			return false
		}
		for _, r := range c.replicas {
			if r.Recovering() || r.ChangingView() || r.State().Applied != operations {
				return false
			}
		}
		return true
	})
	// Drain all intentionally delayed pre-healing packets; periodic clocks stay.
	end := c.runner.Now() + 170
	until(t, c, func() bool { return c.runner.Now() >= end })
	for _, r := range c.replicas {
		if r.State().Applied != operations {
			t.Fatal(r.State())
		}
	}
	if err := c.checker.CheckReply(c.client.Response()); err != nil {
		t.Fatal(err)
	}
}

func crashBeforeReply(t *testing.T, committed bool) *retryCluster {
	c := automaticCluster(t)
	c.drop = func(m vsr.Message) bool { return m.Kind == vsr.Reply || (!committed && m.Kind == vsr.PrepareOK) }
	c.start(t, commands[1])
	until(t, c, func() bool {
		return c.replicas[1].State().Op == 1 && c.replicas[2].State().Op == 1 && (!committed || c.replicas[0].State().Commit == 1)
	})
	c.crash(0)
	c.drop = nil
	until(t, c, func() bool { return !c.client.Pending() })
	if c.client.Response().Result.Value != 1 {
		t.Fatal(c.client.Response())
	}
	healScenario(t, c, 1)
	return c
}

func isolatedPrimary(t *testing.T) *retryCluster {
	c := automaticCluster(t)
	// Keep old request/prepares/commits/replies in flight across a new view.
	c.start(t, commands[1])
	until(t, c, func() bool { return !c.client.Pending() })
	old := c.client.Response()
	data := mustMessage(t, old)
	c.runner.Send(endpoint(old.From), endpoint(old.To), data, 150)
	c.deliveries = func(m vsr.Message) []sim.Time {
		if m.From == 0 || m.To == 0 {
			return []sim.Time{150}
		}
		return []sim.Time{1}
	}
	c.start(t, kv.Command{Op: kv.Put, Key: "x", Value: 9})
	until(t, c, func() bool { return !c.client.Pending() && c.replicas[1].State().View > 0 })
	// A fresh local read at the isolated old primary cannot return successfully.
	before := c.replicas[0].State().Commit
	out := c.replicas[0].Step(vsr.Message{Kind: vsr.Request, From: 3, To: 0, RequestID: 3, Command: kv.Command{Op: kv.Get, Key: "x"}})
	for _, e := range out {
		if e.Kind == vsr.SendEffect && e.Message.Kind == vsr.Reply {
			t.Fatal("isolated read replied")
		}
	}
	c.effects(t, 0, out)
	if c.replicas[0].State().Commit != before {
		t.Fatal("isolated read committed")
	}
	// The real client uses this same identity/command, including any old suffix.
	c.start(t, kv.Command{Op: kv.Get, Key: "x"})
	healScenario(t, c, 3)
	if c.client.Response().Result.Value != 9 {
		t.Fatal(c.client.Response())
	}
	return c
}

func obsoleteRecovery(t *testing.T) *retryCluster {
	c := automaticCluster(t)
	c.start(t, commands[1])
	until(t, c, func() bool { return !c.client.Pending() && c.replicas[2].State().Applied == 1 })
	// A local pre-crash callback and old network PREPARE remain scheduled.
	c.effects(t, 2, []vsr.Effect{{Kind: vsr.ArmEffect, Message: vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 9}}})
	c.runner.Send(endpoint(0), endpoint(2), mustMessage(t, vsr.Message{Kind: vsr.Prepare, From: 0, To: 2, Op: 1, RequestID: 1, Command: commands[1]}), 15)
	c.crash(2)
	c.deliveries = func(m vsr.Message) []sim.Time {
		if m.Kind == vsr.RecoveryResponse {
			return []sim.Time{20}
		}
		return []sim.Time{1}
	}
	c.restart(t, 2)
	oldNonce := c.nextNonce
	until(t, c, func() bool { return c.runner.Now() >= 8 })
	c.crash(2)
	c.drop = func(m vsr.Message) bool { return m.Kind == vsr.RecoveryResponse && m.Nonce != oldNonce }
	c.restart(t, 2)
	until(t, c, func() bool { return c.runner.Now() >= 30 })
	if !c.replicas[2].Recovering() || c.replicas[2].State().Op != 0 {
		t.Fatal("obsolete traffic recovered replica")
	}
	healScenario(t, c, 1)
	// Make the recovered replica primary through real view-change messages;
	// it must have reconstructed its cached result at the same identity.
	c.effects(t, 2, c.replicas[2].Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 2}))
	until(t, c, func() bool { return !c.replicas[2].ChangingView() })
	out := c.replicas[2].Step(vsr.Message{Kind: vsr.Request, From: 3, To: 2, RequestID: 1, Command: commands[1]})
	if len(out) != 1 || out[0].Message.Result.Value != 1 {
		t.Fatal(out)
	}
	if err := c.checker.CheckReply(out[0].Message); err != nil {
		t.Fatal(err)
	}
	c.start(t, commands[1])
	healScenario(t, c, 2)
	return c
}

func laggingNewPrimary(t *testing.T) *retryCluster {
	c := automaticCluster(t)
	c.drop = func(m vsr.Message) bool {
		return m.To == 1 && (m.Kind == vsr.Prepare || m.Kind == vsr.Commit) || m.Kind == vsr.PrepareOK
	}
	c.start(t, commands[1])
	until(t, c, func() bool { return c.replicas[2].State().Op == 1 })
	if c.replicas[1].State().Op != 0 {
		t.Fatal("new primary is not lagging")
	}
	c.crash(0)
	lost := false
	c.drop = func(m vsr.Message) bool {
		if m.Kind == vsr.DoViewChange && !lost {
			lost = true
			return true
		}
		return false
	}
	until(t, c, func() bool { return !c.client.Pending() })
	if !lost || c.client.Response().Result.Value != 1 {
		t.Fatal("view retry fixture", lost, c.client.Response())
	}
	healScenario(t, c, 1)
	// Later view must fence a delayed old START_VIEW; selection fixture checks
	// lastNormalView takes precedence over suffix length and duplicate reports.
	TestViewSelectionAndFencing(t)
	view := c.replicas[1].State().View + 1
	c.effects(t, 1, c.replicas[1].Step(vsr.Message{Kind: vsr.Suspect, From: 1, To: 1, View: view}))
	until(t, c, func() bool { return !c.replicas[1].ChangingView() })
	until(t, c, func() bool { return !c.replicas[2].ChangingView() && c.replicas[2].State().View == view })
	before := c.replicas[2].State()
	c.runner.Send(endpoint(1), endpoint(2), mustMessage(t, vsr.Message{Kind: vsr.StartView, From: 1, To: 2, View: 1}), 1)
	end := c.runner.Now() + 2
	until(t, c, func() bool { return c.runner.Now() >= end })
	if c.replicas[2].State() != before {
		t.Fatal(fmt.Sprint("old view rolled state back", before))
	}
	healScenario(t, c, 1)
	return c
}
