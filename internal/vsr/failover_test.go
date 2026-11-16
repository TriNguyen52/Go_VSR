package vsr_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

func TestViewChangeFencesPendingTransfer(t *testing.T) {
	b := vsr.New(2)
	out := b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 1})
	fetch := out[0].Message
	oldState := vsr.Message{Kind: vsr.StateTransfer, From: 0, To: 2, TransferID: fetch.TransferID,
		Op: 1, Commit: 1, Entries: []vsr.LogEntry{{RequestID: 1, Command: commands[1]}}}
	b.Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 1})
	installed := oldState
	installed.Kind, installed.From, installed.View = vsr.StartView, 1, 1
	b.Step(installed)
	before, snapshot := b.State(), b.Snapshot()
	for _, m := range []vsr.Message{oldState, out[1].Message,
		{Kind: vsr.Prepare, From: 0, To: 2, Op: 1, RequestID: 1, Command: commands[1]},
		{Kind: vsr.StartView, From: 0, To: 2}} {
		if effects := b.Step(m); len(effects) != 0 || b.State() != before || !reflect.DeepEqual(b.Snapshot(), snapshot) {
			t.Fatal("stale message changed installed view", m, effects)
		}
	}
}

func repeatedFailover(t *testing.T) *retryCluster {
	c := automaticCluster(t)
	for round := uint64(1); round <= 3; round++ {
		primary := int(c.replicas[0].State().View % 3)
		c.deliveries = func(m vsr.Message) []sim.Time {
			if m.From == primary || m.To == primary {
				return []sim.Time{150} // Partition heals by delivering old packets late.
			}
			return []sim.Time{1, 2}
		}
		c.start(t, commands[1])
		until(t, c, func() bool { return !c.client.Pending() })
		if c.client.Response().Result.Value != int64(round) {
			t.Fatal(c.client.Response())
		}
		c.deliveries = nil
		until(t, c, func() bool {
			for _, r := range c.replicas {
				if r.ChangingView() || r.State().Applied != round || r.State().View != c.client.Response().View {
					return false
				}
			}
			return true
		})
		end := c.runner.Now() + 160
		until(t, c, func() bool { return c.runner.Now() >= end })
		for id, r := range c.replicas {
			if r.State().Applied != round || c.applies[id] != int(round) {
				t.Fatal("late packets changed committed work", r.State(), c.applies)
			}
		}
	}
	return c
}

func TestAutomaticFailoverFreshProcessTrace(t *testing.T) {
	if os.Getenv("VSR_M7_TRACE_CHILD") == "1" {
		c := repeatedFailover(t)
		fmt.Printf("sha256=%x\n", sha256.Sum256([]byte(c.trace.String()+c.runner.Trace())))
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func() string {
		cmd := exec.Command(executable, "-test.run=^TestAutomaticFailoverFreshProcessTrace$", "-test.count=1")
		cmd.Env = append(os.Environ(), "VSR_M7_TRACE_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("different fresh-process traces: %s / %s", a, b)
	}
}

func TestOldTrafficCannotPostponeTimeout(t *testing.T) {
	r := vsr.New(2)
	r.Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 1})
	for i := uint64(0); i < 2*vsr.PrimaryTimeout; i++ {
		r.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2})
		r.Step(vsr.Message{Kind: vsr.ViewRetry, From: 2, To: 2, View: 1})
		r.Step(vsr.Message{Kind: vsr.StartViewChange, From: 0, To: 2, View: 1})
		r.AdvanceTime()
	}
	if r.State().View != 2 || !r.ChangingView() {
		t.Fatal("old traffic or retry reset escalation", r.State())
	}
}

func automaticCluster(t *testing.T) *retryCluster {
	c := newRetryCluster(t, nil)
	for id := 0; id < 3; id++ {
		c.clockTokens[id] = c.runner.Arm(endpoint(id), "clock", 1)
	}
	return c
}

// Periodic clocks intentionally never drain. Completion is a protocol predicate
// with an explicit event budget, not an empty simulator queue.
func until(t *testing.T, c *retryCluster, done func() bool) {
	t.Helper()
	for i := 0; i < 10000; i++ {
		if done() {
			return
		}
		c.runner.Run(1)
	}
	t.Fatalf("progress budget exhausted at %d\n%s", c.runner.Now(), c.trace.String())
}

func TestIdleCommitRepairsLoss(t *testing.T) {
	c := automaticCluster(t)
	lost := false
	c.drop = func(m vsr.Message) bool {
		if m.Kind == vsr.Commit && m.Commit == 1 && m.To == 2 && !lost {
			lost = true
			return true
		}
		return false
	}
	c.start(t, commands[1])
	until(t, c, func() bool { return !c.client.Pending() })
	if !lost || c.replicas[2].State().Applied != 0 {
		t.Fatal("fixture did not lose final commit")
	}
	until(t, c, func() bool { return c.replicas[2].State().Applied == 1 })
	c.runner.Run(500)
	for id, r := range c.replicas {
		if r.State().View != 0 || c.applies[id] != 1 {
			t.Fatal("idle periodic COMMIT messages caused election or duplicate application", id, r.State())
		}
	}
}

func TestAutomaticFailoverAmbiguousOutcome(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			c := automaticCluster(t)
			c.drop = func(m vsr.Message) bool {
				return m.Kind == vsr.Reply || (!committed && m.Kind == vsr.PrepareOK)
			}
			c.start(t, commands[1])
			until(t, c, func() bool {
				return c.replicas[1].State().Op == 1 && c.replicas[2].State().Op == 1 &&
					(!committed || c.replicas[0].State().Commit == 1)
			})
			c.stopped[0] = true // No restart; queued packets may still arrive.
			c.drop = nil
			until(t, c, func() bool {
				return !c.client.Pending() && c.replicas[1].State().Applied == 1 && c.replicas[2].State().Applied == 1
			})
			if c.client.Response().Result.Value != 1 || c.replicas[1].State().View == 0 || c.applies[1] != 1 || c.applies[2] != 1 {
				t.Fatal(c.client.Response(), c.applies)
			}
			c.start(t, commands[1])
			until(t, c, func() bool { return !c.client.Pending() && c.replicas[2].State().Applied == 2 })
			if c.client.Response().Result.Value != 2 {
				t.Fatal(c.client.Response())
			}
			t.Logf("old primary stopped; view=%d; retried Add=1; next Add=2", c.replicas[1].State().View)
		})
	}
}

func TestUnavailableNextPrimaryAndHealing(t *testing.T) {
	c := automaticCluster(t)
	// Directed loss: r0 cannot reach either backup; r1 cannot reach r0/r2.
	// r2 can still send suspicion to r0, creating a live quorum in view 2.
	c.deliveries = func(m vsr.Message) []sim.Time {
		if m.From < 3 && m.To < 3 && (m.From == 1 || (m.From == 0 && m.View == 0)) {
			return nil
		}
		return []sim.Time{1}
	}
	c.start(t, commands[1])
	until(t, c, func() bool { return !c.client.Pending() && c.replicas[2].State().View >= 2 })
	if c.client.Response().Result.Value != 1 {
		t.Fatal(c.client.Response())
	}
	c.deliveries = nil
	until(t, c, func() bool {
		for _, r := range c.replicas {
			if r.ChangingView() || r.State().Applied != 1 || r.State().View != c.replicas[2].State().View {
				return false
			}
		}
		return true
	})
	c.start(t, kv.Command{Op: kv.Get, Key: "x"})
	until(t, c, func() bool { return !c.client.Pending() })
	if c.client.Response().Result.Value != 1 {
		t.Fatal(c.client.Response())
	}
}

func TestIsolatedPrimaryCannotCommitReadOrWrite(t *testing.T) {
	for _, op := range []kv.Operation{kv.Add, kv.Get} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			c := automaticCluster(t)
			c.drop = func(m vsr.Message) bool { return m.From == 0 || m.To == 0 }
			c.start(t, kv.Command{Op: op, Key: "x", Value: 1})
			until(t, c, func() bool { return !c.client.Pending() })
			if c.replicas[0].State().Commit != 0 || c.replicas[0].State().Applied != 0 {
				t.Fatal("isolated old primary executed without quorum")
			}
			c.drop = nil
			until(t, c, func() bool { return c.replicas[0].State().Applied == 1 && c.replicas[0].State().View > 0 })
		})
	}
}
