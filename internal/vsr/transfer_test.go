package vsr_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

func gapScenario(t *testing.T, lose vsr.Kind) *retryCluster {
	t.Helper()
	c := newRetryCluster(t, nil)
	dropped := false
	c.deliveries = func(m vsr.Message) []sim.Time {
		if m.Kind == lose && !dropped {
			dropped = true
			return nil
		}
		if m.To == 2 && ((m.Kind == vsr.Prepare && m.Op == 1) || (m.Kind == vsr.Commit && m.Commit == 1)) {
			return []sim.Time{60} // Operation 2 overtakes operation 1.
		}
		if m.Kind == vsr.StateTransfer {
			return []sim.Time{5, 25} // A late duplicate must not reapply anything.
		}
		if m.Kind == vsr.Prepare || m.Kind == vsr.PrepareOK {
			return []sim.Time{1, 2}
		}
		return []sim.Time{1}
	}
	for n := 1; n <= 2; n++ {
		c.start(t, commands[1])
		for events := 0; c.client.Pending() && events < 100; events++ {
			c.runner.Run(1)
		}
		if c.client.Pending() || c.client.Response().Result.Value != int64(n) {
			t.Fatal("client did not complete", c.trace.String())
		}
	}
	// The primary and other backup completed both commands, but r2 saw op 2
	// first and has neither acknowledged nor applied an incomplete prefix.
	if c.replicas[2].State().Op != 0 {
		t.Fatal("gap was not exercised", c.trace.String())
	}
	for _, record := range c.diagram.Messages {
		if record.Message.From == 2 && record.Message.Kind == vsr.PrepareOK {
			t.Fatal("acknowledged before filling gap")
		}
	}
	if result := c.runner.Run(300); !result.Complete {
		t.Fatal(result, c.trace.String())
	}
	for id, r := range c.replicas {
		if r.State() != (vsr.Counters{Op: 2, Commit: 2, Applied: 2}) || c.applies[id] != 2 ||
			!reflect.DeepEqual(r.Snapshot(), []kv.Entry{{Key: "x", Value: 2}}) {
			t.Fatal(id, r.State(), r.Snapshot(), c.applies)
		}
	}
	if lose != "" && (!dropped || !strings.Contains(c.trace.String(), "TRANSFER_RETRY")) {
		t.Fatal("transfer loss did not exercise retry")
	}
	return c
}

func TestGapDemo(t *testing.T) {
	c := gapScenario(t, "")
	t.Log("\n" + c.trace.String())
	t.Log("all replicas: op=2 commit=2 applied=2 x=2; two logical Adds")
}

func TestTransferLosses(t *testing.T) {
	for _, kind := range []vsr.Kind{vsr.FetchState, vsr.StateTransfer} {
		t.Run(string(kind), func(t *testing.T) { gapScenario(t, kind) })
	}
}

func transferFixture(t *testing.T) (*vsr.Replica, *vsr.Replica, vsr.Message, vsr.Message) {
	t.Helper()
	p, b := vsr.New(0), vsr.New(2)
	for id := uint64(1); id <= 2; id++ {
		p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, RequestID: id, Command: commands[1]})
		p.Step(vsr.Message{Kind: vsr.PrepareOK, From: 1, Op: id})
	}
	gap := b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 2})
	if len(gap) != 2 || gap[0].Message.Kind != vsr.FetchState {
		t.Fatal(gap)
	}
	response := p.Step(gap[0].Message)[0].Message
	return p, b, response, gap[1].Message
}

func TestTransferValidation(t *testing.T) {
	for _, field := range []string{"sender", "destination", "view", "id", "length", "commit", "short", "zero-request", "duplicate-request", "request-order"} {
		t.Run(field, func(t *testing.T) {
			_, b, response, _ := transferFixture(t)
			bad := response
			bad.Entries = append([]vsr.LogEntry(nil), response.Entries...)
			switch field {
			case "sender":
				bad.From = 1
			case "destination":
				bad.To = 1
			case "view":
				bad.View++
			case "id":
				bad.TransferID++
			case "length":
				bad.Op++
			case "commit":
				bad.Commit = 3
			case "short":
				bad.Op, bad.Commit, bad.Entries = 1, 1, bad.Entries[:1]
			case "zero-request":
				bad.Entries[0].RequestID = 0
			case "duplicate-request":
				bad.Entries[1].RequestID = 1
			case "request-order":
				bad.Entries[0].RequestID, bad.Entries[1].RequestID = 3, 2
			}
			if out := b.Step(bad); len(out) != 0 || b.State() != (vsr.Counters{}) {
				t.Fatal("invalid transfer changed state", out, b.State())
			}
			b.Step(response)
			if b.State().Applied != 2 || b.Snapshot()[0].Value != 2 {
				t.Fatal("valid response failed after rejection")
			}
		})
	}
}

func TestTransferPreservesHistory(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			_, b, response, _ := transferFixture(t)
			b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 2, Op: 1, RequestID: 1, Command: commands[1]})
			if committed {
				b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 1})
			}
			before := b.State()
			response.Entries[0].Command.Value = 999
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("expected conflicting history assertion")
					}
				}()
				b.Step(response)
			}()
			if b.State() != before || b.Log()[0] != commands[1] {
				t.Fatal("mutated before validation")
			}
		})
	}
}

func TestDelayedTransferCannotRollBack(t *testing.T) {
	p, b, old, timer := transferFixture(t)
	// Ordinary prepares fill the gap and apply it before STATE arrives.
	for id := uint64(1); id <= 2; id++ {
		b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 2, Op: id, RequestID: id, Command: commands[1]})
	}
	b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 2})
	if out := b.Step(old); len(out) != 0 {
		t.Fatal("accepted completed transfer", out)
	}
	// A later gap starts another ID. Old responses/timers cannot satisfy it.
	p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, RequestID: 3, Command: commands[1]})
	p.Step(vsr.Message{Kind: vsr.PrepareOK, From: 1, Op: 3})
	fetch := b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 3})[0].Message
	if fetch.TransferID == old.TransferID {
		t.Fatal("reused transfer ID")
	}
	for _, stale := range []vsr.Message{old, timer} {
		if out := b.Step(stale); len(out) != 0 || b.State().Applied != 2 {
			t.Fatal(out, b.State())
		}
	}
	fresh := p.Step(fetch)[0].Message
	b.Step(fresh)
	if b.State().Applied != 3 || b.Snapshot()[0].Value != 3 {
		t.Fatal(b.State())
	}
	if out := b.Step(fresh); len(out) != 0 {
		t.Fatal("duplicate response executed twice")
	}
	// Neither outgoing nor incoming transfer slices alias replica logs.
	fresh.Entries[0].Command.Value = 999
	if p.Log()[0] != commands[1] || b.Log()[0] != commands[1] {
		t.Fatal("mutable transfer aliases log")
	}
}

func TestTransferAcknowledgesOnlyCompletePrefix(t *testing.T) {
	p, b, old, _ := transferFixture(t)
	// Advance the target while the first response is delayed; retry the SAME ID.
	p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, RequestID: 3, Command: commands[1]})
	b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 2, Op: 3, Commit: 2, RequestID: 3, Command: commands[1]})
	if out := b.Step(old); len(out) != 0 || b.State().Op != 0 {
		t.Fatal("installed obsolete snapshot", out)
	}
	response := p.Step(vsr.Message{Kind: vsr.FetchState, From: 2, TransferID: old.TransferID})[0].Message
	out := b.Step(response)
	ack := out[len(out)-1].Message
	if ack.Kind != vsr.PrepareOK || ack.Op != 3 || b.State() != (vsr.Counters{Op: 3, Commit: 2, Applied: 2}) {
		t.Fatal(out, b.State())
	}
	p.Step(ack)
	if p.State().Applied != 3 {
		t.Fatal("transferred pending entry did not form quorum")
	}
}

func TestTransferUnavailableIsIncomplete(t *testing.T) {
	c := newRetryCluster(t, func(m vsr.Message) bool {
		return (m.Kind == vsr.Prepare && m.To == 2) || m.Kind == vsr.StateTransfer
	})
	c.start(t, commands[1])
	if result := c.runner.Run(100); result.Complete || c.client.Pending() || c.replicas[2].State().Applied != 0 {
		t.Fatal(result, c.trace.String())
	}
}

func TestTransferRejectsRegressionWhileActive(t *testing.T) {
	for _, counter := range []string{"op", "commit"} {
		t.Run(counter, func(t *testing.T) {
			_, b, response, _ := transferFixture(t)
			for id := uint64(1); id <= 3; id++ {
				b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 2, Op: id, RequestID: id, Command: commands[1]})
			}
			// Keep transfer active: target commit 2 is still missing. Local op
			// has advanced beyond the snapshot, without starting a new ID.
			if counter == "commit" {
				b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 1})
				response.Op, response.Commit = 3, 0
				response.Entries = append(response.Entries, vsr.LogEntry{RequestID: 3, Command: commands[1]})
			}
			before := b.State()
			if out := b.Step(response); len(out) != 0 || b.State() != before {
				t.Fatal("delayed snapshot moved active replica backward", out, b.State())
			}
		})
	}
}

func TestFetchValidation(t *testing.T) {
	p, b, _, _ := transferFixture(t)
	valid := vsr.Message{Kind: vsr.FetchState, From: 2, TransferID: 1, Commit: 2}
	for _, field := range []string{"self", "nonmember", "negative", "id", "view", "commit"} {
		m := valid
		switch field {
		case "self":
			m.From = 0
		case "nonmember":
			m.From = vsr.ClientEndpoint
		case "negative":
			m.From = -1
		case "id":
			m.TransferID = 0
		case "view":
			m.View = 1
		case "commit":
			m.Commit = 3
		}
		if out := p.Step(m); len(out) != 0 {
			t.Fatal(field, out)
		}
	}
	valid.To, valid.From = 2, 1
	if out := b.Step(valid); len(out) != 0 {
		t.Fatal("backup served primary-only transfer", out)
	}
}

func TestGapFreshProcessTrace(t *testing.T) {
	if os.Getenv("VSR_M5_TRACE_CHILD") == "1" {
		c := gapScenario(t, vsr.StateTransfer)
		trace := c.trace.String() + c.runner.Trace()
		fmt.Print(trace)
		fmt.Printf("sha256=%x\n", sha256.Sum256([]byte(trace)))
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func() string {
		cmd := exec.Command(executable, "-test.run=^TestGapFreshProcessTrace$", "-test.count=1")
		cmd.Env = append(os.Environ(), "VSR_M5_TRACE_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	a, b := run(), run()
	if a != b || !strings.Contains(a, "sha256=") {
		t.Fatal("fresh process traces differ")
	}
	t.Log(strings.Split(a[strings.Index(a, "sha256="):], "\n")[0])
}
