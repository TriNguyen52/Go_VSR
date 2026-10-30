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

func viewScenario(t *testing.T, lose vsr.Kind, committed bool) *retryCluster {
	t.Helper()
	c := newRetryCluster(t, nil)
	// r1, the next primary, misses the entire old history. r0 and r2 have it.
	c.deliveries = func(m vsr.Message) []sim.Time {
		if m.To == 1 && (m.Kind == vsr.Prepare || m.Kind == vsr.Commit) {
			return nil
		}
		if !committed && m.Kind == vsr.PrepareOK {
			return nil
		}
		return []sim.Time{1}
	}
	c.start(t, commands[1])
	for i := 0; i < 30; i++ {
		c.runner.Run(1)
		if (committed && !c.client.Pending()) || (!committed && c.replicas[2].State().Op == 1) {
			break
		}
	}
	if c.replicas[1].State().Op != 0 || c.replicas[2].State().Op != 1 {
		t.Fatal("lagging primary fixture failed")
	}
	dropped := false
	lost := map[[2]int]bool{}
	c.deliveries = func(m vsr.Message) []sim.Time {
		link := [2]int{m.From, m.To}
		if m.Kind == lose && !lost[link] {
			dropped = true
			lost[link] = true
			return nil
		}
		if m.Kind == vsr.DoViewChange {
			return []sim.Time{1, 2}
		}
		return []sim.Time{1}
	}
	c.effects(t, 2, c.replicas[2].Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 1}))
	if result := c.runner.Run(400); !result.Complete || c.client.Pending() {
		t.Fatalf("incomplete %+v\n%s", result, c.trace.String())
	}
	for id, r := range c.replicas {
		if r.State() != (vsr.Counters{View: 1, Op: 1, Commit: 1, Applied: 1}) || r.ChangingView() || r.LastNormalView() != 1 || c.applies[id] != 1 {
			t.Fatal(id, r.State(), c.applies)
		}
	}
	if lose != "" && !dropped {
		t.Fatal("loss not exercised")
	}
	if lose == vsr.DoViewChange && !strings.Contains(c.trace.String(), "VIEW_RETRY") {
		t.Fatal("report loss did not require retry")
	}
	// A stale client view must not prevent the new primary returning its cached result.
	out := c.replicas[1].Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 1, RequestID: 1, Command: commands[1]})
	if len(out) != 1 || out[0].Message.Kind != vsr.Reply || out[0].Message.Result.Value != 1 {
		t.Fatal(out)
	}
	// New work uses the same client and must still complete through normal replication.
	c.start(t, commands[1])
	if result := c.runner.Run(300); !result.Complete || c.client.Pending() || c.client.Response().Result.Value != 2 {
		t.Fatal(result, c.trace.String())
	}
	return c
}

func TestViewChangeLosses(t *testing.T) {
	for _, committed := range []bool{false, true} {
		for _, kind := range []vsr.Kind{vsr.StartViewChange, vsr.DoViewChange, vsr.StartView} {
			t.Run(fmt.Sprintf("%v/%s", committed, kind), func(t *testing.T) { viewScenario(t, kind, committed) })
		}
	}
}
func TestViewChangeDemo(t *testing.T) {
	c := viewScenario(t, vsr.DoViewChange, true)
	t.Log("\n" + c.trace.String())
	t.Log("lagging r1 acquired op 1; all replicas reached view 1; retry returned x=1; next Add returned x=2")
}

func TestViewSelectionAndFencing(t *testing.T) {
	for _, sameView := range []bool{false, true} {
		t.Run(fmt.Sprint(sameView), func(t *testing.T) {
			r := vsr.New(2) // Primary of view 2.
			r.Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 2})
			older := vsr.Message{Kind: vsr.DoViewChange, From: 0, To: 2, View: 2, Op: 2, Entries: []vsr.LogEntry{{RequestID: 1, Command: commands[1]}, {RequestID: 2, Command: commands[1]}}}
			newer := vsr.Message{Kind: vsr.DoViewChange, From: 1, To: 2, View: 2, LastNormalView: 1, Op: 1, Entries: older.Entries[:1]}
			if sameView {
				newer.LastNormalView = 0
			}
			for i := 0; i < 3; i++ {
				r.Step(older)
			}
			if !r.ChangingView() {
				t.Fatal("duplicates formed a quorum")
			}
			out := r.Step(newer)
			want := uint64(1)
			if sameView {
				want = 2
			}
			if r.State().Op != want || r.State().Commit != 0 || r.ChangingView() {
				t.Fatal(r.State())
			}
			for _, e := range out {
				if e.Kind == vsr.SendEffect && e.Message.Kind == vsr.Reply {
					t.Fatal("reply before new-view ack")
				}
			}
			r.Step(vsr.Message{Kind: vsr.PrepareOK, From: 0, To: 2, View: 0, Op: want})
			if r.State().Commit != 0 {
				t.Fatal("old ack committed retained suffix")
			}
			r.Step(vsr.Message{Kind: vsr.PrepareOK, From: 0, To: 2, View: 2, Op: want})
			if r.State().Commit != want {
				t.Fatal("new ack failed")
			}
		})
	}
	b := vsr.New(1)
	b.Step(vsr.Message{Kind: vsr.Suspect, From: 1, To: 1, View: 1})
	for _, view := range []uint64{0, 1} {
		b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 1, View: view, Op: 1, RequestID: 1, Command: commands[1]})
	}
	if b.State().Op != 0 || b.LastNormalView() != 0 {
		t.Fatal("accepted prepare during change")
	}
	b.Step(vsr.Message{Kind: vsr.Suspect, From: 1, To: 1, View: 2})
	if b.LastNormalView() != 0 {
		t.Fatal("unsuccessful view became last normal view")
	}
}

func TestViewReplacesOnlySpeculativeSuffix(t *testing.T) {
	r := vsr.New(0)
	r.Step(vsr.Message{Kind: vsr.Request, From: 3, RequestID: 1, Command: commands[1]})
	r.Step(vsr.Message{Kind: vsr.PrepareOK, From: 1, Op: 1})
	r.Step(vsr.Message{Kind: vsr.Request, From: 3, RequestID: 2, Command: commands[1]})
	replacement := kv.Command{Op: kv.Add, Key: "x", Value: 7}
	m := vsr.Message{Kind: vsr.StartView, From: 1, To: 0, View: 1, Op: 2, Commit: 1, Entries: []vsr.LogEntry{{RequestID: 1, Command: commands[1]}, {RequestID: 3, Command: replacement}}}
	r.Step(m)
	if r.State().Applied != 1 || r.Snapshot()[0].Value != 1 {
		t.Fatal("replayed committed prefix")
	}
	// Later become primary with this suffix, then finish it and retry the discarded ID.
	r.Step(vsr.Message{Kind: vsr.Suspect, From: 0, To: 0, View: 3})
	r.Step(vsr.Message{Kind: vsr.StartViewChange, From: 1, To: 0, View: 3})
	report := m
	report.Kind = vsr.DoViewChange
	report.View = 3
	report.LastNormalView = 1
	r.Step(report)
	r.Step(vsr.Message{Kind: vsr.PrepareOK, From: 1, View: 3, Op: 2})
	out := r.Step(vsr.Message{Kind: vsr.Request, From: 3, RequestID: 1, Command: commands[1]})
	if len(out) != 1 || out[0].Message.Result.Value != 1 || r.Snapshot()[0].Value != 8 {
		t.Fatal(out, r.Snapshot())
	}
	// A discarded request must not retain a cached or pending record.
	if out := r.Step(vsr.Message{Kind: vsr.Request, From: 3, RequestID: 2, Command: commands[1]}); len(out) != 0 {
		t.Fatal(out)
	}
	before := r.State()
	r.Step(m)
	if r.State() != before {
		t.Fatal("stale start view rolled back state")
	}
}

func TestNewerViewCatchup(t *testing.T) {
	c := newRetryCluster(t, nil)
	c.deliveries = func(m vsr.Message) []sim.Time {
		if m.To == 0 {
			return nil
		}
		return []sim.Time{1}
	}
	c.effects(t, 2, c.replicas[2].Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 1}))
	if result := c.runner.Run(100); !result.Complete {
		t.Fatal(result)
	}
	if c.replicas[0].State().View != 0 {
		t.Fatal("fixture")
	}
	c.deliveries = nil
	c.effects(t, 0, c.replicas[0].Step(vsr.Message{Kind: vsr.Commit, From: 1, To: 0, View: 1}))
	if result := c.runner.Run(100); !result.Complete || c.replicas[0].ChangingView() || c.replicas[0].State().View != 1 {
		t.Fatal(result, c.trace.String())
	}
}

func TestViewFreshProcessTrace(t *testing.T) {
	if os.Getenv("VSR_M6_TRACE_CHILD") == "1" {
		c := viewScenario(t, vsr.StartView, false)
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
		cmd := exec.Command(executable, "-test.run=^TestViewFreshProcessTrace$", "-test.count=1")
		cmd.Env = append(os.Environ(), "VSR_M6_TRACE_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	a, b := run(), run()
	if !reflect.DeepEqual(a, b) || !strings.Contains(a, "sha256=") {
		t.Fatal("different traces")
	}
	t.Log(strings.Split(a[strings.Index(a, "sha256="):], "\n")[0])
}

func TestInstallRetainsLocallyKnownCommit(t *testing.T) {
	r := vsr.New(0)
	r.Step(vsr.Message{Kind: vsr.Request, From: 3, RequestID: 1, Command: commands[1]})
	r.Step(vsr.Message{Kind: vsr.PrepareOK, From: 2, Op: 1})
	// New quorum r1+r2 knows the prepared entry but never learned its commit.
	m := vsr.Message{Kind: vsr.StartView, From: 1, To: 0, View: 1, Op: 1, Entries: []vsr.LogEntry{{RequestID: 1, Command: commands[1]}}}
	out := r.Step(m)
	if r.State().Applied != 1 || r.State().Commit != 1 || r.Snapshot()[0].Value != 1 {
		t.Fatal(r.State())
	}
	ack := false
	for _, e := range out {
		if e.Message.Kind == vsr.PrepareOK {
			ack = true
		}
	}
	if !ack {
		t.Fatal("new primary still needs confirmation despite local commitment")
	}
}

func TestViewMessageOwnership(t *testing.T) {
	r := vsr.New(2)
	r.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 2, Op: 1, RequestID: 1, Command: commands[1]})
	r.Step(vsr.Message{Kind: vsr.Suspect, From: 2, To: 2, View: 1})
	out := r.Step(vsr.Message{Kind: vsr.StartViewChange, From: 1, To: 2, View: 1})
	for _, e := range out {
		if e.Message.Kind == vsr.DoViewChange {
			e.Message.Entries[0].Command.Value = 99
		}
	}
	retry := r.Step(vsr.Message{Kind: vsr.ViewRetry, From: 2, To: 2, View: 1})
	for _, e := range retry {
		if e.Message.Kind == vsr.DoViewChange && e.Message.Entries[0].Command != commands[1] {
			t.Fatal("outgoing report aliases frozen retry")
		}
	}
}
