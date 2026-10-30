package vsr_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"go-vsr/internal/checker"
	"go-vsr/internal/client"
	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

// Scripted faults live at the adapter boundary; all protocol work stays in Step.
type retryCluster struct {
	runner     sim.Runner
	replicas   [3]*vsr.Replica
	client     client.Client
	checker    checker.Checker
	tokens     [4]sim.TimerToken
	callbacks  [4]vsr.Message
	drop       func(vsr.Message) bool
	deliveries func(vsr.Message) []sim.Time // One delay per copy; nil drops the packet.
	trace      strings.Builder
	applies    [3]int
	diagram    retryDiagram
}

// Recorded adapter events only; visualization never changes protocol inputs.
type retryDiagram struct {
	diagramTrace
	Timers []retryTimer
}

type retryTimer struct {
	Time     sim.Time
	Endpoint int
	Action   vsr.EffectKind
}

func newRetryCluster(t *testing.T, drop func(vsr.Message) bool) *retryCluster {
	c := &retryCluster{drop: drop}
	for id := 0; id <= vsr.ClientEndpoint; id++ {
		if id < 3 {
			c.replicas[id] = vsr.New(id)
		}
		c.runner.Register(endpoint(id), func(e sim.Event) {
			var m vsr.Message
			if e.Kind == sim.Timeout {
				m = c.callbacks[id]
				c.diagram.Timers = append(c.diagram.Timers, retryTimer{e.Time, id, "fire"})
			} else if err := json.Unmarshal(e.Payload, &m); err != nil {
				t.Fatal(err)
			} else {
				for _, record := range c.diagram.Messages {
					if record.ID == e.Sequence {
						at := e.Time
						record.Delivered = &at
						break
					}
				}
			}
			var effects []vsr.Effect
			if id == vsr.ClientEndpoint {
				effects = c.client.Step(m)
				fmt.Fprintf(&c.trace, "t=%d %-10s client request=%d pending=%v value=%d\n", e.Time, m.Kind, m.RequestID, c.client.Pending(), m.Result.Value)
			} else {
				r := c.replicas[id]
				effects = r.Step(m)
				s := r.State()
				fmt.Fprintf(&c.trace, "t=%d %-10s r=%d view=%d op=%d commit=%d applied=%d request=%d\n", e.Time, m.Kind, id, s.View, s.Op, s.Commit, s.Applied, m.RequestID)
			}
			c.effects(t, id, effects)
			// Compare only equivalent application positions, never arbitrary logs.
			for a := 0; a < vsr.ReplicaCount; a++ {
				for b := a + 1; b < vsr.ReplicaCount; b++ {
					ra, rb := c.replicas[a], c.replicas[b]
					if ra.State().Applied == rb.State().Applied && !reflect.DeepEqual(ra.Snapshot(), rb.Snapshot()) {
						t.Fatalf("state disagreement at equal applied positions: %d, %d", a, b)
					}
				}
			}
		})
	}
	return c
}

func (c *retryCluster) effects(t *testing.T, owner int, effects []vsr.Effect) {
	t.Helper()
	for _, effect := range effects {
		switch effect.Kind {
		case vsr.SendEffect:
			m := effect.Message
			record := &diagramMessage{Sent: c.runner.Now(), Message: m}
			c.diagram.Messages = append(c.diagram.Messages, record)
			if c.drop != nil && c.drop(m) {
				fmt.Fprintf(&c.trace, "t=%d DROP %s from=%d to=%d request=%d op=%d\n", c.runner.Now(), m.Kind, m.From, m.To, m.RequestID, m.Op)
				continue
			}
			data, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			delays := []sim.Time{1}
			if c.deliveries != nil {
				delays = c.deliveries(m)
				fmt.Fprintf(&c.trace, "t=%d SEND %s from=%d to=%d op=%d transfer=%d delays=%v\n", c.runner.Now(), m.Kind, m.From, m.To, m.Op, m.TransferID, delays)
			}
			for i, delay := range delays {
				if i > 0 {
					record = &diagramMessage{Sent: c.runner.Now(), Message: m}
					c.diagram.Messages = append(c.diagram.Messages, record)
				}
				record.ID = c.runner.Send(endpoint(m.From), endpoint(m.To), data, delay)
			}
		case vsr.ArmEffect:
			c.diagram.Timers = append(c.diagram.Timers, retryTimer{c.runner.Now(), owner, effect.Kind})
			delay := sim.Time(10)
			if owner == vsr.ClientEndpoint {
				delay = 30
			}
			c.callbacks[owner] = effect.Message
			c.tokens[owner] = c.runner.Arm(endpoint(owner), "retry", delay)
		case vsr.CancelEffect:
			c.diagram.Timers = append(c.diagram.Timers, retryTimer{c.runner.Now(), owner, effect.Kind})
			c.runner.Cancel(endpoint(owner), c.tokens[owner])
		case vsr.ObserveEffect:
			c.diagram.States = append(c.diagram.States, diagramState{c.runner.Now(), effect.Observation})
			if err := c.checker.Observe(effect.Observation); err != nil {
				t.Fatal(err)
			}
			if effect.Observation.Kind == vsr.Applied {
				c.applies[owner]++
			}
		default:
			t.Fatalf("unknown effect %+v", effect)
		}
	}
}

func (c *retryCluster) start(t *testing.T, command kv.Command) {
	t.Helper()
	effects, err := c.client.Start(command)
	if err != nil {
		t.Fatal(err)
	}
	c.effects(t, vsr.ClientEndpoint, effects)
}

func lossScenario(t *testing.T, kind vsr.Kind) *retryCluster {
	t.Helper()
	dropped := map[[2]int]bool{}
	c := newRetryCluster(t, func(m vsr.Message) bool {
		link := [2]int{m.From, m.To}
		if m.Kind == kind && !dropped[link] {
			dropped[link] = true
			return true
		}
		return false
	})
	c.start(t, commands[1])
	if result := c.runner.Run(200); !result.Complete || c.client.Pending() {
		t.Fatalf("incomplete: %+v\n%s", result, c.trace.String())
	}
	if len(dropped) == 0 || c.client.Response().Result != (kv.Result{Value: 1}) {
		t.Fatal(dropped, c.client.Response())
	}
	if kind == vsr.Prepare || kind == vsr.PrepareOK {
		if !strings.Contains(c.trace.String(), "RETRY      r=0") || strings.Contains(c.trace.String(), "RETRY      client") {
			t.Fatal("primary timer must recover before the client retries", c.trace.String())
		}
	}
	for id, r := range c.replicas {
		if r.State() != (vsr.Counters{Op: 1, Commit: 1, Applied: 1}) || c.applies[id] != 1 || !reflect.DeepEqual(r.Snapshot(), []kv.Entry{{Key: "x", Value: 1}}) {
			t.Fatal(id, r.State(), r.Snapshot(), c.applies)
		}
	}
	return c
}

func TestRetryLosses(t *testing.T) {
	for _, kind := range []vsr.Kind{vsr.Request, vsr.Prepare, vsr.PrepareOK, vsr.Reply} {
		t.Run(string(kind), func(t *testing.T) { lossScenario(t, kind) })
	}
}

func TestLostReplyDemo(t *testing.T) {
	c := lossScenario(t, vsr.Reply)
	t.Log("\n" + c.trace.String())
	t.Logf("final x=1; primary applications=%d; request=%d", c.applies[0], c.client.Response().RequestID)
	if path := os.Getenv("VSR_M4_DIAGRAM_TRACE"); path != "" {
		data, err := json.MarshalIndent(c.diagram, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRetryWithoutQuorumIsIncomplete(t *testing.T) {
	c := newRetryCluster(t, func(m vsr.Message) bool { return m.Kind == vsr.PrepareOK })
	c.start(t, commands[1])
	if result := c.runner.Run(100); result.Complete || !c.client.Pending() || c.replicas[0].State().Applied != 0 {
		t.Fatal(result, c.client.Response(), c.replicas[0].State())
	}
}

func TestSinglePacketLossStillUsesOneBackup(t *testing.T) {
	for _, kind := range []vsr.Kind{vsr.Prepare, vsr.PrepareOK} {
		t.Run(string(kind), func(t *testing.T) {
			dropped := false
			c := newRetryCluster(t, func(m vsr.Message) bool {
				if m.Kind == kind && !dropped {
					dropped = true
					return true
				}
				return false
			})
			c.start(t, commands[1])
			if result := c.runner.Run(100); !result.Complete || c.client.Pending() || !dropped {
				t.Fatal(result)
			}
			if c.client.Response().Result.Value != 1 || c.applies[0] != 1 || c.replicas[0].Acknowledgments() != 1 {
				t.Fatal(c.client.Response(), c.applies)
			}
			if strings.Contains(c.trace.String(), "RETRY") {
				t.Fatal("waited for both backups", c.trace.String())
			}
			// M5 repairs a missing prepare when COMMIT exposes the gap.
		})
	}
}

func TestCachedApplicationError(t *testing.T) {
	c := newRetryCluster(t, nil)
	c.start(t, kv.Command{Op: kv.Put, Key: "x", Value: 9223372036854775807})
	c.runner.Run(100)
	c.start(t, commands[1])
	if result := c.runner.Run(100); !result.Complete || c.client.Pending() {
		t.Fatal(result)
	}
	if c.client.Response().Result.Error != kv.ErrOverflow {
		t.Fatal(c.client.Response())
	}
	out := c.replicas[0].Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, RequestID: 2, Command: commands[1]})
	if len(out) != 1 || out[0].Message.Result != c.client.Response().Result || c.replicas[0].State().Applied != 2 {
		t.Fatal(out)
	}
}

func TestDuplicateRequestsAndPrepares(t *testing.T) {
	p, b := vsr.New(0), vsr.New(1)
	request := vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, RequestID: 7, Command: commands[1]}
	prepare := p.Step(request)[0].Message
	ack := b.Step(prepare)[0].Message
	for i := 0; i < 3; i++ {
		out := p.Step(request)
		if len(out) != 2 || !reflect.DeepEqual(out[0].Message, prepare) || p.State().Op != 1 {
			t.Fatal(out, p.State())
		}
		out = b.Step(prepare)
		if len(out) != 1 || !reflect.DeepEqual(out[0].Message, ack) || b.State().Op != 1 || b.State().Applied != 0 {
			t.Fatal(out, b.State())
		}
	}
	bad := request
	bad.Command.Value = 2
	for _, completed := range []bool{false, true} {
		if completed {
			p.Step(ack)
		}
		out := p.Step(bad)
		if len(out) != 1 || out[0].Message.Kind != vsr.Reject || p.State().Op != 1 {
			t.Fatal(out, p.State())
		}
	}
	for i := 0; i < 3; i++ {
		if out := p.Step(ack); len(out) != 0 || p.Acknowledgments() != 1 || p.State().Applied != 1 {
			t.Fatal(out, p.State())
		}
		out := p.Step(request)
		if len(out) != 1 || out[0].Message.Kind != vsr.Reply || out[0].Message.Result.Value != 1 {
			t.Fatal(out)
		}
	}
	// A different command or identity at an existing backup position is rejected.
	for _, field := range []string{"command", "identity"} {
		bad := prepare
		if field == "command" {
			bad.Command.Value++
		} else {
			bad.RequestID++
		}
		if out := b.Step(bad); len(out) != 0 {
			t.Fatal(out)
		}
	}
	// A later request cannot borrow an older op's ACK or retry callback.
	next := request
	next.RequestID = 8
	p.Step(next)
	for _, stale := range []vsr.Message{ack, {Kind: vsr.PrepareOK, From: 1, Op: 2, View: 1}, {Kind: vsr.Retry, Op: 1}} {
		if out := p.Step(stale); len(out) != 0 || p.Acknowledgments() != 0 || p.State().Applied != 1 {
			t.Fatal(out, p.State())
		}
	}
	p.Step(vsr.Message{Kind: vsr.PrepareOK, From: 1, Op: 2})
	if p.State().Applied != 2 || p.Snapshot()[0].Value != 2 {
		t.Fatal(p.State())
	}
	// Cached result stays 1 even after the state has changed to 2.
	if out := p.Step(request); len(out) != 1 || out[0].Message.Result.Value != 1 {
		t.Fatal(out)
	}
}

func TestRetryFreshProcessTrace(t *testing.T) {
	if os.Getenv("VSR_M4_TRACE_CHILD") == "1" {
		c := lossScenario(t, vsr.Reply)
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
		cmd := exec.Command(executable, "-test.run=^TestRetryFreshProcessTrace$", "-test.count=1")
		cmd.Env = append(os.Environ(), "VSR_M4_TRACE_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	a, b := run(), run()
	if a != b || !strings.Contains(a, "sha256=") {
		t.Fatalf("different traces:\n%s\n%s", a, b)
	}
	t.Log(strings.Split(a[strings.Index(a, "sha256="):], "\n")[0])
}
