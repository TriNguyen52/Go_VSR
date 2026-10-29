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
	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

var commands = []kv.Command{{Op: kv.Put, Key: "x"}, {Op: kv.Add, Key: "x", Value: 1}, {Op: kv.Get, Key: "x"}}

// M3 acceptance contract: exactly three volatile view-zero replicas, one trusted
// client (ID 3), one outstanding request globally, reliable primary-to-backup
// FIFO enforced by this harness. No crashes, partitions, retries, or gap repair.
// See README's Current implementation contract. The runner itself is not FIFO.
type cluster struct {
	runner          sim.Runner
	replicas        [3]*vsr.Replica
	checker         checker.Checker
	trace           strings.Builder
	replies         []vsr.Message
	delay           func(vsr.Message) sim.Time
	diagram         diagramTrace
	lastDue         [vsr.ReplicaCount]sim.Time
	observedApplied [vsr.ReplicaCount]uint64
}

// Visualization is test infrastructure only; times come from the runner.
type diagramMessage struct {
	ID        uint64
	Sent      sim.Time
	Delivered *sim.Time
	Message   vsr.Message
}
type diagramState struct {
	Time        sim.Time
	Observation vsr.Observation
}
type diagramTrace struct {
	Messages []*diagramMessage
	States   []diagramState
}

func (c *cluster) delivered(e sim.Event) {
	for _, m := range c.diagram.Messages {
		if m.ID == e.Sequence {
			at := e.Time
			m.Delivered = &at
			return
		}
	}
	panic("delivery without recorded send")
}

func endpoint(id int) sim.Endpoint { return sim.Endpoint(fmt.Sprint(id)) }

func newCluster(t *testing.T, delay func(vsr.Message) sim.Time) *cluster {
	t.Helper()
	c := &cluster{delay: delay}
	for id := range c.replicas {
		r := vsr.New(id)
		c.replicas[id] = r
		c.runner.Register(endpoint(id), func(e sim.Event) {
			c.delivered(e)
			var m vsr.Message
			if err := json.Unmarshal(e.Payload, &m); err != nil {
				t.Fatal(err)
			}
			effects := r.Step(m)
			s := r.State()
			fmt.Fprintf(&c.trace, "t=%d %-10s r=%d v=%d op=%d commit=%d applied=%d\n", e.Time, m.Kind, id, s.View, s.Op, s.Commit, s.Applied)
			for _, effect := range effects {
				switch effect.Kind {
				case vsr.ObserveEffect:
					c.diagram.States = append(c.diagram.States, diagramState{e.Time, effect.Observation})
					if err := c.checker.Observe(effect.Observation); err != nil {
						t.Fatal(err)
					}
					if effect.Observation.Kind == vsr.Applied {
						c.observedApplied[id] = effect.Observation.Index
					}
				case vsr.SendEffect:
					c.send(t, effect.Message)
				default:
					t.Fatalf("unknown effect: %+v", effect)
				}
			}
		})
	}
	c.runner.Register(endpoint(vsr.ClientEndpoint), func(e sim.Event) {
		c.delivered(e)
		var m vsr.Message
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			t.Fatal(err)
		}
		i := len(c.replies)
		want := []int64{0, 1, 1}
		if i >= len(want) || m.Kind != vsr.Reply || m.From != 0 || m.View != 0 || m.Op != uint64(i+1) || m.Commit != m.Op || m.Result != (kv.Result{Value: want[i]}) {
			t.Fatalf("bad reply: %+v", m)
		}
		c.replies = append(c.replies, m)
		fmt.Fprintf(&c.trace, "t=%d REPLY      r=0 v=%d op=%d commit=%d applied=%d value=%d\n", e.Time, m.View, m.Op, m.Commit, m.Op, m.Result.Value)
		if len(c.replies) < len(commands) {
			c.request(t, commands[len(c.replies)])
		}
	})
	return c
}

func (c *cluster) send(t *testing.T, m vsr.Message) {
	t.Helper()
	if m.Kind == vsr.Reply {
		s := c.replicas[0].State()
		i := len(c.replies)
		if i >= len(commands) || m.From != 0 || m.To != vsr.ClientEndpoint || m.Op != uint64(i+1) ||
			s.Commit < m.Op || s.Applied < m.Op || c.observedApplied[0] < m.Op ||
			c.replicas[0].Log()[m.Op-1] != commands[i] {
			t.Fatalf("reply before checked application or for wrong request: %+v, state=%+v", m, s)
		}
	}
	payload, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	delay := c.delay(m)
	if m.From == 0 && m.To > 0 && m.To < vsr.ReplicaCount {
		due := c.runner.Now() + delay
		if due < c.lastDue[m.To] {
			t.Fatalf("unsupported non-FIFO schedule to replica %d: due=%d previous=%d", m.To, due, c.lastDue[m.To])
		}
		c.lastDue[m.To] = due // Equal deadlines retain Send insertion order.
	}
	id := c.runner.Send(endpoint(m.From), endpoint(m.To), payload, delay)
	c.diagram.Messages = append(c.diagram.Messages, &diagramMessage{ID: id, Sent: c.runner.Now(), Message: m})
}
func (c *cluster) request(t *testing.T, command kv.Command) {
	c.send(t, vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 0, Command: command})
}

func scenario(t *testing.T) *cluster {
	c := newCluster(t, func(m vsr.Message) sim.Time {
		if m.To == 2 {
			return 100
		} // Constant link delay preserves primary message order.
		return 1
	})
	c.request(t, commands[0])
	// The client finishes all three operations before the third replica receives
	// anything. Queue exhaustion alone is not our success criterion.
	for events := 0; len(c.replies) < 3 && events < 100; events++ {
		if c.runner.Now() >= 100 {
			t.Fatal("waited for third replica")
		}
		if result := c.runner.Run(1); result.Complete {
			t.Fatal("drained before third replica caught up")
		}
	}
	if c.replicas[2].State().Op != 0 {
		t.Fatal("third replica was not delayed")
	}
	if result := c.runner.Run(100); !result.Complete {
		t.Fatal(result)
	}
	if len(c.replies) != len(commands) || len(c.diagram.States) != 2*len(commands)*vsr.ReplicaCount {
		t.Fatalf("incomplete workload: replies=%d transitions=%d", len(c.replies), len(c.diagram.States))
	}
	for _, r := range c.replicas {
		if r.State() != (vsr.Counters{Op: 3, Commit: 3, Applied: 3}) ||
			!reflect.DeepEqual(r.Snapshot(), []kv.Entry{{Key: "x", Value: 1}}) {
			t.Fatal(r.State(), r.Snapshot())
		}
	}
	return c
}

func TestNormalDemo(t *testing.T) {
	c := scenario(t)
	t.Log("\n" + c.trace.String())
	if path := os.Getenv("VSR_M3_DIAGRAM_TRACE"); path != "" {
		data, err := json.MarshalIndent(c.diagram, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQuorumGate(t *testing.T) {
	p := vsr.New(0)
	effects := p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 0, Command: commands[1]})
	if len(effects) != 2 || p.State() != (vsr.Counters{Op: 1}) || len(p.Snapshot()) != 0 {
		t.Fatal(effects, p.State())
	}
	// Prepare both backups but hold both replies outside the primary.
	var acks []vsr.Message
	for id := 1; id <= 2; id++ {
		backup := vsr.New(id)
		out := backup.Step(effects[id-1].Message)
		if len(out) != 1 || out[0].Message.Kind != vsr.PrepareOK {
			t.Fatal(out)
		}
		acks = append(acks, out[0].Message)
	}
	if p.State().Commit != 0 {
		t.Fatal("committed without acknowledgment")
	}
	t.Logf("both ACKs held: view=%d op=%d commit=%d applied=%d replies=0", p.State().View, p.State().Op, p.State().Commit, p.State().Applied)
	if got := p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 0, Command: commands[2]}); len(got) != 0 || p.State().Op != 1 {
		t.Fatal("accepted a second pending proposal", got)
	}
	for _, bad := range []vsr.Message{
		{Kind: vsr.PrepareOK, From: 0, To: 0, Op: 1},
		{Kind: vsr.PrepareOK, From: 3, To: 0, Op: 1},
		{Kind: vsr.PrepareOK, From: 1, To: 0, Op: 2},
		{Kind: vsr.PrepareOK, From: 1, To: 0, Op: 1, View: 1},
	} {
		if got := p.Step(bad); len(got) != 0 || p.State().Commit != 0 {
			t.Fatal(got, p.State())
		}
	}
	out := p.Step(acks[0])
	if len(out) != 5 || out[0].Observation.Kind != vsr.Committed || out[1].Observation.Kind != vsr.Applied ||
		out[4].Message.Kind != vsr.Reply || out[4].Message.Result.Value != 1 {
		t.Fatal(out)
	}
	t.Logf("one ACK delivered: view=%d op=%d commit=%d applied=%d reply=%d", p.State().View, p.State().Op, p.State().Commit, p.State().Applied, out[4].Message.Result.Value)
	if got := p.Step(acks[0]); len(got) != 0 || p.State().Applied != 1 {
		t.Fatal("duplicate ACK produced effects", got)
	}
	if got := p.Step(acks[1]); len(got) != 0 || p.State().Applied != 1 {
		t.Fatal(got)
	}
	// A read must also wait for a new quorum; a late ack for the write cannot
	// authorize it, even though the primary already has the requested value.
	p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 0, Command: commands[2]})
	if got := p.Step(acks[0]); len(got) != 0 || p.State().Applied != 1 {
		t.Fatal("read used stale acknowledgment", got)
	}
	read := p.Step(vsr.Message{Kind: vsr.PrepareOK, From: 2, To: 0, Op: 2})
	if len(read) != 5 || read[4].Message.Result.Value != 1 || p.State().Applied != 2 {
		t.Fatal(read)
	}
}

func TestBackupCommitTransitions(t *testing.T) {
	b := vsr.New(1)
	var check checker.Checker
	commands := []kv.Command{{Op: kv.Put, Key: "x", Value: 7}, {Op: kv.Add, Key: "x", Value: 5}, {Op: kv.Put, Key: "x", Value: -2}}
	results := []int64{7, 12, -2}
	for i, command := range commands {
		out := b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 1, Op: uint64(i + 1), Command: command})
		if len(out) != 1 {
			t.Fatal(out)
		}
	}
	// One handler must expose all six intermediate transitions.
	out := b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 1, Commit: 3})
	if len(out) != 6 {
		t.Fatal(out)
	}
	for i, effect := range out {
		index := uint64(i/2 + 1)
		want := vsr.Observation{Kind: vsr.Committed, Replica: 1, State: vsr.Counters{Op: 3, Commit: index, Applied: index - 1}, Index: index, Command: commands[index-1]}
		if i%2 == 1 {
			want.Kind = vsr.Applied
			want.State.Applied = index
			want.Result.Value = results[index-1]
		}
		if effect.Kind != vsr.ObserveEffect || effect.Observation != want {
			t.Fatalf("transition %d: got %+v want %+v", i, effect, want)
		}
		if err := check.Observe(effect.Observation); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 1, Commit: 2}); len(got) != 0 {
		t.Fatal(got)
	}
	copy := b.Log()
	copy[0].Value = 999
	if b.Log()[0].Value != 7 {
		t.Fatal("mutable log escaped")
	}
}

func TestPiggybackAndUnsupportedInputs(t *testing.T) {
	b := vsr.New(1)
	for _, m := range []vsr.Message{
		{Kind: vsr.Prepare, From: 0, To: 1, Op: 2},
		{Kind: vsr.Prepare, From: 2, To: 1, Op: 1},
		{Kind: vsr.Commit, From: 0, To: 1, Commit: 1},
		{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 1},
	} {
		if got := b.Step(m); len(got) != 0 || b.State().Op != 0 {
			t.Fatal(got)
		}
	}
	b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 1, Op: 1, Command: commands[1]})
	out := b.Step(vsr.Message{Kind: vsr.Prepare, From: 0, To: 1, Op: 2, Commit: 1, Command: commands[2]})
	if len(out) != 3 || out[1].Observation.Result.Value != 1 || b.State().Applied != 1 {
		t.Fatal(out)
	}
}

func TestFreshProcessTrace(t *testing.T) {
	if os.Getenv("VSR_M3_TRACE_CHILD") == "1" {
		c := scenario(t)
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
		cmd := exec.Command(executable, "-test.run=^TestFreshProcessTrace$", "-test.count=1")
		cmd.Env = append(os.Environ(), "VSR_M3_TRACE_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	a, b := run(), run()
	if a != b || !strings.Contains(a, "sha256=") {
		t.Fatalf("fresh process traces differ:\n%s\n%s", a, b)
	}
	start := strings.Index(a, "sha256=")
	t.Log(strings.Split(a[start:], "\n")[0])
}
