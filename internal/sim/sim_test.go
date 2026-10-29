package sim_test

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"go-vsr/internal/sim"
)

func demo() *sim.Runner {
	r := new(sim.Runner)
	r.Register("a", func(sim.Event) {})
	r.Register("b", func(sim.Event) {})
	r.Send("a", "b", []byte("second"), 5)
	r.Send("a", "b", []byte("first"), 2)
	r.Arm("b", "wait", 8)
	if result := r.Run(3); !result.Complete {
		panic("demo incomplete")
	}
	return r
}

func ExampleRunner() {
	fmt.Print(demo().Trace())
	// Output:
	// 2 #2 deliver message from="a" to="b" payload=6669727374 timer="" generation=0
	// 5 #1 deliver message from="a" to="b" payload=7365636f6e64 timer="" generation=0
	// 8 #3 deliver timeout from="" to="b" payload= timer="wait" generation=3
}

func TestSimultaneousEvents(t *testing.T) {
	for _, timeoutFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("timeoutFirst=%v", timeoutFirst), func(t *testing.T) {
			var r sim.Runner
			var got []sim.Kind
			r.Register("a", func(e sim.Event) {
				got = append(got, e.Kind)
				if len(got) == 1 {
					r.Send("a", "a", nil, 0)
				}
			})
			send := func() { r.Send("a", "a", nil, 4) }
			arm := func() { r.Arm("a", "wait", 4) }
			want := []sim.Kind{sim.Message, sim.Timeout, sim.Message}
			if timeoutFirst {
				arm()
				send()
				want[0], want[1] = sim.Timeout, sim.Message
			} else {
				send()
				arm()
			}
			if result := r.Run(3); !result.Complete || r.Now() != 4 || !reflect.DeepEqual(got, want) {
				t.Fatalf("result=%+v time=%d kinds=%v; want %v", result, r.Now(), got, want)
			}
		})
	}
}

func TestTimerReplacementCancellationAndRearm(t *testing.T) {
	var r sim.Runner
	var got []sim.TimerToken
	r.Register("a", func(e sim.Event) {
		got = append(got, e.Timer)
		if len(got) == 1 {
			r.Arm("a", "wait", 0)
		}
	})
	old := r.Arm("a", "wait", 1)
	replacement := r.Arm("a", "wait", 2)
	if r.Cancel("a", old) {
		t.Fatal("old token canceled replacement")
	}
	if !r.Cancel("a", replacement) || r.Cancel("a", replacement) {
		t.Fatal("cancel not one-shot")
	}
	current := r.Arm("a", "wait", 3)
	if current.Generation <= replacement.Generation {
		t.Fatal("generation reused")
	}
	if result := r.Run(4); !result.Complete || result.Processed != 4 {
		t.Fatal(result)
	}
	if len(got) != 2 || got[0] != current || got[1].Generation <= current.Generation {
		t.Fatalf("delivered tokens: %v", got)
	}
	if strings.Count(r.Trace(), " stale timeout ") != 2 {
		t.Fatal(r.Trace())
	}
	if r.Cancel("a", got[1]) {
		t.Fatal("fired token remains active")
	}
}

func TestTimersAreScopedToEndpointAndName(t *testing.T) {
	var r sim.Runner
	var got []string
	for _, id := range []sim.Endpoint{"a", "b"} {
		r.Register(id, func(e sim.Event) { got = append(got, string(e.To)+":"+e.Timer.Name) })
	}
	canceled := r.Arm("a", "one", 1)
	r.Arm("b", "one", 1)
	r.Arm("a", "two", 1)
	r.Cancel("a", canceled)
	r.Run(3)
	if !reflect.DeepEqual(got, []string{"b:one", "a:two"}) {
		t.Fatal(got)
	}
}

func TestPayloadOwnership(t *testing.T) {
	var r sim.Runner
	var got []string
	r.Register("a", func(e sim.Event) {
		got = append(got, string(e.Payload))
		e.Payload[0] = 'z'
	})
	buffer := []byte("hello")
	r.Send("a", "a", buffer, 0)
	r.Send("a", "a", buffer, 1)
	buffer[0] = 'x'
	r.Run(2)
	if !reflect.DeepEqual(got, []string{"hello", "hello"}) || string(buffer) != "xello" {
		t.Fatalf("payloads=%v sender=%s", got, buffer)
	}
	if strings.Count(r.Trace(), "payload=68656c6c6f ") != 2 {
		t.Fatal(r.Trace())
	}
}

func TestLimitAndResume(t *testing.T) {
	var r sim.Runner
	loop := true
	r.Register("a", func(sim.Event) {
		if loop {
			r.Send("a", "a", nil, 0)
		}
	})
	r.Send("a", "a", nil, 0)
	if got := r.Run(0); got.Complete || got.Processed != 0 || got.Pending != 1 {
		t.Fatal(got)
	}
	if got := r.Run(10); got.Complete || got.Processed != 10 || got.Pending != 1 || r.Now() != 0 {
		t.Fatal(got)
	}
	loop = false
	if got := r.Run(1); !got.Complete || got.Processed != 1 || got.Pending != 0 {
		t.Fatal(got)
	}
	if got := r.Run(0); !got.Complete {
		t.Fatal(got)
	}
}

func TestStaleTimersConsumeBudget(t *testing.T) {
	var r sim.Runner
	r.Register("a", func(sim.Event) { t.Fatal("canceled timer delivered") })
	for range 2 {
		r.Cancel("a", r.Arm("a", "wait", 1))
	}
	if got := r.Run(1); got.Complete || got.Processed != 1 || got.Pending != 1 {
		t.Fatal(got)
	}
	if got := r.Run(1); !got.Complete {
		t.Fatal(got)
	}
}

func TestInvalidScheduling(t *testing.T) {
	for _, name := range []string{"negative", "overflow", "unknown", "duplicate", "recursive"} {
		t.Run(name, func(t *testing.T) {
			var r sim.Runner
			r.Register("a", func(sim.Event) {
				if name == "recursive" {
					r.Run(1)
				}
			})
			defer func() {
				if recover() == nil {
					t.Fatal("expected programmer-error panic")
				}
			}()
			switch name {
			case "negative":
				r.Send("a", "a", nil, -1)
			case "overflow":
				r.Send("a", "a", nil, sim.Time(math.MaxInt64))
				r.Run(1)
				r.Arm("a", "wait", 1)
			case "unknown":
				r.Send("a", "missing", nil, 0)
			case "duplicate":
				r.Register("a", func(sim.Event) {})
			case "recursive":
				r.Send("a", "a", nil, 0)
				r.Run(1)
			}
		})
	}
}

// The subprocess boundary is test infrastructure only, never simulated logic.
func TestFreshProcessTrace(t *testing.T) {
	if os.Getenv("VSR_M2_TRACE_CHILD") == "1" {
		r := demo()
		fmt.Print(r.Trace())
		fmt.Println("sha256=" + r.Digest())
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func() string {
		cmd := exec.Command(executable, "-test.run=^TestFreshProcessTrace$", "-test.count=1")
		cmd.Env = append(os.Environ(), "VSR_M2_TRACE_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child: %v\n%s", err, out)
		}
		return string(out)
	}
	a, b := run(), run()
	if a != b || !strings.Contains(a, "sha256="+demo().Digest()) {
		t.Fatalf("fresh processes differ:\n%s\n%s", a, b)
	}
	t.Log(strings.TrimSpace(a))
}
