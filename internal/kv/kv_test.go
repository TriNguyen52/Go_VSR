package kv_test

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"go-vsr/internal/kv"
)

func TestCommandSequence(t *testing.T) {
	var first, second kv.Store
	steps := []struct {
		command kv.Command
		want    kv.Result
	}{
		{kv.Command{Op: kv.Get, Key: "missing"}, kv.Result{}},
		{kv.Command{Op: kv.Put, Key: "x", Value: 0}, kv.Result{}},
		{kv.Command{Op: kv.Add, Key: "x", Value: 1}, kv.Result{Value: 1}},
		{kv.Command{Op: kv.Get, Key: "x"}, kv.Result{Value: 1}},
		{kv.Command{Op: kv.Add, Key: "y", Value: -2}, kv.Result{Value: -2}},
		{kv.Command{Op: kv.Put, Key: "x", Value: math.MaxInt64}, kv.Result{Value: math.MaxInt64}},
		{kv.Command{Op: kv.Add, Key: "x", Value: 1}, kv.Result{Error: kv.ErrOverflow}},
		{kv.Command{Op: 255, Key: "x"}, kv.Result{Error: kv.ErrInvalidOperation}},
		{kv.Command{Op: kv.Get, Key: "x", Value: 123}, kv.Result{Value: math.MaxInt64}},
	}
	for i, step := range steps {
		a, b := first.Apply(step.command), second.Apply(step.command)
		if a != step.want || b != step.want {
			t.Fatalf("step %d: results %v, %v; want %v", i, a, b, step.want)
		}
	}
	want := []kv.Entry{{Key: "x", Value: math.MaxInt64}, {Key: "y", Value: -2}}
	if !reflect.DeepEqual(first.Snapshot(), want) || !reflect.DeepEqual(second.Snapshot(), want) {
		t.Fatalf("snapshots %v, %v; want %v", first.Snapshot(), second.Snapshot(), want)
	}
	first.Apply(kv.Command{Op: kv.Put, Key: "x", Value: 9})
	if !reflect.DeepEqual(second.Snapshot(), want) {
		t.Fatal("independent stores share state")
	}
}

func TestInvalidOperationPreservesState(t *testing.T) {
	var s kv.Store
	s.Apply(kv.Command{Op: kv.Put, Key: "x", Value: 9})
	s.Apply(kv.Command{Op: kv.Put, Key: "other", Value: -2})
	before := s.Snapshot()
	for _, op := range []kv.Operation{0, 255} {
		if got := s.Apply(kv.Command{Op: op, Key: "x", Value: 100}); got != (kv.Result{Error: kv.ErrInvalidOperation}) {
			t.Fatal(got)
		}
		if !reflect.DeepEqual(s.Snapshot(), before) {
			t.Fatal("invalid command mutated store")
		}
	}
}

func TestAddBoundaries(t *testing.T) {
	cases := []struct {
		name           string
		initial, delta int64
		want           kv.Result
	}{
		{"positive overflow", math.MaxInt64, 1, kv.Result{Error: kv.ErrOverflow}},
		{"negative overflow", math.MinInt64, -1, kv.Result{Error: kv.ErrOverflow}},
		{"minimum delta overflow", -1, math.MinInt64, kv.Result{Error: kv.ErrOverflow}},
		{"maximum delta overflow", 1, math.MaxInt64, kv.Result{Error: kv.ErrOverflow}},
		{"reach maximum", math.MaxInt64 - 1, 1, kv.Result{Value: math.MaxInt64}},
		{"reach minimum", math.MinInt64 + 1, -1, kv.Result{Value: math.MinInt64}},
		{"minimum delta", 0, math.MinInt64, kv.Result{Value: math.MinInt64}},
		{"opposite extremes", math.MaxInt64, math.MinInt64, kv.Result{Value: -1}},
		{"reverse extremes", math.MinInt64, math.MaxInt64, kv.Result{Value: -1}},
		{"zero at maximum", math.MaxInt64, 0, kv.Result{Value: math.MaxInt64}},
		{"zero at minimum", math.MinInt64, 0, kv.Result{Value: math.MinInt64}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s kv.Store
			s.Apply(kv.Command{Op: kv.Put, Key: "x", Value: tc.initial})
			s.Apply(kv.Command{Op: kv.Put, Key: "other", Value: 7})
			before := s.Snapshot()
			if got := s.Apply(kv.Command{Op: kv.Add, Key: "x", Value: tc.delta}); got != tc.want {
				t.Fatalf("result %v; want %v", got, tc.want)
			}
			if tc.want.Error != "" {
				if !reflect.DeepEqual(s.Snapshot(), before) {
					t.Fatal("overflow changed state")
				}
			} else if got := s.Apply(kv.Command{Op: kv.Get, Key: "x"}); got != tc.want {
				t.Fatalf("stored result %v; want %v", got, tc.want)
			}
		})
	}
}

func TestSnapshotOwnershipAndMissingKeys(t *testing.T) {
	var s kv.Store
	if got := s.Apply(kv.Command{Op: kv.Get, Key: "absent"}); got != (kv.Result{}) {
		t.Fatalf("missing read: %v", got)
	}
	if got := s.Apply(kv.Command{}); got != (kv.Result{Error: kv.ErrInvalidOperation}) {
		t.Fatalf("zero command: %v", got)
	}
	if got := s.Snapshot(); got == nil || len(got) != 0 {
		t.Fatalf("read or invalid command created state: %v", got)
	}
	s.Apply(kv.Command{Op: kv.Put, Key: "z", Value: 3})
	s.Apply(kv.Command{Op: kv.Add, Key: "a", Value: 0})
	s.Apply(kv.Command{Op: kv.Put, Key: "", Value: -1})
	want := []kv.Entry{{Key: "", Value: -1}, {Key: "a", Value: 0}, {Key: "z", Value: 3}}
	old := s.Snapshot()
	if !reflect.DeepEqual(old, want) {
		t.Fatalf("snapshot %v; want %v", old, want)
	}
	changed := s.Snapshot()
	changed[0] = kv.Entry{Key: "corrupted", Value: 99}
	if !reflect.DeepEqual(s.Snapshot(), want) {
		t.Fatal("snapshot mutation changed store")
	}
	s.Apply(kv.Command{Op: kv.Add, Key: "z", Value: 1})
	if !reflect.DeepEqual(old, want) {
		t.Fatal("store mutation changed earlier snapshot")
	}
}

func ExampleStore() {
	var s kv.Store
	fmt.Println("Put(x,0):", s.Apply(kv.Command{Op: kv.Put, Key: "x", Value: 0}).Value)
	fmt.Println("Add(x,1):", s.Apply(kv.Command{Op: kv.Add, Key: "x", Value: 1}).Value)
	fmt.Println("Get(x):", s.Apply(kv.Command{Op: kv.Get, Key: "x"}).Value)
	fmt.Println("Repeated Add(x,1):", s.Apply(kv.Command{Op: kv.Add, Key: "x", Value: 1}).Value)
	fmt.Println("Snapshot:", s.Snapshot())
	// Output:
	// Put(x,0): 0
	// Add(x,1): 1
	// Get(x): 1
	// Repeated Add(x,1): 2
	// Snapshot: [{x 2}]
}
