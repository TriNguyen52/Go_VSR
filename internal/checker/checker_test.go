package checker_test

import (
	"math"
	"strings"
	"testing"

	"go-vsr/internal/checker"
	"go-vsr/internal/kv"
	"go-vsr/internal/vsr"
)

func TestRejectInvalidHistory(t *testing.T) {
	commit := vsr.Observation{Kind: vsr.Committed, Replica: 0,
		State: vsr.Counters{Op: 1, Commit: 1}, Index: 1, RequestID: 1,
		Command: kv.Command{Op: kv.Add, Key: "x", Value: 1}}
	apply := commit
	apply.Kind = vsr.Applied
	apply.State.Applied = 1
	apply.Result.Value = 1
	for _, name := range []string{"conflict", "before commit", "duplicate", "skipped", "wrong result"} {
		t.Run(name, func(t *testing.T) {
			var c checker.Checker
			if name == "before commit" {
				if err := c.Observe(apply); err == nil || !strings.Contains(err.Error(), "application") {
					t.Fatal(err)
				}
				return
			}
			if err := c.Observe(commit); err != nil {
				t.Fatal(err)
			}
			bad := apply
			want := "application"
			switch name {
			case "conflict":
				bad = commit
				bad.Replica = 1
				bad.Command.Value = 2
				want = "conflicting"
			case "duplicate":
				if err := c.Observe(apply); err != nil {
					t.Fatal(err)
				}
			case "skipped":
				bad.Index = 2
			case "wrong result":
				bad.Result.Value = 2
				want = "result"
			}
			if err := c.Observe(bad); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected %s: %v", want, err)
			}
		})
	}
}

func TestIndependentApplicationModel(t *testing.T) {
	p := vsr.New(0)
	var c checker.Checker
	commands := []kv.Command{
		{Op: kv.Get, Key: "missing"},
		{Op: kv.Put, Key: "x", Value: math.MaxInt64},
		{Op: kv.Add, Key: "x", Value: 1},
		{Op: kv.Get, Key: "x"},
		{Op: kv.Put, Key: "x", Value: math.MinInt64},
		{Op: kv.Add, Key: "x", Value: -1},
		{Op: kv.Add, Key: "x", Value: math.MaxInt64},
		{Op: kv.Add, Key: "x", Value: math.MinInt64},
		{Op: 255, Key: "x"},
	}
	for i, command := range commands {
		p.Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 0, RequestID: uint64(i + 1), Command: command})
		for _, effect := range p.Step(vsr.Message{Kind: vsr.PrepareOK, From: 1, To: 0, Op: uint64(i + 1)}) {
			if effect.Kind == vsr.ObserveEffect {
				if err := c.Observe(effect.Observation); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if p.State().Applied != uint64(len(commands)) {
		t.Fatal("model workload did not execute", p.State())
	}
}

func TestRejectDuplicateLogicalRequest(t *testing.T) {
	var c checker.Checker
	o := vsr.Observation{Kind: vsr.Committed, Replica: 0, State: vsr.Counters{Op: 1, Commit: 1}, Index: 1, RequestID: 7, Command: kv.Command{Op: kv.Add, Value: 1}}
	if err := c.Observe(o); err != nil {
		t.Fatal(err)
	}
	o.Index, o.State.Op, o.State.Commit = 2, 2, 2
	if err := c.Observe(o); err == nil || !strings.Contains(err.Error(), "multiple positions") {
		t.Fatal(err)
	}
}
