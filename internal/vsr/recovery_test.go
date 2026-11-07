package vsr_test

import (
	"go-vsr/internal/vsr"
	"reflect"
	"testing"
)

func (c *retryCluster) crash(id int) {
	for other, r := range c.replicas {
		if other != id && (c.stopped[other] || r.Recovering()) {
			panic("failure budget exceeded")
		}
	}
	c.stopped[id] = true
	c.runner.Cancel(endpoint(id), c.tokens[id])
	c.runner.Cancel(endpoint(id), c.clockTokens[id])
	c.incarnation[id]++
}

func (c *retryCluster) restart(t *testing.T, id int) {
	t.Helper()
	if !c.stopped[id] {
		t.Fatal("restart requires crash")
	}
	c.nextNonce++ // Harness-owned source survives every process replacement.
	var out []vsr.Effect
	c.replicas[id], out = vsr.NewRecovering(id, c.nextNonce)
	c.checker.Restart(id)
	c.stopped[id] = false
	c.clockTokens[id] = c.runner.Arm(endpoint(id), "clock", 1)
	c.effects(t, id, out)
}

func TestRecoveryFencingAndHighestView(t *testing.T) {
	r, out := vsr.NewRecovering(2, 42)
	if len(out) != 3 {
		t.Fatal(out)
	}
	for _, kind := range []vsr.Kind{vsr.Prepare, vsr.StartView, vsr.Suspect, vsr.StartViewChange, vsr.DoViewChange, vsr.Request, vsr.Recovery, vsr.Retry, vsr.ViewRetry, vsr.TransferRetry} {
		if got := r.Step(vsr.Message{Kind: kind, From: 0, To: 2, View: 3, Op: 1, RequestID: 1}); len(got) != 0 {
			t.Fatal(kind, got)
		}
	}
	if len(r.AdvanceTime()) != 0 || r.State() != (vsr.Counters{}) {
		t.Fatal(r.State())
	}
	primary := vsr.Message{Kind: vsr.RecoveryResponse, From: 0, To: 2, Nonce: 41}
	r.Step(primary)
	backup := vsr.Message{Kind: vsr.RecoveryResponse, From: 1, To: 2, Nonce: 42, View: 3}
	r.Step(backup)
	r.Step(backup) // Same sender never supplies a quorum.
	primary.Nonce = 42
	r.Step(primary) // View 0 primary cannot satisfy the highest-view requirement.
	if !r.Recovering() {
		t.Fatal("accepted stale primary")
	}
	primary.View, primary.Op, primary.Commit = 3, 1, 1
	primary.Entries = []vsr.LogEntry{{RequestID: 1, Command: commands[1]}}
	r.Step(primary)
	if r.Recovering() || r.State().Applied != 1 {
		t.Fatal(r.State())
	}
	before := r.State()
	r.Step(out[2].Message)
	if r.State() != before {
		t.Fatal("stale recovery callback")
	}
}

func TestRecoveryDemo(t *testing.T) {
	c := automaticCluster(t)
	c.start(t, commands[1])
	until(t, c, func() bool { return c.replicas[2].State().Applied == 1 && !c.client.Pending() })
	c.crash(2)
	// Losing one survivor's recovery responses must block recovery but not service.
	c.drop = func(m vsr.Message) bool { return m.Kind == vsr.RecoveryResponse && m.From == 1 }
	c.restart(t, 2)
	c.runner.Run(100)
	if !c.replicas[2].Recovering() || c.replicas[2].State().Applied != 0 {
		t.Fatal("one response recovered replica")
	}
	c.start(t, commands[1])
	until(t, c, func() bool { return !c.client.Pending() })
	c.drop = nil
	until(t, c, func() bool { return !c.replicas[2].Recovering() })
	if !reflect.DeepEqual(c.replicas[0].Snapshot(), c.replicas[2].Snapshot()) {
		t.Fatal("reconstruction mismatch")
	}
	// Crash old primary; recovered backup is now required for the new quorum.
	c.crash(0)
	until(t, c, func() bool { return c.replicas[1].State().View > 0 && !c.replicas[1].ChangingView() })
	duplicate := c.replicas[1].Step(vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: 1, RequestID: 2, Command: commands[1]})
	if len(duplicate) != 1 || duplicate[0].Message.Kind != vsr.Reply || duplicate[0].Message.Result.Value != 2 {
		t.Fatal(duplicate)
	}
	c.start(t, commands[1])
	until(t, c, func() bool { return !c.client.Pending() && c.replicas[2].State().Applied == 3 })
	if c.client.Response().Result.Value != 3 {
		t.Fatal(c.client.Response())
	}
	t.Log("\n" + c.trace.String())
	t.Log("blank r2 recovered x=2; primary r0 failed; recovered r2 joined quorum; next Add returned 3")
}

func TestRecoveryNonceAndFailureBudget(t *testing.T) {
	c := automaticCluster(t)
	c.crash(2)
	c.restart(t, 2)
	old := vsr.Message{Kind: vsr.RecoveryResponse, From: 0, To: 2, Nonce: c.nextNonce}
	c.crash(2)
	c.restart(t, 2)
	if out := c.replicas[2].Step(old); len(out) != 0 {
		t.Fatal(out)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("allowed overlapping failures")
		}
	}()
	c.crash(0)
}

func TestRecoveryOnlyNormalResponds(t *testing.T) {
	r := vsr.New(0)
	request := vsr.Message{Kind: vsr.Recovery, From: 2, To: 0, Nonce: 1}
	if len(r.Step(request)) != 1 {
		t.Fatal("normal primary did not respond")
	}
	r.Step(vsr.Message{Kind: vsr.Suspect, From: 0, To: 0, View: 1})
	if len(r.Step(request)) != 0 {
		t.Fatal("changing replica responded")
	}
}

func TestRecoveryRejectsMalformedHistoryAndKeepsPending(t *testing.T) {
	r, _ := vsr.NewRecovering(2, 7)
	r.Step(vsr.Message{Kind: vsr.RecoveryResponse, From: 1, To: 2, Nonce: 7})
	m := vsr.Message{Kind: vsr.RecoveryResponse, From: 0, To: 2, Nonce: 7, Op: 1, Commit: 1}
	r.Step(m)
	if !r.Recovering() {
		t.Fatal("accepted truncated history")
	}
	m.Commit = 0
	m.Entries = []vsr.LogEntry{{RequestID: 1, Command: commands[1]}}
	r.Step(m)
	if r.Recovering() || r.State().Op != 1 || r.State().Applied != 0 || len(r.Snapshot()) != 0 {
		t.Fatal("executed speculative entry")
	}
	r.Step(vsr.Message{Kind: vsr.Commit, From: 0, To: 2, Commit: 1})
	if r.State().Applied != 1 {
		t.Fatal("pending history could not commit")
	}
}

func TestRecoveryFencesOldLocalTimers(t *testing.T) {
	c := automaticCluster(t)
	// Queue a local suspicion that would change the replacement's view if fired.
	c.effects(t, 2, []vsr.Effect{{Kind: vsr.ArmEffect, Message: vsr.Message{
		Kind: vsr.Suspect, From: 2, To: 2, View: 9}}})
	c.crash(2)
	c.restart(t, 2)
	until(t, c, func() bool { return !c.replicas[2].Recovering() })
	until(t, c, func() bool { return c.runner.Now() >= 15 })
	if c.replicas[2].State().View != 0 {
		t.Fatal("old local callback reached replacement")
	}
}
