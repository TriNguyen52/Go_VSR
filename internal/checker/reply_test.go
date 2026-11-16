package checker_test

import (
	"strings"
	"testing"

	"go-vsr/internal/checker"
	"go-vsr/internal/kv"
	"go-vsr/internal/vsr"
)

func TestReplyUsesIndependentCommittedOrder(t *testing.T) {
	var c checker.Checker
	for i, command := range []kv.Command{{Op: kv.Add, Key: "x", Value: 1}, {Op: kv.Put, Key: "x", Value: 9}} {
		index := uint64(i + 1)
		if err := c.Observe(vsr.Observation{Kind: vsr.Committed, Replica: 0, State: vsr.Counters{View: 2, Op: index, Commit: index}, Index: index, RequestID: index, Command: command}); err != nil {
			t.Fatal(err)
		}
	}
	// A delayed old-view reply is valid for its own identity and position.
	reply := vsr.Message{Kind: vsr.Reply, RequestID: 1, Op: 1, Commit: 1, Result: kv.Result{Value: 1}}
	if err := c.CheckReply(reply); err != nil {
		t.Fatal(err)
	}
	reply.Result.Value = 9
	if err := c.CheckReply(reply); err == nil || !strings.Contains(err.Error(), "reply result") {
		t.Fatal(err)
	}
	reply.RequestID = 2
	if err := c.CheckReply(reply); err == nil || !strings.Contains(err.Error(), "identify committed") {
		t.Fatal(err)
	}
}
