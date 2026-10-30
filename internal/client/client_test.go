package client_test

import (
	"reflect"
	"testing"

	"go-vsr/internal/client"
	"go-vsr/internal/kv"
	"go-vsr/internal/vsr"
)

func TestOutstandingIdentityAndReplyValidation(t *testing.T) {
	var c client.Client
	command := kv.Command{Op: kv.Add, Key: "x", Value: 1}
	out, err := c.Start(command)
	if err != nil {
		t.Fatal(err)
	}
	request, timer := out[0].Message, out[1].Message
	if _, err := c.Start(command); err == nil {
		t.Fatal("replaced pending request")
	}
	retry := c.Step(timer)
	if len(retry) != 4 || !reflect.DeepEqual(retry[0].Message, request) {
		t.Fatal(retry)
	}
	for id := 0; id < 3; id++ {
		m := retry[id].Message
		if m.To != id || m.RequestID != request.RequestID || m.Command != command {
			t.Fatal(m)
		}
	}
	reply := vsr.Message{Kind: vsr.Reply, To: vsr.ClientEndpoint, RequestID: 1, Op: 1, Commit: 1, Result: kv.Result{Value: 1}}
	for _, field := range []string{"sender", "destination", "identity", "view", "op", "commit"} {
		bad := reply
		switch field {
		case "sender":
			bad.From = 1
		case "destination":
			bad.To = 0
		case "identity":
			bad.RequestID = 2
		case "view":
			bad.View = 1
		case "op":
			bad.Op = 0
		case "commit":
			bad.Commit = 0
		}
		if out := c.Step(bad); len(out) != 0 || !c.Pending() {
			t.Fatal(field, out)
		}
	}
	if out := c.Step(reply); len(out) != 1 || out[0].Kind != vsr.CancelEffect || c.Pending() || !reflect.DeepEqual(c.Response(), reply) {
		t.Fatal(out)
	}
	if out := c.Step(timer); len(out) != 0 {
		t.Fatal("completed timer retried", out)
	}
	next, err := c.Start(command)
	if err != nil || next[0].Message.RequestID != 2 {
		t.Fatal(next, err)
	}
	for _, stale := range []vsr.Message{reply, timer} {
		if out := c.Step(stale); len(out) != 0 || !c.Pending() {
			t.Fatal(out)
		}
	}
	// Protocol rejection is surfaced separately from a successful KV reply.
	reject := vsr.Message{Kind: vsr.Reject, To: vsr.ClientEndpoint, RequestID: 2}
	if out := c.Step(reject); len(out) != 1 || c.Pending() || c.Response().Kind != vsr.Reject {
		t.Fatal(out)
	}
}

func TestReplyAndTimeoutBothOrders(t *testing.T) {
	for _, timeoutFirst := range []bool{false, true} {
		var c client.Client
		out, _ := c.Start(kv.Command{Op: kv.Add, Value: 1})
		timer := out[1].Message
		reply := vsr.Message{Kind: vsr.Reply, To: vsr.ClientEndpoint, RequestID: 1, Op: 1, Commit: 1, Result: kv.Result{Value: 1}}
		if timeoutFirst {
			if out := c.Step(timer); len(out) != 4 {
				t.Fatal(out)
			}
		}
		c.Step(reply)
		if out := c.Step(timer); len(out) != 0 || c.Pending() || !reflect.DeepEqual(c.Response(), reply) {
			t.Fatal(timeoutFirst, out)
		}
	}
}
