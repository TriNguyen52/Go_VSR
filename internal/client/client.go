// Package client implements the single, non-crashing client.
package client

import (
	"fmt"
	"math"

	"go-vsr/internal/kv"
	"go-vsr/internal/vsr"
)

// Client owns a stable endpoint and increase request numbers. The zero value
type Client struct {
	number   uint64
	pending  bool
	request  vsr.Message
	response vsr.Message
	view     uint64
}

func (c *Client) Pending() bool         { return c.pending }
func (c *Client) Response() vsr.Message { return c.response }

// Start refuses to replace outstanding work, including work whose reply is lost.
func (c *Client) Start(command kv.Command) ([]vsr.Effect, error) {
	if c.pending {
		return nil, fmt.Errorf("client: request still pending")
	}
	if c.number == math.MaxUint64 {
		return nil, fmt.Errorf("client: request numbers exhausted")
	}
	c.number++
	c.pending = true
	c.response = vsr.Message{}
	c.request = vsr.Message{Kind: vsr.Request, From: vsr.ClientEndpoint, To: int(c.view % vsr.ReplicaCount), View: c.view, RequestID: c.number, Command: command}
	return c.send(false), nil
}

func (c *Client) send(retry bool) []vsr.Effect {
	effects := []vsr.Effect{{Kind: vsr.SendEffect, Message: c.request}}
	if retry {
		for id := 0; id < vsr.ReplicaCount; id++ {
			if id == c.request.To {
				continue
			}
			m := c.request
			m.To = id
			effects = append(effects, vsr.Effect{Kind: vsr.SendEffect, Message: m})
		}
	}
	return append(effects, vsr.Effect{Kind: vsr.ArmEffect, Message: vsr.Message{
		Kind: vsr.Retry, From: vsr.ClientEndpoint, To: vsr.ClientEndpoint, RequestID: c.number,
	}})
}

// Step consumes a reply or a local retry timer. Old replies and callbacks cannot
// finish or resend a newer request. Rejection is a protocol error, not a KV result.
func (c *Client) Step(m vsr.Message) []vsr.Effect {
	if !c.pending || m.To != vsr.ClientEndpoint || m.RequestID != c.number {
		return nil
	}
	if m.Kind == vsr.Retry && m.From == vsr.ClientEndpoint {
		return c.send(true)
	}
	if m.From != int(m.View%vsr.ReplicaCount) || (m.Kind != vsr.Reply && m.Kind != vsr.Reject) {
		return nil
	}
	if m.Kind == vsr.Reply && (m.Op == 0 || m.Commit < m.Op) {
		return nil
	}
	c.pending = false
	c.view = max(c.view, m.View)
	c.response = m
	return []vsr.Effect{{Kind: vsr.CancelEffect}}
}
