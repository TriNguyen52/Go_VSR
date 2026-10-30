// Package checker checks that replicas commit the same commands in the same order and run each command only after it is committed.
package checker

import (
	"fmt"
	"math/big"

	"go-vsr/internal/kv"
	"go-vsr/internal/vsr"
)

type progress struct {
	commit, applied uint64
	values          map[string]int64
}

// Checker remembers committed commands and checks each replica's results.
type Checker struct {
	history   map[uint64]kv.Command
	requests  map[uint64]uint64 // Log position -> request identity.
	positions map[uint64]uint64 // Request identity -> unique committed position.
	views     map[uint64]uint64 // Earliest observed commitment view per position.
	replicas  [vsr.ReplicaCount]progress
}

// Observe checks one commit or execution reported by a replica.
// View observations additionally check selection and installed history.
func (c *Checker) Observe(o vsr.Observation) error {
	if o.Kind == vsr.Selected || o.Kind == vsr.Installed {
		return c.view(o)
	}
	if o.Replica < 0 || o.Replica >= vsr.ReplicaCount ||
		o.State.Applied > o.State.Commit || o.State.Commit > o.State.Op || o.Index == 0 || o.RequestID == 0 {
		return fmt.Errorf("invalid observation: %+v", o)
	}
	p := &c.replicas[o.Replica]
	switch o.Kind {
	case vsr.Committed:
		if o.Index != p.commit+1 || o.State.Commit != o.Index || o.State.Applied != p.applied {
			return fmt.Errorf("commit order: %+v", o)
		}
		if old, ok := c.history[o.Index]; ok && (old != o.Command || c.requests[o.Index] != o.RequestID) {
			return fmt.Errorf("conflicting commitment at %d", o.Index)
		}
		if c.history == nil {
			c.history = make(map[uint64]kv.Command)
			c.requests = make(map[uint64]uint64)
			c.positions = make(map[uint64]uint64)
			c.views = make(map[uint64]uint64)
		}
		if index, ok := c.positions[o.RequestID]; ok && index != o.Index {
			return fmt.Errorf("request %d committed at multiple positions", o.RequestID)
		}
		c.history[o.Index] = o.Command
		if view, ok := c.views[o.Index]; !ok || o.State.View < view {
			c.views[o.Index] = o.State.View
		}
		c.requests[o.Index] = o.RequestID
		c.positions[o.RequestID] = o.Index
		p.commit = o.Index
	case vsr.Applied:
		command, ok := c.history[o.Index]
		if !ok || command != o.Command || c.requests[o.Index] != o.RequestID || o.Index != p.applied+1 || o.Index > p.commit ||
			o.State.Applied != o.Index || o.State.Commit != p.commit {
			return fmt.Errorf("application before commit or out of order: %+v", o)
		}
		if p.values == nil {
			p.values = make(map[string]int64)
		}
		want := reference(p.values, command)
		if want != o.Result {
			return fmt.Errorf("result at %d: got %+v want %+v", o.Index, o.Result, want)
		}
		p.applied = o.Index
	default:
		return fmt.Errorf("unknown transition %q", o.Kind)
	}
	return nil
}

// reference computes the expected result using a separate key-value map.
func reference(values map[string]int64, command kv.Command) kv.Result {
	switch command.Op {
	case kv.Get:
		return kv.Result{Value: values[command.Key]}
	case kv.Put:
		values[command.Key] = command.Value
	case kv.Add:
		sum := new(big.Int).Add(big.NewInt(values[command.Key]), big.NewInt(command.Value))
		if !sum.IsInt64() {
			return kv.Result{Error: kv.ErrOverflow}
		}
		values[command.Key] = sum.Int64()
	default:
		return kv.Result{Error: kv.ErrInvalidOperation}
	}
	return kv.Result{Value: values[command.Key]}
}
