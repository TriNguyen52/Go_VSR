// Package kv implements the deterministic application for the project. It has no consensus, request identities, or retry handling.
package kv

import (
	"math"
	"sort"
)

// Operation selects a command. The zero value is invalid.
type Operation uint8

// Supported commands; reads use the same deterministic application boundary.
const (
	Put Operation = iota + 1
	Add
	Get
)

// Command is a value-owned application input. Value is ignored for Get.
type Command struct {
	Op    Operation
	Key   string
	Value int64
}

// ErrorCode is a serializable deterministic error code; empty means success.
type ErrorCode string

// Application errors leave the store unchanged and return result value zero.
const (
	ErrOverflow         ErrorCode = "integer overflow"
	ErrInvalidOperation ErrorCode = "invalid operation"
)

// Result returns the value after a successful command, or an error with Value zero.
type Result struct {
	Value int64
	Error ErrorCode
}

// Entry is one stored key/value pair in a snapshot.
type Entry struct {
	Key   string
	Value int64
}

// Store is an in-memory state machine.
type Store struct {
	values map[string]int64
}

// Apply executes one command. It does not identify or suppress duplicate requests.
func (s *Store) Apply(c Command) Result {
	var value int64
	switch c.Op {
	case Put:
		value = c.Value
	case Add:
		current := s.values[c.Key]

		if (c.Value > 0 && current > math.MaxInt64-c.Value) ||
			(c.Value < 0 && current < math.MinInt64-c.Value) {
			return Result{Error: ErrOverflow}
		}
		value = current + c.Value
	case Get:
		return Result{Value: s.values[c.Key]}
	default:
		return Result{Error: ErrInvalidOperation}
	}
	if s.values == nil {
		s.values = make(map[string]int64)
	}
	s.values[c.Key] = value
	return Result{Value: value}
}

// Snapshot returns a fresh slice sorted by Go string order. Mutating it cannot change the store, and later commands cannot change an earlier snapshot.
func (s *Store) Snapshot() []Entry {
	entries := make([]Entry, 0, len(s.values))
	for key, value := range s.values {
		entries = append(entries, Entry{Key: key, Value: value})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}
