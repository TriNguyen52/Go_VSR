// Package vsr implements normal VSR request processing of Viewstamped Replication Revisited.
// It currently supports one primary, three replicas, and no failures or retries.
package vsr

import (
	"fmt"

	"go-vsr/internal/kv"
)

// ReplicaCount is the number of replicas, with IDs 0, 1, and 2.
const ReplicaCount = 3

// ClientEndpoint is the ID of the single client, separate from the replica IDs.
const ClientEndpoint = 3

// Kind identifies a protocol message.
type Kind string

const (
	Request   Kind = "REQUEST"
	Prepare   Kind = "PREPARE"
	PrepareOK Kind = "PREPARE_OK"
	Commit    Kind = "COMMIT"
	Reply     Kind = "REPLY"
)

// Message carries a client request, replication message, or reply.
// From and To identify the sender and receiver; their identities are trusted.
// Command is used in requests and prepares.
// Op identifies a log position in prepares, acknowledgments, and replies.
// Commit tells backups how far the primary has committed.
// Result carries the reply to the client.
type Message struct {
	Kind             Kind
	From, To         int
	View, Op, Commit uint64
	Command          kv.Command
	Result           kv.Result
}

// Counters records a replica's progress.
// Op is the last position in its log, Commit is the last committed position, and Applied is the last command it has executed.
// Applied <= Commit <= Op. View remains 0 in this version.
type Counters struct{ View, Op, Commit, Applied uint64 }

// Transition identifies a report that a command was committed or executed.
type Transition string

const (
	Committed Transition = "committed"
	Applied   Transition = "applied"
)

// Observation reports one command being committed or executed.
// State records the counters at that moment.
// Result is used only when the command has been executed.
type Observation struct {
	Kind    Transition
	Replica int
	State   Counters
	Index   uint64
	Command kv.Command
	Result  kv.Result
}

// EffectKind identifies work returned by Step for the caller to handle.
type EffectKind string

const (
	SendEffect    EffectKind = "send"
	ObserveEffect EffectKind = "observe"
)

// Effect asks the caller to send a message or pass a report to the checker.
// Kind determines whether Message or Observation is used.
// The caller must handle every effect and reject unknown kinds.
type Effect struct {
	Kind        EffectKind
	Message     Message
	Observation Observation
}

// Replica stores one member's log, application state, and progress.
type Replica struct {
	id    int
	state Counters
	log   []kv.Command
	store kv.Store
}

// New creates an empty replica in view 0.
// It panics unless id is 0, 1, or 2. Replica 0 starts as the primary.
func New(id int) *Replica {
	if id < 0 || id >= ReplicaCount {
		panic(fmt.Sprintf("vsr: invalid replica ID %d (want 0..%d)", id, ReplicaCount-1))
	}
	return &Replica{id: id}
}

// State returns a copy of the replica's progress counters.
func (r *Replica) State() Counters { return r.state }

// Log returns a copy of the log, including commands not yet committed.
func (r *Replica) Log() []kv.Command { return append([]kv.Command(nil), r.log...) }

// Snapshot returns a sorted copy of the key-value store.
// It includes commands executed through the Applied position.
func (r *Replica) Snapshot() []kv.Entry { return r.store.Snapshot() }

// Step handles one incoming message and returns messages and reports to process.
// It does not access the network or clock itself.
func (r *Replica) Step(m Message) []Effect {
	if m.To != r.id || m.View != r.state.View {
		return nil
	}
	primary := int(r.state.View % ReplicaCount)
	var effects []Effect
	send := func(m Message) {
		m.From, m.View = r.id, r.state.View
		effects = append(effects, Effect{Kind: SendEffect, Message: m})
	}
	broadcast := func(m Message) {
		for id := 0; id < ReplicaCount; id++ {
			if id != r.id {
				m.To = id
				send(m)
			}
		}
	}
	switch m.Kind {
	case Request:
		if r.id != primary || m.From != ClientEndpoint || r.state.Op != r.state.Commit {
			return nil
		}
		r.log = append(r.log, m.Command)
		r.state.Op++
		broadcast(Message{Kind: Prepare, Op: r.state.Op, Commit: r.state.Commit, Command: m.Command})
	case Prepare:
		if r.id == primary || m.From != primary || m.Op != r.state.Op+1 || m.Commit >= m.Op {
			return nil
		}
		r.log = append(r.log, m.Command)
		r.state.Op++
		r.advance(m.Commit, &effects)
		send(Message{Kind: PrepareOK, To: primary, Op: m.Op})
	case PrepareOK:
		if r.id != primary || m.From < 0 || m.From >= ReplicaCount || m.From == primary ||
			m.Op != r.state.Op || m.Op <= r.state.Commit {
			return nil
		}
		// The primary already has the command. One backup acknowledgment
		// gives us the two replicas needed for a quorum.
		result := r.advance(m.Op, &effects)
		// Tell backups about the commit immediately instead of waiting
		// for the next prepare or a heartbeat.
		broadcast(Message{Kind: Commit, Commit: r.state.Commit})
		send(Message{Kind: Reply, To: ClientEndpoint, Op: m.Op, Commit: r.state.Commit, Result: result})
	case Commit:
		if r.id == primary || m.From != primary || m.Commit > r.state.Op {
			return nil
		}
		r.advance(m.Commit, &effects)
	}
	return effects
}

// advance commits and executes log entries in order through target.
func (r *Replica) advance(target uint64, effects *[]Effect) kv.Result {
	var result kv.Result
	for r.state.Commit < target {
		r.state.Commit++
		command := r.log[r.state.Commit-1]
		observe := func(kind Transition, value kv.Result) {
			*effects = append(*effects, Effect{Kind: ObserveEffect, Observation: Observation{
				Kind: kind, Replica: r.id, State: r.state, Index: r.state.Commit,
				Command: command, Result: value,
			}})
		}
		observe(Committed, kv.Result{})
		result = r.store.Apply(command)
		r.state.Applied++
		observe(Applied, result)
	}
	return result
}
