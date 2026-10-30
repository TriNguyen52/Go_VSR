// Package vsr implements normal VSR request processing of Viewstamped Replication Revisited.
// It supports three replicas, one client, retries, catch-up, and explicit view changes.
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
	Request         Kind = "REQUEST"
	Prepare         Kind = "PREPARE"
	PrepareOK       Kind = "PREPARE_OK"
	Commit          Kind = "COMMIT"
	Reply           Kind = "REPLY"
	Retry           Kind = "RETRY"  // Local timer input, never a network message.
	Reject          Kind = "REJECT" // Request ID reused with different command fields.
	FetchState      Kind = "FETCH_STATE"
	StateTransfer   Kind = "STATE"
	TransferRetry   Kind = "TRANSFER_RETRY" // Local input, never a network message.
	Suspect         Kind = "SUSPECT"        // Local input naming a strictly newer view.
	ViewRetry       Kind = "VIEW_RETRY"
	StartViewChange Kind = "START_VIEW_CHANGE"
	DoViewChange    Kind = "DO_VIEW_CHANGE"
	StartView       Kind = "START_VIEW"
)

// Message carries a client request, replication message, or reply.
// From and To identify the sender and receiver; their identities are trusted.
// Command is used in requests and prepares.
// Op identifies a log position in prepares, acknowledgments, and replies.
// Commit tells backups how far the primary has committed.
// Result carries the reply to the client.
// Entries is read-only during Step; emitted STATE messages own their log copy.
type Message struct {
	LastNormalView   uint64
	Kind             Kind
	From, To         int
	View, Op, Commit uint64
	Command          kv.Command
	Result           kv.Result
	RequestID        uint64     // Client identity is From for REQUEST, otherwise ClientEndpoint.
	TransferID       uint64     // Separate from client request IDs; scoped to requester and view.
	Entries          []LogEntry // STATE only; full log, with positions implied by order.
}

// LogEntry transfers request identity and command, never cached application results.
type LogEntry struct {
	RequestID uint64
	Command   kv.Command
}

// Counters records a replica's progress.
// Op is the last position in its log, Commit is the last committed position, and Applied is the last command it has executed.
// Applied <= Commit <= Op. View increases when entering a view change.
type Counters struct{ View, Op, Commit, Applied uint64 }

// Transition identifies a report that a command was committed or executed.
type Transition string

const (
	Committed Transition = "committed"
	Applied   Transition = "applied"
	Selected  Transition = "selected"
	Installed Transition = "installed"
)

// Observation reports one command being committed or executed.
// State records the counters at that moment.
// Result is used only when the command has been executed.
type Observation struct {
	Entries   []LogEntry
	Reports   []Message // Distinct reports used for history selection.
	Kind      Transition
	Replica   int
	State     Counters
	Index     uint64
	Command   kv.Command
	Result    kv.Result
	RequestID uint64
}

// EffectKind identifies work returned by Step for the caller to handle.
type EffectKind string

const (
	SendEffect    EffectKind = "send"
	ObserveEffect EffectKind = "observe"
	ArmEffect     EffectKind = "arm-retry"
	CancelEffect  EffectKind = "cancel-retry"
)

// Effect asks the caller to send, observe, or arm/cancel its local retry timer.
// ArmEffect stores Message for delivery as a local input after a positive delay.
// The adapter owns timer generations and must discard replaced/canceled events.
// CancelEffect cancels the caller's timer; its Message is unused.
// The caller must handle every effect and reject unknown kinds.
type Effect struct {
	Kind        EffectKind
	Message     Message
	Observation Observation
}

// Replica stores one member's log, application state, and progress.
type Replica struct {
	changing           bool
	lastNormalView     uint64
	starts             [ReplicaCount]bool
	reports            [ReplicaCount]*Message
	report             *Message // Frozen report for retries in this view.
	id                 int
	state              Counters
	log                []entry
	store              kv.Store
	records            map[uint64]uint64 // Request ID -> log position; retained for bounded runs.
	acks               [ReplicaCount]bool
	transferSequence   uint64
	transferID         uint64 // Zero means no catch-up is pending.
	wantOp, wantCommit uint64
}

type entry struct {
	command   kv.Command
	requestID uint64
	result    kv.Result
}

// New creates an empty replica in view 0.
// It panics unless id is 0, 1, or 2. Replica 0 starts as the primary.
func New(id int) *Replica {
	if id < 0 || id >= ReplicaCount {
		panic(fmt.Sprintf("vsr: invalid replica ID %d (want 0..%d)", id, ReplicaCount-1))
	}
	return &Replica{id: id, records: make(map[uint64]uint64)}
}

// State returns a copy of the replica's progress counters.
func (r *Replica) State() Counters { return r.state }

// Log returns a copy of the log, including commands not yet committed.
func (r *Replica) Log() []kv.Command {
	commands := make([]kv.Command, len(r.log))
	for i, e := range r.log {
		commands[i] = e.command
	}
	return commands
}

// Acknowledgments returns the distinct backup count for the latest proposal.
func (r *Replica) Acknowledgments() int {
	n := 0
	for _, ack := range r.acks {
		if ack {
			n++
		}
	}
	return n
}

// Snapshot returns a sorted copy of the key-value store.
// It includes commands executed through the Applied position.
func (r *Replica) Snapshot() []kv.Entry { return r.store.Snapshot() }

// Step handles one incoming message and returns messages and reports to process.
// It does not access the network or clock itself.
func (r *Replica) Step(m Message) []Effect {
	if m.To != r.id {
		return nil
	}
	if effects, handled := r.viewStep(m); handled {
		return effects
	}
	if r.changing || (m.Kind != Request && m.View != r.state.View) {
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
	prepare := func() {
		e := r.log[r.state.Op-1]
		broadcast(Message{Kind: Prepare, Op: r.state.Op, Commit: r.state.Commit, Command: e.command, RequestID: e.requestID})
	}
	arm := func() {
		effects = append(effects, Effect{Kind: ArmEffect, Message: Message{Kind: Retry, From: r.id, To: r.id, View: r.state.View, Op: r.state.Op}})
	}
	switch m.Kind {
	case FetchState, StateTransfer, TransferRetry:
		return r.transfer(m)
	case Request:
		if r.id != primary || m.From != ClientEndpoint || m.RequestID == 0 {
			return nil
		}
		if op, ok := r.records[m.RequestID]; ok {
			e := r.log[op-1]
			if e.command != m.Command {
				send(Message{Kind: Reject, To: m.From, RequestID: m.RequestID})
			} else if op <= r.state.Commit {
				send(Message{Kind: Reply, To: m.From, RequestID: m.RequestID, Op: op, Commit: r.state.Commit, Result: e.result})
			} else {
				prepare() // Reuse the pending position and its existing timer.
			}
			return effects
		}
		if r.state.Op != r.state.Commit || (len(r.log) > 0 && m.RequestID <= r.log[len(r.log)-1].requestID) {
			return nil
		}
		r.log = append(r.log, entry{command: m.Command, requestID: m.RequestID})
		r.state.Op++
		r.records[m.RequestID] = r.state.Op
		r.acks = [ReplicaCount]bool{}
		prepare()
		arm()
	case Retry:
		if r.id != primary || m.From != r.id || m.Op != r.state.Op || r.state.Op == r.state.Commit {
			return nil
		}
		prepare()
		arm()
	case Prepare:
		if r.id == primary || m.From != primary || m.Op == 0 || m.Commit >= m.Op || m.RequestID == 0 {
			return nil
		}
		if m.Op > r.state.Op+1 {
			return r.fetch(m.Op, m.Commit)
		}
		if m.Op <= r.state.Op {
			e := r.log[m.Op-1]
			if e.command != m.Command || e.requestID != m.RequestID {
				return nil
			}
		} else {
			if _, exists := r.records[m.RequestID]; exists {
				return nil
			}
			if len(r.log) > 0 && m.RequestID <= r.log[len(r.log)-1].requestID {
				return nil
			}
			r.log = append(r.log, entry{command: m.Command, requestID: m.RequestID})
			r.state.Op++
			r.records[m.RequestID] = m.Op
		}
		r.advance(m.Commit, &effects)
		send(Message{Kind: PrepareOK, To: primary, Op: m.Op})
		r.finishTransfer(&effects)
	case PrepareOK:
		if r.id != primary || m.From < 0 || m.From >= ReplicaCount || m.From == primary ||
			m.Op != r.state.Op || m.Op <= r.state.Commit {
			return nil
		}
		r.acks[m.From] = true
		// The primary already has the command. One backup acknowledgment
		// gives us the two replicas needed for a quorum.
		result := r.advance(m.Op, &effects)
		// Tell backups about the commit immediately instead of waiting
		// for the next prepare or a heartbeat.
		broadcast(Message{Kind: Commit, Commit: r.state.Commit})
		send(Message{Kind: Reply, To: ClientEndpoint, RequestID: r.log[m.Op-1].requestID, Op: m.Op, Commit: r.state.Commit, Result: result})
		effects = append(effects, Effect{Kind: CancelEffect})
	case Commit:
		if r.id == primary || m.From != primary {
			return nil
		}
		if m.Commit > r.state.Op {
			return r.fetch(m.Commit, m.Commit)
		}
		r.advance(m.Commit, &effects)
		r.finishTransfer(&effects)
	}
	return effects
}

// advance commits and executes log entries in order through target.
func (r *Replica) advance(target uint64, effects *[]Effect) kv.Result {
	var result kv.Result
	for r.state.Commit < target {
		r.state.Commit++
		e := &r.log[r.state.Commit-1]
		command := e.command
		observe := func(kind Transition, value kv.Result) {
			*effects = append(*effects, Effect{Kind: ObserveEffect, Observation: Observation{
				Kind: kind, Replica: r.id, State: r.state, Index: r.state.Commit,
				Command: command, Result: value, RequestID: e.requestID,
			}})
		}
		observe(Committed, kv.Result{})
		result = r.store.Apply(command)
		e.result = result
		r.state.Applied++
		observe(Applied, result)
	}
	return result
}
