// Package sim runs messages and timers using simulated time.
// Events run one at a time in a repeatable order.
package sim

import (
	"container/heap"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
)

// Time counts simulated time units. The simulator advances time without waiting.
type Time int64

// Endpoint names a participant, such as a replica or client.
// Register it before sending messages or setting timers.
type Endpoint string

// Kind identifies whether an event is a message or a timer firing.
type Kind string

const (
	Message Kind = "message"
	Timeout Kind = "timeout"
)

// TimerToken identifies a specific timer setting.
// Generation distinguishes a new setting from an older one with the same name.
type TimerToken struct {
	Name       string
	Generation uint64
}

// Event describes a message or timer scheduled for a participant.
// Events at the same time run in the order they were scheduled.
// The receiver may keep or change Payload.
type Event struct {
	Time     Time
	Sequence uint64
	Kind     Kind
	From, To Endpoint
	Payload  []byte
	Timer    TimerToken
}

type timerKey struct {
	endpoint Endpoint
	name     string
}

// Runner keeps scheduled events and runs their handlers one at a time.
// It is ready to use without initialization. Do not copy it after use or
// access it from multiple goroutines.
//
// Handlers may send messages or change timers, but must return before the next
// event can run. A handler must not call Run.
type Runner struct {
	now      Time
	sequence uint64
	queue    eventHeap
	handlers map[Endpoint]func(Event)
	timers   map[timerKey]uint64
	trace    strings.Builder
	running  bool
}

// Now returns the current simulated time, starting at zero.
func (r *Runner) Now() Time { return r.now }

// Register assigns a handler to a participant.
// It panics if the name is empty, the handler is nil, or the name is already used.
func (r *Runner) Register(id Endpoint, handler func(Event)) {
	if id == "" || handler == nil || r.handlers[id] != nil {
		panic(fmt.Sprintf("sim: empty, nil, or duplicate endpoint %q", id))
	}
	if r.handlers == nil {
		r.handlers = make(map[Endpoint]func(Event))
	}
	r.handlers[id] = handler
}

func (r *Runner) requireEndpoint(id Endpoint) {
	if r.handlers[id] == nil {
		panic(fmt.Sprintf("sim: unknown endpoint %q", id))
	}
}

// schedule adds an event and assigns its delivery time and sequence number.
func (r *Runner) schedule(e Event, delay Time) uint64 {
	if delay < 0 || delay > Time(math.MaxInt64)-r.now {
		panic(fmt.Sprintf("sim: invalid delay %d at time %d", delay, r.now))
	}
	if r.sequence == math.MaxUint64 {
		panic("sim: sequence exhausted")
	}
	r.sequence++
	e.Time, e.Sequence = r.now+delay, r.sequence
	heap.Push(&r.queue, e)
	return e.Sequence
}

// Send schedules one message after the given delay, which may be zero.
// It copies the payload so later changes by the sender cannot change the message.
// This version delivers every message once; different delays can change their
// arrival order. Both participants must be registered.
func (r *Runner) Send(from, to Endpoint, payload []byte, delay Time) uint64 {
	r.requireEndpoint(from)
	r.requireEndpoint(to)
	return r.schedule(Event{Kind: Message, From: from, To: to,
		Payload: append([]byte(nil), payload...)}, delay)
}

// Arm sets a timer for a registered participant.
// Setting the same named timer again replaces its previous setting.
// Old events stay in the queue, but their handlers will not run.
func (r *Runner) Arm(to Endpoint, name string, delay Time) TimerToken {
	r.requireEndpoint(to)
	if name == "" {
		panic(fmt.Sprintf("sim: empty timer name at endpoint %q", to))
	}
	// Use the event's sequence number to identify this timer setting.
	token := TimerToken{Name: name, Generation: r.sequence + 1}
	r.schedule(Event{Kind: Timeout, To: to, Timer: token}, delay)
	if r.timers == nil {
		r.timers = make(map[timerKey]uint64)
	}
	r.timers[timerKey{endpoint: to, name: name}] = token.Generation
	return token
}

// Cancel returns true if it cancels the timer identified by token.
// An old token cannot cancel a newer setting of the same timer.
func (r *Runner) Cancel(to Endpoint, token TimerToken) bool {
	key := timerKey{endpoint: to, name: token.Name}
	if generation, ok := r.timers[key]; ok && generation == token.Generation {
		delete(r.timers, key)
		return true
	}
	return false
}

// Result reports how much work Run processed.
// Complete means the event queue is empty; check client results separately
// to determine whether the workload succeeded.
type Result struct {
	Complete           bool
	Processed, Pending int
}

// Run processes up to limit queued events, including canceled or replaced timers.
// It moves simulated time to each event's scheduled time without waiting.
// If events remain, another call can continue the run.
//
// The limit prevents an endless chain of immediate events from running forever.
// Run panics if limit is negative or a handler calls Run again.
func (r *Runner) Run(limit int) Result {
	if limit < 0 || r.running {
		panic("sim: negative limit or recursive Run")
	}
	r.running = true
	defer func() { r.running = false }()
	processed := 0
	for processed < limit && len(r.queue) > 0 {
		e := heap.Pop(&r.queue).(Event)
		r.now = e.Time
		processed++
		status := "deliver"
		if e.Kind == Timeout {
			key := timerKey{endpoint: e.To, name: e.Timer.Name}
			if r.timers[key] != e.Timer.Generation {
				status = "stale"
			} else {
				// Remove the timer before its handler runs so it fires only once.
				delete(r.timers, key)
			}
		}
		// Record the event before the receiver can change its payload.
		fmt.Fprintf(&r.trace, "%d #%d %s %s from=%q to=%q payload=%x timer=%q generation=%d\n",
			e.Time, e.Sequence, status, e.Kind, e.From, e.To, e.Payload, e.Timer.Name, e.Timer.Generation)
		if status == "deliver" {
			r.handlers[e.To](e)
		}
	}
	return Result{Complete: len(r.queue) == 0, Processed: processed, Pending: len(r.queue)}
}

// Trace returns the event history in processing order, one event per line.
// It includes timers skipped because they were canceled or replaced.
func (r *Runner) Trace() string { return r.trace.String() }

// Digest returns a hash of the trace for comparing repeated runs.
// Matching hashes do not establish that the client requests succeeded.
func (r *Runner) Digest() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(r.Trace())))
}

// eventHeap keeps the earliest event first.
// Sequence breaks ties when events have the same scheduled time.
type eventHeap []Event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].Time != h[j].Time {
		return h[i].Time < h[j].Time
	}
	return h[i].Sequence < h[j].Sequence
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(Event)) }
func (h *eventHeap) Pop() any {
	last := len(*h) - 1
	e := (*h)[last]
	(*h)[last] = Event{}
	*h = (*h)[:last]
	return e
}
