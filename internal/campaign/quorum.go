package campaign

import (
	"errors"
	"fmt"

	"go-vsr/internal/sim"
)

type PermanentFault string

const (
	Stopped     PermanentFault = "stopped"
	Isolated    PermanentFault = "isolated"
	SendOnly    PermanentFault = "send-only"
	ReceiveOnly PermanentFault = "receive-only"

	noFault          PermanentFault = "none"
	probeRequestTime sim.Time       = 100
)

// ErrQuorumUnavailable means the requested case could not start after its fault prefix.
var ErrQuorumUnavailable = errors.New("quorum case ineligible")

type QuorumConfig struct {
	Pair                                [2]int
	Fault                               PermanentFault
	Observe, RequestLimit, CatchUpLimit sim.Time
	Probe                               bool `json:",omitempty"`
}

func DefaultQuorum(pair [2]int, fault PermanentFault) QuorumConfig {
	return QuorumConfig{Pair: pair, Fault: fault, Observe: 500, RequestLimit: 200, CatchUpLimit: 200}
}

// StallProbe reproduces the send-only fault or runs its fault-free control.
func StallProbe(seed uint64, baseline bool) Config {
	config := Default(seed)
	config.FaultUntil = 0
	quorum := DefaultQuorum([2]int{0, 1}, SendOnly)
	quorum.Probe, quorum.RequestLimit = true, 70
	if baseline {
		quorum.Fault = noFault
	}
	config.Quorum = &quorum
	return config
}

func (q QuorumConfig) validate(config Config) error {
	if q.Pair[0] < 0 || q.Pair[0] >= q.Pair[1] || q.Pair[1] >= 3 {
		return fmt.Errorf("invalid healthy pair %v", q.Pair)
	}
	switch q.Fault {
	case Stopped, Isolated, SendOnly, ReceiveOnly:
	case noFault:
		if !q.Probe {
			return fmt.Errorf("fault-free control requires the stall probe")
		}
	default:
		return fmt.Errorf("unknown permanent fault %q", q.Fault)
	}
	if q.Observe <= 0 || q.Observe > config.MaxTime-config.FaultUntil || q.RequestLimit <= 0 || q.CatchUpLimit <= 0 {
		return fmt.Errorf("invalid quorum time limits")
	}
	if q.Probe && (config.FaultUntil != 0 || q.Observe <= probeRequestTime) {
		return fmt.Errorf("stall probe needs a zero-length fault prefix and an observation window beyond tick %d", probeRequestTime)
	}
	return nil
}

type QuorumResult struct {
	Attempted, Eligible                        bool
	Transition, ObservationEnd                 sim.Time
	StartedAtTransition, CompletedAtTransition int
	FreshCompleted, PendingRequest             int
	FirstFreshReply, LongestWait               sim.Time
	ViewsAtTransition                          [3]uint64
}

func (h *host) freezeQuorum() {
	config := h.config.Quorum
	q := h.quorum
	q.Attempted = true
	q.Transition = h.runner.Now()
	q.ObservationEnd = q.Transition + config.Observe
	q.StartedAtTransition = h.started
	q.CompletedAtTransition = h.completed
	for id, replica := range h.replicas {
		q.ViewsAtTransition[id] = replica.State().View
	}
	outside := 3 - config.Pair[0] - config.Pair[1]
	for _, id := range config.Pair {
		if h.stopped[id] || h.replicas[id].Recovering() {
			h.err = fmt.Errorf("%w: healthy replica %d is stopped or recovering", ErrQuorumUnavailable, id)
			return
		}
	}
	if h.stopped[outside] && config.Fault != Stopped {
		h.err = fmt.Errorf("%w: replica %d is already stopped and cannot exercise %s", ErrQuorumUnavailable, outside, config.Fault)
		return
	}
	switch config.Fault {
	case Stopped:
		if !h.stopped[outside] {
			h.stop(outside)
		}
		h.blockReplica(outside, true, true)
	case Isolated:
		h.blockReplica(outside, true, true)
	case SendOnly:
		h.blockReplica(outside, true, false)
	case ReceiveOnly:
		h.blockReplica(outside, false, true)
	case noFault:
		h.blockReplica(outside, false, false)
	}
	q.Eligible = true
	h.j.observe("quorum", q.Transition, struct {
		Pair           [2]int
		Outside        int
		Fault          PermanentFault
		Blocked        [4][4]bool
		Stopped        [3]bool
		ObservationEnd sim.Time
		Views          [3]uint64
	}{config.Pair, outside, config.Fault, h.blocked, h.stopped, q.ObservationEnd, q.ViewsAtTransition})
}

func (h *host) quorumWait() sim.Time {
	return h.runner.Now() - max(h.requestStarted, h.quorum.Transition)
}

// Check deadlines after all events at the current tick have been delivered.
func (h *host) checkQuorumProgress() bool {
	q := h.quorum
	if !q.Eligible {
		return false
	}
	config := h.config.Quorum
	now := h.runner.Now()
	if h.client.Pending() && h.quorumWait() >= config.RequestLimit {
		h.err = fmt.Errorf("liveness deadline: request=%d pending=%d limit=%d starting-views=%v", h.started, h.quorumWait(), config.RequestLimit, q.ViewsAtTransition)
		return false
	}
	if now < q.ObservationEnd {
		return false
	}
	left, right := h.replicas[config.Pair[0]].State(), h.replicas[config.Pair[1]].State()
	if !h.client.Pending() && left.Applied >= uint64(h.completed) && right.Applied >= uint64(h.completed) {
		if h.completed > q.StartedAtTransition {
			return true
		}
		h.err = fmt.Errorf("liveness window ended without completing a fresh request")
		return false
	}
	if now-q.ObservationEnd >= config.CatchUpLimit {
		h.err = fmt.Errorf("catch-up deadline: completed=%d applied=[%d %d] pending=%t", h.completed, left.Applied, right.Applied, h.client.Pending())
	}
	return false
}
