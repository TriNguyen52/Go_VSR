package campaign

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"go-vsr/internal/checker"
	"go-vsr/internal/client"
	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

type host struct {
	runner                             sim.Runner
	replicas                           [3]*vsr.Replica
	client                             client.Client
	checker                            checker.Checker
	j                                  journal
	config                             Config
	workload, network, crashes, nonces stream
	stopped                            [3]bool
	incarnation                        [3]uint64
	retryIncarnation                   [4]uint64
	retry, clocks                      [4]sim.TimerToken
	callbacks                          [4]vsr.Message
	blocked                            [4][4]bool
	healed                             bool
	started, completed, events         int
	lastCompletion, requestStarted     sim.Time
	quorum                             *QuorumResult
	usedNonces                         map[uint64]bool
	err                                error
}

func endpoint(id int) sim.Endpoint { return sim.Endpoint(strconv.Itoa(id)) }

func execute(config Config, build Build, expected *Trace) (*Trace, error) {
	if config.Operations < 1 || config.Operations > 100000 || config.FaultUntil < 0 || config.MaxTime <= config.FaultUntil || config.MaxEvents < 1 {
		return nil, fmt.Errorf("invalid campaign limits")
	}
	if config.Quorum != nil {
		if err := config.Quorum.validate(config); err != nil {
			return nil, err
		}
	}
	trace := &Trace{Header: Header{Version: Version, Build: build, Config: config, Initial: initialState()}}
	h := &host{config: config, usedNonces: make(map[uint64]bool),
		workload: stream(config.Seeds.Workload), network: stream(config.Seeds.Network),
		crashes: stream(config.Seeds.Crashes), nonces: stream(config.Seeds.Nonces),
		j: journal{trace: trace, expected: expected}}
	if config.Quorum != nil {
		h.quorum = &QuorumResult{}
	}
	for id := 0; id < 4; id++ {
		if id < 3 {
			h.replicas[id] = vsr.New(id)
		}
		h.runner.Register(endpoint(id), func(e sim.Event) { h.deliver(id, e) })
		if id < 3 {
			h.clocks[id] = h.runner.Arm(endpoint(id), "clock", clockInterval)
		}
	}
	// Control events share the same deterministic queue as protocol boundaries.
	h.runner.Register("faults", func(e sim.Event) {
		h.j.observe("boundary", e.Time, e)
		if h.j.err != nil {
			return
		}
		if e.Time >= config.FaultUntil {
			if h.quorum == nil {
				h.heal()
			} else {
				h.freezeQuorum()
			}
			return
		}
		h.fault()
		h.runner.Arm("faults", "control", min(sim.Time(20), config.FaultUntil-e.Time))
	})
	h.runner.Arm("faults", "control", min(sim.Time(20), config.FaultUntil))
	func() {
		// Preserve a diagnostic journal even when a core invariant panics.
		defer func() {
			if failure := recover(); failure != nil {
				h.err = fmt.Errorf("panic: %v", failure)
			}
		}()
		for h.err == nil && h.j.err == nil {
			if h.quorum == nil && h.shouldStart() {
				h.start()
			}
			next, queued := h.runner.NextTime()
			if (h.healed && h.converged()) || (h.quorum != nil && next > h.runner.Now() && h.checkQuorumProgress()) {
				trace.Result.Complete = true
				break
			}
			if h.err != nil {
				break
			}
			timeExhausted := h.runner.Now() >= config.MaxTime
			if h.quorum != nil {
				timeExhausted = next > config.MaxTime
			}
			if !queued || h.events >= config.MaxEvents || timeExhausted {
				h.err = fmt.Errorf("incomplete campaign: phase=%s started=%d completed=%d events=%d time=%d", h.phase(), h.started, h.completed, h.events, h.runner.Now())
				break
			}
			if h.quorum != nil && h.shouldStart() {
				h.start()
			}
			if h.err != nil || h.j.err != nil {
				break
			}
			h.runner.Run(1)
			h.events++
		}
	}()
	trace.Result.Started, trace.Result.Completed, trace.Result.Events = h.started, h.completed, h.events
	trace.Result.Time, trace.Result.TraceSHA256 = h.runner.Now(), h.runner.Digest()
	trace.Result.Quorum = h.quorum
	if h.quorum != nil && h.quorum.Eligible {
		h.quorum.FreshCompleted = max(0, h.completed-h.quorum.StartedAtTransition)
		if h.client.Pending() {
			h.quorum.PendingRequest = h.started
			h.quorum.LongestWait = max(h.quorum.LongestWait, h.quorumWait())
		}
	}
	for id, r := range h.replicas {
		trace.Result.States[id], trace.Result.Snapshots[id] = r.State(), r.Snapshot()
	}
	if h.j.err != nil {
		h.err = h.j.err
	}
	if h.err != nil {
		h.err = fmt.Errorf("phase=%s time=%d events=%d: %w", h.phase(), h.runner.Now(), h.events, h.err)
		trace.Failure = h.err.Error()
	}
	if expected != nil && h.j.err == nil && (h.j.position != len(expected.Entries) || !reflect.DeepEqual(trace.Result, expected.Result) || trace.Failure != expected.Failure) {
		h.err = fmt.Errorf("replay divergence at final result or journal length")
	}
	return trace, h.err
}

func (h *host) phase() string {
	if h.quorum != nil && h.quorum.Attempted {
		return "quorum"
	}
	if h.healed {
		return "healing"
	}
	return "faults"
}

func (h *host) shouldStart() bool {
	if h.client.Pending() {
		return false
	}
	if h.quorum == nil {
		return h.started < h.config.Operations
	}
	if h.config.Quorum.Probe && h.runner.Now() < probeRequestTime {
		return false
	}
	if !h.quorum.Eligible {
		return h.runner.Now() < h.config.FaultUntil && h.started < h.config.Operations
	}
	return h.runner.Now() < h.quorum.ObservationEnd
}

func (h *host) start() {
	var command kv.Command
	h.j.decide("workload", h.runner.Now(), &command, func() {
		if h.config.Quorum != nil && h.config.Quorum.Probe && h.started == 0 {
			command = kv.Command{Op: kv.Add, Key: "x", Value: 1}
			return
		}
		command = kv.Command{Op: kv.Operation(1 + h.workload.next()%3), Key: []string{"x", "y", "z"}[h.workload.next()%3], Value: int64(h.workload.next()%11) - 5}
	})
	if h.j.err != nil {
		return
	}
	if command.Op < kv.Put || command.Op > kv.Get {
		h.err = fmt.Errorf("invalid workload command")
		return
	}
	out, err := h.client.Start(command)
	if err != nil {
		h.err = err
		return
	}
	h.started++
	h.requestStarted = h.runner.Now()
	h.effects(vsr.ClientEndpoint, out)
}

func (h *host) deliver(id int, e sim.Event) {
	h.j.observe("boundary", e.Time, e)
	if h.j.err != nil || h.err != nil {
		return
	}
	if id < 3 && h.stopped[id] && (h.quorum == nil || e.Kind != sim.Message) {
		return
	}
	if e.Kind == sim.Timeout && e.Timer.Name == "clock" {
		if e.Timer != h.clocks[id] {
			return
		}
		h.effects(id, h.replicas[id].AdvanceTime())
		h.clocks[id] = h.runner.Arm(endpoint(id), "clock", clockInterval)
	} else {
		var m vsr.Message
		if e.Kind == sim.Timeout {
			if id < 3 && h.retryIncarnation[id] != h.incarnation[id] {
				return
			}
			m = h.callbacks[id]
		} else if err := json.Unmarshal(e.Payload, &m); err != nil {
			h.err = err
			return
		} else if h.quorum != nil && h.quorum.Eligible && h.blocked[m.From][m.To] {
			// The permanent partition also drops pre-transition packets still in flight.
			h.j.observe("blocked-delivery", e.Time, m)
			return
		}
		if id < 3 && h.stopped[id] {
			return
		}
		if id == vsr.ClientEndpoint {
			pending := h.client.Pending()
			h.effects(id, h.client.Step(m))
			if pending && !h.client.Pending() {
				h.err = h.checker.CheckReply(h.client.Response())
				if h.err != nil {
					return
				}
				h.completed++
				h.lastCompletion = h.runner.Now()
				if h.quorum != nil && h.quorum.Eligible {
					h.quorum.LongestWait = max(h.quorum.LongestWait, h.quorumWait())
					if h.completed == h.quorum.StartedAtTransition+1 {
						h.quorum.FirstFreshReply = h.runner.Now()
					}
				}
			}
		} else {
			h.effects(id, h.replicas[id].Step(m))
		}
	}
	h.check()
}

func (h *host) effects(owner int, effects []vsr.Effect) {
	for _, effect := range effects {
		if h.err != nil || h.j.err != nil {
			return
		}
		h.j.observe("effect", h.runner.Now(), effect)
		switch effect.Kind {
		case vsr.SendEffect:
			m := effect.Message
			var delays []sim.Time
			blocked := h.blocked[m.From][m.To]
			reliable := h.healed || h.quorum != nil && h.quorum.Eligible
			h.j.decide("network", h.runner.Now(), &delays, func() {
				if blocked || !reliable && h.network.next()%5 == 0 {
					return
				}
				delays = []sim.Time{healedDelay}
				if !reliable {
					delays[0] += sim.Time(h.network.next() % 12)
					if h.network.next()%5 == 0 {
						delays = append(delays, 1+sim.Time(h.network.next()%12))
					}
				}
			})
			if h.j.err != nil {
				return
			}
			if len(delays) > 2 || blocked && len(delays) != 0 ||
				reliable && !blocked && (len(delays) != 1 || delays[0] != healedDelay) {
				h.err = fmt.Errorf("invalid network decision")
				return
			}
			data, err := json.Marshal(m)
			if err != nil {
				h.err = err
				return
			}
			for _, delay := range delays {
				if delay < 1 || delay > 13 {
					h.err = fmt.Errorf("invalid network delay")
					return
				}
				h.runner.Send(endpoint(m.From), endpoint(m.To), data, delay)
			}
		case vsr.ArmEffect:
			delay := replicaRetry
			if owner == vsr.ClientEndpoint {
				delay = clientRetry
			} else {
				h.retryIncarnation[owner] = h.incarnation[owner]
			}
			h.callbacks[owner] = effect.Message
			h.retry[owner] = h.runner.Arm(endpoint(owner), "retry", delay)
		case vsr.CancelEffect:
			h.runner.Cancel(endpoint(owner), h.retry[owner])
		case vsr.ObserveEffect:
			h.err = h.checker.Observe(effect.Observation)
		default:
			h.err = fmt.Errorf("unknown effect %q", effect.Kind)
		}
	}
}

type faultChoice struct {
	Action  string
	Replica int
}

func (h *host) unavailable() int {
	for id, r := range h.replicas {
		if h.stopped[id] || r.Recovering() {
			return id
		}
	}
	return -1
}

func (h *host) fault() {
	var choice faultChoice
	h.j.decide("fault", h.runner.Now(), &choice, func() {
		id := h.unavailable()
		if id >= 0 {
			choice = faultChoice{"wait", id}
			if h.stopped[id] {
				choice.Action = "restart"
			}
			return
		}
		choice = faultChoice{[]string{"crash", "isolate", "connect"}[h.crashes.next()%3], int(h.crashes.next() % 3)}
	})
	if h.j.err != nil {
		return
	}
	if choice.Replica < 0 || choice.Replica >= 3 {
		h.err = fmt.Errorf("invalid fault replica")
		return
	}
	id := choice.Replica
	switch choice.Action {
	case "wait":
		if h.unavailable() != id {
			h.err = fmt.Errorf("invalid recovery wait")
		}
	case "restart":
		h.restart(id)
	case "crash":
		if h.unavailable() >= 0 {
			h.err = fmt.Errorf("failure budget exceeded")
			return
		}
		h.stop(id)
	case "isolate":
		h.blockReplica(id, true, true)
	case "connect":
		h.blocked = [4][4]bool{}
	default:
		h.err = fmt.Errorf("invalid fault action %q", choice.Action)
	}
	h.check()
}

func (h *host) blockReplica(id int, incoming, outgoing bool) {
	h.blocked = [4][4]bool{}
	for peer := range h.blocked {
		if peer != id {
			h.blocked[peer][id] = incoming
			h.blocked[id][peer] = outgoing
		}
	}
}

func (h *host) stop(id int) {
	h.stopped[id] = true
	h.incarnation[id]++
	h.runner.Cancel(endpoint(id), h.retry[id])
	h.runner.Cancel(endpoint(id), h.clocks[id])
}

func (h *host) restart(id int) {
	if !h.stopped[id] {
		h.err = fmt.Errorf("restart of running replica")
		return
	}
	var nonce uint64
	h.j.decide("nonce", h.runner.Now(), &nonce, func() {
		for nonce == 0 {
			nonce = h.nonces.next()
		}
	})
	if h.j.err != nil {
		return
	}
	if nonce == 0 || h.usedNonces[nonce] {
		h.err = fmt.Errorf("zero or reused recovery nonce")
		return
	}
	h.usedNonces[nonce] = true
	var out []vsr.Effect
	h.replicas[id], out = vsr.NewRecovering(id, nonce)
	h.checker.Restart(id)
	h.stopped[id] = false
	h.clocks[id] = h.runner.Arm(endpoint(id), "clock", clockInterval)
	h.effects(id, out)
}

func (h *host) heal() {
	h.healed, h.blocked = true, [4][4]bool{}
	h.j.observe("heal", h.runner.Now(), "bounded delay=1; restart stopped replicas")
	for id := range h.replicas {
		if h.stopped[id] {
			h.restart(id)
		}
	}
}

func (h *host) check() {
	for a, r := range h.replicas {
		s := r.State()
		if s.Applied > s.Commit || s.Commit > s.Op {
			h.err = fmt.Errorf("invalid counters r%d", a)
			return
		}
		for b := a + 1; b < 3; b++ {
			if s.Applied == h.replicas[b].State().Applied && !reflect.DeepEqual(r.Snapshot(), h.replicas[b].Snapshot()) {
				h.err = fmt.Errorf("state disagreement at equal applied positions r%d/r%d", a, b)
				return
			}
		}
	}
}

func (h *host) converged() bool {
	if h.client.Pending() || h.completed != h.config.Operations {
		return false
	}
	// All packets sent before healing or the last response have arrived. Clocks
	// remain armed forever; draining means quiescent work, not an empty heap.
	if h.runner.Now() < max(h.config.FaultUntil, h.lastCompletion)+14 {
		return false
	}
	for id, r := range h.replicas {
		if h.stopped[id] || r.Recovering() || r.ChangingView() || r.State().Applied != uint64(h.config.Operations) || r.State().View != h.replicas[0].State().View {
			return false
		}
	}
	return true
}
