package vsr

// CommitInterval is the number of virtual time units between COMMIT messages. The adapter
// calls AdvanceTime at a regular local cadence, serialized with Step, even when idle.
const CommitInterval uint64 = 5

// PrimaryTimeout is the initial silence/view-change deadline. Later views have
// longer deadlines to allow progress once communication becomes timely.
const PrimaryTimeout uint64 = 30

// AdvanceTime advances the replica's local clock by one virtual time unit.
// It is a local input, not a network message.
// Its effects use the same adapter as Step. The periodic clock must have a
// separate timer from ArmEffect retries, and must stop when the replica stops.
// Safety does not depend on timing; liveness needs a communicating quorum and
// delivery/processing delays eventually shorter than the timeout deadline.
func (r *Replica) AdvanceTime() []Effect {
	if r.recovering {
		return nil
	}
	r.elapsed++
	if !r.changing && r.id == int(r.state.View%ReplicaCount) {
		if r.elapsed < CommitInterval {
			return nil
		}
		r.elapsed = 0
		var out []Effect
		for id := 0; id < ReplicaCount; id++ {
			if id != r.id {
				out = append(out, Effect{Kind: SendEffect, Message: Message{
					Kind: Commit, From: r.id, To: id, View: r.state.View, Commit: r.state.Commit,
				}})
			}
		}
		return out
	}
	// Saturate instead of wrapping either the deadline or the view number.
	deadline := ^uint64(0)
	if r.state.View < deadline/PrimaryTimeout {
		deadline = (r.state.View + 1) * PrimaryTimeout
	}
	if r.elapsed < deadline {
		return nil
	}
	if r.state.View == ^uint64(0) {
		panic("vsr: view numbers exhausted")
	}
	r.enterView(r.state.View + 1)
	return r.viewRetry()
}
