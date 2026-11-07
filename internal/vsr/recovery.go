package vsr

// NewRecovering constructs an empty, non-voting replacement. The adapter must
// inject a nonzero nonce never reused for this replica, fence old local callbacks,
// and deliver the returned effects. Already-sent network packets may still arrive.
func NewRecovering(id int, nonce uint64) (*Replica, []Effect) {
	r := New(id)
	if nonce == 0 {
		panic("vsr: recovery nonce must be nonzero")
	}
	r.recovering, r.nonce = true, nonce
	return r, r.recoveryRetry()
}

func (r *Replica) Recovering() bool { return r.recovering }

func (r *Replica) recoveryRetry() []Effect {
	var out []Effect
	for id := 0; id < ReplicaCount; id++ {
		if id != r.id {
			out = append(out, Effect{Kind: SendEffect, Message: Message{
				Kind: Recovery, From: r.id, To: id, Nonce: r.nonce}})
		}
	}
	return append(out, Effect{Kind: ArmEffect, Message: Message{
		Kind: RecoveryRetry, From: r.id, To: r.id, Nonce: r.nonce}})
}

func (r *Replica) recoveryStep(m Message) ([]Effect, bool) {
	member := m.From >= 0 && m.From < ReplicaCount && m.From != r.id
	if m.Kind == Recovery {
		if !member || m.Nonce == 0 || r.recovering || r.changing {
			return nil, true
		}
		response := Message{Kind: RecoveryResponse, From: r.id, To: m.From, View: r.state.View, Nonce: m.Nonce}
		if r.id == int(r.state.View%ReplicaCount) {
			response = r.historyMessage(RecoveryResponse, m.From)
			response.Nonce = m.Nonce
		}
		return []Effect{{Kind: SendEffect, Message: response}}, true
	}
	if !r.recovering {
		return nil, m.Kind == RecoveryResponse || m.Kind == RecoveryRetry
	}
	if m.Kind == RecoveryRetry && m.From == r.id && m.Nonce == r.nonce {
		return r.recoveryRetry(), true
	}
	// Fence every other input, including view changes, requests, and old retries.
	if m.Kind != RecoveryResponse || !member || m.Nonce != r.nonce {
		return nil, true
	}
	if m.From == int(m.View%ReplicaCount) && !validHistory(m) {
		return nil, true
	}
	old := r.recoveryReports[m.From]
	if old != nil && (m.View < old.View || (m.View == old.View && (m.Op < old.Op || m.Commit < old.Commit))) {
		return nil, true
	}
	m = copyHistory(m)
	r.recoveryReports[m.From] = &m
	var count int
	var view uint64
	for _, report := range r.recoveryReports {
		if report != nil {
			count++
			view = max(view, report.View)
		}
	}
	primary := r.recoveryReports[view%ReplicaCount]
	if count < 2 || primary == nil || primary.View != view {
		return nil, true
	}
	var out []Effect
	// install rebuilds request records and replays the committed prefix into the
	// fresh application, including cached results. Only then allow participation.
	r.install(*primary, &out)
	r.recovering = false
	r.recoveryReports = [ReplicaCount]*Message{}
	return out, true
}
