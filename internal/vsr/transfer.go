package vsr

// fetch remembers the largest gap seen. One request ID survives retries; a new
// catch-up gets a new ID. Normal traffic may fill the gap while this is pending.
func (r *Replica) fetch(op, commit uint64) []Effect {
	r.wantOp = max(r.wantOp, op)
	r.wantCommit = max(r.wantCommit, commit)
	if r.transferID != 0 {
		return nil
	}
	if r.transferSequence == ^uint64(0) {
		panic("vsr: transfer IDs exhausted")
	}
	r.transferSequence++
	r.transferID = r.transferSequence
	return r.fetchEffects()
}

func (r *Replica) fetchEffects() []Effect {
	return []Effect{
		{Kind: SendEffect, Message: Message{Kind: FetchState, From: r.id,
			To: int(r.state.View % ReplicaCount), View: r.state.View,
			TransferID: r.transferID, Commit: r.state.Commit}},
		{Kind: ArmEffect, Message: Message{Kind: TransferRetry, From: r.id,
			To: r.id, View: r.state.View, TransferID: r.transferID}},
	}
}

func (r *Replica) finishTransfer(effects *[]Effect) {
	if r.transferID != 0 && r.state.Op >= r.wantOp && r.state.Commit >= r.wantCommit {
		r.transferID, r.wantOp, r.wantCommit = 0, 0, 0
		*effects = append(*effects, Effect{Kind: CancelEffect})
	}
}

// transfer implements the project's full-log, current-primary-only wire format.
// Step has already checked destination and view. No newer-view installation or
// crash recovery is implied by receiving STATE.
func (r *Replica) transfer(m Message) []Effect {
	primary := int(r.state.View % ReplicaCount)
	switch m.Kind {
	case TransferRetry:
		if r.id != primary && m.From == r.id && r.transferID != 0 && m.TransferID == r.transferID {
			return r.fetchEffects()
		}
	case FetchState:
		if r.id != primary || m.From < 0 || m.From >= ReplicaCount || m.From == primary ||
			m.TransferID == 0 || m.Commit > r.state.Commit {
			return nil
		}
		entries := make([]LogEntry, len(r.log))
		for i, e := range r.log {
			entries[i] = LogEntry{RequestID: e.requestID, Command: e.command}
		}
		return []Effect{{Kind: SendEffect, Message: Message{Kind: StateTransfer,
			From: r.id, To: m.From, View: r.state.View, TransferID: m.TransferID,
			Op: r.state.Op, Commit: r.state.Commit, Entries: entries}}}
	case StateTransfer:
		if r.id == primary || m.From != primary || r.transferID == 0 || m.TransferID != r.transferID ||
			m.Op != uint64(len(m.Entries)) || m.Commit > m.Op ||
			m.Op < max(r.state.Op, r.wantOp) || m.Commit < max(r.state.Commit, r.wantCommit) {
			return nil
		}
		// Validate the entire response before changing anything. In a single view
		// even the prepared suffix is immutable, so preserve ALL local entries.
		var previous uint64
		for i, e := range m.Entries {
			if e.RequestID <= previous {
				return nil
			}
			previous = e.RequestID
			if i < len(r.log) && (e.RequestID != r.log[i].requestID || e.Command != r.log[i].command) {
				panic("vsr: same-view state transfer conflicts with local history")
			}
		}
		for _, e := range m.Entries[len(r.log):] {
			r.log = append(r.log, entry{command: e.Command, requestID: e.RequestID})
			r.records[e.RequestID] = uint64(len(r.log))
		}
		r.state.Op = m.Op
		var effects []Effect
		r.advance(m.Commit, &effects)
		r.finishTransfer(&effects)
		if r.state.Op > r.state.Commit {
			effects = append(effects, Effect{Kind: SendEffect, Message: Message{Kind: PrepareOK,
				From: r.id, To: primary, View: r.state.View, Op: r.state.Op}})
		}
		return effects
	}
	return nil
}
