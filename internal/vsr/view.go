package vsr

// ChangingView reports whether normal participation is fenced.
func (r *Replica) ChangingView() bool     { return r.changing }
func (r *Replica) LastNormalView() uint64 { return r.lastNormalView }

func copyHistory(m Message) Message {
	m.Entries = append([]LogEntry(nil), m.Entries...)
	return m
}

func (r *Replica) historyMessage(kind Kind, to int) Message {
	m := Message{Kind: kind, From: r.id, To: to, View: r.state.View,
		LastNormalView: r.lastNormalView, Op: r.state.Op, Commit: r.state.Commit}
	for _, e := range r.log {
		m.Entries = append(m.Entries, LogEntry{e.requestID, e.command})
	}
	return m
}

func validHistory(m Message) bool {
	if m.Op != uint64(len(m.Entries)) || m.Commit > m.Op {
		return false
	}
	var previous uint64
	for _, e := range m.Entries {
		if e.RequestID <= previous {
			return false
		}
		previous = e.RequestID
	}
	return true
}

func (r *Replica) enterView(view uint64) {
	r.state.View, r.changing = view, true
	r.starts = [ReplicaCount]bool{}
	r.starts[r.id] = true
	r.reports = [ReplicaCount]*Message{}
	r.report = nil
	r.acks = [ReplicaCount]bool{}
	r.transferID, r.wantOp, r.wantCommit = 0, 0, 0
}

func (r *Replica) viewRetry() []Effect {
	var out []Effect
	for id := 0; id < ReplicaCount; id++ {
		if id != r.id {
			out = append(out, Effect{Kind: SendEffect, Message: Message{Kind: StartViewChange, From: r.id, To: id, View: r.state.View}})
		}
	}
	if r.report != nil {
		out = append(out, Effect{Kind: SendEffect, Message: copyHistory(*r.report)})
	}
	return append(out, Effect{Kind: ArmEffect, Message: Message{Kind: ViewRetry, From: r.id, To: r.id, View: r.state.View}})
}

// install preserves the executed prefix and its results, and replaces only the
// speculative suffix. It never replays into an already populated application.
func (r *Replica) install(m Message, out *[]Effect) {
	if m.Op < r.state.Commit {
		panic("vsr: installation loses committed history")
	}
	for i := uint64(0); i < r.state.Commit; i++ {
		if m.Entries[i].RequestID != r.log[i].requestID || m.Entries[i].Command != r.log[i].command {
			panic("vsr: installation conflicts with committed history")
		}
	}
	r.log = r.log[:r.state.Commit]
	for _, e := range m.Entries[r.state.Commit:] {
		r.log = append(r.log, entry{requestID: e.RequestID, command: e.Command})
	}
	r.records = make(map[uint64]uint64)
	for i, e := range r.log {
		r.records[e.requestID] = uint64(i + 1)
	}
	r.state.View, r.state.Op = m.View, m.Op
	r.changing = false
	r.lastNormalView = m.View
	r.transferID, r.wantOp, r.wantCommit = 0, 0, 0
	r.acks = [ReplicaCount]bool{}
	*out = append(*out, Effect{Kind: CancelEffect}, Effect{Kind: ObserveEffect, Observation: Observation{
		Kind: Installed, Replica: r.id, State: r.state, Entries: append([]LogEntry(nil), m.Entries...),
	}})
	r.advance(max(m.Commit, r.state.Commit), out)
}

func (r *Replica) viewStep(m Message) ([]Effect, bool) {
	if m.Kind == Suspect {
		if m.From != r.id || m.View <= r.state.View {
			return nil, true
		}
		r.enterView(m.View)
		return r.viewRetry(), true
	}
	if m.Kind == ViewRetry {
		if m.From == r.id && m.View == r.state.View && r.changing {
			return r.viewRetry(), true
		}
		return nil, true
	}
	member := m.From >= 0 && m.From < ReplicaCount && m.From != r.id
	control := m.Kind == StartViewChange || m.Kind == DoViewChange || m.Kind == StartView
	if !member {
		if control {
			return nil, true
		}
		return nil, false
	}
	// A normal primary helps a replica that missed START_VIEW, even if that
	// replica is still sending an older-view fetch or view-change retry.
	if !r.changing && r.id == int(r.state.View%ReplicaCount) && r.state.View > 0 && m.View <= r.state.View &&
		(m.Kind == StartViewChange || m.Kind == DoViewChange || (m.Kind == FetchState && m.View < r.state.View)) {
		return []Effect{{Kind: SendEffect, Message: r.historyMessage(StartView, m.From)}}, true
	}
	if m.Kind == StartView {
		if m.From != int(m.View%ReplicaCount) || m.View < r.state.View || !validHistory(m) {
			return nil, true
		}
		if m.View == r.state.View && !r.changing {
			return nil, true
		} // Never roll back an installed view.
		var out []Effect
		r.install(m, &out)
		if m.Op > m.Commit {
			out = append(out, Effect{Kind: SendEffect, Message: Message{Kind: PrepareOK, From: r.id, To: m.From, View: m.View, Op: m.Op}})
		}
		return out, true
	}
	if !control {
		// A newer normal-primary message is evidence of an installed view. Fence
		// the old view and solicit START_VIEW through the same retry protocol.
		if m.View > r.state.View && m.From == int(m.View%ReplicaCount) && (m.Kind == Prepare || m.Kind == Commit) {
			r.enterView(m.View)
			return r.viewRetry(), true
		}
		return nil, false
	}
	if m.View < r.state.View {
		return nil, true
	}
	if m.Kind == DoViewChange && (r.id != int(m.View%ReplicaCount) || !validHistory(m) || m.LastNormalView >= m.View) {
		return nil, true
	}
	var out []Effect
	if m.View > r.state.View {
		r.enterView(m.View)
		out = r.viewRetry()
	}
	if !r.changing {
		return nil, true
	}
	if m.Kind == StartViewChange {
		r.starts[m.From] = true
	}
	count := 0
	for _, yes := range r.starts {
		if yes {
			count++
		}
	}
	if count >= 2 && r.report == nil {
		report := r.historyMessage(DoViewChange, int(r.state.View%ReplicaCount))
		r.report = &report
		if report.To == r.id {
			r.reports[r.id] = &report
		} else {
			out = append(out, Effect{Kind: SendEffect, Message: copyHistory(report)})
		}
	}
	if m.Kind == DoViewChange && r.reports[m.From] == nil {
		m.Entries = append([]LogEntry(nil), m.Entries...)
		r.reports[m.From] = &m
	}
	if r.id != int(r.state.View%ReplicaCount) {
		return out, true
	}
	var reports []Message
	var best *Message
	var commit uint64
	for _, report := range r.reports {
		if report != nil {
			reports = append(reports, copyHistory(*report))
			commit = max(commit, report.Commit)
			if best == nil || report.LastNormalView > best.LastNormalView || (report.LastNormalView == best.LastNormalView && report.Op > best.Op) {
				best = report
			}
		}
	}
	if len(reports) < 2 {
		return out, true
	}
	selected := *best
	selected.Commit = commit
	if commit > selected.Op {
		panic("vsr: selected log shorter than reported commit")
	}
	out = append(out, Effect{Kind: ObserveEffect, Observation: Observation{Kind: Selected, Replica: r.id,
		State: Counters{View: r.state.View, Op: selected.Op, Commit: commit}, Entries: append([]LogEntry(nil), selected.Entries...), Reports: reports}})
	before := r.state.Commit
	r.install(selected, &out)
	for id := 0; id < ReplicaCount; id++ {
		if id != r.id {
			out = append(out, Effect{Kind: SendEffect, Message: r.historyMessage(StartView, id)})
		}
	}
	for op := before + 1; op <= r.state.Commit; op++ {
		e := r.log[op-1]
		out = append(out, Effect{Kind: SendEffect, Message: Message{Kind: Reply, From: r.id, To: ClientEndpoint, View: r.state.View, Op: op, Commit: r.state.Commit, RequestID: e.requestID, Result: e.result}})
	}
	if r.state.Op > r.state.Commit {
		out = append(out, Effect{Kind: ArmEffect, Message: Message{Kind: Retry, From: r.id, To: r.id, View: r.state.View, Op: r.state.Op}})
	}
	return out, true
}
