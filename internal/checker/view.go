package checker

import (
	"fmt"
	"reflect"

	"go-vsr/internal/vsr"
)

func (c *Checker) view(o vsr.Observation) error {
	if o.Replica < 0 || o.Replica >= vsr.ReplicaCount || o.State.Op != uint64(len(o.Entries)) || o.State.Commit > o.State.Op {
		return fmt.Errorf("invalid view observation")
	}
	if o.Kind == vsr.Selected {
		seen := [vsr.ReplicaCount]bool{}
		winner := -1
		var commit uint64
		for i, m := range o.Reports {
			if m.From < 0 || m.From >= vsr.ReplicaCount || seen[m.From] || m.View != o.State.View || m.LastNormalView >= m.View || m.Op != uint64(len(m.Entries)) || m.Commit > m.Op {
				return fmt.Errorf("invalid selection report")
			}
			seen[m.From] = true
			commit = max(commit, m.Commit)
			if winner < 0 || m.LastNormalView > o.Reports[winner].LastNormalView || (m.LastNormalView == o.Reports[winner].LastNormalView && m.Op > o.Reports[winner].Op) {
				winner = i
			}
		}
		if len(o.Reports) < 2 || o.State.Commit != commit || !reflect.DeepEqual(o.Entries, o.Reports[winner].Entries) {
			return fmt.Errorf("incorrect history selection")
		}
	}
	for index := uint64(1); index <= uint64(len(c.history)); index++ {
		command := c.history[index]
		if c.views[index] >= o.State.View {
			continue
		}
		if index > uint64(len(o.Entries)) || o.Entries[index-1].Command != command || o.Entries[index-1].RequestID != c.requests[index] {
			return fmt.Errorf("installation omits committed history at %d", index)
		}
	}
	return nil
}
