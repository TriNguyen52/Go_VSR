package vsr_test

import (
	"fmt"
	"os"
	"testing"

	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

// These demonstrations use the real protocol, client, and independent history
// checker. Each subtest creates a fresh cluster. No sockets or wall-clock sleeps.
func TestNetworkLab(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		labState(t, c, "SETUP: no faults; predict when Add becomes safe to apply")
		c.start(t, commands[1])
		labConverge(t, c, 1)
		labState(t, c, "RESULT: one Add; all replicas have x=1 in view 0")
	})
	t.Run("duplicates", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		c.deliveries = func(m vsr.Message) []sim.Time { return []sim.Time{1, 2} }
		labState(t, c, "SETUP: deliver every packet twice; predict application count")
		c.start(t, commands[1])
		labConverge(t, c, 1)
		until(t, c, func() bool { return c.runner.Now() >= 15 })
		labConverge(t, c, 1)
		labState(t, c, "RESULT: duplicates delivered; each replica applied Add exactly once")
	})
	t.Run("reordering", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		c.deliveries = func(m vsr.Message) []sim.Time {
			if m.To == 2 && ((m.Kind == vsr.Prepare && m.Op == 1) || (m.Kind == vsr.Commit && m.Commit == 1)) {
				return []sim.Time{20}
			}
			return []sim.Time{1}
		}
		c.start(t, commands[1])
		until(t, c, func() bool { return !c.client.Pending() })
		c.start(t, commands[1])
		until(t, c, func() bool { return !c.client.Pending() })
		if c.replicas[2].State().Op != 0 {
			t.Fatal("fixture did not expose a gap")
		}
		labState(t, c, "GAP: r2 received op 2 first; it must fetch op 1 before acknowledging")
		labConverge(t, c, 2)
		until(t, c, func() bool { return c.runner.Now() >= 30 })
		labConverge(t, c, 2)
		labState(t, c, "RESULT: two Adds applied in order; delayed originals cannot repeat them")
	})
	for _, kind := range []vsr.Kind{vsr.Request, vsr.Prepare, vsr.PrepareOK, vsr.Reply} {
		t.Run("loss/"+string(kind), func(t *testing.T) {
			c := lossScenario(t, kind)
			labState(t, c, "first packet on each matching link lost; retry completes Add exactly once")
			labTrace(t, c)
		})
	}
	for _, kind := range []vsr.Kind{"", vsr.FetchState, vsr.StateTransfer} {
		name := string(kind)
		if name == "" {
			name = "reorder-duplicate"
		}
		t.Run("gap/"+name, func(t *testing.T) {
			c := gapScenario(t, kind)
			labState(t, c, "op 2 overtakes op 1; state transfer repairs gap; late duplicates do not reapply")
			labTrace(t, c)
		})
	}
	for _, kind := range []vsr.Kind{vsr.StartViewChange, vsr.DoViewChange, vsr.StartView} {
		t.Run("election-loss/"+string(kind), func(t *testing.T) {
			c := viewScenario(t, kind, true)
			labState(t, c, "explicit suspicion; lagging new primary preserves history despite election packet loss")
			labTrace(t, c)
		})
	}
	t.Run("slow-backup", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		c.deliveries = func(m vsr.Message) []sim.Time {
			if m.To == 2 {
				return []sim.Time{20}
			}
			return []sim.Time{1}
		}
		c.start(t, commands[1])
		until(t, c, func() bool { return !c.client.Pending() })
		labState(t, c, "client completed while slow backup still has no entry")
		if c.replicas[2].State().Op != 0 {
			t.Fatal("slow backup fixture did not lag")
		}
		labConverge(t, c, 1)
		for _, r := range c.replicas {
			if r.State().View != 0 {
				t.Fatal("unexpected election")
			}
		}
		labState(t, c, "slow backup caught up in view 0")
	})
	t.Run("backup-stop", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		c.stopped[2] = true
		c.start(t, commands[1])
		labConverge(t, c, 1)
		if c.replicas[0].State().View != 0 || c.replicas[2].State().Applied != 0 {
			t.Fatal("unexpected state")
		}
		labState(t, c, "r2 stopped; r0 and r1 complete without changing primary")
	})
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("primary-stop/committed=%v", committed), func(t *testing.T) {
			c := automaticCluster(t)
			defer labTrace(t, c)
			c.drop = func(m vsr.Message) bool { return m.Kind == vsr.Reply || (!committed && m.Kind == vsr.PrepareOK) }
			c.start(t, commands[1])
			until(t, c, func() bool {
				return c.replicas[1].State().Op == 1 && c.replicas[2].State().Op == 1 && (!committed || c.replicas[0].State().Commit == 1)
			})
			c.stopped[0] = true
			labState(t, c, "STOP r0; client has no reply and cannot know whether Add committed")
			c.drop = nil
			labConverge(t, c, 1)
			if c.client.Response().View == 0 {
				t.Fatal("did not fail over")
			}
			labState(t, c, "automatic election; same request returns 1 once")
			c.start(t, commands[1])
			labConverge(t, c, 2)
			labState(t, c, "next distinct Add returns 2")
		})
	}
	t.Run("partition-heal", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		// Client can reach every replica; only inter-replica links are cut.
		c.drop = func(m vsr.Message) bool { return m.From < 3 && m.To < 3 && (m.From == 0 || m.To == 0) }
		c.start(t, commands[1])
		until(t, c, func() bool {
			return !c.client.Pending() && c.replicas[1].State().Applied == 1 && c.replicas[2].State().Applied == 1
		})
		if c.replicas[0].State().Commit != 0 || c.replicas[0].State().Applied != 0 {
			t.Fatal("minority executed")
		}
		labState(t, c, "majority completes; isolated old primary has only a speculative entry")
		c.drop = nil
		labConverge(t, c, 1)
		labState(t, c, "HEAL links; old primary installs newer view and catches up")
	})
	t.Run("no-quorum", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		c.drop = func(m vsr.Message) bool { return m.From < 3 && m.To < 3 }
		c.start(t, commands[1])
		until(t, c, func() bool { return c.runner.Now() >= 120 })
		if !c.client.Pending() {
			t.Fatal("client completed without quorum")
		}
		for _, r := range c.replicas {
			if r.State().Commit != 0 || r.State().Applied != 0 {
				t.Fatal("executed without quorum")
			}
		}
		labState(t, c, "EXPECTED STALL: all replicas isolated for 120 ticks; no commit or application")
		c.drop = nil
		labConverge(t, c, 1)
		labState(t, c, "HEAL links; pending request completes")
	})
	t.Run("latency-spike", func(t *testing.T) {
		c := automaticCluster(t)
		defer labTrace(t, c)
		c.deliveries = func(m vsr.Message) []sim.Time {
			if m.From < 3 && m.To < 3 {
				return []sim.Time{80}
			}
			return []sim.Time{1}
		}
		c.start(t, commands[1])
		until(t, c, func() bool { return c.runner.Now() >= 40 })
		if !c.client.Pending() || c.replicas[1].State().View == 0 {
			t.Fatal("latency did not trigger suspicion")
		}
		labState(t, c, "all nodes alive; 80-tick latency exceeds initial 30-tick deadline and triggers suspicion")
		c.deliveries = nil // Already queued slow packets retain their delivery times.
		labConverge(t, c, 1)
		until(t, c, func() bool { return c.runner.Now() >= 140 })
		labConverge(t, c, 1)
		labState(t, c, "network timely again; delayed old packets drained without repeating Add")
	})
}

func labConverge(t *testing.T, c *retryCluster, n uint64) {
	t.Helper()
	until(t, c, func() bool {
		if c.client.Pending() {
			return false
		}
		for id, r := range c.replicas {
			if !c.stopped[id] && (r.ChangingView() || r.State().Applied != n || r.State().View != c.client.Response().View) {
				return false
			}
		}
		return true
	})
	if c.client.Response().Result != (kv.Result{Value: int64(n)}) {
		t.Fatal(c.client.Response())
	}
	for id, r := range c.replicas {
		if !c.stopped[id] && (c.applies[id] != int(n) || len(r.Snapshot()) != 1 || r.Snapshot()[0].Value != int64(n)) {
			t.Fatal("unexpected application", id, r.Snapshot(), c.applies)
		}
	}
}

func labState(t *testing.T, c *retryCluster, label string) {
	t.Helper()
	t.Logf("t=%d %s; client pending=%v", c.runner.Now(), label, c.client.Pending())
	for id, r := range c.replicas {
		s := r.State()
		t.Logf("  r%d stopped=%v changing=%v view=%d op=%d commit=%d applied=%d kv=%v", id, c.stopped[id], r.ChangingView(), s.View, s.Op, s.Commit, s.Applied, r.Snapshot())
	}
}

func labTrace(t *testing.T, c *retryCluster) {
	t.Helper()
	if os.Getenv("VSR_FAULT_TRACE") == "1" || t.Failed() {
		t.Log("\n" + c.trace.String())
	}
}
