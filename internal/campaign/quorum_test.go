package campaign

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

func TestStallProbeAndReplay(t *testing.T) {
	baseline, err := Run(StallProbe(1, true), testBuild)
	if err != nil || !baseline.Result.Complete || baseline.Result.Quorum.FirstFreshReply > 105 || baseline.Result.Quorum.FreshCompleted < 2 {
		t.Fatalf("baseline did not sustain progress: %+v, %v", baseline.Result, err)
	}
	trace, err := Run(StallProbe(1, false), testBuild)
	if err == nil || !strings.Contains(err.Error(), "liveness deadline") || trace.Result.Complete ||
		trace.Result.Time != 170 || trace.Result.Quorum.PendingRequest != 1 || trace.Result.Quorum.LongestWait != 70 {
		t.Fatalf("stall was not detected at the deadline: %+v, %v", trace.Result, err)
	}
	if trace.Result.States[0].View != 2 || trace.Result.States[1].View != 2 {
		t.Fatalf("healthy pair was not waiting in view 2: %v", trace.Result.States)
	}
	replayed, replayErr := Replay(trace, testBuild)
	if replayErr == nil || replayErr.Error() != err.Error() || !reflect.DeepEqual(replayed, trace) {
		t.Fatalf("stall did not replay: %v", replayErr)
	}
	for i := range trace.Entries {
		if trace.Entries[i].Kind == "quorum" {
			trace.Entries[i].Data = json.RawMessage(`{}`)
			_, err := Replay(trace, testBuild)
			if err == nil || !strings.Contains(err.Error(), "replay divergence") {
				t.Fatalf("replay accepted altered topology: %v", err)
			}
			return
		}
	}
	t.Fatal("missing topology in replay journal")
}

func TestQuorumFaultMatrix(t *testing.T) {
	for _, pair := range [][2]int{{0, 1}, {0, 2}, {1, 2}} {
		for _, fault := range []PermanentFault{Stopped, Isolated, SendOnly, ReceiveOnly} {
			t.Run(fmt.Sprintf("%v/%s", pair, fault), func(t *testing.T) {
				config := Default(1)
				config.FaultUntil = 0
				quorum := DefaultQuorum(pair, fault)
				config.Quorum = &quorum
				trace, err := Run(config, testBuild)
				if !trace.Result.Quorum.Eligible || (err != nil && (fault != SendOnly || !strings.Contains(err.Error(), "liveness deadline"))) {
					t.Fatalf("unexpected verdict: %+v, %v", trace.Result, err)
				}
				checkPermanentLinks(t, trace, 3-pair[0]-pair[1], fault)
				if err == nil {
					for _, id := range pair {
						if trace.Result.States[id].Applied < uint64(trace.Result.Completed) {
							t.Fatalf("replica %d has not caught up: %+v", id, trace.Result)
						}
					}
				}
			})
		}
	}
}

func TestQuorumDeadlines(t *testing.T) {
	control := StallProbe(1, true)
	control.Quorum.RequestLimit = 4
	trace, err := Run(control, testBuild)
	if err != nil || trace.Result.Quorum.FirstFreshReply != 104 || trace.Result.Quorum.LongestWait != 4 {
		t.Fatalf("reply on the deadline should count: %+v, %v", trace.Result, err)
	}

	config := Default(1)
	config.FaultUntil = 0
	quorum := DefaultQuorum([2]int{0, 1}, Stopped)
	quorum.Observe, quorum.CatchUpLimit = 2, 1
	config.Quorum = &quorum
	trace, err = Run(config, testBuild)
	if err == nil || !strings.Contains(err.Error(), "catch-up deadline") || trace.Result.Time != 3 {
		t.Fatalf("catch-up deadline did not stop the run: %+v, %v", trace.Result, err)
	}
}

func checkPermanentLinks(t *testing.T, trace *Trace, outside int, fault PermanentFault) {
	t.Helper()
	var send vsr.Message
	for _, entry := range trace.Entries {
		switch entry.Kind {
		case "fault", "nonce":
			t.Fatalf("fault state changed during quorum phase: %+v", entry)
		case "effect":
			var effect vsr.Effect
			if err := json.Unmarshal(entry.Data, &effect); err != nil {
				t.Fatal(err)
			}
			if fault == Stopped && effect.Kind == vsr.SendEffect && effect.Message.From == outside {
				t.Fatal("stopped replica sent a message")
			}
			send = effect.Message
		case "network":
			var delays []sim.Time
			if err := json.Unmarshal(entry.Data, &delays); err != nil {
				t.Fatal(err)
			}
			incoming := send.To == outside && fault != ReceiveOnly
			outgoing := send.From == outside && fault != SendOnly
			if incoming || outgoing {
				if len(delays) != 0 {
					t.Fatalf("blocked message scheduled: %+v", send)
				}
			} else if len(delays) != 1 || delays[0] != 1 {
				t.Fatalf("unblocked message lacks bounded delivery: %+v, %v", send, delays)
			}
		}
	}
}

func TestQuorumPrefixPendingAndRecoveryEligibility(t *testing.T) {
	var pending, ineligible, blockedDelivery bool
	for seed := uint64(1); seed <= 25 && !(pending && ineligible && blockedDelivery); seed++ {
		config := Default(seed)
		config.FaultUntil = 25
		quorum := DefaultQuorum([2]int{0, 1}, Isolated)
		config.Quorum = &quorum
		trace, err := Run(config, testBuild)
		if errors.Is(err, ErrQuorumUnavailable) {
			ineligible = true
			if !trace.Result.Quorum.Attempted || trace.Result.Quorum.Eligible || trace.Result.Complete {
				t.Fatalf("ineligible case reported as runnable: %+v", trace.Result)
			}
			continue
		}
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		q := trace.Result.Quorum
		if q.StartedAtTransition > q.CompletedAtTransition {
			pending = true
			if trace.Result.Completed != trace.Result.Started || q.FreshCompleted == 0 {
				t.Fatalf("pending or fresh work failed to finish: %+v", trace.Result)
			}
		}
		for _, entry := range trace.Entries {
			blockedDelivery = blockedDelivery || entry.Kind == "blocked-delivery"
		}
	}
	if !pending || !ineligible || !blockedDelivery {
		t.Fatalf("missing prefix coverage: pending=%t ineligible=%t blocked delivery=%t", pending, ineligible, blockedDelivery)
	}
}
