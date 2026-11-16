package campaign

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"go-vsr/internal/vsr"
)

var testBuild = Build{Revision: "test", Toolchain: "test", ExecutableSHA256: "test"}

func TestSmallCampaign(t *testing.T) {
	coverage := make(map[string]int)
	events := 0
	for seed := uint64(1); seed <= 25; seed++ {
		trace, err := Run(Default(seed), testBuild)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !trace.Result.Complete || trace.Result.Completed != 20 {
			t.Fatal(trace.Result)
		}
		events += trace.Result.Events
		for _, e := range trace.Entries {
			switch e.Kind {
			case "fault":
				var choice faultChoice
				if err := json.Unmarshal(e.Data, &choice); err != nil {
					t.Fatal(err)
				}
				coverage[choice.Action]++
			case "effect":
				var effect vsr.Effect
				if err := json.Unmarshal(e.Data, &effect); err != nil {
					t.Fatal(err)
				}
				if effect.Kind == vsr.ObserveEffect {
					coverage[string(effect.Observation.Kind)]++
				}
			case "network":
				var delays []int64
				if err := json.Unmarshal(e.Data, &delays); err != nil {
					t.Fatal(err)
				}
				if len(delays) == 0 {
					coverage["drop"]++
				}
				if len(delays) == 2 {
					coverage["duplicate"]++
				}
			}
		}
	}
	for _, kind := range []string{"crash", "restart", "isolate", "drop", "duplicate", "committed", "applied", "selected", "installed"} {
		if coverage[kind] == 0 {
			t.Fatalf("campaign did not exercise %s", kind)
		}
	}
	t.Logf("25 seeds x 20 operations drained; events=%d coverage=%v", events, coverage)
}

func recorded(t *testing.T) *Trace {
	t.Helper()
	trace, err := Run(Default(7), testBuild)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the actual pretty-printed file representation.
	data, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var decoded Trace
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	return &decoded
}

func TestSeedRepeatAndRecordedReplay(t *testing.T) {
	a, err := Run(Default(7), testBuild)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Run(Default(7), testBuild)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("same seed differs", err)
	}
	replayed, err := Replay(recorded(t), testBuild)
	if err != nil || !reflect.DeepEqual(a, replayed) {
		t.Fatal("replay differs", err)
	}
}

func TestReplayConsumesRecordedDecisions(t *testing.T) {
	trace := recorded(t)
	// The seeds document the original run. Replay must consume the journal,
	// even if regenerating choices from these seeds would produce another run.
	trace.Header.Config.Seeds = Seeds{}
	replayed, err := Replay(trace, testBuild)
	if err != nil || !reflect.DeepEqual(replayed.Result, trace.Result) {
		t.Fatal(err)
	}
}

func TestCampaignFreshProcesses(t *testing.T) {
	if os.Getenv("VSR_M9_CHILD") == "1" {
		trace, err := Run(Default(7), testBuild)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("digest=%s completed=%d events=%d\n", trace.Result.TraceSHA256, trace.Result.Completed, trace.Result.Events)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func() string {
		command := exec.Command(executable, "-test.run=^TestCampaignFreshProcesses$", "-test.count=1")
		command.Env = append(os.Environ(), "VSR_M9_CHILD=1")
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	if a, b := run(), run(); a != b || !strings.Contains(a, "digest=") {
		t.Fatalf("fresh process traces differ: %s / %s", a, b)
	}
}

func TestReplayRejectsDivergence(t *testing.T) {
	for _, kind := range []string{"version", "build", "initial", "boundary", "effect", "truncated", "extra", "result", "nonce"} {
		t.Run(kind, func(t *testing.T) {
			trace := recorded(t)
			build := testBuild
			switch kind {
			case "version":
				trace.Header.Version = "future"
			case "build":
				build.ExecutableSHA256 = "different"
			case "initial":
				trace.Header.Initial.Primary = 1
			case "boundary", "effect":
				for i := range trace.Entries {
					if trace.Entries[i].Kind == kind {
						trace.Entries[i].Time++
						break
					}
				}
			case "truncated":
				trace.Entries = trace.Entries[:len(trace.Entries)/2]
			case "extra":
				trace.Entries = append(trace.Entries, Entry{Kind: "extra"})
			case "result":
				trace.Result.Completed++
			case "nonce":
				found := false
				for i := range trace.Entries {
					if trace.Entries[i].Kind == "nonce" {
						trace.Entries[i].Data = json.RawMessage("0")
						found = true
						break
					}
				}
				if !found {
					t.Fatal("fixture has no restart")
				}
			}
			_, err := Replay(trace, build)
			if err == nil || !(strings.Contains(err.Error(), "divergence") || strings.Contains(err.Error(), "nonce")) {
				t.Fatal(err)
			}
		})
	}
}

func TestIncompleteBudgetsAreFailures(t *testing.T) {
	for _, kind := range []string{"events", "time"} {
		t.Run(kind, func(t *testing.T) {
			config := Default(1)
			if kind == "events" {
				config.MaxEvents = 3
			} else {
				config.MaxTime = config.FaultUntil + 1
			}
			trace, err := Run(config, testBuild)
			if err == nil || trace == nil || trace.Result.Complete || !strings.Contains(trace.Failure, "incomplete") {
				t.Fatal(trace, err)
			}
			// A failed run remains replayable and reproduces the original failure.
			replayed, replayErr := Replay(trace, testBuild)
			if replayErr == nil || replayed.Failure != trace.Failure || strings.Contains(replayErr.Error(), "divergence") {
				t.Fatal(replayErr)
			}
		})
	}
}
