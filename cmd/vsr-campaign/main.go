// vsr-campaign records bounded fault campaigns and verifies exact replays.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"go-vsr/internal/campaign"
	"go-vsr/internal/sim"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildIdentity() (campaign.Build, error) {
	path, err := os.Executable()
	if err != nil {
		return campaign.Build{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return campaign.Build{}, err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return campaign.Build{}, err
	}
	b := campaign.Build{Revision: "unknown", Toolchain: runtime.Version(), ExecutableSHA256: fmt.Sprintf("%x", hash.Sum(nil))}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				b.Revision = setting.Value
			}
			if setting.Key == "vcs.modified" {
				b.Dirty = setting.Value == "true"
			}
		}
	}
	// go run does not always embed VCS settings. Only read the local repository;
	// no global Git settings are changed and no network access is performed.
	if b.Revision == "unknown" {
		cwd, err := os.Getwd()
		if err != nil {
			return b, err
		}
		git := func(args ...string) ([]byte, error) {
			return exec.Command("git", append([]string{"-c", "safe.directory=" + filepath.ToSlash(cwd)}, args...)...).Output()
		}
		out, err := git("rev-parse", "HEAD")
		if err != nil {
			return b, fmt.Errorf("cannot record project revision: %w; build inside the project", err)
		}
		b.Revision = strings.TrimSpace(string(out))
		out, err = git("status", "--porcelain")
		if err != nil {
			return b, err
		}
		b.Dirty = len(out) > 0
	}
	return b, nil
}

func run() error {
	seed := flag.Uint64("seed", 1, "first master seed (four independent streams are derived)")
	count := flag.Int("seeds", 1, "number of consecutive seeds")
	ops := flag.Int("operations", 20, "logical operations per seed in full-heal mode, or during the quorum fault prefix")
	faultUntil := flag.Int64("fault-until", 200, "virtual time when the random fault phase ends")
	maxTime := flag.Int64("max-time", 5000, "total virtual-time limit per seed")
	maxEvents := flag.Int("max-events", 100000, "total dequeued-event limit per seed")
	quorum := flag.String("quorum", "", "healthy pair (0,1; 0,2; 1,2) or matrix for all pairs and faults")
	permanent := flag.String("permanent-fault", "", "fault outside a named pair: stopped (default), isolated, send-only, receive-only")
	observe := flag.Int64("observe", 500, "quorum observation window in virtual ticks")
	requestLimit := flag.Int64("request-limit", 200, "maximum request wait in quorum mode in virtual ticks")
	catchUp := flag.Int64("catch-up-limit", 200, "virtual ticks allowed for the healthy pair to catch up after observation")
	probe := flag.String("probe", "", "fixed send-only stall reproduction: stall or baseline")
	out := flag.String("out", "campaign-traces", "directory for success and failure JSON journals")
	replay := flag.String("replay", "", "replay one journal with this exact executable")
	flag.Parse()
	build, err := buildIdentity()
	if err != nil {
		return err
	}
	if *replay != "" {
		f, err := os.Open(*replay)
		if err != nil {
			return err
		}
		defer f.Close()
		var trace campaign.Trace
		decoder := json.NewDecoder(f)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&trace); err != nil {
			return err
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return fmt.Errorf("invalid trailing trace data")
		}
		result, err := campaign.Replay(&trace, build)
		if err != nil {
			if result != nil && trace.Failure == err.Error() {
				fmt.Printf("replay identical: reproduced failure time=%d events=%d digest=%s\n", result.Result.Time, result.Result.Events, result.Result.TraceSHA256)
			}
			return err
		}
		fmt.Printf("replay identical: completed=%d events=%d time=%d digest=%s\n", result.Result.Completed, result.Result.Events, result.Result.Time, result.Result.TraceSHA256)
		return nil
	}
	if *count < 1 || uint64(*count-1) > ^uint64(0)-*seed {
		return fmt.Errorf("invalid seed range")
	}
	if *probe != "" && (*quorum != "" || *permanent != "" || (*probe != "stall" && *probe != "baseline")) {
		return fmt.Errorf("use -probe stall or -probe baseline by itself")
	}
	cases, err := quorumCases(*quorum, *permanent)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	totalEvents, passed, failed, ineligible := 0, 0, 0, 0
	for n := 0; n < *count; n++ {
		current := *seed + uint64(n)
		for _, scenario := range cases {
			config := campaign.Default(current)
			config.Operations, config.FaultUntil = *ops, sim.Time(*faultUntil)
			name := fmt.Sprintf("seed-%d", current)
			if *probe != "" {
				config = campaign.StallProbe(current, *probe == "baseline")
				name += "-probe-" + *probe
			} else if *quorum != "" {
				scenario.Observe, scenario.RequestLimit, scenario.CatchUpLimit = sim.Time(*observe), sim.Time(*requestLimit), sim.Time(*catchUp)
				config.Quorum = &scenario
				name += fmt.Sprintf("-pair-%d%d-%s", scenario.Pair[0], scenario.Pair[1], scenario.Fault)
			}
			config.MaxTime, config.MaxEvents = sim.Time(*maxTime), *maxEvents
			trace, runErr := campaign.Run(config, build)
			if trace == nil {
				return runErr
			}
			path := filepath.Join(*out, name+".json")
			if err := writeTrace(path, trace); err != nil {
				return err
			}
			totalEvents += trace.Result.Events
			if runErr != nil {
				failure := fmt.Errorf("%s: %w; replay with -replay \"%s\"", name, runErr, path)
				if *quorum != "matrix" {
					return failure
				}
				if errors.Is(runErr, campaign.ErrQuorumUnavailable) {
					ineligible++
				} else {
					failed++
				}
				fmt.Fprintln(os.Stderr, failure)
				continue
			}
			passed++
			if config.Quorum == nil {
				fmt.Printf("seed=%d completed=%d/%d events=%d time=%d digest=%s trace=%s\n", current, trace.Result.Completed, config.Operations, trace.Result.Events, trace.Result.Time, trace.Result.TraceSHA256, path)
			} else {
				fmt.Printf("%s completed=%d fresh=%d longest-wait=%d events=%d time=%d digest=%s trace=%s\n", name, trace.Result.Completed, trace.Result.Quorum.FreshCompleted, trace.Result.Quorum.LongestWait, trace.Result.Events, trace.Result.Time, trace.Result.TraceSHA256, path)
			}
		}
	}
	if *quorum != "" || *probe != "" {
		fmt.Printf("quorum campaign: passed=%d failed=%d ineligible=%d events=%d\n", passed, failed, ineligible, totalEvents)
		if failed+ineligible != 0 {
			return fmt.Errorf("quorum campaign did not pass every case")
		}
	} else {
		fmt.Printf("campaign passed: seeds=%d operations/seed=%d events=%d fault-until=%d max-time=%d max-events=%d\n", *count, *ops, totalEvents, *faultUntil, *maxTime, *maxEvents)
	}
	return nil
}

func quorumCases(pair, fault string) ([]campaign.QuorumConfig, error) {
	pairs := [][2]int{{0, 1}, {0, 2}, {1, 2}}
	faults := []campaign.PermanentFault{campaign.Stopped, campaign.Isolated, campaign.SendOnly, campaign.ReceiveOnly}
	switch pair {
	case "":
		if fault != "" {
			return nil, fmt.Errorf("-permanent-fault requires a named healthy pair")
		}
		return []campaign.QuorumConfig{{}}, nil
	case "matrix":
		if fault != "" {
			return nil, fmt.Errorf("matrix runs all permanent faults")
		}
	case "0,1", "0,2", "1,2":
		i := map[string]int{"0,1": 0, "0,2": 1, "1,2": 2}[pair]
		pairs = pairs[i : i+1]
		if fault == "" {
			fault = string(campaign.Stopped)
		}
		faults = []campaign.PermanentFault{campaign.PermanentFault(fault)}
	default:
		return nil, fmt.Errorf("unknown healthy pair %q", pair)
	}
	var cases []campaign.QuorumConfig
	for _, pair := range pairs {
		for _, fault := range faults {
			cases = append(cases, campaign.DefaultQuorum(pair, fault))
		}
	}
	return cases, nil
}

func writeTrace(path string, trace *campaign.Trace) error {
	data, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}
