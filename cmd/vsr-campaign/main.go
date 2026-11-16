// vsr-campaign records bounded fault campaigns and verifies exact replays.
package main

import (
	"crypto/sha256"
	"encoding/json"
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
	ops := flag.Int("operations", 20, "logical operations per seed; one client")
	faultUntil := flag.Int64("fault-until", 200, "virtual time when faults stop and all links heal")
	maxTime := flag.Int64("max-time", 5000, "total virtual-time limit per seed")
	maxEvents := flag.Int("max-events", 100000, "total dequeued-event limit per seed")
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
			return err
		}
		fmt.Printf("replay identical: completed=%d events=%d time=%d digest=%s\n", result.Result.Completed, result.Result.Events, result.Result.Time, result.Result.TraceSHA256)
		return nil
	}
	if *count < 1 || uint64(*count-1) > ^uint64(0)-*seed {
		return fmt.Errorf("invalid seed range")
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	totalEvents := 0
	for n := 0; n < *count; n++ {
		current := *seed + uint64(n)
		config := campaign.Default(current)
		config.Operations, config.FaultUntil, config.MaxTime, config.MaxEvents = *ops, sim.Time(*faultUntil), sim.Time(*maxTime), *maxEvents
		trace, runErr := campaign.Run(config, build)
		if trace == nil {
			return runErr
		}
		path := filepath.Join(*out, fmt.Sprintf("seed-%d.json", current))
		data, err := json.MarshalIndent(trace, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			return err
		}
		if runErr != nil {
			return fmt.Errorf("seed=%d: %w; replay: %s -replay %s", current, runErr, os.Args[0], path)
		}
		totalEvents += trace.Result.Events
		fmt.Printf("seed=%d completed=%d/%d events=%d time=%d digest=%s trace=%s\n", current, trace.Result.Completed, config.Operations, trace.Result.Events, trace.Result.Time, trace.Result.TraceSHA256, path)
	}
	fmt.Printf("campaign passed: seeds=%d operations/seed=%d events=%d fault-until=%d max-time=%d max-events=%d\n", *count, *ops, totalEvents, *faultUntil, *maxTime, *maxEvents)
	return nil
}
