// Package campaign hosts the real VSR core in bounded, repeatable fault searches.
package campaign

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"go-vsr/internal/kv"
	"go-vsr/internal/sim"
	"go-vsr/internal/vsr"
)

const Version = "vsr-campaign-1"

const (
	clockInterval sim.Time = 1
	replicaRetry  sim.Time = 10
	clientRetry   sim.Time = 30
	healedDelay   sim.Time = 1
)

// Build identifies the code executing a trace, not just its source commit.
// The CLI supplies the executable digest so uncommitted edits are distinguished.
type Build struct {
	Revision, Toolchain, ExecutableSHA256 string
	Dirty                                 bool
}

type Seeds struct{ Workload, Network, Crashes, Nonces uint64 }

// stream is SplitMix64: a specified, local stream independent of Go's global RNG.
// Its output permutation also supplies nonrepeating recovery nonces (skip zero).
type stream uint64

func (s *stream) next() uint64 {
	*s += 0x9e3779b97f4a7c15
	z := uint64(*s)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func SeedsFrom(seed uint64) Seeds {
	s := stream(seed)
	return Seeds{s.next(), s.next(), s.next(), s.next()}
}

// Initial membership, view, empty stores, timer cadence and delay rules are
// fixed by Version. All variable workload and execution limits are explicit.
type Config struct {
	Seeds               Seeds
	Operations          int
	FaultUntil, MaxTime sim.Time
	MaxEvents           int
}

func Default(seed uint64) Config {
	return Config{Seeds: SeedsFrom(seed), Operations: 20, FaultUntil: 200, MaxTime: 5000, MaxEvents: 100000}
}

type Header struct {
	Version string
	Build   Build
	Config  Config
	Initial Initial
}

// Initial records the fixed topology and empty volatile starting state.
type Initial struct {
	ReplicaIDs                                            [3]int
	View                                                  uint64
	Primary, Client                                       int
	Empty                                                 bool
	ClockInterval, ReplicaRetry, ClientRetry, HealedDelay sim.Time
}

func initialState() Initial {
	return Initial{ReplicaIDs: [3]int{0, 1, 2}, Client: vsr.ClientEndpoint, Empty: true,
		ClockInterval: clockInterval, ReplicaRetry: replicaRetry, ClientRetry: clientRetry, HealedDelay: healedDelay}
}

// Entry stores one decision or one observed boundary in execution order.
// Data is owned JSON, so later protocol mutations cannot alter recorded history.
type Entry struct {
	Kind string
	Time sim.Time
	Data json.RawMessage
}

type Result struct {
	Complete                   bool
	Started, Completed, Events int
	Time                       sim.Time
	States                     [3]vsr.Counters
	Snapshots                  [3][]kv.Entry
	TraceSHA256                string
}

type Trace struct {
	Header  Header
	Entries []Entry
	Result  Result
	Failure string `json:",omitempty"`
}

type journal struct {
	trace    *Trace
	expected *Trace
	position int
	err      error
}

func (j *journal) observe(kind string, at sim.Time, value any) {
	if j.err != nil {
		return
	}
	data, err := json.Marshal(value)
	if err != nil {
		j.err = err
		return
	}
	e := Entry{kind, at, data}
	if j.expected != nil {
		matches := false
		if j.position < len(j.expected.Entries) {
			want := j.expected.Entries[j.position]
			var compact bytes.Buffer
			if json.Compact(&compact, want.Data) == nil {
				want.Data = compact.Bytes()
				matches = reflect.DeepEqual(e, want)
			}
		}
		if !matches {
			j.err = fmt.Errorf("replay divergence at entry %d time=%d kind=%s", j.position, at, kind)
			return
		}
	}
	j.trace.Entries = append(j.trace.Entries, e)
	j.position++
}

// decide consumes recorded choices during replay; it never consults randomness.
func (j *journal) decide(kind string, at sim.Time, target any, generate func()) {
	if j.err != nil {
		return
	}
	if j.expected == nil {
		generate()
	} else {
		if j.position >= len(j.expected.Entries) {
			j.err = fmt.Errorf("replay divergence: missing %s at entry %d", kind, j.position)
			return
		}
		e := j.expected.Entries[j.position]
		if e.Kind != kind || e.Time != at {
			j.err = fmt.Errorf("replay divergence: expected %s time=%d at entry %d", kind, at, j.position)
			return
		}
		if err := json.Unmarshal(e.Data, target); err != nil {
			j.err = fmt.Errorf("replay divergence: invalid %s: %w", kind, err)
			return
		}
	}
	j.observe(kind, at, target)
}

func Run(config Config, build Build) (*Trace, error) { return execute(config, build, nil) }

func Replay(trace *Trace, build Build) (*Trace, error) {
	if trace == nil || trace.Header.Version != Version || trace.Header.Build != build || trace.Header.Initial != initialState() {
		return nil, fmt.Errorf("replay divergence: incompatible simulator version or build")
	}
	return execute(trace.Header.Config, build, trace)
}
