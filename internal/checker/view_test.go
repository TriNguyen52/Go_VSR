package checker_test

import (
	"go-vsr/internal/checker"
	"go-vsr/internal/kv"
	"go-vsr/internal/vsr"
	"strings"
	"testing"
)

func TestViewCheckerRejectsBadSelectionAndOmission(t *testing.T) {
	entry := vsr.LogEntry{RequestID: 1, Command: kv.Command{Op: kv.Add, Key: "x", Value: 1}}
	for _, name := range []string{"duplicates", "wrong winner", "wrong commit", "omission"} {
		t.Run(name, func(t *testing.T) {
			var c checker.Checker
			o := vsr.Observation{Kind: vsr.Selected, Replica: 1, State: vsr.Counters{View: 2, Op: 1}, Entries: []vsr.LogEntry{entry}, Reports: []vsr.Message{
				{From: 0, View: 2, LastNormalView: 1, Op: 1, Entries: []vsr.LogEntry{entry}},
				{From: 1, View: 2},
			}}
			if err := c.Observe(o); err != nil {
				t.Fatal(err)
			}
			want := ""
			switch name {
			case "duplicates":
				o.Reports[1].From = 0
				want = "report"
			case "wrong winner":
				o.Entries = nil
				o.State.Op = 0
				want = "selection"
			case "wrong commit":
				o.State.Commit = 1
				want = "selection"
			case "omission":
				if err := c.Observe(vsr.Observation{Kind: vsr.Committed, Replica: 0, State: vsr.Counters{Op: 1, Commit: 1}, Index: 1, RequestID: 1, Command: entry.Command}); err != nil {
					t.Fatal(err)
				}
				o.Kind = vsr.Installed
				o.State.Op = 0
				o.Entries = nil
				want = "omits committed"
			}
			if err := c.Observe(o); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatal(err)
			}
		})
	}
}
