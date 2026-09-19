package engine

import (
	"context"
	"errors"
	"testing"
)

// TestNotifySkip_EntryAdmissionReportsSkippedUnits covers the entry-admission
// skip site, the one reached from Engine.SeedExecutionFromEntry.
//
// It is a separate call path from advance and group commit, and it is the one
// an integrating host sees first: an entry-unit admission is where a Kafka
// batch is consumed, so a skip here is the shape where the offset advances and
// the downstream never sees the data.
func TestNotifySkip_EntryAdmissionReportsSkippedUnits(t *testing.T) {
	g := buildSingleGroupGraph(t)
	unitIdx := findNonGroupUnit(t, g)

	obs := &recordingSkipObserver{}
	eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{
		State:       AdmissionStateAccepted,
		ExecutionID: "exec-entry-skip",
		Skipped:     []SkippedUnit{{NodeName: "downstream", Count: 3}},
	}, nil, WithNodeSkipObserver(obs))

	if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		AdmissionKey: "ns/wf/v1/unit/topic/0/1-9",
		Graph:        g,
		EntryUnitIdx: unitIdx,
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry() error = %v", err)
	}

	skips := obs.take()
	if len(skips) != 1 {
		t.Fatalf("skips = %+v, want exactly one observation", skips)
	}
	got := skips[0]
	if got.flow != flowEntry {
		t.Fatalf("skip flow = %q, want %q", got.flow, flowEntry)
	}
	if got.node != "downstream" {
		t.Fatalf("skip node = %q, want %q", got.node, "downstream")
	}
	if got.count != 3 {
		t.Fatalf("skip count = %d, want 3 (the backend's count, passed through)", got.count)
	}
}

// TestNotifySkip_EntryAdmissionDuplicateReportsNothing pins the gate on the
// entry path.
//
// A duplicate admission is an idempotent retry that applied no transition, and
// a conflict or a transport error applied nothing at all. None of them skipped
// anything, so counting their response's (empty) skip list would be harmless
// while counting a non-empty one would be a double-count of a transition that
// happened once — and on the at-least-once delivery this path is built for,
// duplicates are the expected case rather than an edge one.
func TestNotifySkip_EntryAdmissionDuplicateReportsNothing(t *testing.T) {
	cases := []struct {
		name string
		resp SeedExecutionFromEntryResponse
		err  error
	}{
		{
			name: "duplicate retry",
			resp: SeedExecutionFromEntryResponse{
				State:       AdmissionStateAccepted,
				ExecutionID: "exec-entry-dup",
				Duplicate:   true,
				// Deliberately non-empty: a backend that reports the original
				// transition's skips on the retry must not be counted twice.
				Skipped: []SkippedUnit{{NodeName: "downstream", Count: 3}},
			},
		},
		{
			name: "conflict",
			resp: SeedExecutionFromEntryResponse{
				State:       AdmissionStateConflict,
				ExecutionID: "exec-entry-conflict",
				Skipped:     []SkippedUnit{{NodeName: "downstream", Count: 3}},
			},
		},
		{
			name: "transport error",
			resp: SeedExecutionFromEntryResponse{
				State:   AdmissionStateAccepted,
				Skipped: []SkippedUnit{{NodeName: "downstream", Count: 3}},
			},
			err: errors.New("seed failed"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := buildSingleGroupGraph(t)
			unitIdx := findNonGroupUnit(t, g)

			obs := &recordingSkipObserver{}
			eng := newEntryAdmissionEngine(t, tc.resp, tc.err, WithNodeSkipObserver(obs))

			if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
				AdmissionKey: "ns/wf/v1/unit/topic/0/1-9",
				Graph:        g,
				EntryUnitIdx: unitIdx,
			}); err != nil && tc.err == nil {
				t.Fatalf("SeedExecutionFromEntry() error = %v", err)
			}

			if len(obs.take()) != 0 {
				t.Fatalf("skips = %+v, want none: %s applied no transition", obs.take(), tc.name)
			}
		})
	}
}
