package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// NodesStore is the surface the node timing contract needs. store.Nodes
// satisfies it; it is narrowed deliberately so a backend that persists node
// rows can run the contract without claiming the whole store.Store.
type NodesStore interface {
	UpsertNode(ctx context.Context, rec *store.NodeRecord) error
	GetNode(ctx context.Context, id types.ExecutionID, name string) (*store.NodeRecord, error)
}

// NodesTimingContract pins the write-once semantics of started_at/finished_at
// that both the SQL and in-memory projection must share. They are what the
// editor times a node with, so "a retry moved the start line" and "a duplicate
// commit moved the end line" are the failures this catches: neither is visible
// to a test that only asserts the terminal row once.
func NodesTimingContract(t *testing.T, s NodesStore, execID types.ExecutionID) {
	t.Helper()
	ctx := context.Background()

	at := func(min int) *time.Time {
		v := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC).Add(time.Duration(min) * time.Minute)
		return &v
	}
	mustGet := func(name string) *store.NodeRecord {
		t.Helper()
		rec, err := s.GetNode(ctx, execID, name)
		if err != nil {
			t.Fatalf("GetNode(%q) error = %v", name, err)
		}
		return rec
	}

	// A retry refreshes the contract fields (Attempt below) but must keep the
	// FIRST lease's start time.
	if err := s.UpsertNode(ctx, &store.NodeRecord{
		ExecutionID: execID, NodeName: "retry", Status: types.NodeStatusRunning,
		Attempt: 1, StartedAt: at(0),
	}); err != nil {
		t.Fatalf("UpsertNode(first attempt) error = %v", err)
	}
	if got := mustGet("retry").StartedAt; got == nil || !got.Equal(*at(0)) {
		t.Fatalf("StartedAt after first attempt = %v, want %v", got, at(0))
	}
	if err := s.UpsertNode(ctx, &store.NodeRecord{
		ExecutionID: execID, NodeName: "retry", Status: types.NodeStatusRunning,
		Attempt: 2, StartedAt: at(1),
	}); err != nil {
		t.Fatalf("UpsertNode(retry) error = %v", err)
	}
	got := mustGet("retry")
	if got.Attempt != 2 {
		t.Fatalf("Attempt after retry = %d, want 2: the refresh contract broke", got.Attempt)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(*at(0)) {
		t.Fatalf("StartedAt after retry = %v, want the first lease's %v: a retry "+
			"must not move the start line", got.StartedAt, at(0))
	}

	// The end line is written by the terminal upsert and not restamped by a
	// later one (the duplicate-commit / cancel-retry shape).
	if err := s.UpsertNode(ctx, &store.NodeRecord{
		ExecutionID: execID, NodeName: "retry", Status: types.NodeStatusSuccess,
		Attempt: 2, FinishedAt: at(2),
	}); err != nil {
		t.Fatalf("UpsertNode(terminal) error = %v", err)
	}
	if got := mustGet("retry").FinishedAt; got == nil || !got.Equal(*at(2)) {
		t.Fatalf("FinishedAt after terminal upsert = %v, want %v", got, at(2))
	}
	if err := s.UpsertNode(ctx, &store.NodeRecord{
		ExecutionID: execID, NodeName: "retry", Status: types.NodeStatusSuccess,
		Attempt: 2, FinishedAt: at(3),
	}); err != nil {
		t.Fatalf("UpsertNode(duplicate terminal) error = %v", err)
	}
	if got := mustGet("retry").FinishedAt; got == nil || !got.Equal(*at(2)) {
		t.Fatalf("FinishedAt after duplicate terminal = %v, want the first %v: a "+
			"duplicate terminal write must not move the end line", got, at(2))
	}

	// A row may first be projected before its lease (started_at NULL) and learn
	// the start time on a later upsert. Without this the COALESCE could be
	// replaced by DO NOTHING and everything above still passed.
	if err := s.UpsertNode(ctx, &store.NodeRecord{
		ExecutionID: execID, NodeName: "late-start", Status: types.NodeStatusPending,
	}); err != nil {
		t.Fatalf("UpsertNode(late-start pending) error = %v", err)
	}
	if got := mustGet("late-start").StartedAt; got != nil {
		t.Fatalf("StartedAt before any lease = %v, want nil", got)
	}
	if err := s.UpsertNode(ctx, &store.NodeRecord{
		ExecutionID: execID, NodeName: "late-start", Status: types.NodeStatusRunning,
		StartedAt: at(4),
	}); err != nil {
		t.Fatalf("UpsertNode(late-start running) error = %v", err)
	}
	if got := mustGet("late-start").StartedAt; got == nil || !got.Equal(*at(4)) {
		t.Fatalf("StartedAt after first lease = %v, want %v", got, at(4))
	}
}
