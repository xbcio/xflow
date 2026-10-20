package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
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

// NodesContract exercises store.Nodes against one backend.
//
// Every case uses its own execution id, so a persistent backend may hold rows
// from earlier cases or runs without contaminating the assertions. The one
// cross-execution query, ListExpiredSuspensions, is therefore asserted by
// filtering the returned rows down to this contract's execution ids rather
// than by expecting an exact table state; that keeps the contract runnable
// against a shared database while still pinning the only interesting property,
// which is which suspensions are considered expired.
func NodesContract(t *testing.T, s store.Nodes) {
	t.Helper()
	ctx := context.Background()

	t.Run("UpsertGetRoundTrip", func(t *testing.T) {
		id := types.ExecutionID("nodes-contract/round-trip")
		timeout := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
		rec := &store.NodeRecord{
			ExecutionID:  id,
			NodeName:     "http",
			NodeType:     "http.request",
			Status:       types.NodeStatusSuspended,
			LeaseID:      "lease-round-trip",
			LeaseToken:   "token-round-trip",
			Attempt:      2,
			Output:       []byte(`{"ok":true}`),
			Port:         "main",
			SignalName:   "approve",
			SignalConfig: []byte(`{"mode":"any"}`),
			Timeout:      &timeout,
		}
		if err := s.UpsertNode(ctx, rec); err != nil {
			t.Fatalf("UpsertNode() error = %v", err)
		}
		got, err := s.GetNode(ctx, id, "http")
		if err != nil {
			t.Fatalf("GetNode() error = %v", err)
		}
		if got == nil {
			t.Fatal("GetNode() = nil after UpsertNode")
		}
		if got.ExecutionID != id || got.NodeName != "http" {
			t.Fatalf("identity = (%q, %q), want (%q, %q)", got.ExecutionID, got.NodeName, id, "http")
		}
		if got.NodeType != "http.request" {
			t.Fatalf("NodeType = %q, want http.request", got.NodeType)
		}
		if got.Status != types.NodeStatusSuspended {
			t.Fatalf("Status = %q, want %q", got.Status, types.NodeStatusSuspended)
		}
		if got.LeaseID != "lease-round-trip" || got.LeaseToken != "token-round-trip" {
			t.Fatalf("lease = (%q, %q), want (lease-round-trip, token-round-trip)", got.LeaseID, got.LeaseToken)
		}
		if got.Attempt != 2 {
			t.Fatalf("Attempt = %d, want 2", got.Attempt)
		}
		if !jsonSemanticallyEqual(got.Output, []byte(`{"ok":true}`)) {
			t.Fatalf("Output = %s, want {\"ok\":true}", got.Output)
		}
		if got.Port != "main" {
			t.Fatalf("Port = %q, want main", got.Port)
		}
		if got.SignalName != "approve" {
			t.Fatalf("SignalName = %q, want approve", got.SignalName)
		}
		if !jsonSemanticallyEqual(got.SignalConfig, []byte(`{"mode":"any"}`)) {
			t.Fatalf("SignalConfig = %s, want {\"mode\":\"any\"}", got.SignalConfig)
		}
		if got.Timeout == nil || !got.Timeout.Equal(timeout) {
			t.Fatalf("Timeout = %v, want %v", got.Timeout, timeout)
		}
		if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
			t.Fatalf("timestamps not populated: created=%v updated=%v", got.CreatedAt, got.UpdatedAt)
		}
	})

	// A node is upserted on every lifecycle transition (claim, commit, retry,
	// suspend, resume). The conflict key is (execution_id, node_name), and the
	// refreshed field set is store.UpsertNodeUpdateFields; a backend that
	// refreshed only status would serve a stale lease/attempt/output to every
	// reader. The per-backend reflection tests bind each implementation to the
	// exact list; this case pins the shared lifecycle contract they must agree
	// on.
	t.Run("UpsertRefreshesTheLifecycleFields", func(t *testing.T) {
		id := types.ExecutionID("nodes-contract/upsert")
		firstTimeout := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
		if err := s.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID:  id,
			NodeName:     "n1",
			NodeType:     "http.request",
			Status:       types.NodeStatusRunning,
			LeaseID:      "lease-1",
			LeaseToken:   "token-1",
			Attempt:      1,
			Output:       []byte(`{"v":1}`),
			Port:         "main",
			SignalName:   "approve",
			SignalConfig: []byte(`{"n":1}`),
			Timeout:      &firstTimeout,
		}); err != nil {
			t.Fatalf("first UpsertNode() error = %v", err)
		}
		first, err := s.GetNode(ctx, id, "n1")
		if err != nil {
			t.Fatalf("GetNode() after first upsert: %v", err)
		}

		if err := s.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID: id,
			NodeName:    "n1",
			NodeType:    "script.python",
			Status:      types.NodeStatusSuccess,
			Attempt:     3,
			Output:      []byte(`{"v":2}`),
			Port:        "error",
		}); err != nil {
			t.Fatalf("second UpsertNode() error = %v", err)
		}
		got, err := s.GetNode(ctx, id, "n1")
		if err != nil {
			t.Fatalf("GetNode() after second upsert: %v", err)
		}
		if got.NodeType != "script.python" {
			t.Fatalf("NodeType after upsert = %q, want script.python", got.NodeType)
		}
		if got.Status != types.NodeStatusSuccess {
			t.Fatalf("Status after upsert = %q, want %q", got.Status, types.NodeStatusSuccess)
		}
		if got.LeaseID != "" || got.LeaseToken != "" {
			t.Fatalf("lease after upsert = (%q, %q), want both cleared", got.LeaseID, got.LeaseToken)
		}
		if got.Attempt != 3 {
			t.Fatalf("Attempt after upsert = %d, want 3", got.Attempt)
		}
		if !jsonSemanticallyEqual(got.Output, []byte(`{"v":2}`)) {
			t.Fatalf("Output after upsert = %s, want {\"v\":2}", got.Output)
		}
		if got.Port != "error" {
			t.Fatalf("Port after upsert = %q, want error", got.Port)
		}
		if got.SignalName != "" || len(got.SignalConfig) != 0 {
			t.Fatalf("signal fields after upsert = (%q, %s), want both cleared", got.SignalName, got.SignalConfig)
		}
		if got.Timeout != nil {
			t.Fatalf("Timeout after upsert = %v, want nil", got.Timeout)
		}
		// created_at is the row's origin and must survive a re-upsert; the two
		// backends maintain it differently (the memstore preserves the stored
		// value, SQL excludes it from the ON CONFLICT update set), so the
		// observable result is what is pinned.
		if !got.CreatedAt.Equal(first.CreatedAt) {
			t.Fatalf("CreatedAt after upsert = %v, want the original %v", got.CreatedAt, first.CreatedAt)
		}
		if got.UpdatedAt.Before(first.UpdatedAt) {
			t.Fatalf("UpdatedAt moved backwards: %v -> %v", first.UpdatedAt, got.UpdatedAt)
		}
	})

	t.Run("GetNodeMissingIsNotFound", func(t *testing.T) {
		got, err := s.GetNode(ctx, types.ExecutionID("nodes-contract/missing"), "nope")
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetNode(missing) error = %v, want store.ErrNotFound", err)
		}
		if got != nil {
			t.Fatalf("GetNode(missing) = %+v, want nil", got)
		}
	})

	t.Run("ListNodesIsScopedToExecution", func(t *testing.T) {
		execA := types.ExecutionID("nodes-contract/list-a")
		execB := types.ExecutionID("nodes-contract/list-b")
		for _, rec := range []*store.NodeRecord{
			{ExecutionID: execA, NodeName: "a1", Status: types.NodeStatusRunning},
			{ExecutionID: execA, NodeName: "a2", Status: types.NodeStatusSuccess},
			{ExecutionID: execB, NodeName: "b1", Status: types.NodeStatusRunning},
		} {
			if err := s.UpsertNode(ctx, rec); err != nil {
				t.Fatalf("UpsertNode(%q/%q): %v", rec.ExecutionID, rec.NodeName, err)
			}
		}
		list := func(id types.ExecutionID) []string {
			t.Helper()
			recs, err := s.ListNodes(ctx, id, store.ListOptions{})
			if err != nil {
				t.Fatalf("ListNodes(%q) error = %v", id, err)
			}
			return nodeNames(recs)
		}
		if got := list(execA); !reflect.DeepEqual(got, []string{"a1", "a2"}) {
			t.Fatalf("ListNodes(execA) = %v, want [a1 a2]", got)
		}
		if got := list(execB); !reflect.DeepEqual(got, []string{"b1"}) {
			t.Fatalf("ListNodes(execB) = %v, want [b1]", got)
		}
		if got := list(types.ExecutionID("nodes-contract/list-none")); len(got) != 0 {
			t.Fatalf("ListNodes(absent) = %v, want none", got)
		}
	})

	t.Run("ListSuspendedBySignal", func(t *testing.T) {
		id := types.ExecutionID("nodes-contract/suspended")
		other := types.ExecutionID("nodes-contract/suspended-other")
		for _, rec := range []*store.NodeRecord{
			{ExecutionID: id, NodeName: "approve", Status: types.NodeStatusSuspended, SignalName: "approve"},
			{ExecutionID: id, NodeName: "reject", Status: types.NodeStatusSuspended, SignalName: "reject"},
			{ExecutionID: id, NodeName: "running", Status: types.NodeStatusRunning, SignalName: "approve"},
			{ExecutionID: id, NodeName: "no-signal", Status: types.NodeStatusSuspended},
			{ExecutionID: other, NodeName: "approve", Status: types.NodeStatusSuspended, SignalName: "approve"},
		} {
			if err := s.UpsertNode(ctx, rec); err != nil {
				t.Fatalf("UpsertNode(%q/%q): %v", rec.ExecutionID, rec.NodeName, err)
			}
		}
		assertSuspendedBySignal := func(signal string, want []string) {
			t.Helper()
			got, err := s.ListSuspendedBySignal(ctx, id, signal)
			if err != nil {
				t.Fatalf("ListSuspendedBySignal(%q) error = %v", signal, err)
			}
			for _, rec := range got {
				if rec.ExecutionID != id {
					t.Errorf("ListSuspendedBySignal(%q) returned execution %q, want only %q", signal, rec.ExecutionID, id)
				}
				if rec.Status != types.NodeStatusSuspended {
					t.Errorf("ListSuspendedBySignal(%q) returned status %q, want suspended", signal, rec.Status)
				}
				if rec.SignalName != signal {
					t.Errorf("ListSuspendedBySignal(%q) returned node waiting for %q", signal, rec.SignalName)
				}
			}
			if names := nodeNames(got); !reflect.DeepEqual(names, want) {
				t.Fatalf("ListSuspendedBySignal(%q) = %v, want %v", signal, names, want)
			}
		}
		assertSuspendedBySignal("approve", []string{"approve"})
		assertSuspendedBySignal("reject", []string{"reject"})
		assertSuspendedBySignal("none", []string{})
	})

	t.Run("ListExpiredSuspensions", func(t *testing.T) {
		id := types.ExecutionID("nodes-contract/expired")
		recent := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
		longAgo := recent.Add(-time.Hour)
		future := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
		for _, rec := range []*store.NodeRecord{
			{ExecutionID: id, NodeName: "past", Status: types.NodeStatusSuspended, Timeout: &longAgo},
			{ExecutionID: id, NodeName: "recent", Status: types.NodeStatusSuspended, Timeout: &recent},
			{ExecutionID: id, NodeName: "future", Status: types.NodeStatusSuspended, Timeout: &future},
			{ExecutionID: id, NodeName: "no-timeout", Status: types.NodeStatusSuspended},
			{ExecutionID: id, NodeName: "not-suspended", Status: types.NodeStatusRunning, Timeout: &longAgo},
		} {
			if err := s.UpsertNode(ctx, rec); err != nil {
				t.Fatalf("UpsertNode(%q/%q): %v", rec.ExecutionID, rec.NodeName, err)
			}
		}
		now := time.Now().UTC()
		all, err := s.ListExpiredSuspensions(ctx, now, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListExpiredSuspensions() error = %v", err)
		}
		mine := make([]*store.NodeRecord, 0, len(all))
		for _, rec := range all {
			if rec.ExecutionID == id {
				mine = append(mine, rec)
			}
			if rec.Status != types.NodeStatusSuspended {
				t.Errorf("ListExpiredSuspensions returned status %q for %q", rec.Status, rec.NodeName)
			}
			if rec.Timeout == nil || rec.Timeout.After(now) {
				t.Errorf("ListExpiredSuspensions returned a node whose timeout is %v (now %v)", rec.Timeout, now)
			}
		}
		if names := nodeNames(mine); !reflect.DeepEqual(names, []string{"past", "recent"}) {
			t.Fatalf("expired suspensions = %v, want [past recent]", names)
		}

		// Pagination is part of the listing contract; the order is not (the
		// memstore iterates a map, SQL orders by id). A one-row page and an
		// offset past every row are order-independent observations.
		page, err := s.ListExpiredSuspensions(ctx, now, store.ListOptions{Limit: 1})
		if err != nil {
			t.Fatalf("ListExpiredSuspensions(limit 1) error = %v", err)
		}
		if len(page) > 1 {
			t.Fatalf("ListExpiredSuspensions(limit 1) returned %d rows", len(page))
		}
		if len(all) > 0 && len(page) == 0 {
			t.Fatalf("ListExpiredSuspensions(limit 1) returned nothing while the unbounded scan returned %d rows", len(all))
		}
		beyond, err := s.ListExpiredSuspensions(ctx, now, store.ListOptions{Offset: 1 << 30})
		if err != nil {
			t.Fatalf("ListExpiredSuspensions(offset beyond end) error = %v", err)
		}
		if len(beyond) != 0 {
			t.Fatalf("ListExpiredSuspensions(offset beyond end) returned %d rows", len(beyond))
		}
	})
}

func nodeNames(recs []*store.NodeRecord) []string {
	names := make([]string, len(recs))
	for i, rec := range recs {
		names[i] = rec.NodeName
	}
	sort.Strings(names)
	return names
}

// jsonSemanticallyEqual compares two JSON documents by value rather than by
// bytes: MySQL's JSON column normalizes whitespace and key order, so a byte
// comparison would fail on SQL for a reason that has nothing to do with the
// contract.
func jsonSemanticallyEqual(got, want []byte) bool {
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		return false
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		return false
	}
	return reflect.DeepEqual(gotValue, wantValue)
}
