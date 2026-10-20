package storetest

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// SignalsContract exercises store.Signals against one backend.
//
// The contract is about what "active" means: SaveSignal creates or revives an
// active signal, ConsumeSignal and RevokeSignal retire it exactly once, and
// every read (Count/List) reports only active signals scoped to one execution.
// Those rules are what the engine's signal quorum and resumption logic assume;
// a backend that let a consumed signal count again would let a multi-signal
// wait resume early.
//
// Cases use distinct execution ids and signal names so a persistent backend
// can hold rows from earlier runs without changing the answers.
func SignalsContract(t *testing.T, s store.Signals) {
	t.Helper()
	ctx := context.Background()
	exec := types.ExecutionID("signals-contract/main")
	other := types.ExecutionID("signals-contract/other")
	approve := []byte(`{"approved_by":"ops"}`)
	reject := []byte(`{"reason":"policy"}`)
	approvedLater := []byte(`{"approved_by":"oncall"}`)

	t.Run("SaveIsActiveAndReadable", func(t *testing.T) {
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: exec, SignalName: "approve", Payload: approve}); err != nil {
			t.Fatalf("SaveSignal(approve) error = %v", err)
		}
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: other, SignalName: "approve", Payload: approve}); err != nil {
			t.Fatalf("SaveSignal(other/approve) error = %v", err)
		}
		if n, err := s.CountSignalsByNames(ctx, exec, []string{"approve"}); err != nil || n != 1 {
			t.Fatalf("CountSignalsByNames(approve) = (%d, %v), want (1, nil)", n, err)
		}
		recs, err := s.ListSignalsByNames(ctx, exec, []string{"approve"}, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListSignalsByNames(approve) error = %v", err)
		}
		if len(recs) != 1 {
			t.Fatalf("ListSignalsByNames(approve) returned %d records, want 1", len(recs))
		}
		got := recs[0]
		if got.ExecutionID != exec || got.SignalName != "approve" {
			t.Fatalf("signal identity = (%q, %q), want (%q, approve)", got.ExecutionID, got.SignalName, exec)
		}
		if !jsonSemanticallyEqual(got.Payload, approve) {
			t.Fatalf("Payload = %s, want %s", got.Payload, approve)
		}
	})

	t.Run("ReadsAreScopedToExecutionAndActiveOnly", func(t *testing.T) {
		// Seed both executions here rather than relying on the case above, so
		// the case is meaningful when run alone with -run.
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: exec, SignalName: "approve", Payload: approve}); err != nil {
			t.Fatalf("SaveSignal(exec/approve) error = %v", err)
		}
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: exec, SignalName: "reject", Payload: reject}); err != nil {
			t.Fatalf("SaveSignal(reject) error = %v", err)
		}
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: other, SignalName: "approve", Payload: approve}); err != nil {
			t.Fatalf("SaveSignal(other/approve) error = %v", err)
		}
		if n, err := s.CountSignalsByNames(ctx, exec, []string{"approve", "reject"}); err != nil || n != 2 {
			t.Fatalf("CountSignalsByNames(exec) = (%d, %v), want (2, nil)", n, err)
		}
		if n, err := s.CountSignalsByNames(ctx, other, []string{"approve", "reject"}); err != nil || n != 1 {
			t.Fatalf("CountSignalsByNames(other) = (%d, %v), want (1, nil) — reads must not cross executions", n, err)
		}
		if n, err := s.CountSignalsByNames(ctx, exec, []string{"unknown"}); err != nil || n != 0 {
			t.Fatalf("CountSignalsByNames(unknown) = (%d, %v), want (0, nil)", n, err)
		}
		if recs, err := s.ListSignalsByNames(ctx, exec, []string{"approve", "reject"}, store.ListOptions{}); err != nil {
			t.Fatalf("ListSignalsByNames(exec) error = %v", err)
		} else if names := signalNames(recs); !reflect.DeepEqual(names, []string{"approve", "reject"}) {
			t.Fatalf("ListSignalsByNames(exec) = %v, want [approve reject]", names)
		}
		if recs, err := s.ListSignalsByNames(ctx, other, []string{"approve", "reject"}, store.ListOptions{}); err != nil {
			t.Fatalf("ListSignalsByNames(other) error = %v", err)
		} else if names := signalNames(recs); !reflect.DeepEqual(names, []string{"approve"}) {
			t.Fatalf("ListSignalsByNames(other) = %v, want [approve]", names)
		}
	})

	t.Run("EmptyNameSetMatchesNothing", func(t *testing.T) {
		for _, names := range [][]string{nil, {}} {
			if n, err := s.CountSignalsByNames(ctx, exec, names); err != nil || n != 0 {
				t.Fatalf("CountSignalsByNames(%v) = (%d, %v), want (0, nil)", names, n, err)
			}
			recs, err := s.ListSignalsByNames(ctx, exec, names, store.ListOptions{})
			if err != nil {
				t.Fatalf("ListSignalsByNames(%v) error = %v", names, err)
			}
			if len(recs) != 0 {
				t.Fatalf("ListSignalsByNames(%v) returned %d records, want none", names, len(recs))
			}
		}
	})

	t.Run("ConsumeIsSingleUse", func(t *testing.T) {
		id := types.ExecutionID("signals-contract/consume")
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: id, SignalName: "go", Payload: approve}); err != nil {
			t.Fatalf("SaveSignal() error = %v", err)
		}
		got, err := s.ConsumeSignal(ctx, id, "go")
		if err != nil {
			t.Fatalf("ConsumeSignal() error = %v", err)
		}
		if got == nil || got.ExecutionID != id || got.SignalName != "go" {
			t.Fatalf("ConsumeSignal() = %+v, want the consumed signal", got)
		}
		if !jsonSemanticallyEqual(got.Payload, approve) {
			t.Fatalf("ConsumeSignal().Payload = %s, want %s", got.Payload, approve)
		}
		if n, err := s.CountSignalsByNames(ctx, id, []string{"go"}); err != nil || n != 0 {
			t.Fatalf("count after consume = (%d, %v), want (0, nil)", n, err)
		}
		if recs, err := s.ListSignalsByNames(ctx, id, []string{"go"}, store.ListOptions{}); err != nil || len(recs) != 0 {
			t.Fatalf("list after consume = (%d records, %v), want none", len(recs), err)
		}
		if _, err := s.ConsumeSignal(ctx, id, "go"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("second ConsumeSignal error = %v, want store.ErrNotFound", err)
		}
		if _, err := s.ConsumeSignal(ctx, id, "never-saved"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("ConsumeSignal(absent) error = %v, want store.ErrNotFound", err)
		}
	})

	t.Run("RevokeIsSingleUseAndIdempotent", func(t *testing.T) {
		id := types.ExecutionID("signals-contract/revoke")
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: id, SignalName: "go", Payload: approve}); err != nil {
			t.Fatalf("SaveSignal() error = %v", err)
		}
		if revoked, err := s.RevokeSignal(ctx, id, "go"); err != nil || !revoked {
			t.Fatalf("first RevokeSignal() = (%v, %v), want (true, nil)", revoked, err)
		}
		if revoked, err := s.RevokeSignal(ctx, id, "go"); err != nil || revoked {
			t.Fatalf("second RevokeSignal() = (%v, %v), want (false, nil)", revoked, err)
		}
		if revoked, err := s.RevokeSignal(ctx, id, "never-saved"); err != nil || revoked {
			t.Fatalf("RevokeSignal(absent) = (%v, %v), want (false, nil)", revoked, err)
		}
		if n, err := s.CountSignalsByNames(ctx, id, []string{"go"}); err != nil || n != 0 {
			t.Fatalf("count after revoke = (%d, %v), want (0, nil)", n, err)
		}
		if _, err := s.ConsumeSignal(ctx, id, "go"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("ConsumeSignal(revoked) error = %v, want store.ErrNotFound", err)
		}
	})

	// A revoked or consumed signal can be delivered again by a later Save (a
	// retried approval, a re-armed trigger). SaveSignal is an upsert that
	// revives the row as active with the new payload; a backend that kept the
	// retired status would silently drop the second delivery.
	t.Run("SaveRevivesARetiredSignal", func(t *testing.T) {
		id := types.ExecutionID("signals-contract/revive")
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: id, SignalName: "go", Payload: approve}); err != nil {
			t.Fatalf("first SaveSignal() error = %v", err)
		}
		if _, err := s.ConsumeSignal(ctx, id, "go"); err != nil {
			t.Fatalf("ConsumeSignal() error = %v", err)
		}
		if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: id, SignalName: "go", Payload: approvedLater}); err != nil {
			t.Fatalf("second SaveSignal() error = %v", err)
		}
		if n, err := s.CountSignalsByNames(ctx, id, []string{"go"}); err != nil || n != 1 {
			t.Fatalf("count after revive = (%d, %v), want (1, nil)", n, err)
		}
		got, err := s.ConsumeSignal(ctx, id, "go")
		if err != nil {
			t.Fatalf("ConsumeSignal() after revive error = %v", err)
		}
		if !jsonSemanticallyEqual(got.Payload, approvedLater) {
			t.Fatalf("revived payload = %s, want %s", got.Payload, approvedLater)
		}
	})

	t.Run("ListPagination", func(t *testing.T) {
		id := types.ExecutionID("signals-contract/pagination")
		names := []string{"p1", "p2", "p3"}
		for _, name := range names {
			if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: id, SignalName: name, Payload: approve}); err != nil {
				t.Fatalf("SaveSignal(%q) error = %v", name, err)
			}
		}
		all, err := s.ListSignalsByNames(ctx, id, names, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListSignalsByNames() error = %v", err)
		}
		if got := signalNames(all); !reflect.DeepEqual(got, names) {
			t.Fatalf("ListSignalsByNames() = %v, want %v", got, names)
		}
		// The listing order is not part of the contract (the memstore iterates a
		// map, SQL orders by id), so pagination is asserted as page-size and
		// offset bounds rather than as a partition across calls.
		page, err := s.ListSignalsByNames(ctx, id, names, store.ListOptions{Limit: 2})
		if err != nil {
			t.Fatalf("ListSignalsByNames(limit 2) error = %v", err)
		}
		if len(page) != 2 {
			t.Fatalf("ListSignalsByNames(limit 2) returned %d records, want 2", len(page))
		}
		for _, rec := range page {
			if rec.ExecutionID != id {
				t.Fatalf("ListSignalsByNames(limit 2) returned execution %q", rec.ExecutionID)
			}
		}
		beyond, err := s.ListSignalsByNames(ctx, id, names, store.ListOptions{Offset: 1 << 30})
		if err != nil {
			t.Fatalf("ListSignalsByNames(offset beyond end) error = %v", err)
		}
		if len(beyond) != 0 {
			t.Fatalf("ListSignalsByNames(offset beyond end) returned %d records", len(beyond))
		}
	})
}

func signalNames(recs []*store.SignalRecord) []string {
	names := make([]string, len(recs))
	for i, rec := range recs {
		names[i] = rec.SignalName
	}
	sort.Strings(names)
	return names
}
