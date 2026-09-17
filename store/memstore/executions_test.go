package memstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/storetest"
	"github.com/xbcio/xflow/types"
)

// TestExecutionsContract runs the shared cross-backend contract against the
// in-memory backend. The SQL backend runs the same suite from
// test/integration/sqlstore_executions_test.go (build tag `integration`,
// requires MySQL); keeping one suite for both is what stops the two backends
// from drifting — and the property under test is tenant isolation, where a
// drift is a data leak rather than an inconvenience.
//
// The prefix is a legal namespace: no ':' and no glob metacharacters.
func TestExecutionsContract(t *testing.T) {
	storetest.ExecutionsContract(t, New(), fmt.Sprintf("memexec%d", time.Now().UnixNano()))
}

// TestListExecutionsScopeIsRequired pins the fail-closed reading of an empty
// scope on the in-memory backend specifically. It is redundant with the shared
// contract on purpose: the contract is the cross-backend assertion, and this is
// the one-line statement of the rule that a reviewer of this file should find
// without reading the contract helper.
func TestListExecutionsScopeIsRequired(t *testing.T) {
	s := New()
	if _, err := s.ListExecutions(context.Background(), "", store.ExecutionFilter{}, store.DefaultListOptions()); !errors.Is(err, store.ErrInvalidNamespace) {
		t.Fatalf("ListExecutions with an empty namespace = %v, want store.ErrInvalidNamespace; "+
			"an empty scope must never read as \"all namespaces\"", err)
	}
}

// TestListExecutionsUnattributedRowIsUnreachable is the isolation property in
// its smallest form: a row whose namespace is unknown cannot be listed, no
// matter what namespace the caller asks for or how it filters.
func TestListExecutionsUnattributedRowIsUnreachable(t *testing.T) {
	s := New()
	ctx := context.Background()

	now := time.Now()
	rows := []*store.ExecutionRecord{
		{ExecutionID: "unattr-1", Namespace: "", Status: types.ExecutionStatusSuccess, CreatedAt: now},
		{ExecutionID: "owned-1", Namespace: "tenant-one", Status: types.ExecutionStatusSuccess, CreatedAt: now},
	}
	for _, rec := range rows {
		if err := s.CreateExecution(ctx, rec); err != nil {
			t.Fatalf("CreateExecution(%s): %v", rec.ExecutionID, err)
		}
	}

	// The row exists — it is only unreachable through a listing.
	if _, err := s.GetExecution(ctx, "unattr-1"); err != nil {
		t.Fatalf("GetExecution(unattr-1): %v; the row should exist", err)
	}

	// Every legal scope, and a scope that a naive implementation might treat as
	// a match for the empty string.
	for _, ns := range []namespace.Namespace{"tenant-one", "tenant-two", "default", "unattr"} {
		recs, err := s.ListExecutions(ctx, ns, store.ExecutionFilter{}, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListExecutions(%q): %v", ns, err)
		}
		for _, rec := range recs {
			if rec.ExecutionID == "unattr-1" {
				t.Fatalf("ListExecutions(%q) returned the unattributed row; a listing must never "+
					"return a row whose namespace was assumed", ns)
			}
		}
	}
}

// TestListExecutionsOrderIsIndependentOfMapIteration would catch a backend that
// forgot store.ExecutionOrder's id tiebreak. Go deliberately randomizes map
// iteration, so an unsorted implementation returns rows in a different order on
// nearly every call, and the repeated comparison below is what makes that
// visible without depending on any particular other bug.
func TestListExecutionsOrderIsIndependentOfMapIteration(t *testing.T) {
	s := New()
	ctx := context.Background()
	ns := namespace.Namespace("order-stability")

	// One shared timestamp, so created_at alone cannot order these rows.
	at := time.Now()
	for i := 0; i < 24; i++ {
		if err := s.CreateExecution(ctx, &store.ExecutionRecord{
			ExecutionID: types.ExecutionID(fmt.Sprintf("order-%02d", i)),
			Namespace:   string(ns),
			Status:      types.ExecutionStatusRunning,
			CreatedAt:   at,
			UpdatedAt:   at,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
	}

	first, err := s.ListExecutions(ctx, ns, store.ExecutionFilter{}, store.ListOptions{})
	if err != nil {
		t.Fatalf("ListExecutions: %v", err)
	}
	if len(first) != 24 {
		t.Fatalf("listing returned %d rows, want 24", len(first))
	}
	// Insertion order is id order ascending, so newest-first is id descending:
	// the last-created row leads.
	if first[0].ExecutionID != "order-23" || first[23].ExecutionID != "order-00" {
		t.Fatalf("listing is not newest-first by id: first=%s last=%s", first[0].ExecutionID, first[23].ExecutionID)
	}
	for i := 1; i < len(first); i++ {
		if first[i-1].ID <= first[i].ID {
			t.Fatalf("row ids are not strictly descending at %d: %d then %d; timestamps are equal, "+
				"so the id tiebreak is what must order them", i, first[i-1].ID, first[i].ID)
		}
	}

	for attempt := 0; attempt < 8; attempt++ {
		again, err := s.ListExecutions(ctx, ns, store.ExecutionFilter{}, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListExecutions (attempt %d): %v", attempt, err)
		}
		for i := range again {
			if again[i].ExecutionID != first[i].ExecutionID {
				t.Fatalf("order changed between calls at %d: %s then %s; map iteration order is "+
					"leaking through, so offset pagination would duplicate and skip rows",
					i, first[i].ExecutionID, again[i].ExecutionID)
			}
		}
	}
}
