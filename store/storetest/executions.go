package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ExecutionsStore is the surface the executions contract needs. It is declared
// here, narrowly, rather than taking a whole store.Store: the contract is about
// the listing methods, and a backend that only claims those should be able to
// run it. store.Executions satisfies it.
type ExecutionsStore interface {
	CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error
	// UpdateExecutionStatus drives the lifecycle transitions the write-once
	// finished_at contract is asserted through.
	UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error
	GetExecution(ctx context.Context, id types.ExecutionID) (*store.ExecutionRecord, error)
	ListExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter, opts store.ListOptions) ([]*store.ExecutionRecord, error)
	CountExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (int64, error)
}

// emptyWorkflowDef stands in for the workflow definition on fixtures that are
// never executed. db/xflow_schema.sql declares workflow_def NOT NULL, so a
// fixture without it fails at CreateExecution against MySQL — with an error
// about a column that has nothing to do with what the contract is testing.
var emptyWorkflowDef = []byte(`{}`)

// ExecutionsContract exercises the namespace-scoped execution listing against
// one backend. Every backend must satisfy it identically: this is the contract
// that makes "the memstore is the loose one" impossible to ship, which matters
// here more than for a typical contract, because the property under test is a
// tenant-isolation property rather than a convenience.
//
// nsPrefix disambiguates rows between backends and between reruns against a
// persistent database — a MySQL run must not see a previous run's rows, since
// the isolation assertions below would otherwise be satisfied (or broken) by
// leftovers. Callers must use a prefix that is a legal namespace: no ':' or
// glob metacharacters (namespace.Validate).
func ExecutionsContract(t *testing.T, s ExecutionsStore, nsPrefix string) {
	t.Helper()
	ctx := context.Background()

	nsA := namespace.Namespace(nsPrefix + "-a")
	nsB := namespace.Namespace(nsPrefix + "-b")
	nsEmpty := namespace.Namespace(nsPrefix + "-empty")
	// nsC holds one namespace-attributed row whose WORKFLOW is unattributed
	// (WorkflowID ""). It is what distinguishes the two fail-closed rules: a
	// workflow filter must not match it, while an unfiltered listing must still
	// show it.
	nsC := namespace.Namespace(nsPrefix + "-c")

	ids := contractExecIDs(nsPrefix)

	// A budget of "old enough that no backend's clock can be confused by it":
	// one hour in the past, so created_at ordering below is unambiguous even if
	// the store's clock differs from this process's by minutes.
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	// The ids are in NEITHER listing order; the fixtures' created_at values
	// decide that (see `seed` below), so the assertions spell out the order they
	// expect rather than reusing this slice positionally.

	// --- fixtures ---------------------------------------------------------
	// Three rows in nsA, two in nsB, one row in nsC whose namespace is set but
	// whose workflow is not, and one UNATTRIBUTED row (Namespace "") that must
	// never be visible to anyone. Statuses, timestamps and workflow ids are
	// chosen so every filter below has a discriminating answer.
	//
	// ids[0..4] ascend with created_at, so a listing (newest first) reverses
	// them; ids[5] is deliberately in the middle of the id range and outside
	// every namespace, so an implementation that leaked it would be caught by
	// the set assertions rather than only by the explicit probes.
	//
	// WorkflowDef is set on every row even though these rows are never executed:
	// db/xflow_schema.sql declares workflow_def NOT NULL, so a fixture that left
	// it nil would fail at the first CreateExecution against MySQL with an
	// error that says nothing about listings. `{}` is the same placeholder
	// test/integration's emptyJSON uses for the same reason.
	// finishedAt is set on precisely one row: it makes the new execution-table
	// columns observable on a round trip, and leaves the other terminal row
	// (ids[2]) as the "written before the column existed" case that must read
	// back as nil rather than as a manufactured instant.
	finishedAt := base.Add(1500 * time.Millisecond)
	seed := []*store.ExecutionRecord{
		{ExecutionID: ids[0], Namespace: string(nsA), WorkflowID: "wfid-alpha", WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, TriggerKind: "timer", NodeTotal: 4, FinishedAt: &finishedAt, CreatedAt: base, UpdatedAt: base},
		{ExecutionID: ids[1], Namespace: string(nsA), WorkflowID: "wfid-alpha", WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusFailed, CreatedAt: base.Add(time.Second), UpdatedAt: base.Add(time.Second)},
		{ExecutionID: ids[2], Namespace: string(nsA), WorkflowID: "wfid-beta", WorkflowName: "wf-beta", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, TriggerKind: "webhook", NodeTotal: 2, CreatedAt: base.Add(2 * time.Second), UpdatedAt: base.Add(2 * time.Second)},
		{ExecutionID: ids[3], Namespace: string(nsB), WorkflowID: "wfid-alpha", WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(3 * time.Second), UpdatedAt: base.Add(3 * time.Second)},
		{ExecutionID: ids[4], Namespace: string(nsB), WorkflowID: "wfid-gamma", WorkflowName: "wf-gamma", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusRunning, CreatedAt: base.Add(4 * time.Second), UpdatedAt: base.Add(4 * time.Second)},
		// The unattributed row. Its execution_id is still readable through
		// GetExecution — the row exists and is not hidden from a caller who
		// already knows its id — it is only unreachable from any listing. Its
		// WorkflowID is deliberately empty too: a workflow filter must never
		// attribute it.
		{ExecutionID: ids[5], Namespace: "", WorkflowID: "", WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(5 * time.Second), UpdatedAt: base.Add(5 * time.Second)},
		// nsC's row is attributed to a namespace but NOT to a workflow. A
		// workflow filter must never return it; an unfiltered listing must.
		{ExecutionID: types.ExecutionID(fmt.Sprintf("%s-c1", nsPrefix)), Namespace: string(nsC), WorkflowID: "", WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(6 * time.Second), UpdatedAt: base.Add(6 * time.Second)},
	}
	for _, rec := range seed {
		if err := s.CreateExecution(ctx, rec); err != nil {
			t.Fatalf("CreateExecution(%s, ns=%q): %v", rec.ExecutionID, rec.Namespace, err)
		}
	}

	// --- a write refuses an id the identity column cannot hold -------------
	// Truncating it (or relying on the database's own error) could attribute a
	// row to another workflow; the refusal must be the same in every backend.
	overlong := &store.ExecutionRecord{
		ExecutionID: types.ExecutionID(nsPrefix + "-overlong"),
		Namespace:   string(nsB),
		WorkflowID:  types.WorkflowID(repeatByte('w', store.MaxExecutionWorkflowIDLen+1)),
		WorkflowDef: emptyWorkflowDef,
		Status:      types.ExecutionStatusSuccess,
		CreatedAt:   base,
		UpdatedAt:   base,
	}
	if err := s.CreateExecution(ctx, overlong); !errors.Is(err, store.ErrInvalidWorkflowID) {
		t.Fatalf("CreateExecution with a %d-character workflow id error = %v, want store.ErrInvalidWorkflowID",
			store.MaxExecutionWorkflowIDLen+1, err)
	}
	if _, err := s.GetExecution(ctx, overlong.ExecutionID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected over-long workflow id left a row: GetExecution error = %v, want store.ErrNotFound", err)
	}

	// --- fail closed on an unusable scope ---------------------------------
	// Each of these is a distinct way to ask for "everything", and each must be
	// refused rather than answered more broadly than intended.
	for _, tc := range []struct {
		name string
		ns   namespace.Namespace
	}{
		{name: "empty", ns: ""},
		{name: "too long", ns: namespace.Namespace(repeatByte('n', namespace.MaxNameLen+1))},
		{name: "colon delimiter", ns: namespace.Namespace(nsPrefix + ":a")},
		{name: "glob metacharacter", ns: namespace.Namespace(nsPrefix + "-*")},
		{name: "brace", ns: namespace.Namespace(nsPrefix + "-{a}")},
		{name: "backslash", ns: namespace.Namespace(nsPrefix + `-a\`)},
	} {
		t.Run("fail_closed/"+tc.name, func(t *testing.T) {
			got, err := s.ListExecutions(ctx, tc.ns, store.ExecutionFilter{}, store.DefaultListOptions())
			if !errors.Is(err, store.ErrInvalidNamespace) {
				t.Fatalf("ListExecutions(ns=%q) error = %v, want store.ErrInvalidNamespace; "+
					"an unusable scope must be REFUSED, never widened to every namespace", tc.ns, err)
			}
			if got != nil {
				t.Fatalf("ListExecutions(ns=%q) returned %d rows alongside an error; "+
					"a refused scope must return nothing at all", tc.ns, len(got))
			}
			if _, err := s.CountExecutions(ctx, tc.ns, store.ExecutionFilter{}); !errors.Is(err, store.ErrInvalidNamespace) {
				t.Fatalf("CountExecutions(ns=%q) error = %v, want store.ErrInvalidNamespace; "+
					"list and count must refuse the same scopes", tc.ns, err)
			}
		})
	}

	// --- a valid namespace with no rows is empty, not an error -------------
	t.Run("unknown_namespace_is_empty", func(t *testing.T) {
		got, err := s.ListExecutions(ctx, nsEmpty, store.ExecutionFilter{}, store.DefaultListOptions())
		if err != nil {
			t.Fatalf("ListExecutions on an empty namespace: %v, want nil error", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListExecutions on an empty namespace returned %d rows, want 0", len(got))
		}
		if got == nil {
			t.Fatal("ListExecutions returned a nil slice for an empty result; " +
				"a list payload must serialize as [] rather than null")
		}
		total, err := s.CountExecutions(ctx, nsEmpty, store.ExecutionFilter{})
		if err != nil {
			t.Fatalf("CountExecutions on an empty namespace: %v", err)
		}
		if total != 0 {
			t.Fatalf("CountExecutions on an empty namespace = %d, want 0", total)
		}
	})

	// --- namespace isolation ----------------------------------------------
	t.Run("isolation", func(t *testing.T) {
		gotA := mustListAll(t, s, ctx, nsA)
		gotB := mustListAll(t, s, ctx, nsB)

		assertExactly(t, "nsA listing", gotA, []types.ExecutionID{ids[2], ids[1], ids[0]})
		assertExactly(t, "nsB listing", gotB, []types.ExecutionID{ids[4], ids[3]})

		// The unattributed row and every other namespace's row must be absent
		// from both.
		for _, rec := range append(append([]*store.ExecutionRecord{}, gotA...), gotB...) {
			if rec.Namespace != string(nsA) && rec.Namespace != string(nsB) {
				t.Fatalf("listing for one namespace returned a row in namespace %q", rec.Namespace)
			}
			if rec.ExecutionID == ids[5] {
				t.Fatal("a namespace listing returned the unattributed row (namespace \"\")")
			}
		}

		if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{}); total != 3 {
			t.Fatalf("CountExecutions(nsA) = %d, want 3", total)
		}
		if total := mustCount(t, s, ctx, nsB, store.ExecutionFilter{}); total != 2 {
			t.Fatalf("CountExecutions(nsB) = %d, want 2", total)
		}

		// The unattributed row is not merely unlisted: no scope reaches it.
		if got := mustListAll(t, s, ctx, namespace.Namespace("")); got != nil {
			t.Fatalf("ListExecutions(\"\") returned %d rows; it must have refused the scope", len(got))
		}
	})

	// --- ordering ----------------------------------------------------------
	t.Run("ordering_newest_first", func(t *testing.T) {
		got := mustListAll(t, s, ctx, nsA)
		// created_at is strictly increasing across these fixtures, so the id
		// tiebreak is not what decides this order — but the direction is.
		want := []types.ExecutionID{ids[2], ids[1], ids[0]}
		gotIDs := executionIDs(got)
		if !equalIDs(gotIDs, want) {
			t.Fatalf("listing order = %v, want newest-first %v (store.ExecutionOrder = %q)",
				gotIDs, want, store.ExecutionOrder)
		}
	})

	// --- pagination boundaries --------------------------------------------
	t.Run("limit_offset_boundaries", func(t *testing.T) {
		all := mustListAll(t, s, ctx, nsA) // newest first: ids[2], ids[1], ids[0]

		for _, tc := range []struct {
			name string
			opts store.ListOptions
			want []types.ExecutionID
		}{
			{name: "offset zero limit one", opts: store.ListOptions{Limit: 1}, want: []types.ExecutionID{ids[2]}},
			{name: "second page", opts: store.ListOptions{Limit: 1, Offset: 1}, want: []types.ExecutionID{ids[1]}},
			{name: "third page", opts: store.ListOptions{Limit: 1, Offset: 2}, want: []types.ExecutionID{ids[0]}},
			// An offset past the last row is an empty page, not an error.
			{name: "offset at the end", opts: store.ListOptions{Limit: 10, Offset: 3}, want: nil},
			{name: "offset past the end", opts: store.ListOptions{Limit: 10, Offset: 99}, want: nil},
			// ListOptions.Normalized clamps a negative offset to 0.
			{name: "negative offset clamps to zero", opts: store.ListOptions{Limit: 1, Offset: -5}, want: []types.ExecutionID{ids[2]}},
			{name: "limit larger than the set", opts: store.ListOptions{Limit: 100, Offset: 0}, want: []types.ExecutionID{ids[2], ids[1], ids[0]}},
			// store.ListOptions documents 0 as "every match", not SQL's LIMIT 0.
			{name: "limit zero is unbounded", opts: store.ListOptions{}, want: []types.ExecutionID{ids[2], ids[1], ids[0]}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{}, tc.opts)
				if err != nil {
					t.Fatalf("ListExecutions(%+v): %v", tc.opts, err)
				}
				if !equalIDs(executionIDs(got), tc.want) {
					t.Fatalf("ListExecutions(%+v) = %v, want %v", tc.opts, executionIDs(got), tc.want)
				}
			})
		}

		// Every page of size 1 must partition the set exactly: no duplicates,
		// no skips. This is the property the ordering exists to provide.
		var walked []types.ExecutionID
		for offset := 0; ; offset++ {
			page, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{}, store.ListOptions{Limit: 1, Offset: offset})
			if err != nil {
				t.Fatalf("page at offset %d: %v", offset, err)
			}
			if len(page) == 0 {
				break
			}
			walked = append(walked, executionIDs(page)...)
			if offset > len(all) {
				t.Fatal("pagination never terminated; a full page was returned past the end of the set")
			}
		}
		if !equalIDs(walked, executionIDs(all)) {
			t.Fatalf("page-by-page walk = %v, want exactly %v: offset pagination duplicated or skipped a row",
				walked, executionIDs(all))
		}

		// Total is the size of the filtered set, not the page.
		if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{}); total != int64(len(all)) {
			t.Fatalf("CountExecutions = %d, want %d for the same scope and filter", total, len(all))
		}
	})

	// --- ordering stability when rows share a timestamp --------------------
	// The fixtures above have distinct created_at values, so they cannot tell a
	// total order from a merely-plausible one. These rows share one timestamp
	// exactly, which is where a non-unique sort key would let MySQL return them
	// in different orders on consecutive queries.
	t.Run("same_timestamp_is_ordered_totally", func(t *testing.T) {
		nsTie := namespace.Namespace(nsPrefix + "-tie")
		tieBase := base.Add(10 * time.Second)
		tieIDs := []types.ExecutionID{
			types.ExecutionID(fmt.Sprintf("%s-tie-1", nsPrefix)),
			types.ExecutionID(fmt.Sprintf("%s-tie-2", nsPrefix)),
			types.ExecutionID(fmt.Sprintf("%s-tie-3", nsPrefix)),
			types.ExecutionID(fmt.Sprintf("%s-tie-4", nsPrefix)),
		}
		for _, id := range tieIDs {
			rec := &store.ExecutionRecord{
				ExecutionID: id, Namespace: string(nsTie), WorkflowDef: emptyWorkflowDef,
				Status: types.ExecutionStatusRunning, CreatedAt: tieBase, UpdatedAt: tieBase,
			}
			if err := s.CreateExecution(ctx, rec); err != nil {
				t.Fatalf("CreateExecution(%s): %v", id, err)
			}
		}

		first := mustListAll(t, s, ctx, nsTie)
		if len(first) != len(tieIDs) {
			t.Fatalf("tie fixture listing returned %d rows, want %d", len(first), len(tieIDs))
		}
		// Repeat: an unstable order is not guaranteed to differ on any single
		// repeat, but it very reliably does across several, and the paged walk
		// below is the assertion that actually matters.
		for i := 0; i < 4; i++ {
			again := mustListAll(t, s, ctx, nsTie)
			if !equalIDs(executionIDs(again), executionIDs(first)) {
				t.Fatalf("listing order changed between calls with no write in between: %v then %v; "+
					"the order is not total, so offset pagination can duplicate or skip rows",
					executionIDs(first), executionIDs(again))
			}
		}

		// Walk the tie set one row at a time. Every row exactly once.
		var walked []types.ExecutionID
		for offset := 0; offset < len(tieIDs); offset++ {
			page, err := s.ListExecutions(ctx, nsTie, store.ExecutionFilter{}, store.ListOptions{Limit: 1, Offset: offset})
			if err != nil {
				t.Fatalf("tie page at offset %d: %v", offset, err)
			}
			if len(page) != 1 {
				t.Fatalf("tie page at offset %d returned %d rows, want 1", offset, len(page))
			}
			walked = append(walked, page[0].ExecutionID)
		}
		seen := map[types.ExecutionID]bool{}
		for _, id := range walked {
			if seen[id] {
				t.Fatalf("row %s appeared twice in a page-by-page walk of same-timestamp rows: %v", id, walked)
			}
			seen[id] = true
		}
		if len(walked) != len(tieIDs) {
			t.Fatalf("walk of same-timestamp rows saw %d of %d rows: %v", len(walked), len(tieIDs), walked)
		}
	})

	// --- supported filters -------------------------------------------------
	t.Run("filters", func(t *testing.T) {
		t.Run("status", func(t *testing.T) {
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatusSuccess}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("status filter: %v", err)
			}
			assertExactly(t, "successful executions in nsA", got, []types.ExecutionID{ids[2], ids[0]})
			if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatusSuccess}); total != 2 {
				t.Fatalf("CountExecutions(status=success) = %d, want 2", total)
			}
			// A status that exists but matches nothing here is an empty page,
			// not an error.
			empty, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatusTimeout}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("status filter with no match: %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("status=timeout returned %d rows, want 0", len(empty))
			}
		})

		t.Run("status_is_scoped_to_the_namespace", func(t *testing.T) {
			// nsB holds a success and a running row. Filtering by success must
			// not reach nsA's successes.
			got, err := s.ListExecutions(ctx, nsB, store.ExecutionFilter{Status: types.ExecutionStatusSuccess}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("status filter in nsB: %v", err)
			}
			assertExactly(t, "successful executions in nsB", got, []types.ExecutionID{ids[3]})
		})

		t.Run("statuses_multi_select", func(t *testing.T) {
			// The multi-select form must describe the same set as the union of
			// the single-status listings, in the same order.
			multi, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				Statuses: []types.ExecutionStatus{types.ExecutionStatusSuccess, types.ExecutionStatusFailed},
			}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("multi-status filter: %v", err)
			}
			assertExactly(t, "success+failed executions in nsA", multi, []types.ExecutionID{ids[2], ids[1], ids[0]})
			if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{
				Statuses: []types.ExecutionStatus{types.ExecutionStatusSuccess, types.ExecutionStatusFailed},
			}); total != 3 {
				t.Fatalf("CountExecutions(statuses=success,failed) = %d, want 3", total)
			}

			// A one-element multi-select is the same set as the scalar form.
			one, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				Statuses: []types.ExecutionStatus{types.ExecutionStatusSuccess},
			}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("one-element multi-status filter: %v", err)
			}
			assertExactly(t, "statuses=[success] in nsA", one, []types.ExecutionID{ids[2], ids[0]})

			// Duplicates cannot change an IN set.
			dup, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				Statuses: []types.ExecutionStatus{types.ExecutionStatusSuccess, types.ExecutionStatusSuccess},
			}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("duplicated multi-status filter: %v", err)
			}
			assertExactly(t, "statuses=[success,success] in nsA", dup, []types.ExecutionID{ids[2], ids[0]})

			// The two forms are mutually exclusive: setting both is a caller
			// bug, not a filter to guess at.
			if _, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				Status:   types.ExecutionStatusSuccess,
				Statuses: []types.ExecutionStatus{types.ExecutionStatusSuccess},
			}, store.DefaultListOptions()); err == nil {
				t.Fatal("a filter with both status and statuses was accepted; the two forms cannot both apply")
			}

			// One unknown member rejects the whole filter, exactly like an
			// unknown scalar status.
			if _, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				Statuses: []types.ExecutionStatus{types.ExecutionStatusSuccess, types.ExecutionStatus("succes")},
			}, store.DefaultListOptions()); err == nil {
				t.Fatal("an unrecognized status in statuses was accepted; a typo must not read back as an empty page")
			}
			if _, err := s.CountExecutions(ctx, nsA, store.ExecutionFilter{
				Statuses: []types.ExecutionStatus{types.ExecutionStatus("succes")},
			}); err == nil {
				t.Fatal("CountExecutions accepted an unrecognized status in statuses that ListExecutions rejects")
			}
		})

		t.Run("query", func(t *testing.T) {
			// The fixtures are <prefix>-a1, -a2, -a3, -b1, -b2, -orphan, and
			// the nsA rows are attributed to workflow ids wfid-alpha/wfid-beta.
			prefix := nsPrefix + "-a"
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: prefix}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("query filter: %v", err)
			}
			assertExactly(t, "execution ids starting with "+prefix, got, []types.ExecutionID{ids[2], ids[1], ids[0]})
			if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{Query: prefix}); total != 3 {
				t.Fatalf("CountExecutions(query=%q) = %d, want 3", prefix, total)
			}

			// A full execution id as the query is an exact-id lookup.
			exact, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: string(ids[0])}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("exact-id query filter: %v", err)
			}
			assertExactly(t, "execution id equal to "+string(ids[0]), exact, []types.ExecutionID{ids[0]})

			// The query matches the WORKFLOW id column too, ORed with the
			// execution id: one search box answers "this run, or the runs of
			// this workflow?" without the caller knowing which identity the
			// string is.
			wfPrefix := "wfid-alpha"
			wfGot, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: wfPrefix}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow-id query filter: %v", err)
			}
			assertExactly(t, "workflow ids starting with "+wfPrefix, wfGot, []types.ExecutionID{ids[1], ids[0]})
			if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{Query: wfPrefix}); total != 2 {
				t.Fatalf("CountExecutions(query=%q) = %d, want 2", wfPrefix, total)
			}
			inB, err := s.ListExecutions(ctx, nsB, store.ExecutionFilter{Query: wfPrefix}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow-id query filter in nsB: %v", err)
			}
			assertExactly(t, "workflow ids starting with wfid-alpha in nsB", inB, []types.ExecutionID{ids[3]})

			// Query matching stays inside the scope: nsB's -b rows share the
			// fixture prefix, and its wfid-gamma row shares the workflow
			// column's shape, but neither must answer an nsA query.
			cross, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: nsPrefix + "-b"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("cross-namespace query filter: %v", err)
			}
			if len(cross) != 0 {
				t.Fatalf("query filter in nsA returned %d nsB rows: %v", len(cross), executionIDs(cross))
			}
			if wfCross, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: "wfid-gamma"}, store.DefaultListOptions()); err != nil {
				t.Fatalf("cross-namespace workflow-id query filter: %v", err)
			} else if len(wfCross) != 0 {
				t.Fatalf("workflow-id query filter in nsA returned %d nsB rows: %v", len(wfCross), executionIDs(wfCross))
			}

			// A single query must reach one row through EITHER column: an
			// AND-shaped implementation would drop the workflow-only row, and
			// an execution-id-only one would drop it too.
			nsOr := namespace.Namespace(nsPrefix + "-or")
			orBase := base.Add(10 * time.Second)
			orRows := []*store.ExecutionRecord{
				{ExecutionID: types.ExecutionID(nsPrefix + "-or-1"), Namespace: string(nsOr), WorkflowID: "or-wf-1", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: orBase, UpdatedAt: orBase},
				{ExecutionID: types.ExecutionID(nsPrefix + "-or-2"), Namespace: string(nsOr), WorkflowID: "other-wf-2", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: orBase.Add(time.Second), UpdatedAt: orBase.Add(time.Second)},
				{ExecutionID: types.ExecutionID(nsPrefix + "-zeta-3"), Namespace: string(nsOr), WorkflowID: "or-wf-3", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: orBase.Add(2 * time.Second), UpdatedAt: orBase.Add(2 * time.Second)},
			}
			for _, rec := range orRows {
				if err := s.CreateExecution(ctx, rec); err != nil {
					t.Fatalf("CreateExecution(%s): %v", rec.ExecutionID, err)
				}
			}
			orGot, err := s.ListExecutions(ctx, nsOr, store.ExecutionFilter{Query: "or-wf"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("either-column query filter: %v", err)
			}
			assertExactly(t, "rows matching or-wf through either identity", orGot,
				[]types.ExecutionID{types.ExecutionID(nsPrefix + "-zeta-3"), types.ExecutionID(nsPrefix + "-or-1")})
			if total := mustCount(t, s, ctx, nsOr, store.ExecutionFilter{Query: "or-wf"}); total != 2 {
				t.Fatalf("CountExecutions(query=or-wf) = %d, want 2", total)
			}

			// LIKE metacharacters in the query are data, not wildcards. '_'
			// is in the id alphabet, so this is reachable through the HTTP
			// boundary and must be escaped on BOTH sides of the OR; an
			// unescaped '_' would also match the 'a' rows.
			nsWild := namespace.Namespace(nsPrefix + "-wild")
			for _, id := range []types.ExecutionID{
				types.ExecutionID(nsPrefix + "-wild-x_1"),
				types.ExecutionID(nsPrefix + "-wild-xa1"),
			} {
				rec := &store.ExecutionRecord{
					ExecutionID: id, Namespace: string(nsWild), WorkflowDef: emptyWorkflowDef,
					Status: types.ExecutionStatusSuccess, CreatedAt: base, UpdatedAt: base,
				}
				if err := s.CreateExecution(ctx, rec); err != nil {
					t.Fatalf("CreateExecution(%s): %v", id, err)
				}
			}
			wild, err := s.ListExecutions(ctx, nsWild, store.ExecutionFilter{Query: nsPrefix + "-wild-x_"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("underscore query filter: %v", err)
			}
			assertExactly(t, "literal underscore query", wild, []types.ExecutionID{types.ExecutionID(nsPrefix + "-wild-x_1")})

			nsWildWF := namespace.Namespace(nsPrefix + "-wildwf")
			for i, wf := range []types.WorkflowID{"wfid_alpha", "wfidXalpha"} {
				rec := &store.ExecutionRecord{
					ExecutionID: types.ExecutionID(fmt.Sprintf("%s-wildwf-%d", nsPrefix, i)),
					Namespace:   string(nsWildWF), WorkflowID: wf, WorkflowDef: emptyWorkflowDef,
					Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(time.Duration(i) * time.Second), UpdatedAt: base.Add(time.Duration(i) * time.Second),
				}
				if err := s.CreateExecution(ctx, rec); err != nil {
					t.Fatalf("CreateExecution(%s): %v", rec.ExecutionID, err)
				}
			}
			wildWF, err := s.ListExecutions(ctx, nsWildWF, store.ExecutionFilter{Query: "wfid_a"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("underscore workflow-id query filter: %v", err)
			}
			assertExactly(t, "literal underscore workflow-id query", wildWF,
				[]types.ExecutionID{types.ExecutionID(fmt.Sprintf("%s-wildwf-0", nsPrefix))})

			// '%' and other LIKE metacharacters are outside the id alphabet and
			// must be refused before they reach a LIKE expression: `q=a%` must
			// never widen into a match-all.
			for _, bad := range []string{nsPrefix + "-a%", nsPrefix + "-a_%", "a!b", "a b"} {
				if _, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: bad}, store.DefaultListOptions()); err == nil {
					t.Fatalf("query filter %q was accepted; it contains a character outside the id alphabet", bad)
				}
			}
			// A prefix longer than the id column can never match a stored id.
			long := repeatByte('a', store.MaxExecutionIDQueryLen+1)
			if _, err := s.CountExecutions(ctx, nsA, store.ExecutionFilter{Query: long}); err == nil {
				t.Fatal("CountExecutions accepted an over-long query that ListExecutions rejects")
			}
		})

		t.Run("workflow_id", func(t *testing.T) {
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{WorkflowID: "wfid-alpha"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow filter: %v", err)
			}
			assertExactly(t, "wfid-alpha executions in nsA", got, []types.ExecutionID{ids[1], ids[0]})
			if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{WorkflowID: "wfid-alpha"}); total != 2 {
				t.Fatalf("CountExecutions(workflow_id=wfid-alpha) = %d, want 2", total)
			}
			// A real id with no matching row is an empty page, not an error.
			empty, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{WorkflowID: "wfid-missing"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow filter with no match: %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("workflow_id=wfid-missing returned %d rows, want 0", len(empty))
			}
			// The filter composes with the other filters rather than replacing
			// them: alpha has one success and one failure in nsA.
			composed, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				WorkflowID: "wfid-alpha",
				Status:     types.ExecutionStatusFailed,
			}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow+status filter: %v", err)
			}
			assertExactly(t, "failed wfid-alpha executions in nsA", composed, []types.ExecutionID{ids[1]})
		})

		t.Run("workflow_id_is_scoped_to_the_namespace", func(t *testing.T) {
			// nsB holds a row under the SAME workflow id as nsA's rows. A
			// workflow filter narrows within one namespace; it never reaches
			// across one.
			got, err := s.ListExecutions(ctx, nsB, store.ExecutionFilter{WorkflowID: "wfid-alpha"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow filter in nsB: %v", err)
			}
			assertExactly(t, "wfid-alpha executions in nsB", got, []types.ExecutionID{ids[3]})
		})

		t.Run("workflow_id_never_matches_an_unattributed_row", func(t *testing.T) {
			// nsC's row has a namespace but no workflow id. It must be invisible
			// to every non-empty workflow filter, while a listing without that
			// filter still shows it — under-reporting a filtered answer is the
			// intended fail-closed direction, hiding the row entirely is not.
			got, err := s.ListExecutions(ctx, nsC, store.ExecutionFilter{WorkflowID: "wfid-alpha"}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("workflow filter in nsC: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("workflow_id filter returned %d unattributed rows, want 0", len(got))
			}
			if total := mustCount(t, s, ctx, nsC, store.ExecutionFilter{WorkflowID: "wfid-alpha"}); total != 0 {
				t.Fatalf("CountExecutions(workflow_id) = %d for unattributed rows, want 0", total)
			}
			all := mustListAll(t, s, ctx, nsC)
			if len(all) != 1 {
				t.Fatalf("unfiltered nsC listing = %d rows, want the one unattributed row", len(all))
			}
		})

		t.Run("overlong_workflow_id_is_rejected", func(t *testing.T) {
			long := types.WorkflowID(repeatByte('w', store.MaxExecutionWorkflowIDLen+1))
			if _, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{WorkflowID: long}, store.DefaultListOptions()); err == nil {
				t.Fatal("a workflow id filter longer than the column width was accepted; it can never match a stored id")
			}
			if _, err := s.CountExecutions(ctx, nsA, store.ExecutionFilter{WorkflowID: long}); err == nil {
				t.Fatal("CountExecutions accepted an overlong workflow id filter that ListExecutions rejects")
			}
		})

		t.Run("unknown_status_is_rejected", func(t *testing.T) {
			// A typo must fail loudly. Answering "empty page" would make a
			// misspelled status indistinguishable from a real status with no
			// matching rows.
			_, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatus("succes")}, store.DefaultListOptions())
			if err == nil {
				t.Fatal("an unrecognized status filter was accepted; a typo must not read back as an empty page")
			}
			if _, err := s.CountExecutions(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatus("succes")}); err == nil {
				t.Fatal("CountExecutions accepted an unrecognized status filter that ListExecutions rejects")
			}
		})

		t.Run("created_at_range_is_exclusive_at_both_ends", func(t *testing.T) {
			// Window (base, base+2s) excludes the row at base and the row at
			// base+2s, leaving only the row at base+1s.
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				CreatedAfter:  base,
				CreatedBefore: base.Add(2 * time.Second),
			}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("time range filter: %v", err)
			}
			assertExactly(t, "executions in (base, base+2s)", got, []types.ExecutionID{ids[1]})
			if total := mustCount(t, s, ctx, nsA, store.ExecutionFilter{
				CreatedAfter:  base,
				CreatedBefore: base.Add(2 * time.Second),
			}); total != 1 {
				t.Fatalf("CountExecutions with a time range = %d, want 1", total)
			}
		})

		t.Run("time_bounds_are_independent", func(t *testing.T) {
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{CreatedAfter: base.Add(time.Second)}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("CreatedAfter only: %v", err)
			}
			assertExactly(t, "executions after base+1s", got, []types.ExecutionID{ids[2]})

			got, err = s.ListExecutions(ctx, nsA, store.ExecutionFilter{CreatedBefore: base.Add(time.Second)}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("CreatedBefore only: %v", err)
			}
			assertExactly(t, "executions before base+1s", got, []types.ExecutionID{ids[0]})
		})

		t.Run("time_range_is_scoped_to_the_namespace", func(t *testing.T) {
			// A window wide enough to contain every nx fixture row must still
			// return only nsA's rows.
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{
				CreatedAfter:  base.Add(-time.Hour),
				CreatedBefore: base.Add(time.Hour),
			}, store.DefaultListOptions())
			if err != nil {
				t.Fatalf("wide time range filter: %v", err)
			}
			assertExactly(t, "nsA executions in a wide window", got, []types.ExecutionID{ids[2], ids[1], ids[0]})
		})

		t.Run("filters_within_the_page", func(t *testing.T) {
			// Filters and pagination compose: page 2 of the success filter.
			got, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatusSuccess},
				store.ListOptions{Limit: 1, Offset: 1})
			if err != nil {
				t.Fatalf("filtered page: %v", err)
			}
			assertExactly(t, "second page of successful executions in nsA", got, []types.ExecutionID{ids[0]})
		})
	})

	// --- unattributed rows -------------------------------------------------
	t.Run("unattributed_row_is_never_listed", func(t *testing.T) {
		// It is genuinely there, and genuinely reachable by id. The listing is
		// what must not see it: listing is the operation whose contract would be
		// broken by guessing a namespace for it.
		got, err := s.GetExecution(ctx, ids[5])
		if err != nil {
			t.Fatalf("GetExecution on the unattributed row: %v; the row must exist "+
				"(it is only unreachable from a listing, not invisible)", err)
		}
		if got.Namespace != "" {
			t.Fatalf("the seed row's namespace = %q, want \"\"; the fixture is not testing "+
				"what it claims to", got.Namespace)
		}

		// Exhaustive over every namespace the fixture uses, plus a scope that
		// deliberately looks like the empty one.
		scopes := []namespace.Namespace{nsA, nsB, nsEmpty, namespace.Namespace(nsPrefix + "-a-b-c")}
		for _, ns := range scopes {
			for _, filter := range []store.ExecutionFilter{
				{},
				{Status: types.ExecutionStatusSuccess},
				{Status: types.ExecutionStatusRunning},
				{CreatedAfter: base.Add(-time.Hour), CreatedBefore: base.Add(time.Hour)},
			} {
				rows, err := s.ListExecutions(ctx, ns, filter, store.ListOptions{})
				if err != nil {
					t.Fatalf("ListExecutions(ns=%q, filter=%+v): %v", ns, filter, err)
				}
				for _, rec := range rows {
					if rec.ExecutionID == ids[5] {
						t.Fatalf("ListExecutions(ns=%q, filter=%+v) returned the unattributed row %s; "+
							"a row whose namespace is unknown must never be listed under an assumed one",
							ns, filter, ids[5])
					}
				}
				total, err := s.CountExecutions(ctx, ns, filter)
				if err != nil {
					t.Fatalf("CountExecutions(ns=%q, filter=%+v): %v", ns, filter, err)
				}
				if int(total) != len(rows) {
					t.Fatalf("CountExecutions(ns=%q, filter=%+v) = %d but the listing returned %d rows; "+
						"the count must describe the same set", ns, filter, total, len(rows))
				}
			}
		}
	})

	// --- execution-table columns added with the statistics work ------------
	t.Run("trigger_kind_node_total_finished_at_round_trip", func(t *testing.T) {
		// ids[0] was written with all three values; ids[1] (terminal, written
		// without them) is the pre-column row that must read back as the zero
		// value, never as a manufactured one.
		got, err := s.GetExecution(ctx, ids[0])
		if err != nil {
			t.Fatalf("GetExecution(%s): %v", ids[0], err)
		}
		if got.TriggerKind != "timer" {
			t.Fatalf("TriggerKind = %q, want %q", got.TriggerKind, "timer")
		}
		if got.NodeTotal != 4 {
			t.Fatalf("NodeTotal = %d, want 4", got.NodeTotal)
		}
		if got.FinishedAt == nil || !got.FinishedAt.Equal(finishedAt) {
			t.Fatalf("FinishedAt = %v, want %v", got.FinishedAt, finishedAt)
		}

		plain, err := s.GetExecution(ctx, ids[1])
		if err != nil {
			t.Fatalf("GetExecution(%s): %v", ids[1], err)
		}
		if plain.TriggerKind != "" || plain.NodeTotal != 0 || plain.FinishedAt != nil {
			t.Fatalf("a row written without the new columns read back as %q/%d/%v, want empty/0/nil",
				plain.TriggerKind, plain.NodeTotal, plain.FinishedAt)
		}

		// The listing projection must carry the same columns: the API's
		// execution list reads them from here.
		listed, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Query: string(ids[0])}, store.DefaultListOptions())
		if err != nil {
			t.Fatalf("ListExecutions(%s): %v", ids[0], err)
		}
		if len(listed) != 1 {
			t.Fatalf("ListExecutions(%s) returned %d rows, want 1", ids[0], len(listed))
		}
		if listed[0].TriggerKind != "timer" || listed[0].NodeTotal != 4 || listed[0].FinishedAt == nil || !listed[0].FinishedAt.Equal(finishedAt) {
			t.Fatalf("listed row lost the new columns: trigger_kind=%q node_total=%d finished_at=%v",
				listed[0].TriggerKind, listed[0].NodeTotal, listed[0].FinishedAt)
		}
	})

	// --- finished_at is write-once ------------------------------------------
	t.Run("finished_at_is_write_once", func(t *testing.T) {
		nsFin := namespace.Namespace(nsPrefix + "-fin")
		id := types.ExecutionID(nsPrefix + "-fin-1")
		created := base
		if err := s.CreateExecution(ctx, &store.ExecutionRecord{
			ExecutionID: id, Namespace: string(nsFin), WorkflowDef: emptyWorkflowDef,
			Status: types.ExecutionStatusRunning, CreatedAt: created, UpdatedAt: created,
		}); err != nil {
			t.Fatalf("CreateExecution(%s): %v", id, err)
		}
		read := func(when string) *store.ExecutionRecord {
			t.Helper()
			rec, err := s.GetExecution(ctx, id)
			if err != nil {
				t.Fatalf("GetExecution(%s): %v", id, err)
			}
			return rec
		}

		// A non-terminal transition does not set the fact.
		if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling): %v", err)
		}
		if rec := read("after canceling"); rec.FinishedAt != nil {
			t.Fatalf("FinishedAt = %v after a non-terminal transition, want nil", rec.FinishedAt)
		}

		// The first terminal transition sets it.
		if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(success): %v", err)
		}
		first := read("after success")
		if first.FinishedAt == nil {
			t.Fatal("FinishedAt = nil after the first terminal transition, want the completion instant")
		}

		// A duplicate terminal write (a retry, a cancel/complete race) must not
		// move it, even though the status and error do change.
		if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, "late failure"); err != nil {
			t.Fatalf("UpdateExecutionStatus(failed): %v", err)
		}
		second := read("after failed")
		if second.Status != types.ExecutionStatusFailed || second.Error != "late failure" {
			t.Fatalf("second terminal write did not apply status/error: %+v", second)
		}
		if second.FinishedAt == nil || !second.FinishedAt.Equal(*first.FinishedAt) {
			t.Fatalf("FinishedAt = %v after a duplicate terminal write, want the first instant %v",
				second.FinishedAt, first.FinishedAt)
		}

		// A later non-terminal transition cannot clear it either.
		if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling again): %v", err)
		}
		if third := read("after second canceling"); third.FinishedAt == nil || !third.FinishedAt.Equal(*first.FinishedAt) {
			t.Fatalf("FinishedAt = %v after a later non-terminal transition, want the first instant %v",
				third.FinishedAt, first.FinishedAt)
		}
	})

	// --- namespace round trip ---------------------------------------------
	t.Run("namespace_round_trips", func(t *testing.T) {
		got, err := s.GetExecution(ctx, ids[0])
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if got.Namespace != string(nsA) {
			t.Fatalf("GetExecution returned namespace %q, want %q; the column was not persisted "+
				"or not read back", got.Namespace, nsA)
		}
		listed, err := s.ListExecutions(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatusSuccess}, store.DefaultListOptions())
		if err != nil {
			t.Fatalf("ListExecutions: %v", err)
		}
		for _, rec := range listed {
			if rec.Namespace != string(nsA) {
				t.Fatalf("listed row %s carries namespace %q, want %q; the listing projection "+
					"dropped the column", rec.ExecutionID, rec.Namespace, nsA)
			}
		}
	})
}

// contractExecIDs builds ids that are unique per backend and per run but still
// fit types.ExecutionID's varchar(64) column, and sorts in a known order for
// readability of failures. The prefix is a legal namespace, so the ids are too.
func contractExecIDs(nsPrefix string) []types.ExecutionID {
	suffixes := []string{"a1", "a2", "a3", "b1", "b2", "orphan"}
	out := make([]types.ExecutionID, 0, len(suffixes))
	for _, s := range suffixes {
		out = append(out, types.ExecutionID(fmt.Sprintf("%s-%s", nsPrefix, s)))
	}
	return out
}

// mustListAll lists with the unbounded options. A refused scope returns nil
// rather than failing the test, because the empty scope is not a legal scope and
// the callers that probe it assert on the refusal separately.
func mustListAll(t *testing.T, s ExecutionsStore, ctx context.Context, ns namespace.Namespace) []*store.ExecutionRecord {
	t.Helper()
	got, err := s.ListExecutions(ctx, ns, store.ExecutionFilter{}, store.ListOptions{})
	if err != nil {
		if errors.Is(err, store.ErrInvalidNamespace) {
			return nil
		}
		t.Fatalf("ListExecutions(ns=%q): %v", ns, err)
	}
	return got
}

func mustCount(t *testing.T, s ExecutionsStore, ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) int64 {
	t.Helper()
	total, err := s.CountExecutions(ctx, ns, filter)
	if err != nil {
		t.Fatalf("CountExecutions(ns=%q, filter=%+v): %v", ns, filter, err)
	}
	return total
}

func executionIDs(recs []*store.ExecutionRecord) []types.ExecutionID {
	out := make([]types.ExecutionID, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.ExecutionID)
	}
	return out
}

// assertExactly compares a listing against the exact set and order expected,
// and separately reports a duplicate so the failure says which row repeated
// rather than only that the sequences differ.
func assertExactly(t *testing.T, what string, got []*store.ExecutionRecord, want []types.ExecutionID) {
	t.Helper()
	gotIDs := executionIDs(got)
	if equalIDs(gotIDs, want) {
		return
	}
	seen := map[types.ExecutionID]int{}
	for _, id := range gotIDs {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("%s = %v, want %v: row %s appears %d times", what, gotIDs, want, id, n)
		}
	}
	t.Fatalf("%s = %v, want %v", what, gotIDs, want)
}

func equalIDs(a, b []types.ExecutionID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func repeatByte(c byte, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = c
	}
	return string(buf)
}
