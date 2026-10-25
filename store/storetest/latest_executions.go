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

// LatestExecutionStore is the surface LatestExecutionsContract needs: write
// fixtures, then read them back through the optional capability. It is
// declared narrowly so a backend that does not implement the full
// store.Executions can still run the contract; store.LatestExecutionReader is
// satisfied by both the memstore and store.Executions implementations that
// choose to provide it.
type LatestExecutionStore interface {
	CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error
	LatestExecutions(ctx context.Context, ns namespace.Namespace, workflowIDs []types.WorkflowID) (map[types.WorkflowID]*store.ExecutionRecord, error)
}

// LatestExecutionsContract exercises the optional batch "newest execution per
// workflow" capability against one backend. Every backend that implements
// store.LatestExecutionReader must pass it: the memstore and the SQL provider
// are contract-tested against each other here, and a drift between them is a
// wrong "latest run" column rather than an inconvenience.
//
// nsPrefix disambiguates fixtures between backends and between reruns against
// a persistent database. It must be a legal namespace (no ':' and no glob
// metacharacters, namespace.Validate).
func LatestExecutionsContract(t *testing.T, s LatestExecutionStore, nsPrefix string) {
	t.Helper()
	ctx := context.Background()

	nsA := namespace.Namespace(nsPrefix + "-a")
	nsB := namespace.Namespace(nsPrefix + "-b")
	nsEmpty := namespace.Namespace(nsPrefix + "-empty")

	alpha := types.WorkflowID("wfid-alpha")
	beta := types.WorkflowID("wfid-beta")
	tie := types.WorkflowID("wfid-tie")
	gamma := types.WorkflowID("wfid-gamma")
	missing := types.WorkflowID("wfid-missing")

	execID := func(suffix string) types.ExecutionID {
		return types.ExecutionID(fmt.Sprintf("%s-%s", nsPrefix, suffix))
	}

	// Millisecond-truncated timestamps: MySQL's DATETIME(3) and SQLite's
	// time format both store milliseconds, and the equality used for the
	// tie-break must mean the same thing on both sides of the contract.
	base := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)

	// Fixtures. Two properties are built in deliberately:
	//
	//   - The newest row for wfid-alpha in nsA is created FIRST. An
	//     implementation that returns "the last row written" (or orders by the
	//     auto-increment id) therefore returns a-alpha-old, while the contract
	//     demands a-alpha-new.
	//   - Several nsA rows (gamma, the unattributed row) are newer than every
	//     wfid-alpha row, so an implementation that forgets to group by
	//     workflow_id returns one of those instead of alpha's own newest.
	//
	// nsB's wfid-alpha row is newer than all of nsA's rows. A lookup for nsA
	// that returns it has crossed the namespace boundary, which is the one
	// failure this capability must never have.
	seed := []*store.ExecutionRecord{
		{ExecutionID: execID("a-alpha-new"), Namespace: string(nsA), WorkflowID: alpha, WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(20 * time.Second), UpdatedAt: base.Add(20 * time.Second)},
		{ExecutionID: execID("a-alpha-old"), Namespace: string(nsA), WorkflowID: alpha, WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusFailed, CreatedAt: base.Add(1 * time.Second), UpdatedAt: base.Add(1 * time.Second)},
		{ExecutionID: execID("a-beta-only"), Namespace: string(nsA), WorkflowID: beta, WorkflowName: "wf-beta", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusRunning, CreatedAt: base.Add(2 * time.Second), UpdatedAt: base.Add(2 * time.Second)},
		// tie-1 and tie-2 share a timestamp; ascending ExecutionID makes tie-1
		// the newest. tie-2 is inserted second so a last-write-wins bug is
		// visible as the wrong row rather than merely as a flaky one.
		{ExecutionID: execID("a-tie-1"), Namespace: string(nsA), WorkflowID: tie, WorkflowName: "wf-tie", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(30 * time.Second), UpdatedAt: base.Add(30 * time.Second)},
		{ExecutionID: execID("a-tie-2"), Namespace: string(nsA), WorkflowID: tie, WorkflowName: "wf-tie", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusFailed, CreatedAt: base.Add(30 * time.Second), UpdatedAt: base.Add(30 * time.Second)},
		{ExecutionID: execID("a-gamma"), Namespace: string(nsA), WorkflowID: gamma, WorkflowName: "wf-gamma", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(40 * time.Second), UpdatedAt: base.Add(40 * time.Second)},
		// An unattributed-workflow row: it is in nsA, but it names no workflow.
		// No requested id may claim it, and the empty id (which would match it
		// literally) must not be passed to storage.
		{ExecutionID: execID("a-unattributed"), Namespace: string(nsA), WorkflowID: "", WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusSuccess, CreatedAt: base.Add(50 * time.Second), UpdatedAt: base.Add(50 * time.Second)},
		// Same workflow id in another namespace, newer than every nsA row.
		{ExecutionID: execID("b-alpha"), Namespace: string(nsB), WorkflowID: alpha, WorkflowName: "wf-alpha", WorkflowDef: emptyWorkflowDef, Status: types.ExecutionStatusTimeout, CreatedAt: base.Add(60 * time.Second), UpdatedAt: base.Add(60 * time.Second)},
	}
	for _, rec := range seed {
		if err := s.CreateExecution(ctx, rec); err != nil {
			t.Fatalf("CreateExecution(%s, ns=%q): %v", rec.ExecutionID, rec.Namespace, err)
		}
	}

	t.Run("latest_per_workflow", func(t *testing.T) {
		got, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{alpha, beta, missing})
		if err != nil {
			t.Fatalf("LatestExecutions: %v", err)
		}
		if got == nil {
			t.Fatal("LatestExecutions returned a nil map; the contract requires a non-nil map so callers can index it")
		}
		if len(got) != 2 {
			t.Fatalf("LatestExecutions returned %d entries, want 2 (alpha and beta; missing is absent): %v",
				len(got), latestIDs(got))
		}
		alphaRec, ok := got[alpha]
		if !ok {
			t.Fatalf("LatestExecutions has no entry for %q", alpha)
		}
		if alphaRec.ExecutionID != execID("a-alpha-new") {
			t.Fatalf("latest for %q = %s, want %s (CreatedAt is what makes it newest; it was inserted first)",
				alpha, alphaRec.ExecutionID, execID("a-alpha-new"))
		}
		if alphaRec.WorkflowID != alpha {
			t.Fatalf("latest for %q carries workflow id %q", alpha, alphaRec.WorkflowID)
		}
		if alphaRec.Namespace != string(nsA) {
			t.Fatalf("latest for %q carries namespace %q, want %q", alpha, alphaRec.Namespace, nsA)
		}
		if alphaRec.Status != types.ExecutionStatusSuccess {
			t.Fatalf("latest for %q carries status %q, want %q", alpha, alphaRec.Status, types.ExecutionStatusSuccess)
		}
		betaRec, ok := got[beta]
		if !ok {
			t.Fatalf("LatestExecutions has no entry for %q", beta)
		}
		if betaRec.ExecutionID != execID("a-beta-only") {
			t.Fatalf("latest for %q = %s, want %s", beta, betaRec.ExecutionID, execID("a-beta-only"))
		}
		if _, ok := got[missing]; ok {
			t.Fatalf("LatestExecutions returned an entry for %q, which has no execution in this namespace", missing)
		}
	})

	t.Run("only_requested_workflows", func(t *testing.T) {
		got, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{beta})
		if err != nil {
			t.Fatalf("LatestExecutions: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("LatestExecutions returned %d entries for one requested id: %v", len(got), latestIDs(got))
		}
		if rec, ok := got[beta]; !ok || rec.ExecutionID != execID("a-beta-only") {
			t.Fatalf("LatestExecutions(beta) = %v, want exactly %s", latestIDs(got), execID("a-beta-only"))
		}
		for _, unrequested := range []types.WorkflowID{alpha, tie, gamma, ""} {
			if _, ok := got[unrequested]; ok {
				t.Fatalf("LatestExecutions returned an entry for unrequested workflow id %q", unrequested)
			}
		}
	})

	t.Run("duplicate_requested_ids", func(t *testing.T) {
		got, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{alpha, beta, alpha, beta})
		if err != nil {
			t.Fatalf("LatestExecutions: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("LatestExecutions with duplicate ids returned %d entries, want 2: %v", len(got), latestIDs(got))
		}
		if rec := got[alpha]; rec == nil || rec.ExecutionID != execID("a-alpha-new") {
			t.Fatalf("latest for duplicated %q = %v, want %s", alpha, rec, execID("a-alpha-new"))
		}
	})

	t.Run("tie_break_is_total_and_deterministic", func(t *testing.T) {
		got, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{tie})
		if err != nil {
			t.Fatalf("LatestExecutions: %v", err)
		}
		rec, ok := got[tie]
		if !ok {
			t.Fatalf("LatestExecutions has no entry for %q", tie)
		}
		if rec.ExecutionID != execID("a-tie-1") {
			t.Fatalf("latest for %q = %s, want %s: equal CreatedAt is broken by ExecutionID ASCENDING",
				tie, rec.ExecutionID, execID("a-tie-1"))
		}
		// Repeated reads must agree. A partial order is not guaranteed to
		// differ on any single repeat, so check several.
		for i := 0; i < 8; i++ {
			again, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{tie})
			if err != nil {
				t.Fatalf("LatestExecutions (attempt %d): %v", i, err)
			}
			if gotRec := again[tie]; gotRec == nil || gotRec.ExecutionID != rec.ExecutionID {
				t.Fatalf("latest for %q changed between reads with no write in between: %s then %v; "+
					"the CreatedAt/ExecutionID order is not total", tie, rec.ExecutionID, gotRec)
			}
		}
	})

	t.Run("namespace_isolation", func(t *testing.T) {
		gotA, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{alpha})
		if err != nil {
			t.Fatalf("LatestExecutions(nsA): %v", err)
		}
		if rec := gotA[alpha]; rec == nil || rec.ExecutionID != execID("a-alpha-new") {
			t.Fatalf("latest for %q in nsA = %v, want %s; nsB has a NEWER row under the same "+
				"workflow id and it must not cross the scope", alpha, rec, execID("a-alpha-new"))
		}

		gotB, err := s.LatestExecutions(ctx, nsB, []types.WorkflowID{alpha})
		if err != nil {
			t.Fatalf("LatestExecutions(nsB): %v", err)
		}
		recB := gotB[alpha]
		if recB == nil || recB.ExecutionID != execID("b-alpha") {
			t.Fatalf("latest for %q in nsB = %v, want %s", alpha, recB, execID("b-alpha"))
		}
		if recB.Namespace != string(nsB) {
			t.Fatalf("latest for %q in nsB carries namespace %q, want %q", alpha, recB.Namespace, nsB)
		}

		// A valid namespace with no executions answers "nothing", not an error.
		gotEmpty, err := s.LatestExecutions(ctx, nsEmpty, []types.WorkflowID{alpha, beta})
		if err != nil {
			t.Fatalf("LatestExecutions(nsEmpty): %v", err)
		}
		if gotEmpty == nil {
			t.Fatal("LatestExecutions on an empty namespace returned a nil map")
		}
		if len(gotEmpty) != 0 {
			t.Fatalf("LatestExecutions on an empty namespace returned %d entries", len(gotEmpty))
		}
	})

	t.Run("empty_request_is_an_empty_map", func(t *testing.T) {
		for _, ids := range [][]types.WorkflowID{nil, {}} {
			got, err := s.LatestExecutions(ctx, nsA, ids)
			if err != nil {
				t.Fatalf("LatestExecutions(%v): %v", ids, err)
			}
			if got == nil {
				t.Fatalf("LatestExecutions(%v) returned a nil map; an empty request must return an empty map", ids)
			}
			if len(got) != 0 {
				t.Fatalf("LatestExecutions(%v) returned %d entries", ids, len(got))
			}
		}
	})

	t.Run("invalid_namespace_is_refused", func(t *testing.T) {
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
			t.Run(tc.name, func(t *testing.T) {
				got, err := s.LatestExecutions(ctx, tc.ns, []types.WorkflowID{alpha})
				if !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("LatestExecutions(ns=%q) error = %v, want store.ErrInvalidNamespace; "+
						"an unusable scope must be REFUSED, never widened", tc.ns, err)
				}
				if got != nil {
					t.Fatalf("LatestExecutions(ns=%q) returned %d entries alongside an error", tc.ns, len(got))
				}
				// The refusal must not depend on there being something to read:
				// an empty request against an unusable scope is still a refusal,
				// not a free pass.
				if got, err := s.LatestExecutions(ctx, tc.ns, nil); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("LatestExecutions(ns=%q, no ids) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				} else if got != nil {
					t.Fatalf("LatestExecutions(ns=%q, no ids) returned %d entries alongside an error", tc.ns, len(got))
				}
			})
		}
	})

	t.Run("overlong_workflow_id_cannot_match", func(t *testing.T) {
		long := types.WorkflowID(repeatByte('w', store.MaxExecutionWorkflowIDLen+1))
		got, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{long, alpha})
		if err != nil {
			t.Fatalf("LatestExecutions with an over-long id: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("LatestExecutions with an over-long id returned %d entries, want only %q: %v",
				len(got), alpha, latestIDs(got))
		}
		if _, ok := got[long]; ok {
			t.Fatal("LatestExecutions returned an entry for an over-long workflow id; it can never match a stored id")
		}
		if rec := got[alpha]; rec == nil || rec.ExecutionID != execID("a-alpha-new") {
			t.Fatalf("latest for %q = %v, want %s; one unusable id must not drop the usable ones",
				alpha, rec, execID("a-alpha-new"))
		}

		only, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{long})
		if err != nil {
			t.Fatalf("LatestExecutions with only an over-long id: %v", err)
		}
		if only == nil || len(only) != 0 {
			t.Fatalf("LatestExecutions with only an over-long id = %v, want a non-nil empty map", only)
		}
	})

	t.Run("unattributed_workflow_is_never_claimed", func(t *testing.T) {
		// The empty id names no workflow. If an implementation passed it to
		// storage it would select the unattributed row in nsA, which is newer
		// than alpha's newest and would therefore be returned under "".
		got, err := s.LatestExecutions(ctx, nsA, []types.WorkflowID{alpha, ""})
		if err != nil {
			t.Fatalf("LatestExecutions: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("LatestExecutions(alpha, \"\") returned %d entries, want only %q: %v",
				len(got), alpha, latestIDs(got))
		}
		if _, ok := got[""]; ok {
			t.Fatal("LatestExecutions returned the unattributed-workflow row under the empty id; " +
				"unattributed executions must never be attributed to a workflow")
		}
		if rec := got[alpha]; rec == nil || rec.ExecutionID != execID("a-alpha-new") {
			t.Fatalf("latest for %q = %v, want %s", alpha, rec, execID("a-alpha-new"))
		}
	})
}

func latestIDs(got map[types.WorkflowID]*store.ExecutionRecord) []types.WorkflowID {
	out := make([]types.WorkflowID, 0, len(got))
	for id := range got {
		out = append(out, id)
	}
	return out
}
