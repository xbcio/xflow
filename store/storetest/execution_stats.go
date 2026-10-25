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

// ExecutionStatsStore is the surface ExecutionStatsContract needs: write
// fixtures, read them back through the optional capability, and compare
// namespace-wide totals against the plain count. It is declared narrowly so a
// backend can run the contract without implementing the full store.Executions.
type ExecutionStatsStore interface {
	CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error
	// UpsertNode writes a node row. ExecutionNodeProgress is an aggregate over
	// these rows, so the contract needs a way to create them; it is the same
	// store.Nodes.UpsertNode the runtime uses, not a test-only seam.
	UpsertNode(ctx context.Context, rec *store.NodeRecord) error
	CountExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (int64, error)
	store.ExecutionStatsReader
}

// ExecutionStatsContract exercises the optional execution-statistics capability
// against one backend. Every backend that implements store.ExecutionStatsReader
// MUST pass it: the memstore and the SQL provider are contract-tested against
// each other here, and a drift between them is a wrong dashboard, not an
// inconvenience.
//
// The properties pinned below are the ones a plausible implementation gets
// wrong: namespace isolation (including unattributed rows), exclusive window
// bounds, the agreement between status counts and CountExecutions, the
// nearest-rank percentile on a fixed sample, the "most recent ≤ sampleCap"
// sample rule, recency order with its deterministic tie-break, and UTC-aligned
// histogram buckets with zero-filled gaps.
//
// nsPrefix disambiguates fixtures between backends and between reruns against a
// persistent database. It must be a legal namespace (no ':' and no glob
// metacharacters, namespace.Validate).
func ExecutionStatsContract(t *testing.T, s ExecutionStatsStore, nsPrefix string) {
	t.Helper()
	ctx := context.Background()

	nsA := namespace.Namespace(nsPrefix + "-a")
	nsB := namespace.Namespace(nsPrefix + "-b")
	nsEmpty := namespace.Namespace(nsPrefix + "-empty")

	alpha := types.WorkflowID("wfid-alpha")
	beta := types.WorkflowID("wfid-beta")
	gamma := types.WorkflowID("wfid-gamma")
	missing := types.WorkflowID("wfid-missing")

	// A fixed, UTC, whole-hour anchor: the histogram assertions below depend on
	// bucket alignment, not on when the suite happens to run. Every instant is
	// millisecond-exact, the precision both DATETIME(3) and sqlitestore keep.
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	execID := func(suffix string) types.ExecutionID {
		return types.ExecutionID(fmt.Sprintf("%s-%s", nsPrefix, suffix))
	}
	record := func(suffix string, ns namespace.Namespace, workflow types.WorkflowID, status types.ExecutionStatus, created time.Time, endedAfter time.Duration) *store.ExecutionRecord {
		return &store.ExecutionRecord{
			ExecutionID:  execID(suffix),
			Namespace:    string(ns),
			WorkflowID:   workflow,
			WorkflowName: "wf-" + string(workflow),
			WorkflowDef:  emptyWorkflowDef,
			Status:       status,
			CreatedAt:    created,
			UpdatedAt:    created.Add(endedAfter),
		}
	}

	// Fixtures, and why each exists:
	//
	//   - alpha has one row of every terminal status plus a running row whose
	//     huge UpdatedAt delta would dominate a percentile if non-terminal rows
	//     were sampled;
	//   - gamma holds two rows at the SAME CreatedAt, so the recent-list
	//     tie-break (ExecutionID ascending) is observable;
	//   - a-unattributed is in nsA but names no workflow: it counts in the
	//     namespace-wide totals (CountExecutions counts it) and must be absent
	//     from every per-workflow aggregation;
	//   - nsB holds a newer alpha row, and a row with an EMPTY namespace exists
	//     under alpha too: neither may cross into nsA's aggregates.
	seed := []*store.ExecutionRecord{
		record("a-alpha-1", nsA, alpha, types.ExecutionStatusSuccess, base.Add(1*time.Hour), 100*time.Millisecond),
		record("a-alpha-2", nsA, alpha, types.ExecutionStatusFailed, base.Add(2*time.Hour), 500*time.Millisecond),
		record("a-alpha-3", nsA, alpha, types.ExecutionStatusCanceled, base.Add(3*time.Hour), 300*time.Millisecond),
		record("a-alpha-4", nsA, alpha, types.ExecutionStatusTimeout, base.Add(4*time.Hour), 700*time.Millisecond),
		record("a-alpha-5", nsA, alpha, types.ExecutionStatusRunning, base.Add(5*time.Hour), 9000*time.Millisecond),
		record("a-beta-1", nsA, beta, types.ExecutionStatusSuccess, base.Add(30*time.Minute), 50*time.Millisecond),
		record("a-beta-2", nsA, beta, types.ExecutionStatusSuccess, base.Add(90*time.Minute), 150*time.Millisecond),
		record("a-beta-3", nsA, beta, types.ExecutionStatusFailed, base.Add(150*time.Minute), 250*time.Millisecond),
		record("a-gamma-1", nsA, gamma, types.ExecutionStatusSuccess, base.Add(2*time.Hour), 200*time.Millisecond),
		record("a-gamma-2", nsA, gamma, types.ExecutionStatusFailed, base.Add(2*time.Hour), 400*time.Millisecond),
		record("a-unattributed", nsA, "", types.ExecutionStatusSuccess, base.Add(10*time.Minute), 20*time.Millisecond),
		record("b-alpha", nsB, alpha, types.ExecutionStatusSuccess, base.Add(6*time.Hour), 600*time.Millisecond),
		record("unscoped-alpha", "", alpha, types.ExecutionStatusSuccess, base.Add(20*time.Minute), 30*time.Millisecond),
	}
	// Failure narratives: TopFailingWorkflows must report the NEWEST failure's
	// error per workflow. alpha's newest failure is the timeout at +4h, not the
	// failed row at +2h.
	seed[1].Error = "alpha failed"
	seed[3].Error = "alpha timeout"
	seed[7].Error = "beta failed"
	seed[9].Error = "gamma failed"
	for _, rec := range seed {
		if err := s.CreateExecution(ctx, rec); err != nil {
			t.Fatalf("CreateExecution(%s, ns=%q): %v", rec.ExecutionID, rec.Namespace, err)
		}
	}

	assertCounts := func(t *testing.T, got, want map[types.ExecutionStatus]int64) {
		t.Helper()
		if got == nil {
			t.Fatal("counts map is nil; the contract requires a non-nil map with every status key")
		}
		for _, status := range store.ExecutionStatuses() {
			if _, ok := got[status]; !ok {
				t.Fatalf("counts map is missing status %q (every known status must be present, zero-filled)", status)
			}
			if got[status] != want[status] {
				t.Fatalf("count %q = %d, want %d (all counts: %v)", status, got[status], want[status], got)
			}
		}
		var sum int64
		for _, n := range got {
			sum += n
		}
		var wantSum int64
		for _, n := range want {
			wantSum += n
		}
		if sum != wantSum {
			t.Fatalf("counts sum = %d, want %d", sum, wantSum)
		}
	}

	t.Run("invalid_namespace_is_refused", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			ns   namespace.Namespace
		}{
			{name: "empty", ns: ""},
			{name: "colon delimiter", ns: namespace.Namespace(nsPrefix + ":a")},
			{name: "glob metacharacter", ns: namespace.Namespace(nsPrefix + "-*")},
			{name: "too long", ns: namespace.Namespace(repeatByte('n', namespace.MaxNameLen+1))},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := s.CountExecutionsByStatus(ctx, tc.ns, store.ExecutionFilter{}); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("CountExecutionsByStatus(ns=%q) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				// The refusal must not depend on there being something to read:
				// an empty request against an unusable scope is still a refusal.
				if _, err := s.WorkflowExecutionStats(ctx, tc.ns, nil, time.Time{}, 0); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("WorkflowExecutionStats(ns=%q, no ids) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				if _, err := s.RecentExecutions(ctx, tc.ns, nil, 0); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("RecentExecutions(ns=%q, no ids) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				if _, err := s.ExecutionHistogram(ctx, tc.ns, store.ExecutionFilter{}, time.Hour); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("ExecutionHistogram(ns=%q) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				if _, err := s.ExecutionNodeProgress(ctx, tc.ns, nil); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("ExecutionNodeProgress(ns=%q, no ids) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				if _, err := s.ExecutionDurations(ctx, tc.ns, store.ExecutionFilter{}, 10); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("ExecutionDurations(ns=%q) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				if _, err := s.CountActiveWorkflows(ctx, tc.ns, store.ExecutionFilter{}); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("CountActiveWorkflows(ns=%q) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
				if _, err := s.TopFailingWorkflows(ctx, tc.ns, time.Time{}, 5); !errors.Is(err, store.ErrInvalidNamespace) {
					t.Fatalf("TopFailingWorkflows(ns=%q) error = %v, want store.ErrInvalidNamespace", tc.ns, err)
				}
			})
		}
	})

	t.Run("counts_by_status_agree_with_count_executions", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			ns     namespace.Namespace
			filter store.ExecutionFilter
			want   map[types.ExecutionStatus]int64
		}{
			{
				name: "namespace totals include the unattributed workflow",
				ns:   nsA,
				want: map[types.ExecutionStatus]int64{
					types.ExecutionStatusSuccess:  5,
					types.ExecutionStatusFailed:   3,
					types.ExecutionStatusCanceled: 1,
					types.ExecutionStatusTimeout:  1,
					types.ExecutionStatusRunning:  1,
				},
			},
			{
				name:   "workflow filter",
				ns:     nsA,
				filter: store.ExecutionFilter{WorkflowID: alpha},
				want: map[types.ExecutionStatus]int64{
					types.ExecutionStatusSuccess:  1,
					types.ExecutionStatusFailed:   1,
					types.ExecutionStatusCanceled: 1,
					types.ExecutionStatusTimeout:  1,
					types.ExecutionStatusRunning:  1,
				},
			},
			{
				name:   "status filter",
				ns:     nsA,
				filter: store.ExecutionFilter{Status: types.ExecutionStatusSuccess},
				want:   map[types.ExecutionStatus]int64{types.ExecutionStatusSuccess: 5},
			},
			{
				name: "another namespace is isolated",
				ns:   nsB,
				want: map[types.ExecutionStatus]int64{types.ExecutionStatusSuccess: 1},
			},
			{
				name: "a namespace with no executions answers zeroes",
				ns:   nsEmpty,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := s.CountExecutionsByStatus(ctx, tc.ns, tc.filter)
				if err != nil {
					t.Fatalf("CountExecutionsByStatus: %v", err)
				}
				assertCounts(t, got, tc.want)

				total, err := s.CountExecutions(ctx, tc.ns, tc.filter)
				if err != nil {
					t.Fatalf("CountExecutions: %v", err)
				}
				var sum int64
				for _, n := range got {
					sum += n
				}
				if sum != total {
					t.Fatalf("sum of status counts = %d, CountExecutions = %d; the two must describe the same set", sum, total)
				}
			})
		}

		if _, err := s.CountExecutionsByStatus(ctx, nsA, store.ExecutionFilter{Status: "not-a-status"}); err == nil {
			t.Fatal("CountExecutionsByStatus accepted an unknown status filter; a typo must fail loudly, not read as zero rows")
		}
	})

	t.Run("window_bounds_are_exclusive", func(t *testing.T) {
		// alpha-1 sits exactly ON the lower bound and alpha-3 exactly ON the
		// upper one, so a half-open-window bug shows up as 3 rows instead of 1.
		filter := store.ExecutionFilter{
			WorkflowID:    alpha,
			CreatedAfter:  base.Add(1 * time.Hour),
			CreatedBefore: base.Add(3 * time.Hour),
		}
		got, err := s.CountExecutionsByStatus(ctx, nsA, filter)
		if err != nil {
			t.Fatalf("CountExecutionsByStatus: %v", err)
		}
		assertCounts(t, got, map[types.ExecutionStatus]int64{types.ExecutionStatusFailed: 1})
		if total, err := s.CountExecutions(ctx, nsA, filter); err != nil || total != 1 {
			t.Fatalf("CountExecutions = %d, %v; want 1, nil (both bounds exclusive)", total, err)
		}
	})

	t.Run("workflow_stats", func(t *testing.T) {
		got, err := s.WorkflowExecutionStats(ctx, nsA, []types.WorkflowID{alpha, beta, missing}, base, 100)
		if err != nil {
			t.Fatalf("WorkflowExecutionStats: %v", err)
		}
		if got == nil {
			t.Fatal("WorkflowExecutionStats returned a nil map")
		}
		if len(got) != 2 {
			t.Fatalf("WorkflowExecutionStats returned %d entries, want 2 (missing has no executions): %v", len(got), got)
		}

		a, ok := got[alpha]
		if !ok {
			t.Fatalf("no stats for %q", alpha)
		}
		if a.Runs != 5 || a.Succeeded != 1 || a.Failed != 2 {
			t.Fatalf("alpha stats = %+v, want runs=5 succeeded=1 failed=2 (failed counts failed+timeout)", a)
		}
		if a.SuccessRate == nil || *a.SuccessRate != 0.25 {
			t.Fatalf("alpha success_rate = %v, want 0.25 (1 success / 4 terminal; canceled is in the denominator)", a.SuccessRate)
		}
		if a.SampleSize != 4 {
			t.Fatalf("alpha sample_size = %d, want 4 (the running row must not be sampled)", a.SampleSize)
		}
		// Durations 100, 500, 300, 700 ms; nearest-rank: p50 = 2nd smallest = 300,
		// p95 = 4th = 700.
		if a.P50MS == nil || *a.P50MS != 300 {
			t.Fatalf("alpha p50 = %v, want 300", a.P50MS)
		}
		if a.P95MS == nil || *a.P95MS != 700 {
			t.Fatalf("alpha p95 = %v, want 700 (9000 would mean the running row leaked into the sample)", a.P95MS)
		}

		b, ok := got[beta]
		if !ok {
			t.Fatalf("no stats for %q", beta)
		}
		// 2/3 is not exactly representable in binary; compare within 1e-9.
		if b.SuccessRate == nil || *b.SuccessRate < 0.666666666 || *b.SuccessRate > 0.666666667 {
			t.Fatalf("beta success_rate = %v, want 2/3", b.SuccessRate)
		}
		// Durations 50, 150, 250: p50 = 2nd = 150, p95 = 3rd = 250.
		if b.P50MS == nil || *b.P50MS != 150 {
			t.Fatalf("beta p50 = %v, want 150", b.P50MS)
		}
		if b.P95MS == nil || *b.P95MS != 250 {
			t.Fatalf("beta p95 = %v, want 250", b.P95MS)
		}

		if _, ok := got[missing]; ok {
			t.Fatalf("WorkflowExecutionStats invented an entry for %q, which has no executions", missing)
		}

		// nsB's newer alpha row and the empty-namespace row must not leak in.
		gotB, err := s.WorkflowExecutionStats(ctx, nsB, []types.WorkflowID{alpha}, base, 100)
		if err != nil {
			t.Fatalf("WorkflowExecutionStats(nsB): %v", err)
		}
		if bStats := gotB[alpha]; bStats.Runs != 1 || bStats.Succeeded != 1 {
			t.Fatalf("alpha stats in nsB = %+v, want runs=1 succeeded=1", bStats)
		}
	})

	t.Run("workflow_stats_window_and_sample_cap", func(t *testing.T) {
		// since=base+1h excludes alpha-1 (exactly at the bound).
		got, err := s.WorkflowExecutionStats(ctx, nsA, []types.WorkflowID{alpha}, base.Add(time.Hour), 0)
		if err != nil {
			t.Fatalf("WorkflowExecutionStats: %v", err)
		}
		a := got[alpha]
		if a.Runs != 4 || a.Succeeded != 0 || a.Failed != 2 {
			t.Fatalf("alpha windowed stats = %+v, want runs=4 succeeded=0 failed=2 (failed includes the timeout)", a)
		}
		if a.SuccessRate == nil || *a.SuccessRate != 0 {
			t.Fatalf("alpha windowed success_rate = %v, want a non-nil 0 (there are terminal rows)", a.SuccessRate)
		}
		if a.SampleSize != 0 || a.P50MS != nil || a.P95MS != nil {
			t.Fatalf("sampleCap 0 must produce no percentile sample: %+v", a)
		}

		// The cap keeps the MOST RECENT terminal rows: alpha-4 (700ms),
		// alpha-3 (300ms), alpha-2 (500ms) → sorted 300,500,700 → p50=500 p95=700.
		got, err = s.WorkflowExecutionStats(ctx, nsA, []types.WorkflowID{alpha}, base, 3)
		if err != nil {
			t.Fatalf("WorkflowExecutionStats (cap 3): %v", err)
		}
		a = got[alpha]
		if a.SampleSize != 3 {
			t.Fatalf("sample_size = %d, want 3", a.SampleSize)
		}
		if a.P50MS == nil || *a.P50MS != 500 {
			t.Fatalf("capped p50 = %v, want 500 (nearest-rank 2nd of 300,500,700)", a.P50MS)
		}
		if a.P95MS == nil || *a.P95MS != 700 {
			t.Fatalf("capped p95 = %v, want 700", a.P95MS)
		}
	})

	t.Run("recent_executions", func(t *testing.T) {
		got, err := s.RecentExecutions(ctx, nsA, []types.WorkflowID{alpha, beta, gamma, missing}, 2)
		if err != nil {
			t.Fatalf("RecentExecutions: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("RecentExecutions returned %d entries, want 3 (missing absent): %v", len(got), got)
		}
		wantAlpha := []types.ExecutionID{execID("a-alpha-5"), execID("a-alpha-4")}
		assertSummaryIDs(t, "alpha", got[alpha], wantAlpha)
		wantBeta := []types.ExecutionID{execID("a-beta-3"), execID("a-beta-2")}
		assertSummaryIDs(t, "beta", got[beta], wantBeta)
		// Equal CreatedAt: the ascending ExecutionID is the documented tie-break,
		// so gamma-1 wins even though gamma-2 was inserted last.
		wantGamma := []types.ExecutionID{execID("a-gamma-1"), execID("a-gamma-2")}
		assertSummaryIDs(t, "gamma", got[gamma], wantGamma)

		full, err := s.RecentExecutions(ctx, nsA, []types.WorkflowID{alpha}, 10)
		if err != nil {
			t.Fatalf("RecentExecutions (10): %v", err)
		}
		wantFull := []types.ExecutionID{
			execID("a-alpha-5"), execID("a-alpha-4"), execID("a-alpha-3"),
			execID("a-alpha-2"), execID("a-alpha-1"),
		}
		assertSummaryIDs(t, "alpha (10)", full[alpha], wantFull)
		if summary := full[alpha][0]; summary.Status != types.ExecutionStatusRunning {
			t.Fatalf("newest alpha row status = %q, want running (newest is newest regardless of status)", summary.Status)
		}
		if summary := full[alpha][0]; !summary.CreatedAt.Equal(base.Add(5*time.Hour)) || !summary.UpdatedAt.Equal(base.Add(5*time.Hour+9*time.Second)) {
			t.Fatalf("newest alpha timestamps = %v / %v, want the row's own instants", summary.CreatedAt, summary.UpdatedAt)
		}

		// The unattributed row in nsA is never claimed by any id, and the newer
		// alpha row in nsB never crosses the scope.
		empty, err := s.RecentExecutions(ctx, nsA, []types.WorkflowID{missing}, 10)
		if err != nil {
			t.Fatalf("RecentExecutions(missing): %v", err)
		}
		if empty == nil || len(empty) != 0 {
			t.Fatalf("RecentExecutions(missing) = %v, want a non-nil empty map", empty)
		}
		if _, ok := got[""]; ok {
			t.Fatal("RecentExecutions returned entries under the empty workflow id; unattributed rows must never be claimed")
		}
	})

	t.Run("recent_executions_normalizes_ids", func(t *testing.T) {
		long := types.WorkflowID(repeatByte('w', store.MaxExecutionWorkflowIDLen+1))
		got, err := s.RecentExecutions(ctx, nsA, []types.WorkflowID{"", long, alpha, alpha}, 1)
		if err != nil {
			t.Fatalf("RecentExecutions: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("RecentExecutions returned %d entries, want only alpha: %v", len(got), got)
		}
		assertSummaryIDs(t, "alpha", got[alpha], []types.ExecutionID{execID("a-alpha-5")})

		none, err := s.RecentExecutions(ctx, nsA, []types.WorkflowID{alpha}, 0)
		if err != nil {
			t.Fatalf("RecentExecutions(perWorkflow=0): %v", err)
		}
		if none == nil || len(none) != 0 {
			t.Fatalf("RecentExecutions(perWorkflow=0) = %v, want a non-nil empty map", none)
		}
	})

	t.Run("histogram_buckets_and_empty_buckets", func(t *testing.T) {
		filter := store.ExecutionFilter{
			CreatedAfter:  base,
			CreatedBefore: base.Add(5 * time.Hour),
		}
		buckets, err := s.ExecutionHistogram(ctx, nsA, filter, time.Hour)
		if err != nil {
			t.Fatalf("ExecutionHistogram: %v", err)
		}
		if len(buckets) != 5 {
			t.Fatalf("ExecutionHistogram returned %d buckets, want 5", len(buckets))
		}
		wants := []map[types.ExecutionStatus]int64{
			{types.ExecutionStatusSuccess: 2},
			{types.ExecutionStatusSuccess: 2},
			{types.ExecutionStatusSuccess: 1, types.ExecutionStatusFailed: 3},
			{types.ExecutionStatusCanceled: 1},
			{types.ExecutionStatusTimeout: 1},
		}
		var sum int64
		for i, bucket := range buckets {
			if want := base.Add(time.Duration(i) * time.Hour); !bucket.Start.Equal(want) {
				t.Fatalf("bucket %d start = %v, want %v (UTC-aligned)", i, bucket.Start, want)
			}
			if bucket.Start.Location() != time.UTC {
				t.Fatalf("bucket %d start location = %v, want UTC", i, bucket.Start.Location())
			}
			assertCounts(t, bucket.Counts, wants[i])
			for _, n := range bucket.Counts {
				sum += n
			}
		}
		total, err := s.CountExecutions(ctx, nsA, filter)
		if err != nil {
			t.Fatalf("CountExecutions: %v", err)
		}
		if sum != total {
			t.Fatalf("histogram sum = %d, CountExecutions = %d; the buckets must tile the filtered set", sum, total)
		}

		// A 30-minute bucket width over the same window yields gaps; every gap
		// is present with all-zero counts.
		halfHour, err := s.ExecutionHistogram(ctx, nsA, filter, 30*time.Minute)
		if err != nil {
			t.Fatalf("ExecutionHistogram (30m): %v", err)
		}
		if len(halfHour) != 10 {
			t.Fatalf("30m histogram returned %d buckets, want 10", len(halfHour))
		}
		// [4h30m, 5h) holds no row; [4h, 4h30m) holds alpha-4's timeout.
		for _, i := range []int{9} {
			assertCounts(t, halfHour[i].Counts, map[types.ExecutionStatus]int64{})
			if want := base.Add(4*time.Hour + 30*time.Minute); !halfHour[i].Start.Equal(want) {
				t.Fatalf("gap bucket start = %v, want %v", halfHour[i].Start, want)
			}
		}
		assertCounts(t, halfHour[8].Counts, map[types.ExecutionStatus]int64{types.ExecutionStatusTimeout: 1})

		// A window that starts mid-bucket still begins at the aligned instant.
		offsetFilter := store.ExecutionFilter{
			CreatedAfter:  base.Add(5 * time.Minute),
			CreatedBefore: base.Add(90 * time.Minute),
		}
		offset, err := s.ExecutionHistogram(ctx, nsA, offsetFilter, time.Hour)
		if err != nil {
			t.Fatalf("ExecutionHistogram (offset window): %v", err)
		}
		if len(offset) != 2 {
			t.Fatalf("offset histogram returned %d buckets, want 2", len(offset))
		}
		if !offset[0].Start.Equal(base) {
			t.Fatalf("first bucket start = %v, want aligned base %v", offset[0].Start, base)
		}
		// base+10m (unattributed, success) and base+30m (beta-1, success).
		assertCounts(t, offset[0].Counts, map[types.ExecutionStatus]int64{types.ExecutionStatusSuccess: 2})
		// alpha-1 at exactly base+1h is inside this window (its upper bound is
		// base+90m); beta-2 at exactly base+90m is outside it, so the bucket
		// holds only alpha-1.
		assertCounts(t, offset[1].Counts, map[types.ExecutionStatus]int64{types.ExecutionStatusSuccess: 1})
	})

	t.Run("histogram_refuses_an_unbounded_or_oversized_window", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			filter store.ExecutionFilter
			bucket time.Duration
		}{
			{name: "no bounds", filter: store.ExecutionFilter{}, bucket: time.Hour},
			{name: "no upper bound", filter: store.ExecutionFilter{CreatedAfter: base}, bucket: time.Hour},
			{name: "no lower bound", filter: store.ExecutionFilter{CreatedBefore: base.Add(time.Hour)}, bucket: time.Hour},
			{name: "inverted", filter: store.ExecutionFilter{CreatedAfter: base.Add(time.Hour), CreatedBefore: base}, bucket: time.Hour},
			{name: "zero bucket", filter: store.ExecutionFilter{CreatedAfter: base, CreatedBefore: base.Add(time.Hour)}, bucket: 0},
			{name: "negative bucket", filter: store.ExecutionFilter{CreatedAfter: base, CreatedBefore: base.Add(time.Hour)}, bucket: -time.Hour},
			{name: "sub-second bucket", filter: store.ExecutionFilter{CreatedAfter: base, CreatedBefore: base.Add(time.Second)}, bucket: 500 * time.Millisecond},
			{
				name:   "more than 168 buckets",
				filter: store.ExecutionFilter{CreatedAfter: base, CreatedBefore: base.Add(30 * 24 * time.Hour)},
				bucket: 5 * time.Minute,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := s.ExecutionHistogram(ctx, nsA, tc.filter, tc.bucket)
				if !errors.Is(err, store.ErrInvalidExecutionStatsWindow) {
					t.Fatalf("ExecutionHistogram error = %v, want store.ErrInvalidExecutionStatsWindow", err)
				}
				if got != nil {
					t.Fatalf("ExecutionHistogram returned %d buckets alongside an error", len(got))
				}
			})
		}
	})

	t.Run("node_progress", func(t *testing.T) {
		// alpha-1 gets three nodes: retries = (1-1) + (3-1) + (2-1) = 3 and
		// done = 2 (success and failed are terminal; running is not).
		nodes := []*store.NodeRecord{
			{ExecutionID: execID("a-alpha-1"), NodeName: "n1", NodeType: "action", Status: types.NodeStatusSuccess, Attempt: 1},
			{ExecutionID: execID("a-alpha-1"), NodeName: "n2", NodeType: "action", Status: types.NodeStatusFailed, Attempt: 3},
			{ExecutionID: execID("a-alpha-1"), NodeName: "n3", NodeType: "action", Status: types.NodeStatusRunning, Attempt: 2},
			// A node in another tenant exists, but must never be answered under
			// nsA even when its execution id is guessed.
			{ExecutionID: execID("b-alpha"), NodeName: "n1", NodeType: "action", Status: types.NodeStatusSuccess, Attempt: 1},
		}
		for _, node := range nodes {
			if err := s.UpsertNode(ctx, node); err != nil {
				t.Fatalf("UpsertNode(%s/%s): %v", node.ExecutionID, node.NodeName, err)
			}
		}

		got, err := s.ExecutionNodeProgress(ctx, nsA, []types.ExecutionID{
			execID("a-alpha-1"),      // has nodes
			execID("a-alpha-2"),      // exists, no nodes: absent, not zero
			execID("b-alpha"),        // exists in another namespace: dropped
			execID("unscoped-alpha"), // namespace "": dropped
			"no-such-execution",      // unknown: dropped
		})
		if err != nil {
			t.Fatalf("ExecutionNodeProgress: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("ExecutionNodeProgress returned %d entries, want only a-alpha-1: %v", len(got), got)
		}
		p, ok := got[execID("a-alpha-1")]
		if !ok {
			t.Fatalf("ExecutionNodeProgress has no entry for a-alpha-1: %v", got)
		}
		if p.Retries != 3 {
			t.Fatalf("retries = %d, want 3 (attempts 1,2,3 contribute 0+1+2)", p.Retries)
		}
		if p.Done != 2 {
			t.Fatalf("done = %d, want 2 (success and failed are terminal; running is not)", p.Done)
		}

		empty, err := s.ExecutionNodeProgress(ctx, nsA, nil)
		if err != nil {
			t.Fatalf("ExecutionNodeProgress(nil): %v", err)
		}
		if empty == nil || len(empty) != 0 {
			t.Fatalf("ExecutionNodeProgress(nil) = %v, want a non-nil empty map", empty)
		}

		if _, err := s.ExecutionNodeProgress(ctx, "", []types.ExecutionID{execID("a-alpha-1")}); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("ExecutionNodeProgress(empty scope) error = %v, want store.ErrInvalidNamespace", err)
		}
	})

	t.Run("execution_durations", func(t *testing.T) {
		terminal := store.ExecutionFilter{Statuses: store.TerminalExecutionStatuses()}

		got, err := s.ExecutionDurations(ctx, nsA, terminal, 100)
		if err != nil {
			t.Fatalf("ExecutionDurations: %v", err)
		}
		if got.Sampled {
			t.Fatal("Sampled = true even though every matching row was returned")
		}
		// Newest first, in store.ExecutionOrder (created_at DESC, id DESC).
		// Every fixture row has no FinishedAt, so the documented UpdatedAt
		// fallback is what produces the duration (the fixture's endedAfter). The
		// three rows created at +2h come back in descending insertion order:
		// gamma-2 (400), gamma-1 (200), alpha-2 (500).
		want := []int64{700, 300, 250, 400, 200, 500, 150, 100, 50, 20}
		if len(got.DurationsMS) != len(want) {
			t.Fatalf("ExecutionDurations returned %d durations, want %d: %v", len(got.DurationsMS), len(want), got.DurationsMS)
		}
		for i := range want {
			if got.DurationsMS[i] != want[i] {
				t.Fatalf("duration %d = %d, want %d (all: %v)", i, got.DurationsMS[i], want[i], got.DurationsMS)
			}
		}

		// limit + 1 is the truncation probe: three rows wanted, more exist.
		capped, err := s.ExecutionDurations(ctx, nsA, terminal, 3)
		if err != nil {
			t.Fatalf("ExecutionDurations(limit 3): %v", err)
		}
		if !capped.Sampled {
			t.Fatal("Sampled = false when more terminal rows matched than the limit")
		}
		if len(capped.DurationsMS) != 3 || capped.DurationsMS[0] != 700 || capped.DurationsMS[1] != 300 || capped.DurationsMS[2] != 250 {
			t.Fatalf("capped durations = %v, want [700 300 250]", capped.DurationsMS)
		}

		// A non-terminal row is skipped even when the filter names its status.
		running, err := s.ExecutionDurations(ctx, nsA, store.ExecutionFilter{Status: types.ExecutionStatusRunning}, 10)
		if err != nil {
			t.Fatalf("ExecutionDurations(running): %v", err)
		}
		if len(running.DurationsMS) != 0 || running.Sampled {
			t.Fatalf("ExecutionDurations(running) = %+v, want no durations", running)
		}

		if zero, err := s.ExecutionDurations(ctx, nsA, terminal, 0); err != nil || len(zero.DurationsMS) != 0 {
			t.Fatalf("ExecutionDurations(limit 0) = %+v, %v; want no durations", zero, err)
		}

		// FinishedAt wins over UpdatedAt when both are present, so a retry that
		// bumps updated_at cannot stretch a finished run's duration.
		nsDur := namespace.Namespace(nsPrefix + "-dur")
		delta := types.WorkflowID("wfid-delta")
		finished := base.Add(time.Second)
		finishedRec := record("dur-finished", nsDur, delta, types.ExecutionStatusSuccess, base, 9*time.Second)
		finishedRec.FinishedAt = &finished
		estimatedRec := record("dur-estimated", nsDur, delta, types.ExecutionStatusSuccess, base, 2*time.Second)
		for _, rec := range []*store.ExecutionRecord{finishedRec, estimatedRec} {
			if err := s.CreateExecution(ctx, rec); err != nil {
				t.Fatalf("CreateExecution(%s): %v", rec.ExecutionID, err)
			}
		}
		// Same CreatedAt, so the row inserted second (larger id) comes first:
		// dur-estimated's 2000ms (updated_at fallback), then dur-finished's
		// 1000ms (finished_at wins over its 9000ms updated_at delta).
		precise, err := s.ExecutionDurations(ctx, nsDur, terminal, 10)
		if err != nil {
			t.Fatalf("ExecutionDurations(finished_at): %v", err)
		}
		if len(precise.DurationsMS) != 2 || precise.DurationsMS[0] != 2000 || precise.DurationsMS[1] != 1000 {
			t.Fatalf("durations with finished_at = %v, want [2000 1000] (finished_at must win over updated_at)", precise.DurationsMS)
		}
	})

	t.Run("count_active_workflows", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			ns     namespace.Namespace
			filter store.ExecutionFilter
			want   int64
		}{
			{name: "namespace totals", ns: nsA, want: 3}, // alpha, beta, gamma; unattributed excluded
			{name: "status filter", ns: nsA, filter: store.ExecutionFilter{Status: types.ExecutionStatusSuccess}, want: 3},  // alpha, beta, gamma
			{name: "window filter", ns: nsA, filter: store.ExecutionFilter{CreatedAfter: base.Add(2 * time.Hour)}, want: 2}, // alpha + beta
			{name: "another namespace", ns: nsB, want: 1},
			{name: "empty namespace", ns: nsEmpty, want: 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := s.CountActiveWorkflows(ctx, tc.ns, tc.filter)
				if err != nil {
					t.Fatalf("CountActiveWorkflows: %v", err)
				}
				if got != tc.want {
					t.Fatalf("CountActiveWorkflows = %d, want %d", got, tc.want)
				}
			})
		}
		if _, err := s.CountActiveWorkflows(ctx, "", store.ExecutionFilter{}); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("CountActiveWorkflows(empty scope) error = %v, want store.ErrInvalidNamespace", err)
		}
	})

	t.Run("top_failing_workflows", func(t *testing.T) {
		got, err := s.TopFailingWorkflows(ctx, nsA, base.Add(-time.Hour), 10)
		if err != nil {
			t.Fatalf("TopFailingWorkflows: %v", err)
		}
		// alpha has two failures (failed at +2h, timeout at +4h); beta and gamma
		// one each, ordered by workflow id ascending on the tie.
		if len(got) != 3 {
			t.Fatalf("TopFailingWorkflows returned %d rows, want 3: %v", len(got), got)
		}
		if got[0].WorkflowID != alpha || got[0].Failed != 2 {
			t.Fatalf("rank 0 = %s (%d failures), want alpha with 2", got[0].WorkflowID, got[0].Failed)
		}
		if got[1].WorkflowID != beta || got[2].WorkflowID != gamma {
			t.Fatalf("tie order = %s, %s, want beta then gamma (workflow id ascending)", got[1].WorkflowID, got[2].WorkflowID)
		}
		// alpha's newest failure is the timeout at +4h; its error is what the
		// dashboard must show, not the +2h failure's.
		if got[0].LastError != "alpha timeout" {
			t.Fatalf("alpha last_error = %q, want %q", got[0].LastError, "alpha timeout")
		}
		if want := base.Add(4*time.Hour + 700*time.Millisecond); !got[0].LastFailedAt.Equal(want) {
			t.Fatalf("alpha last_failed_at = %v, want %v (updated_at fallback when finished_at is nil)", got[0].LastFailedAt, want)
		}
		if got[1].LastError != "beta failed" {
			t.Fatalf("beta last_error = %q, want %q", got[1].LastError, "beta failed")
		}
		if got[2].LastError != "gamma failed" {
			t.Fatalf("gamma last_error = %q, want %q", got[2].LastError, "gamma failed")
		}

		capped, err := s.TopFailingWorkflows(ctx, nsA, base.Add(-time.Hour), 2)
		if err != nil {
			t.Fatalf("TopFailingWorkflows(limit 2): %v", err)
		}
		if len(capped) != 2 || capped[0].WorkflowID != alpha || capped[1].WorkflowID != beta {
			t.Fatalf("capped ranking = %v, want [alpha beta]", capped)
		}

		// since is exclusive: only alpha-4's timeout at +4h stays.
		windowed, err := s.TopFailingWorkflows(ctx, nsA, base.Add(3*time.Hour), 10)
		if err != nil {
			t.Fatalf("TopFailingWorkflows(since): %v", err)
		}
		if len(windowed) != 1 || windowed[0].WorkflowID != alpha || windowed[0].Failed != 1 {
			t.Fatalf("windowed ranking = %v, want only alpha with 1 failure", windowed)
		}

		if zeroSince, err := s.TopFailingWorkflows(ctx, nsA, time.Time{}, 10); err != nil || len(zeroSince) != 3 {
			t.Fatalf("TopFailingWorkflows(zero since) = %v, %v; a zero since must mean unbounded", zeroSince, err)
		}

		empty, err := s.TopFailingWorkflows(ctx, nsEmpty, base.Add(-time.Hour), 10)
		if err != nil {
			t.Fatalf("TopFailingWorkflows(empty namespace): %v", err)
		}
		if empty == nil || len(empty) != 0 {
			t.Fatalf("TopFailingWorkflows(empty namespace) = %v, want a non-nil empty slice", empty)
		}

		if _, err := s.TopFailingWorkflows(ctx, "", base, 10); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("TopFailingWorkflows(empty scope) error = %v, want store.ErrInvalidNamespace", err)
		}
	})

	t.Run("recent_executions_finished_at", func(t *testing.T) {
		nsRecent := namespace.Namespace(nsPrefix + "-recent")
		epsilon := types.WorkflowID("wfid-epsilon")
		finished := base.Add(time.Second)
		withFinish := record("recent-finished", nsRecent, epsilon, types.ExecutionStatusSuccess, base, 9*time.Second)
		withFinish.FinishedAt = &finished
		withoutFinish := record("recent-estimated", nsRecent, epsilon, types.ExecutionStatusFailed, base, 2*time.Second)
		for _, rec := range []*store.ExecutionRecord{withFinish, withoutFinish} {
			if err := s.CreateExecution(ctx, rec); err != nil {
				t.Fatalf("CreateExecution(%s): %v", rec.ExecutionID, err)
			}
		}
		got, err := s.RecentExecutions(ctx, nsRecent, []types.WorkflowID{epsilon}, 10)
		if err != nil {
			t.Fatalf("RecentExecutions: %v", err)
		}
		rows := got[epsilon]
		if len(rows) != 2 {
			t.Fatalf("RecentExecutions returned %d rows, want 2: %v", len(rows), rows)
		}
		// Same CreatedAt: ascending id puts "recent-estimated" first.
		if rows[0].ExecutionID != execID("recent-estimated") || rows[0].FinishedAt != nil {
			t.Fatalf("row 0 = %s with finished_at %v, want recent-estimated with nil", rows[0].ExecutionID, rows[0].FinishedAt)
		}
		if rows[1].ExecutionID != execID("recent-finished") {
			t.Fatalf("row 1 = %s, want recent-finished", rows[1].ExecutionID)
		}
		if rows[1].FinishedAt == nil || !rows[1].FinishedAt.Equal(finished) {
			t.Fatalf("row 1 finished_at = %v, want %v", rows[1].FinishedAt, finished)
		}
	})
}

func assertSummaryIDs(t *testing.T, label string, got []store.ExecutionSummary, want []types.ExecutionID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d summaries, want %d: %v", label, len(got), len(want), summaryIDs(got))
	}
	for i := range want {
		if got[i].ExecutionID != want[i] {
			t.Fatalf("%s: summary %d = %s, want %s (full order: %v)", label, i, got[i].ExecutionID, want[i], summaryIDs(got))
		}
	}
}

func summaryIDs(got []store.ExecutionSummary) []types.ExecutionID {
	out := make([]types.ExecutionID, len(got))
	for i, s := range got {
		out[i] = s.ExecutionID
	}
	return out
}
