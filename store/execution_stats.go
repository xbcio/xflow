package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// MaxExecutionStatsBuckets is the hard ceiling on the number of histogram
// buckets one ExecutionHistogram call may materialize. It is a cost bound, not
// a preference: the caller's window and bucket width are client input, and an
// unconstrained pair (30 days at one-second buckets) would turn one request
// into millions of rows of response body. The API layer refuses a request that
// would exceed it with a 400; the store refuses it too, because the store is
// the last line that any caller — including a future one that is not this HTTP
// handler — reaches.
const MaxExecutionStatsBuckets = 168

// MaxExecutionStatsSampleCap is the ceiling on the terminal-row sample a
// percentile is computed from, per workflow. Percentiles are the one
// aggregation this package cannot delegate to SQL (neither MySQL 8 nor SQLite
// ships a percentile function), so they are computed in Go from at most this
// many narrow rows per workflow — never from the whole workflow's history.
const MaxExecutionStatsSampleCap = 100

// MaxRecentExecutionsPerWorkflow is the ceiling on the "recent runs" list a
// workflow list row may carry.
const MaxRecentExecutionsPerWorkflow = 10

// ErrInvalidExecutionStatsWindow is returned when a statistics request names a
// window or bucket that cannot be answered as asked: an unbounded or inverted
// window, a non-positive or sub-second bucket, or a bucket count above
// MaxExecutionStatsBuckets. It is a caller-input refusal (the HTTP layer maps
// it to 400), never a storage failure.
var ErrInvalidExecutionStatsWindow = errors.New("store: invalid execution statistics window")

// ErrExecutionStatsUnsupported is returned by an ExecutionStatsReader that
// cannot answer for its backing storage (for example a SQL dialect with no
// bucket expression). It is "this backend cannot do this", not "the read
// failed", and the HTTP layer maps it to 501 so a client can tell the two
// apart.
var ErrExecutionStatsUnsupported = errors.New("store: execution statistics unsupported")

// StatusBucket is one bucket of an execution histogram: the bucket's
// UTC-aligned start instant and how many executions in it hold each status.
// Counts always carries every known status key (zero-filled), so a consumer
// never has to distinguish "no rows with this status" from "status not
// reported".
type StatusBucket struct {
	Start  time.Time
	Counts map[types.ExecutionStatus]int64
}

// ExecutionSummary is the narrow projection behind a "recent executions" list:
// identity, status and the two timestamps. It deliberately carries none of a
// record's JSON columns (workflow_def, params, runtime).
//
// FinishedAt is the exact completion instant, write-once on the terminal
// transition. It is nil for a live execution and for a row written before the
// column existed; a reader must fall back to UpdatedAt for those (and say the
// duration is an estimate) rather than treating the absence as zero.
type ExecutionSummary struct {
	ExecutionID types.ExecutionID
	Status      types.ExecutionStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
	FinishedAt  *time.Time
}

// WorkflowStats is the per-workflow aggregate behind a workflow list row's
// `stats` object.
//
// Terminal means one of success, failed, canceled, timeout — queued, paused,
// running, canceling, pending and (the persisted default) pending are not
// terminal. Runs counts every row in the window, terminal or not. Succeeded
// counts success; Failed counts failed + timeout, the two statuses that mean
// "this run did not do what it was asked". SuccessRate is Succeeded divided by
// the number of TERMINAL runs (canceled included in the denominator), and is
// nil when there is no terminal run to divide by — absent is not zero.
//
// P50MS/P95MS are nearest-rank percentiles (rank = ceil(p/100 × n), clamped to
// [1,n]) over the durations of the most recent terminal rows, at most
// MaxExecutionStatsSampleCap of them. They are nil when the sample is empty.
// SampleSize is how many rows the percentiles were computed from, so a caller
// can tell a 3-sample p95 from a 100-sample one.
type WorkflowStats struct {
	Runs        int64
	Succeeded   int64
	Failed      int64
	SuccessRate *float64
	P50MS       *int64
	P95MS       *int64
	SampleSize  int
}

// MaxExecutionDurationSample bounds how many terminal-row durations one
// overview percentile read may sample. Like MaxExecutionStatsSampleCap it is a
// cost bound: percentiles are computed in Go, so a dashboard refresh must not
// be able to pull a namespace's whole history into memory.
const MaxExecutionDurationSample = 5000

// MaxTopFailingWorkflows is the fixed size of the overview's failing-workflow
// list.
const MaxTopFailingWorkflows = 5

// ExecutionNodeProgress is the per-execution node aggregate behind a list row's
// retries and node_progress.done fields.
//
// Retries is the sum of (attempt − 1) over the execution's node rows: how many
// extra attempts the execution's nodes consumed. Done counts the node rows that
// reached a terminal status. Neither is a substitute for the node bodies; this
// is a narrow aggregate, not a detail read.
type ExecutionNodeProgress struct {
	Retries int
	Done    int
}

// ExecutionDurationSample is the terminal-duration sample behind an overview
// percentile. DurationsMS are whole milliseconds, newest first. Sampled is true
// when more terminal rows matched than the requested cap, so the caller knows
// the percentiles describe a sample rather than the whole window.
type ExecutionDurationSample struct {
	DurationsMS []int64
	Sampled     bool
}

// FailingWorkflow is one row of the overview's "top failing workflows" list.
// LastError/LastFailedAt describe the newest failure in the window; both are
// omitted (empty/zero) when that row could not be read rather than guessed.
type FailingWorkflow struct {
	WorkflowID   types.WorkflowID
	Failed       int64
	LastError    string
	LastFailedAt time.Time
}

// ExecutionDurationMS returns a terminal execution's duration, and whether that
// duration is an ESTIMATE.
//
// When FinishedAt is known the answer is exact. When it is nil — a live row, or
// one written before the column existed — the fallback is updated_at −
// created_at, flagged estimated so a caller can label it. Non-terminal rows have
// no duration at all (nil, not zero). A clock-skewed inverted interval is
// reported as zero rather than as a negative number.
func ExecutionDurationMS(status types.ExecutionStatus, createdAt, updatedAt time.Time, finishedAt *time.Time) (*int64, bool) {
	if !IsTerminalExecutionStatus(status) {
		return nil, false
	}
	end := updatedAt
	estimated := true
	if finishedAt != nil {
		end = *finishedAt
		estimated = false
	}
	ms := end.Sub(createdAt).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return &ms, estimated
}

// ExecutionStatsReader is an optional capability of an execution store: the
// aggregations behind GET /v1/executions/stats and the `stats` /
// `recent_executions` fields of a workflow list. It follows
// LatestExecutionReader's pattern exactly — an optional interface rather than
// methods on Executions, so a backend that cannot answer these reads is not
// obliged to grow them, and every backend that DOES implement it must be run
// through store/storetest.ExecutionStatsContract.
//
// Every method takes ns as a REQUIRED SCOPE with the fail-closed rule of
// ListExecutions: empty or malformed => ErrInvalidNamespace, checked even when
// there is nothing to read, and never widened across namespaces. Rows whose
// persisted namespace is empty (unattributed) match no scope. Aggregations are
// performed over one namespace only; there is no cross-namespace entry point on
// this interface at all.
type ExecutionStatsReader interface {
	// CountExecutionsByStatus returns, per status, the number of executions in
	// ns that match filter. Every known status key is present (zero-filled), so
	// callers never treat a missing key as anything but zero.
	CountExecutionsByStatus(ctx context.Context, ns namespace.Namespace, filter ExecutionFilter) (map[types.ExecutionStatus]int64, error)

	// ExecutionHistogram returns the status counts of ns's executions in
	// filter's window, grouped into UTC-aligned buckets of the given width.
	//
	// filter MUST carry both CreatedAfter and CreatedBefore (exclusive, as
	// everywhere in ExecutionFilter); bucket must be a positive whole number of
	// seconds, and the resulting bucket count must not exceed
	// MaxExecutionStatsBuckets. Anything else is ErrInvalidExecutionStatsWindow.
	// Every bucket in [aligned start, CreatedBefore) is returned, including
	// empty ones (all-zero counts), in ascending Start order.
	ExecutionHistogram(ctx context.Context, ns namespace.Namespace, filter ExecutionFilter, bucket time.Duration) ([]StatusBucket, error)

	// WorkflowExecutionStats returns per-workflow counts for the requested ids
	// in ns, over executions created strictly after since (since zero means
	// unbounded). Ids with no execution in the window are ABSENT from the map;
	// an empty ids slice returns an empty, non-nil map.
	//
	// Percentiles use at most sampleCap of the most recent terminal rows per
	// workflow (newest first by CreatedAt descending, then ExecutionID
	// ascending). sampleCap <= 0 means "counts only, no percentiles"; a value
	// above MaxExecutionStatsSampleCap is clamped to it. Rows with an empty
	// WorkflowID (unattributed) are never attributed to any requested id.
	WorkflowExecutionStats(ctx context.Context, ns namespace.Namespace, ids []types.WorkflowID, since time.Time, sampleCap int) (map[types.WorkflowID]WorkflowStats, error)

	// RecentExecutions returns, per requested id, that workflow's newest
	// executions in ns — newest first (CreatedAt descending, then ExecutionID
	// ascending), at most perWorkflow of them. perWorkflow <= 0 means "none";
	// a value above MaxRecentExecutionsPerWorkflow is clamped to it. Ids with
	// no execution are absent from the map; an empty ids slice returns an
	// empty, non-nil map.
	RecentExecutions(ctx context.Context, ns namespace.Namespace, ids []types.WorkflowID, perWorkflow int) (map[types.WorkflowID][]ExecutionSummary, error)

	// ExecutionNodeProgress returns the node aggregate for the requested
	// executions in ns. An id with no node rows is ABSENT from the map; an empty
	// ids slice returns an empty, non-nil map. The scope is enforced against the
	// executions table, not just the ids: a forged id from another namespace is
	// dropped rather than answered.
	//
	// The projection is narrow by contract: retry count and terminal-node count
	// only. Node output, ports, signal payloads and lease tokens are never part
	// of it.
	ExecutionNodeProgress(ctx context.Context, ns namespace.Namespace, ids []types.ExecutionID) (map[types.ExecutionID]ExecutionNodeProgress, error)

	// ExecutionDurations returns the durations of the executions matching
	// filter, newest first, at most limit of them (limit is clamped to
	// MaxExecutionDurationSample). It is the percentile input for the overview:
	// callers select the terminal statuses in the filter and compute the
	// percentile in Go with NearestRankPercentileMS.
	//
	// Durations use FinishedAt when present and UpdatedAt as the documented
	// fallback for rows written before that column existed. Non-terminal rows
	// are skipped even if the filter names one.
	ExecutionDurations(ctx context.Context, ns namespace.Namespace, filter ExecutionFilter, limit int) (ExecutionDurationSample, error)

	// CountActiveWorkflows returns how many DISTINCT workflow ids have at least
	// one execution matching filter in ns. Rows with an empty workflow id
	// (unattributed) are never counted — they name no workflow.
	CountActiveWorkflows(ctx context.Context, ns namespace.Namespace, filter ExecutionFilter) (int64, error)

	// TopFailingWorkflows returns up to limit workflows with the most executions
	// in the failed/timeout statuses created after since, ordered by failure
	// count descending then workflow id ascending so the answer is deterministic.
	// Rows with an empty workflow id are excluded. limit is clamped to
	// MaxTopFailingWorkflows.
	TopFailingWorkflows(ctx context.Context, ns namespace.Namespace, since time.Time, limit int) ([]FailingWorkflow, error)
}

// ExecutionStatuses returns every lifecycle status an execution row may hold,
// in a stable declaration order. It exists so the zero-filled maps this
// package's readers promise (CountExecutionsByStatus, StatusBucket.Counts) all
// agree on the key set, and so a caller can render the same columns whether or
// not a row has ever held that status. The slice is a copy: mutating it cannot
// corrupt the package's own view.
func ExecutionStatuses() []types.ExecutionStatus {
	out := make([]types.ExecutionStatus, len(allExecutionStatuses))
	copy(out, allExecutionStatuses)
	return out
}

var allExecutionStatuses = []types.ExecutionStatus{
	types.ExecutionStatusPending,
	types.ExecutionStatusQueued,
	types.ExecutionStatusPaused,
	types.ExecutionStatusRunning,
	types.ExecutionStatusSuccess,
	types.ExecutionStatusFailed,
	types.ExecutionStatusCanceling,
	types.ExecutionStatusCanceled,
	types.ExecutionStatusTimeout,
}

// TerminalExecutionStatuses returns the statuses an execution cannot leave:
// success, failed, canceled, timeout. It is the denominator of a success rate
// and the sample set of a duration percentile.
func TerminalExecutionStatuses() []types.ExecutionStatus {
	return []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout,
	}
}

// IsTerminalExecutionStatus reports whether s is one of the terminal statuses
// above.
func IsTerminalExecutionStatus(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout:
		return true
	}
	return false
}

// ExecutionFailureStatuses returns the statuses an overview counts as a failed
// run: failed and timeout. Canceled is terminal but deliberate, so it is not a
// failure; success obviously is not. Both backends use this one definition so
// the KPI's failure count and the top-failing list cannot diverge.
func ExecutionFailureStatuses() []types.ExecutionStatus {
	return []types.ExecutionStatus{
		types.ExecutionStatusFailed,
		types.ExecutionStatusTimeout,
	}
}

// ZeroedExecutionStatusCounts returns a map containing every known status with
// a zero count. Both backends start their projections from it, so "all keys,
// missing = 0" is one implementation rather than two that can drift.
func ZeroedExecutionStatusCounts() map[types.ExecutionStatus]int64 {
	out := make(map[types.ExecutionStatus]int64, len(allExecutionStatuses))
	for _, s := range allExecutionStatuses {
		out[s] = 0
	}
	return out
}

// NormalizeExecutionStatsSampleCap applies the documented sampleCap rules:
// a non-positive value selects "no sample" (0), and anything above the ceiling
// is clamped. It is exported so both backends clamp identically.
func NormalizeExecutionStatsSampleCap(sampleCap int) int {
	if sampleCap <= 0 {
		return 0
	}
	if sampleCap > MaxExecutionStatsSampleCap {
		return MaxExecutionStatsSampleCap
	}
	return sampleCap
}

// NormalizeExecutionWorkflowIDs drops the ids that can never name a stored
// workflow — the empty id (which could only select unattributed rows, and must
// never be attributed to a workflow) and one longer than the persisted column —
// and deduplicates the rest while preserving first-seen order. Both backends
// use it before building a query, so "which ids reach storage" is one rule.
func NormalizeExecutionWorkflowIDs(ids []types.WorkflowID) []types.WorkflowID {
	out := make([]types.WorkflowID, 0, len(ids))
	seen := make(map[types.WorkflowID]struct{}, len(ids))
	for _, id := range ids {
		if id == "" || ValidateExecutionWorkflowID(string(id)) != nil {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// NormalizeRecentExecutionsPerWorkflow applies the documented
// MaxRecentExecutionsPerWorkflow rule to a per-workflow limit.
func NormalizeRecentExecutionsPerWorkflow(perWorkflow int) int {
	if perWorkflow <= 0 {
		return 0
	}
	if perWorkflow > MaxRecentExecutionsPerWorkflow {
		return MaxRecentExecutionsPerWorkflow
	}
	return perWorkflow
}

// NormalizeExecutionDurationLimit applies the documented
// MaxExecutionDurationSample rule to a duration-sample limit.
func NormalizeExecutionDurationLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > MaxExecutionDurationSample {
		return MaxExecutionDurationSample
	}
	return limit
}

// NormalizeTopFailingLimit applies the documented MaxTopFailingWorkflows rule.
func NormalizeTopFailingLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > MaxTopFailingWorkflows {
		return MaxTopFailingWorkflows
	}
	return limit
}

// NearestRankPercentileMS returns the nearest-rank percentile (p in (0,100]) of
// durations, in whole milliseconds, truncating each duration toward zero.
// rank = ceil(p/100 × n), clamped to [1,n]; the result is the rank-th smallest
// value, so for n=4 p=50 the result is the 2nd smallest and for p=95 the 4th.
//
// It returns nil for an empty sample. Negative durations (a row whose
// updated_at precedes its created_at, possible only under clock skew) count as
// zero, so a percentile can never be negative.
//
// This helper is exported because the two backends must compute the same
// percentile from the same sample; it is a shared definition, not a general
// statistics library.
func NearestRankPercentileMS(durations []time.Duration, p float64) *int64 {
	n := len(durations)
	if n == 0 {
		return nil
	}
	ms := make([]int64, n)
	for i, d := range durations {
		v := d.Milliseconds()
		if v < 0 {
			v = 0
		}
		ms[i] = v
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] < ms[j] })
	rank := int(float64(n)*p/100.0 + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	v := ms[rank-1]
	return &v
}

// ExecutionHistogramBuckets validates a histogram request and returns the
// bucket starts it covers, in ascending order. It is the one definition of
// "which buckets exist" for both backends, so the memstore and SQL providers
// cannot disagree about alignment or the empty-bucket set.
//
// The starts are epoch-aligned UTC instants (a 1h bucket starts on the hour,
// a 1d bucket at midnight UTC). The first bucket is the one containing
// filter.CreatedAfter — it may begin before it, since the bound itself is
// exclusive — and buckets continue while their start is before
// filter.CreatedBefore. An unbounded window, an inverted one, a sub-second or
// non-positive bucket, or more than MaxExecutionStatsBuckets buckets is
// ErrInvalidExecutionStatsWindow.
func ExecutionHistogramBuckets(filter ExecutionFilter, bucket time.Duration) ([]time.Time, error) {
	if bucket <= 0 || bucket%time.Second != 0 {
		return nil, fmt.Errorf("%w: bucket must be a positive whole number of seconds", ErrInvalidExecutionStatsWindow)
	}
	if filter.CreatedAfter.IsZero() || filter.CreatedBefore.IsZero() {
		return nil, fmt.Errorf("%w: a histogram needs both a lower and an upper bound", ErrInvalidExecutionStatsWindow)
	}
	if !filter.CreatedBefore.After(filter.CreatedAfter) {
		return nil, fmt.Errorf("%w: the window must end after it starts", ErrInvalidExecutionStatsWindow)
	}
	first := executionStatsBucketStart(filter.CreatedAfter, bucket)
	out := make([]time.Time, 0, MaxExecutionStatsBuckets)
	for t := first; t.Before(filter.CreatedBefore); t = t.Add(bucket) {
		if len(out) == MaxExecutionStatsBuckets {
			return nil, fmt.Errorf("%w: more than %d buckets", ErrInvalidExecutionStatsWindow, MaxExecutionStatsBuckets)
		}
		out = append(out, t)
	}
	return out, nil
}

// executionStatsBucketStart returns the start of the UTC-aligned bucket of the
// given width containing t. The arithmetic is done on Unix seconds (floored,
// not truncated, so pre-epoch instants still align downward).
func executionStatsBucketStart(t time.Time, bucket time.Duration) time.Time {
	secs := int64(bucket / time.Second)
	unix := t.Unix()
	q := unix / secs
	if unix < 0 && unix%secs != 0 {
		q--
	}
	return time.Unix(q*secs, 0).UTC()
}

// ExecutionStatsBucketIndex returns which bucket of the given width contains t,
// counted from the Unix epoch: the value is floor(t.Unix()/bucketSeconds).
// Callers convert it back to a start with time.Unix(index*secs, 0).UTC().
func ExecutionStatsBucketIndex(t time.Time, bucket time.Duration) int64 {
	secs := int64(bucket / time.Second)
	unix := t.Unix()
	q := unix / secs
	if unix < 0 && unix%secs != 0 {
		q--
	}
	return q
}

// BuildWorkflowStats assembles a WorkflowStats from already-aggregated counts
// and the terminal durations sampled for it. Both backends use it so the
// success-rate and naming rules live in one place.
func BuildWorkflowStats(counts map[types.ExecutionStatus]int64, durations []time.Duration) WorkflowStats {
	var runs, succeeded, failed, terminal int64
	for status, n := range counts {
		runs += n
		switch status {
		case types.ExecutionStatusSuccess:
			succeeded += n
			terminal += n
		case types.ExecutionStatusFailed, types.ExecutionStatusTimeout:
			failed += n
			terminal += n
		case types.ExecutionStatusCanceled:
			terminal += n
		}
	}
	stats := WorkflowStats{
		Runs:       runs,
		Succeeded:  succeeded,
		Failed:     failed,
		SampleSize: len(durations),
	}
	if terminal > 0 {
		rate := float64(succeeded) / float64(terminal)
		stats.SuccessRate = &rate
	}
	stats.P50MS = NearestRankPercentileMS(durations, 50)
	stats.P95MS = NearestRankPercentileMS(durations, 95)
	return stats
}
