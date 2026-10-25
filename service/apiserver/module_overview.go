package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

// This file serves GET /v1/overview: one dashboard read over the caller's OWN
// namespace. Every aggregate comes from store.ExecutionStatsReader under an
// explicit namespace scope; this layer only parses the window, shapes the
// response and caches it for 30 seconds.
//
// Security posture (the same as every execution read):
//
//   - namespace comes from namespace.FromContext (server-issued), never from a
//     query parameter;
//   - window is a closed enum (24h/7d), so the scan and the 24-bucket
//     materialization are both bounded before the store is called;
//   - the `cluster` segment is a SECOND authorization decision on
//     OpManagementRunnerList. Without it the segment is omitted entirely, never
//     zero-filled: the runner roster is cluster-wide and must not leak to a
//     namespace-scoped principal.
//
// Caching: the response is cached in-process for overviewCacheTTL keyed by
// (namespace, window, cluster-permission). The permission bit is part of the
// key because the cached payload can contain the cluster segment; a single
// shared entry would hand it to the next caller regardless of its grants. A
// hand-rolled singleflight collapses concurrent cold misses onto one build.

const (
	// overviewCacheTTL is how long one dashboard response is reused. It is
	// short enough that the KPIs still track a live pipeline and long enough
	// that a dashboard auto-refreshing every few seconds does not re-run every
	// aggregate per viewer.
	overviewCacheTTL = 30 * time.Second
	// overviewBucketCount is the fixed sparkline/histogram width. The design
	// fixes 24 buckets; 24h gives 1h buckets and 7d gives 7h buckets.
	overviewBucketCount = 24
	// overviewMaxErrorLen caps `top_failing[].last_error`, which is diagnostic
	// text and must not become an unbounded body.
	overviewMaxErrorLen = 200
)

// overviewWindows is the accepted `window` enum. The keys are the only wire
// spellings; the values drive the scan range and the bucket width.
var overviewWindows = map[string]time.Duration{
	"24h": 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
}

// overviewResponse is the `data` payload of GET /v1/overview.
type overviewResponse struct {
	// Window echoes the requested enum label (24h / 7d).
	Window string `json:"window"`
	// GeneratedAt is when the payload was BUILT. On a cache hit it is older
	// than the response instant, which is exactly how a client learns the data
	// is up to 30 seconds stale.
	GeneratedAt time.Time               `json:"generated_at"`
	KPIs        overviewKPIs            `json:"kpis"`
	Histogram   []executionStatsBucket  `json:"histogram"`
	TopFailing  []overviewFailingRow    `json:"top_failing"`
	Cluster     *overviewClusterSegment `json:"cluster,omitempty"`
}

// overviewKPIs is the KPI block. success_rate / p50_ms / p95_ms are nullable:
// null means "no terminal run to compute it from", never zero.
type overviewKPIs struct {
	Runs            int64             `json:"runs"`
	RunsPrevious    int64             `json:"runs_previous"`
	FailedPrevious  int64             `json:"failed_previous"`
	SuccessRate     *float64          `json:"success_rate"`
	P50MS           *int64            `json:"p50_ms"`
	P95MS           *int64            `json:"p95_ms"`
	ActiveWorkflows int64             `json:"active_workflows"`
	Sparkline       overviewSparkline `json:"sparkline"`
}

// overviewSparkline is the fixed-width 24-bucket series. success_rate entries
// are null for buckets with no terminal run.
type overviewSparkline struct {
	Runs        []int64    `json:"runs"`
	SuccessRate []*float64 `json:"success_rate"`
}

// overviewFailingRow is one element of top_failing. last_error is truncated to
// overviewMaxErrorLen; workflow_name is best effort and omitted when the
// registry cannot name the id (a deleted workflow still has execution rows).
type overviewFailingRow struct {
	WorkflowID   string     `json:"workflow_id"`
	WorkflowName string     `json:"workflow_name,omitempty"`
	Failed       int64      `json:"failed"`
	LastError    string     `json:"last_error,omitempty"`
	LastFailedAt *time.Time `json:"last_failed_at,omitempty"`
}

// overviewClusterSegment is present only for a caller that also holds
// OpManagementRunnerList. The deployment exposes leadership as a boolean (the
// same shape GET /v1/management/leader returns), not as a leader identity.
type overviewClusterSegment struct {
	IsLeader bool                  `json:"is_leader"`
	Runners  overviewRunnerSummary `json:"runners"`
}

// overviewRunnerSummary aggregates the LIVE directory members. Capacity and
// in_flight are summed only over live runners: an offline runner's capacity is
// not schedulable, so including it would overstate what the cluster can take.
type overviewRunnerSummary struct {
	Online   int `json:"online"`
	Total    int `json:"total"`
	Capacity int `json:"capacity"`
	InFlight int `json:"in_flight"`
}

// handleOverview serves GET /v1/overview.
func (m *workflowControlModule) handleOverview(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	st := m.executions
	if st == nil {
		if m.log != nil {
			m.log.Error("overview_no_store")
		}
		writeFail(w, r, http.StatusNotImplemented, "overview_unsupported",
			"the configured execution store does not support the overview")
		return
	}
	if _, ok := st.(store.ExecutionStatsReader); !ok {
		if m.log != nil {
			m.log.Error("overview_reader_unsupported")
		}
		writeFail(w, r, http.StatusNotImplemented, "overview_unsupported",
			"the configured execution store does not support the overview")
		return
	}
	ns := namespace.FromContext(r.Context())
	if err := store.ValidateNamespaceScope(ns); err != nil {
		if m.log != nil {
			m.log.Error("overview_namespace_invalid", "err", err)
		}
		writeFail(w, r, http.StatusForbidden, "execution_namespace_invalid", "forbidden")
		return
	}
	label, window, ok := overviewWindowFrom(w, r)
	if !ok {
		return
	}
	clusterAllowed := m.overviewClusterReadable(r)
	key := overviewCacheKey{namespace: string(ns), window: label, cluster: clusterAllowed}
	resp, err := m.overviewCacheFor().get(r.Context(), key, func() (*overviewResponse, error) {
		return m.buildOverview(r.Context(), ns, label, window, clusterAllowed)
	})
	if err != nil {
		m.failOverview(w, r, "overview_build_failed", err)
		return
	}
	writeData(w, r, http.StatusOK, resp)
}

// overviewWindowFrom parses and validates ?window=. Absent means 24h; an
// unknown value is a 400 rather than a silently substituted default, because a
// caller that asked for 30d and got 24h would misread every number.
func overviewWindowFrom(w http.ResponseWriter, r *http.Request) (string, time.Duration, bool) {
	label := r.URL.Query().Get("window")
	if label == "" {
		label = "24h"
	}
	window, ok := overviewWindows[label]
	if !ok {
		writeFail(w, r, http.StatusBadRequest, "overview_window_invalid",
			"window must be one of 24h, 7d")
		return "", 0, false
	}
	return label, window, true
}

// overviewClusterReadable is the second authorization decision behind the
// cluster segment. It repeats the wrapper's own rule: a configured Authorizer
// decides; without one, the reference scope policy does. No principal (the
// legacy bare-auth branch) means no grant, so the segment is omitted.
func (m *workflowControlModule) overviewClusterReadable(r *http.Request) bool {
	principal, ok := principalFromRequest(r)
	if !ok {
		return false
	}
	req := AuthorizationRequest{Principal: principal, Operation: OpManagementRunnerList}
	if m.authorizer != nil {
		decision, err := m.authorizer.Authorize(r.Context(), req)
		return err == nil && decision == DecisionAllow
	}
	return principalAllowsOperation(principal, OpManagementRunnerList, scopeForOperation(OpManagementRunnerList))
}

// buildOverview runs every aggregate for one (namespace, window). Each store
// call is scoped and bounded; a failure aborts the whole build so a partial
// dashboard is never cached as if it were complete.
func (m *workflowControlModule) buildOverview(ctx context.Context, ns namespace.Namespace, label string, window time.Duration, clusterAllowed bool) (*overviewResponse, error) {
	reader := m.executions.(store.ExecutionStatsReader)
	// 24 buckets of exactly window/24: the window end is aligned UP to the next
	// bucket boundary, so start is boundary-aligned too and the histogram has
	// exactly overviewBucketCount buckets with no partial edge.
	bucket := window / overviewBucketCount
	now := time.Now().UTC()
	endIdx := store.ExecutionStatsBucketIndex(now, bucket)
	end := time.Unix((endIdx+1)*int64(bucket/time.Second), 0).UTC()
	start := end.Add(-window)
	filter := store.ExecutionFilter{CreatedAfter: start, CreatedBefore: end}
	previous := store.ExecutionFilter{CreatedAfter: start.Add(-window), CreatedBefore: start}

	counts, err := reader.CountExecutionsByStatus(ctx, ns, filter)
	if err != nil {
		return nil, err
	}
	previousCounts, err := reader.CountExecutionsByStatus(ctx, ns, previous)
	if err != nil {
		return nil, err
	}
	buckets, err := reader.ExecutionHistogram(ctx, ns, filter, bucket)
	if err != nil {
		return nil, err
	}
	// Percentiles sample TERMINAL rows only; the filter selects that status set
	// explicitly (store.ExecutionDurationMS then skips anything non-terminal,
	// so even a mis-set filter cannot invent a duration).
	durationFilter := filter
	durationFilter.Statuses = store.TerminalExecutionStatuses()
	durations, err := reader.ExecutionDurations(ctx, ns, durationFilter, store.MaxExecutionDurationSample)
	if err != nil {
		return nil, err
	}
	activeWorkflows, err := reader.CountActiveWorkflows(ctx, ns, filter)
	if err != nil {
		return nil, err
	}
	failing, err := reader.TopFailingWorkflows(ctx, ns, start, store.MaxTopFailingWorkflows)
	if err != nil {
		return nil, err
	}

	current := store.BuildWorkflowStats(counts, nil)
	before := store.BuildWorkflowStats(previousCounts, nil)
	resp := &overviewResponse{
		Window:      label,
		GeneratedAt: time.Now().UTC(),
		KPIs: overviewKPIs{
			Runs:            current.Runs,
			RunsPrevious:    before.Runs,
			FailedPrevious:  before.Failed,
			SuccessRate:     current.SuccessRate,
			P50MS:           store.NearestRankPercentileMS(overviewDurations(durations.DurationsMS), 50),
			P95MS:           store.NearestRankPercentileMS(overviewDurations(durations.DurationsMS), 95),
			ActiveWorkflows: activeWorkflows,
			Sparkline:       overviewSparklineFrom(buckets),
		},
		Histogram:  make([]executionStatsBucket, len(buckets)),
		TopFailing: m.overviewFailingRows(ctx, ns, failing),
	}
	for i, b := range buckets {
		resp.Histogram[i] = executionStatsBucket{Start: b.Start, Counts: b.Counts}
	}
	if clusterAllowed {
		resp.Cluster = m.overviewCluster(ctx)
	}
	return resp, nil
}

// overviewSparklineFrom derives the 24-point series from the same buckets the
// histogram returns, so the two can never disagree about a bucket's counts.
func overviewSparklineFrom(buckets []store.StatusBucket) overviewSparkline {
	out := overviewSparkline{
		Runs:        make([]int64, len(buckets)),
		SuccessRate: make([]*float64, len(buckets)),
	}
	for i, b := range buckets {
		stats := store.BuildWorkflowStats(b.Counts, nil)
		out.Runs[i] = stats.Runs
		out.SuccessRate[i] = stats.SuccessRate
	}
	return out
}

// overviewDurations converts the store's millisecond sample to the duration
// slice the shared percentile helper takes.
func overviewDurations(ms []int64) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, v := range ms {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

// overviewFailingRows projects the store rows and resolves workflow names.
// A name is added only when the registry record is in the caller's namespace
// (or unattributed, the same rule GET /v1/workflows/{id} applies); a registry
// miss omits the name rather than failing the dashboard.
func (m *workflowControlModule) overviewFailingRows(ctx context.Context, ns namespace.Namespace, failing []store.FailingWorkflow) []overviewFailingRow {
	out := make([]overviewFailingRow, 0, len(failing))
	registry := m.registry()
	for _, f := range failing {
		row := overviewFailingRow{
			WorkflowID: string(f.WorkflowID),
			Failed:     f.Failed,
			LastError:  truncateOverviewError(f.LastError),
		}
		if !f.LastFailedAt.IsZero() {
			at := f.LastFailedAt
			row.LastFailedAt = &at
		}
		if registry != nil {
			if rec, err := registry.GetWorkflow(ctx, f.WorkflowID); err == nil {
				if rec.Namespace == "" || rec.Namespace == string(ns) {
					row.WorkflowName = rec.Name
				}
			}
		}
		out = append(out, row)
	}
	return out
}

// truncateOverviewError caps diagnostic text at overviewMaxErrorLen RUNES, so a
// multi-byte error cannot be cut mid-character.
func truncateOverviewError(msg string) string {
	runes := []rune(msg)
	if len(runes) <= overviewMaxErrorLen {
		return msg
	}
	return string(runes[:overviewMaxErrorLen])
}

// overviewCluster reads the runner directory the same way the management roster
// does: enumerate ids, read each snapshot, apply the production liveness
// threshold. It returns nil — omitting the segment — when the directory cannot
// answer; a permission-bearing caller then sees no cluster block rather than a
// manufactured "0 runners online".
func (m *workflowControlModule) overviewCluster(ctx context.Context) *overviewClusterSegment {
	if m.cp == nil {
		return nil
	}
	dir := m.cp.RunnerDirectory()
	if isNilValue(dir) {
		return nil
	}
	lister, ok := dir.(runnerLister)
	if !ok {
		return nil
	}
	snapshotter, ok := dir.(runnerSnapshotter)
	if !ok {
		return nil
	}
	ids, err := lister.ListRunners(ctx)
	if err != nil {
		if m.log != nil {
			m.log.Error("overview_cluster_list_failed", "err", err)
		}
		return nil
	}
	now := time.Now()
	selector := control.DefaultRunnerSelector()
	summary := overviewRunnerSummary{Total: len(ids)}
	for _, id := range ids {
		snap, ok := snapshotter.Runner(ctx, id)
		if !ok || !selector.IsLive(snap, now) {
			continue
		}
		summary.Online++
		summary.Capacity += snap.Capacity
		summary.InFlight += snap.InFlight
	}
	return &overviewClusterSegment{IsLeader: m.cp.IsLeader(), Runners: summary}
}

func (m *workflowControlModule) failOverview(w http.ResponseWriter, r *http.Request, event string, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidExecutionStatsWindow):
		writeFail(w, r, http.StatusBadRequest, "overview_window_invalid", "the requested window is not supported")
	case errors.Is(err, store.ErrExecutionStatsUnsupported):
		if m.log != nil {
			m.log.Error(event+"_unsupported", "err", err)
		}
		writeFail(w, r, http.StatusNotImplemented, "overview_unsupported",
			"the configured execution store does not support the overview")
	default:
		if m.log != nil {
			m.log.Error(event, "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// overviewCacheKey is the cache identity. cluster is part of it because the
// cached payload can carry the cluster-wide roster; sharing one entry across
// callers with different grants would leak it.
type overviewCacheKey struct {
	namespace string
	window    string
	cluster   bool
}

type overviewCacheEntry struct {
	response  *overviewResponse
	expiresAt time.Time
}

// overviewFlight is one in-progress build other callers wait on.
type overviewFlight struct {
	done     chan struct{}
	response *overviewResponse
	err      error
}

// overviewCache is a TTL cache with a hand-rolled singleflight. The repository
// deliberately does not depend on golang.org/x/sync, so the flight is a channel
// per key: the first caller builds, the rest wait for its result (or their own
// context cancellation).
type overviewCache struct {
	mu      sync.Mutex
	entries map[overviewCacheKey]overviewCacheEntry
	flights map[overviewCacheKey]*overviewFlight
}

func newOverviewCache() *overviewCache {
	return &overviewCache{
		entries: make(map[overviewCacheKey]overviewCacheEntry),
		flights: make(map[overviewCacheKey]*overviewFlight),
	}
}

// get returns a cached response or builds one, collapsing concurrent misses.
func (c *overviewCache) get(ctx context.Context, key overviewCacheKey, build func() (*overviewResponse, error)) (*overviewResponse, error) {
	now := time.Now()
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && now.Before(entry.expiresAt) {
		resp := entry.response
		c.mu.Unlock()
		return resp, nil
	}
	if flight, ok := c.flights[key]; ok {
		c.mu.Unlock()
		select {
		case <-flight.done:
			return flight.response, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Opportunistic sweep: a miss is the only moment this map grows, and an
	// expired entry is never returned again. Bounded by the number of distinct
	// (namespace, window, permission) keys seen recently.
	for k, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, k)
		}
	}
	flight := &overviewFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.mu.Unlock()

	return c.runFlight(key, flight, build)
}

// runFlight executes one flight's build and ALWAYS publishes its outcome —
// response, error, or panic — before closing done and removing the flight.
//
// The cleanup is a defer, not straight-line code after build(): a panic in the
// build must not leave the flight registered. A stranded flight is never
// removed (only a successful or failed build deletes it), so every later
// caller for the same key would wait on a done channel that is never closed,
// blocking until its own context expires. Recovering here turns the panic into
// an ordinary error for this caller and for everyone already waiting, and the
// failure is not cached, so the next request rebuilds.
func (c *overviewCache) runFlight(key overviewCacheKey, flight *overviewFlight, build func() (*overviewResponse, error)) (resp *overviewResponse, err error) {
	defer func() {
		if p := recover(); p != nil {
			resp, err = nil, fmt.Errorf("overview: build panicked: %v", p)
		}
		c.mu.Lock()
		flight.response, flight.err = resp, err
		if err == nil && resp != nil {
			c.entries[key] = overviewCacheEntry{response: resp, expiresAt: time.Now().Add(overviewCacheTTL)}
		}
		delete(c.flights, key)
		c.mu.Unlock()
		close(flight.done)
	}()
	return build()
}
