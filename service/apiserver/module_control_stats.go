package apiserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// This file serves the execution statistics read (GET /v1/executions/stats)
// and the two execution-derived fields of a workflow list row (`stats` and
// `recent_executions`). Both are thin projections of
// store.ExecutionStatsReader: every aggregation happens in the store under an
// explicit namespace scope, and this layer only parses the window and shapes
// the response.
//
// Security posture, shared with every other execution read:
//
//   - namespace comes from namespace.FromContext (server-issued), never from a
//     query parameter;
//   - the window is bounded here (≤ 30 days) and the bucket count is checked
//     against store.MaxExecutionStatsBuckets BEFORE the store is called, so a
//     caller cannot turn one request into an unbounded scan;
//   - every value reaches SQL as a bind parameter (store/sqlstore);
//   - errors are logged and answered with a generic message — a 500 body never
//     carries storage detail.
//
// Workflow-list stats are additionally gated by a SECOND authorization check on
// OpExecutionRead: the list itself is a workflow.read operation, and a token
// that may read definitions but not executions must not learn execution
// aggregates through this side door. A denial OMITS the fields rather than
// failing the page (a page that loses two columns is strictly more useful than
// a 403 on a page the caller may otherwise read).

const (
	// maxExecutionStatsWindow is the widest window GET /v1/executions/stats
	// accepts. It bounds both the scan and the bucket count.
	maxExecutionStatsWindow = 30 * 24 * time.Hour
	// defaultWorkflowStatsWindow is the `window` default for
	// ?include=stats,recent.
	defaultWorkflowStatsWindow = 7 * 24 * time.Hour
	// maxWorkflowListPageSizeWithStats tightens the workflow-list page size when
	// stats are requested: each row costs one entry in a per-workflow aggregate,
	// and the sample cap makes the cost proportional to page_size.
	maxWorkflowListPageSizeWithStats = 50
)

// executionStatsBucketDurations is the accepted `bucket` enum. The keys are the
// only wire spellings; the values are what the store groups by. A caller cannot
// name an arbitrary duration.
var executionStatsBucketDurations = map[string]time.Duration{
	"5m": 5 * time.Minute,
	"1h": time.Hour,
	"1d": 24 * time.Hour,
}

// workflowStatsWindows is the accepted `window` enum for a workflow list row's
// statistics.
var workflowStatsWindows = map[string]time.Duration{
	"24h": 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
	"30d": 30 * 24 * time.Hour,
}

// executionStatsResponse is the `data` payload of GET /v1/executions/stats.
// Histogram is present only when the request asked for a bucket; its absence
// means "not requested", not "no buckets".
type executionStatsResponse struct {
	WindowStart    time.Time                       `json:"window_start"`
	WindowEnd      time.Time                       `json:"window_end"`
	Total          int64                           `json:"total"`
	CountsByStatus map[types.ExecutionStatus]int64 `json:"counts_by_status"`
	Histogram      []executionStatsBucket          `json:"histogram,omitempty"`
}

// executionStatsBucket is one histogram bucket. Counts carries every known
// status key (zero-filled), matching counts_by_status.
type executionStatsBucket struct {
	Start  time.Time                       `json:"start"`
	Counts map[types.ExecutionStatus]int64 `json:"counts"`
}

// workflowExecutionStats is a workflow list row's `stats` object. The field
// set and semantics are store.WorkflowStats': success_rate null means "no
// terminal run in the window", not 0.
type workflowExecutionStats struct {
	Window      string   `json:"window"`
	Runs        int64    `json:"runs"`
	Succeeded   int64    `json:"succeeded"`
	Failed      int64    `json:"failed"`
	SuccessRate *float64 `json:"success_rate"`
	P50MS       *int64   `json:"p50_ms,omitempty"`
	P95MS       *int64   `json:"p95_ms,omitempty"`
	SampleSize  int      `json:"sample_size"`
}

// workflowRecentExecution is one element of a workflow list row's
// recent_executions array. duration_ms is present only for a terminal row and
// is an ESTIMATE for now (updated_at − created_at, because executions have no
// finished_at column yet); it is omitted for a non-terminal row and when the
// two timestamps describe an inverted interval.
type workflowRecentExecution struct {
	ExecutionID string                `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	CreatedAt   time.Time             `json:"created_at"`
	DurationMS  *int64                `json:"duration_ms,omitempty"`
}

// workflowListInclude is the parsed ?include=...&window=... pair of a workflow
// list request.
type workflowListInclude struct {
	stats  bool
	recent bool
	window time.Duration
	label  string
}

func (i workflowListInclude) requested() bool { return i.stats || i.recent }

// handleExecutionStats serves GET /v1/executions/stats.
//
// Availability mirrors GET /v1/executions: a server with no execution store,
// or one whose store does not implement the optional statistics capability,
// answers 501 execution_stats_unsupported. That is deliberately distinct from
// both a 500 and an empty-but-successful response: "this deployment cannot
// answer the question" must be renderable without inventing zeroes.
func (m *workflowControlModule) handleExecutionStats(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	st := m.executions
	if st == nil {
		if m.log != nil {
			m.log.Error("execution_stats_no_store")
		}
		writeFail(w, r, http.StatusNotImplemented, "execution_stats_unsupported",
			"the configured execution store does not support execution statistics")
		return
	}
	reader, ok := st.(store.ExecutionStatsReader)
	if !ok {
		if m.log != nil {
			m.log.Error("execution_stats_reader_unsupported")
		}
		writeFail(w, r, http.StatusNotImplemented, "execution_stats_unsupported",
			"the configured execution store does not support execution statistics")
		return
	}
	ns := namespace.FromContext(r.Context())
	if err := store.ValidateNamespaceScope(ns); err != nil {
		if m.log != nil {
			m.log.Error("execution_stats_namespace_invalid", "err", err)
		}
		writeFail(w, r, http.StatusForbidden, "execution_namespace_invalid", "forbidden")
		return
	}
	filter, bucket, ok := executionStatsQueryFrom(w, r)
	if !ok {
		return
	}

	counts, err := reader.CountExecutionsByStatus(r.Context(), ns, filter)
	if err != nil {
		m.failExecutionStats(w, r, "execution_stats_counts_failed", err)
		return
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	resp := executionStatsResponse{
		WindowStart:    filter.CreatedAfter,
		WindowEnd:      filter.CreatedBefore,
		Total:          total,
		CountsByStatus: counts,
	}
	if bucket > 0 {
		buckets, err := reader.ExecutionHistogram(r.Context(), ns, filter, bucket)
		if err != nil {
			m.failExecutionStats(w, r, "execution_stats_histogram_failed", err)
			return
		}
		resp.Histogram = make([]executionStatsBucket, len(buckets))
		for i, b := range buckets {
			resp.Histogram[i] = executionStatsBucket{Start: b.Start, Counts: b.Counts}
		}
	}
	writeData(w, r, http.StatusOK, resp)
}

// executionStatsQueryFrom parses and validates the statistics filters.
//
// Both bounds are REQUIRED and exclusive, and their span may not exceed
// maxExecutionStatsWindow. `bucket` is optional and restricted to the enum in
// executionStatsBucketDurations; the resulting bucket count is checked against
// store.MaxExecutionStatsBuckets here so an oversized request is a stable 400
// rather than whichever error the store would produce. All failures share the
// `stats_window_invalid` / `stats_bucket_invalid` codes: a filter that cannot
// be answered must not be silently widened or narrowed.
//
// ok=false means the failure response has already been written.
func executionStatsQueryFrom(w http.ResponseWriter, r *http.Request) (store.ExecutionFilter, time.Duration, bool) {
	q := r.URL.Query()
	var filter store.ExecutionFilter

	rawAfter := q.Get("created_after")
	rawBefore := q.Get("created_before")
	if rawAfter == "" || rawBefore == "" {
		writeFail(w, r, http.StatusBadRequest, "stats_window_invalid",
			"created_after and created_before are required")
		return store.ExecutionFilter{}, 0, false
	}
	after, err := time.Parse(time.RFC3339Nano, rawAfter)
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "stats_window_invalid",
			"created_after must be an RFC3339 date-time")
		return store.ExecutionFilter{}, 0, false
	}
	before, err := time.Parse(time.RFC3339Nano, rawBefore)
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "stats_window_invalid",
			"created_before must be an RFC3339 date-time")
		return store.ExecutionFilter{}, 0, false
	}
	if !before.After(after) {
		writeFail(w, r, http.StatusBadRequest, "stats_window_invalid",
			"created_before must be after created_after")
		return store.ExecutionFilter{}, 0, false
	}
	if before.Sub(after) > maxExecutionStatsWindow {
		writeFail(w, r, http.StatusBadRequest, "stats_window_invalid",
			"the window must not exceed 30 days")
		return store.ExecutionFilter{}, 0, false
	}
	filter.CreatedAfter = after
	filter.CreatedBefore = before

	if raw := q.Get("workflow_id"); raw != "" {
		if err := store.ValidateExecutionWorkflowIDFilter(raw); err != nil {
			writeFail(w, r, http.StatusBadRequest, "execution_workflow_id_invalid",
				"workflow_id must be at most 64 characters")
			return store.ExecutionFilter{}, 0, false
		}
		filter.WorkflowID = types.WorkflowID(raw)
	}

	var bucket time.Duration
	if raw := q.Get("bucket"); raw != "" {
		b, ok := executionStatsBucketDurations[raw]
		if !ok {
			writeFail(w, r, http.StatusBadRequest, "stats_bucket_invalid",
				"bucket must be one of 5m, 1h, 1d")
			return store.ExecutionFilter{}, 0, false
		}
		// The store repeats this validation; doing it here turns the common
		// mistake (a long window at a small bucket) into a 400 instead of a 500.
		if _, err := store.ExecutionHistogramBuckets(filter, b); err != nil {
			writeFail(w, r, http.StatusBadRequest, "stats_bucket_invalid",
				"bucket produces more than 168 buckets for this window")
			return store.ExecutionFilter{}, 0, false
		}
		bucket = b
	}
	return filter, bucket, true
}

// failExecutionStats maps a store statistics error to its HTTP response. A
// window error is the caller's (400), an unsupported backend is a capability
// statement (501), and anything else is logged and answered generically (500) —
// the storage error text never reaches the body.
func (m *workflowControlModule) failExecutionStats(w http.ResponseWriter, r *http.Request, event string, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidExecutionStatsWindow):
		writeFail(w, r, http.StatusBadRequest, "stats_window_invalid",
			"the requested window or bucket is not supported")
	case errors.Is(err, store.ErrExecutionStatsUnsupported):
		if m.log != nil {
			m.log.Error(event+"_unsupported", "err", err)
		}
		writeFail(w, r, http.StatusNotImplemented, "execution_stats_unsupported",
			"the configured execution store does not support execution statistics")
	default:
		if m.log != nil {
			m.log.Error(event, "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// workflowListIncludeFrom parses ?include=stats,recent&window=... on
// GET /v1/workflows.
//
// `include` is a comma-separated enum: an unknown member is a 400 rather than
// being ignored, because a dropped member would return rows without the fields
// the caller asked for and look like "no data". `window` is one of 24h/7d/30d
// and defaults to 7d; it is validated even when include is empty, so a typo is
// never silently accepted.
//
// ok=false means the failure response has already been written.
func workflowListIncludeFrom(w http.ResponseWriter, r *http.Request) (workflowListInclude, bool) {
	inc := workflowListInclude{window: defaultWorkflowStatsWindow, label: "7d"}
	if raw := strings.TrimSpace(r.URL.Query().Get("include")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			switch strings.TrimSpace(part) {
			case "":
				// `stats,,recent` is a malformed list, not "no fields": refuse it
				// rather than guessing which member was meant.
				writeFail(w, r, http.StatusBadRequest, "workflow_include_invalid",
					"include must be a comma-separated subset of stats, recent")
				return workflowListInclude{}, false
			case "stats":
				inc.stats = true
			case "recent":
				inc.recent = true
			default:
				writeFail(w, r, http.StatusBadRequest, "workflow_include_invalid",
					"include must be a comma-separated subset of stats, recent")
				return workflowListInclude{}, false
			}
		}
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("window")); raw != "" {
		window, ok := workflowStatsWindows[raw]
		if !ok {
			writeFail(w, r, http.StatusBadRequest, "workflow_window_invalid",
				"window must be one of 24h, 7d, 30d")
			return workflowListInclude{}, false
		}
		inc.window = window
		inc.label = raw
	}
	return inc, true
}

// executionReadAuthorized answers the SECOND authorization question behind
// every execution-derived field a workflow list row may carry (latest_execution,
// stats, recent_executions): may this caller read executions at all?
//
// The list route itself requires workflow.read, and a token that holds only
// that must not learn execution data through it — the latest run's status is
// execution data exactly as the aggregates are, which is why the check gates
// all three fields, not only the ?include= ones.
//
// The check is deliberately conservative in every direction:
//
//   - the bare/legacy branch has no principal in context at all (it runs
//     without an authz wrapper), so there is no scope model to consult and the
//     fields are attached as every other bare-mode read is;
//   - a principal is present: the wired Authorizer decides, and an error is a
//     denial.
func (m *workflowControlModule) executionReadAuthorized(r *http.Request) bool {
	principal, ok := principalFromRequest(r)
	if !ok {
		return true
	}
	req := AuthorizationRequest{Principal: principal, Operation: OpExecutionRead}
	if m.authorizer != nil {
		decision, err := m.authorizer.Authorize(r.Context(), req)
		return err == nil && decision == DecisionAllow
	}
	return principalAllowsOperation(principal, OpExecutionRead, scopeForOperation(OpExecutionRead))
}

// attachExecutionStats fills Stats and/or RecentExecutions on a page of
// workflow rows with two batch store reads for the whole page (never one read
// per row).
//
// Every failure mode degrades to "fields absent" rather than failing the list:
// a store error is logged and the corresponding field is left off. Absent means
// UNKNOWN for a failed read; for `stats`, a workflow the store did not mention
// is a KNOWN zero (the store answered for the namespace and the ids), so it is
// rendered as runs=0 / success_rate=null rather than omitted. recent_executions
// has no such distinction — an empty list is simply omitted.
func (m *workflowControlModule) attachExecutionStats(ctx context.Context, ns namespace.Namespace, items []workflowListItem, inc workflowListInclude) {
	if len(items) == 0 || !inc.requested() {
		return
	}
	reader, ok := m.executions.(store.ExecutionStatsReader)
	if !ok {
		return
	}
	ids := make([]types.WorkflowID, 0, len(items))
	seen := make(map[types.WorkflowID]struct{}, len(items))
	for _, item := range items {
		id := types.WorkflowID(item.ID)
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	since := time.Now().UTC().Add(-inc.window)

	if inc.stats {
		stats, err := reader.WorkflowExecutionStats(ctx, ns, ids, since, store.MaxExecutionStatsSampleCap)
		if err != nil {
			if m.log != nil {
				m.log.Error("workflow_list_stats_failed", "err", err)
			}
		} else {
			for i := range items {
				s := stats[types.WorkflowID(items[i].ID)]
				items[i].Stats = &workflowExecutionStats{
					Window:      inc.label,
					Runs:        s.Runs,
					Succeeded:   s.Succeeded,
					Failed:      s.Failed,
					SuccessRate: s.SuccessRate,
					P50MS:       s.P50MS,
					P95MS:       s.P95MS,
					SampleSize:  s.SampleSize,
				}
			}
		}
	}
	if inc.recent {
		recent, err := reader.RecentExecutions(ctx, ns, ids, store.MaxRecentExecutionsPerWorkflow)
		if err != nil {
			if m.log != nil {
				m.log.Error("workflow_list_recent_failed", "err", err)
			}
		} else {
			for i := range items {
				summaries := recent[types.WorkflowID(items[i].ID)]
				if len(summaries) == 0 {
					continue
				}
				rows := make([]workflowRecentExecution, len(summaries))
				for j, s := range summaries {
					rows[j] = workflowRecentExecution{
						ExecutionID: string(s.ExecutionID),
						Status:      s.Status,
						CreatedAt:   s.CreatedAt,
						DurationMS:  executionSummaryDurationMS(s),
					}
				}
				items[i].RecentExecutions = rows
			}
		}
	}
}

// executionSummaryDurationMS derives a recent execution's duration. It uses
// the exact finished_at when the row carries one and falls back to
// updated_at − created_at for rows written before that column existed (the
// shared store.ExecutionDurationMS definition, so this projection and the
// /v1/overview percentiles cannot disagree). A non-terminal row has no
// duration (nil), and an inverted interval (clock skew) is reported as zero
// rather than as a negative number.
func executionSummaryDurationMS(s store.ExecutionSummary) *int64 {
	ms, _ := store.ExecutionDurationMS(s.Status, s.CreatedAt, s.UpdatedAt, s.FinishedAt)
	return ms
}
