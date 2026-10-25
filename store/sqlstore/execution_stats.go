package sqlstore

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

var _ store.ExecutionStatsReader = (*executionRepo)(nil)

// executionBucketIndexExprs are the dialect hooks for the bucket index: a
// FIXED SQL fragment, plus the bind arguments it takes, that maps created_at to
// floor((created_at - anchor) / secs). The anchor is the first bucket's start
// (an epoch-aligned UTC instant), so index i is exactly out[i] of
// store.ExecutionHistogramBuckets and the result never depends on how the
// database labels time zones. The hooks receive no caller input and are keyed
// by the dialector's own name, so no request value can ever become SQL text.
//
// MySQL subtracts two DATETIME values with TIMESTAMPDIFF. The anchor is bound
// as a Go time.Time, so the driver writes it in the same zone (its loc= setting,
// UTC by default) it used for every stored created_at, and the difference is a
// pure wall-clock subtraction. UNIX_TIMESTAMP(created_at) must not be used
// here: it reinterprets the stored wall clock in the SESSION time_zone, which
// is unrelated to the driver's loc= and shifts every row by the server's UTC
// offset whenever the two differ. DIV is integer division; MySQL's `/` yields a
// DECIMAL rounded to div_precision_increment places, which would round
// 86399/86400 up to 1 and put a row in the next bucket.
//
// SQLite's strftime('%s') reads the stored text as UTC, which is exactly how
// sqlitestore's _timefmt writes it, so the anchor is passed as its Unix second.
// Both hooks yield a non-negative integer for every row the filter admits
// (created_at > CreatedAfter >= anchor).
var executionBucketIndexExprs = map[string]func(anchor time.Time, secs int64) (string, []any){
	"mysql": func(anchor time.Time, secs int64) (string, []any) {
		return "TIMESTAMPDIFF(SECOND, ?, created_at) DIV ?", []any{anchor, secs}
	},
	"sqlite": func(anchor time.Time, secs int64) (string, []any) {
		return "(CAST(strftime('%s', created_at) AS INTEGER) - ?) / ?", []any{anchor.Unix(), secs}
	},
}

// executionBucketIndexExpr resolves this repo's dialect hook. A dialect not in
// the table is unsupported rather than approximated: a guessed bucket
// expression would silently mis-bin every row, and the caller can express
// "this backend cannot do this" (501) but not "this backend is wrong".
func (r *executionRepo) executionBucketIndexExpr(anchor time.Time, secs int64) (string, []any, error) {
	if r.db == nil || r.db.Dialector == nil {
		return "", nil, fmt.Errorf("%w: no database configured", store.ErrExecutionStatsUnsupported)
	}
	name := r.db.Name()
	hook, ok := executionBucketIndexExprs[name]
	if !ok {
		return "", nil, fmt.Errorf("%w: dialect %q has no bucket expression", store.ErrExecutionStatsUnsupported, name)
	}
	expr, args := hook(anchor, secs)
	return expr, args, nil
}

// CountExecutionsByStatus groups by status in SQL: one GROUP BY statement
// returns at most nine rows, so the row count never depends on how many
// executions the namespace holds. The scope and every filter are the same
// predicates ListExecutions and CountExecutions build — executionQuery is the
// single definition — so these counts describe exactly the set those methods
// page and count.
func (r *executionRepo) CountExecutionsByStatus(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (map[types.ExecutionStatus]int64, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	var rows []struct {
		Status types.ExecutionStatus
		Cnt    int64
	}
	err := r.executionQuery(ctx, ns, filter).
		Select("status, COUNT(*) AS cnt").
		Group("status").
		Scan(&rows).Error
	if err := wrapDBErr("count executions by status in scope", err); err != nil {
		return nil, err
	}
	out := store.ZeroedExecutionStatusCounts()
	for _, row := range rows {
		out[row.Status] = row.Cnt
	}
	return out, nil
}

// ExecutionHistogram groups in SQL by (bucket index, status). The bucket list
// — alignment, empty buckets, the 168 cap — comes from
// store.ExecutionHistogramBuckets, the same helper the memstore uses, so the
// two backends return the same bucket set for the same request; the query only
// says how many rows landed in each non-empty bucket. No row ever leaves the
// database except one (bucket, status, count) triple.
func (r *executionRepo) ExecutionHistogram(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter, bucket time.Duration) ([]store.StatusBucket, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	starts, err := store.ExecutionHistogramBuckets(filter, bucket)
	if err != nil {
		return nil, err
	}
	secs := int64(bucket / time.Second)
	expr, exprArgs, err := r.executionBucketIndexExpr(starts[0], secs)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		Bucket int64
		Status types.ExecutionStatus
		Cnt    int64
	}
	err = r.executionQuery(ctx, ns, filter).
		Select(expr+" AS bucket, status, COUNT(*) AS cnt", exprArgs...).
		Group("bucket, status").
		Scan(&rows).Error
	if err := wrapDBErr("execution histogram in scope", err); err != nil {
		return nil, err
	}

	out := make([]store.StatusBucket, len(starts))
	for i, start := range starts {
		out[i] = store.StatusBucket{Start: start, Counts: store.ZeroedExecutionStatusCounts()}
	}
	for _, row := range rows {
		if row.Bucket >= 0 && row.Bucket < int64(len(out)) {
			out[row.Bucket].Counts[row.Status] = row.Cnt
		}
	}
	return out, nil
}

// WorkflowExecutionStats runs two bounded statements: one GROUP BY for the
// counts, and one windowed sample for the percentile input.
//
// The sample is capped by ROW_NUMBER() over the workflow's terminal rows, so
// the database returns at most sampleCap rows per workflow (≤
// MaxExecutionStatsSampleCap of them, by contract) no matter how many
// executions exist. Both dialects this package targets support window
// functions (MySQL 8+, SQLite 3.25+), which is why the cap does not need a
// dialect hook. The ordering inside the window is the same total order
// LatestExecutions uses, so "most recent" means the same row on every backend.
//
// Durations are FinishedAt - CreatedAt, with UpdatedAt as the documented
// estimate only for rows written before finished_at existed (see
// store.ExecutionDurationMS, which both backends share). The percentile
// definition itself is store.NearestRankPercentileMS.
func (r *executionRepo) WorkflowExecutionStats(ctx context.Context, ns namespace.Namespace, ids []types.WorkflowID, since time.Time, sampleCap int) (map[types.WorkflowID]store.WorkflowStats, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.WorkflowID]store.WorkflowStats, len(ids))
	normalized := store.NormalizeExecutionWorkflowIDs(ids)
	if len(normalized) == 0 {
		return out, nil
	}
	sampleCap = store.NormalizeExecutionStatsSampleCap(sampleCap)

	countsQuery := r.db.WithContext(ctx).
		Model(&dbExecution{}).
		Select("workflow_id, status, COUNT(*) AS cnt").
		Where("namespace = ?", string(ns)).
		Where("workflow_id IN ?", normalized)
	if !since.IsZero() {
		countsQuery = countsQuery.Where("created_at > ?", since)
	}
	var countRows []struct {
		WorkflowID string
		Status     types.ExecutionStatus
		Cnt        int64
	}
	if err := wrapDBErr("workflow execution counts in scope",
		countsQuery.Group("workflow_id, status").Scan(&countRows).Error); err != nil {
		return nil, err
	}
	counts := make(map[types.WorkflowID]map[types.ExecutionStatus]int64, len(countRows))
	for _, row := range countRows {
		id := types.WorkflowID(row.WorkflowID)
		c, ok := counts[id]
		if !ok {
			c = make(map[types.ExecutionStatus]int64)
			counts[id] = c
		}
		c[row.Status] = row.Cnt
	}
	if len(counts) == 0 {
		return out, nil
	}
	if sampleCap == 0 {
		for id, c := range counts {
			out[id] = store.BuildWorkflowStats(c, nil)
		}
		return out, nil
	}

	// The status list is bound as a parameter; its contents are the contract's
	// terminal set, not caller input.
	sampleSQL := `
SELECT workflow_id, status, created_at, updated_at, finished_at
FROM (
    SELECT workflow_id, status, created_at, updated_at, finished_at,
           ROW_NUMBER() OVER (
               PARTITION BY workflow_id
               ORDER BY created_at DESC, execution_id ASC
           ) AS rn
    FROM xflow_executions
    WHERE namespace = ? AND workflow_id IN ? AND status IN ?`
	sampleArgs := []any{string(ns), toStrings(normalized), statusStrings(store.TerminalExecutionStatuses())}
	if !since.IsZero() {
		sampleSQL += ` AND created_at > ?`
		sampleArgs = append(sampleArgs, since)
	}
	sampleSQL += `
) AS ranked
WHERE rn <= ?
ORDER BY workflow_id ASC, created_at DESC`
	sampleArgs = append(sampleArgs, sampleCap)

	var sampleRows []struct {
		WorkflowID string
		Status     types.ExecutionStatus
		CreatedAt  time.Time
		UpdatedAt  time.Time
		FinishedAt *time.Time
	}
	if err := wrapDBErr("workflow execution sample in scope",
		r.db.WithContext(ctx).Raw(sampleSQL, sampleArgs...).Scan(&sampleRows).Error); err != nil {
		return nil, err
	}
	durations := make(map[types.WorkflowID][]time.Duration, len(counts))
	for _, row := range sampleRows {
		id := types.WorkflowID(row.WorkflowID)
		ms, _ := store.ExecutionDurationMS(row.Status, row.CreatedAt, row.UpdatedAt, row.FinishedAt)
		if ms != nil {
			durations[id] = append(durations[id], time.Duration(*ms)*time.Millisecond)
		}
	}
	for id, c := range counts {
		out[id] = store.BuildWorkflowStats(c, durations[id])
	}
	return out, nil
}

// RecentExecutions returns each workflow's newest rows from ONE windowed
// statement. The projection is the four narrow columns plus the id; the JSON
// columns are never read, so a page of fifty workflow rows costs at most
// 50 × MaxRecentExecutionsPerWorkflow narrow rows. perWorkflow is a bind
// parameter — only the bound changes, never the SQL text.
func (r *executionRepo) RecentExecutions(ctx context.Context, ns namespace.Namespace, ids []types.WorkflowID, perWorkflow int) (map[types.WorkflowID][]store.ExecutionSummary, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.WorkflowID][]store.ExecutionSummary, len(ids))
	normalized := store.NormalizeExecutionWorkflowIDs(ids)
	limit := store.NormalizeRecentExecutionsPerWorkflow(perWorkflow)
	if len(normalized) == 0 || limit == 0 {
		return out, nil
	}

	var rows []struct {
		WorkflowID  string
		ExecutionID types.ExecutionID
		Status      types.ExecutionStatus
		CreatedAt   time.Time
		UpdatedAt   time.Time
		FinishedAt  *time.Time
	}
	err := r.db.WithContext(ctx).Raw(`
SELECT workflow_id, execution_id, status, created_at, updated_at, finished_at
FROM (
    SELECT workflow_id, execution_id, status, created_at, updated_at, finished_at,
           ROW_NUMBER() OVER (
               PARTITION BY workflow_id
               ORDER BY created_at DESC, execution_id ASC
           ) AS rn
    FROM xflow_executions
    WHERE namespace = ? AND workflow_id IN ?
) AS ranked
WHERE rn <= ?
ORDER BY workflow_id ASC, created_at DESC, execution_id ASC`,
		string(ns), toStrings(normalized), limit).Scan(&rows).Error
	if err := wrapDBErr("recent executions in scope", err); err != nil {
		return nil, err
	}
	for _, row := range rows {
		id := types.WorkflowID(row.WorkflowID)
		out[id] = append(out[id], store.ExecutionSummary{
			ExecutionID: row.ExecutionID,
			Status:      row.Status,
			CreatedAt:   row.CreatedAt,
			UpdatedAt:   row.UpdatedAt,
			FinishedAt:  row.FinishedAt,
		})
	}
	return out, nil
}

// ExecutionNodeProgress aggregates the node rows of the requested executions in
// ONE statement: a JOIN to xflow_executions applies the namespace scope, so a
// forged execution id from another tenant yields no row rather than a count.
// The projection is attempt arithmetic and a terminal-status tally only; node
// outputs, ports and lease tokens are never read.
func (r *executionRepo) ExecutionNodeProgress(ctx context.Context, ns namespace.Namespace, ids []types.ExecutionID) (map[types.ExecutionID]store.ExecutionNodeProgress, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.ExecutionID]store.ExecutionNodeProgress, len(ids))
	normalized := normalizeExecutionIDs(ids)
	if len(normalized) == 0 {
		return out, nil
	}
	var rows []struct {
		ExecutionID types.ExecutionID
		Retries     int64
		Done        int64
	}
	err := r.db.WithContext(ctx).Raw(`
SELECT n.execution_id,
       COALESCE(SUM(CASE WHEN n.attempt > 1 THEN n.attempt - 1 ELSE 0 END), 0) AS retries,
       COALESCE(SUM(CASE WHEN n.status IN ? THEN 1 ELSE 0 END), 0) AS done
FROM xflow_nodes AS n
JOIN xflow_executions AS e ON e.execution_id = n.execution_id
WHERE e.namespace = ? AND n.execution_id IN ?
GROUP BY n.execution_id`,
		terminalNodeStatusStrings(), string(ns), executionIDStrings(normalized)).Scan(&rows).Error
	if err := wrapDBErr("execution node progress in scope", err); err != nil {
		return nil, err
	}
	for _, row := range rows {
		out[row.ExecutionID] = store.ExecutionNodeProgress{Retries: int(row.Retries), Done: int(row.Done)}
	}
	return out, nil
}

// ExecutionDurations reads at most limit+1 narrow rows, newest first: the extra
// row is only the truncation probe that sets Sampled, and it never enters the
// sample. FinishedAt/UpdatedAt are resolved in Go by store.ExecutionDurationMS
// so both backends compute the identical duration.
func (r *executionRepo) ExecutionDurations(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter, limit int) (store.ExecutionDurationSample, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return store.ExecutionDurationSample{}, err
	}
	if err := filter.Validate(); err != nil {
		return store.ExecutionDurationSample{}, err
	}
	limit = store.NormalizeExecutionDurationLimit(limit)
	if limit == 0 {
		return store.ExecutionDurationSample{}, nil
	}
	var rows []struct {
		Status     types.ExecutionStatus
		CreatedAt  time.Time
		UpdatedAt  time.Time
		FinishedAt *time.Time
	}
	err := r.executionQuery(ctx, ns, filter).
		Where("status IN ?", statusStrings(store.TerminalExecutionStatuses())).
		Select("status, created_at, updated_at, finished_at").
		Order(store.ExecutionOrder).
		Limit(limit + 1).
		Scan(&rows).Error
	if err := wrapDBErr("execution durations in scope", err); err != nil {
		return store.ExecutionDurationSample{}, err
	}
	sample := store.ExecutionDurationSample{DurationsMS: make([]int64, 0, min(len(rows), limit))}
	if len(rows) > limit {
		sample.Sampled = true
		rows = rows[:limit]
	}
	for _, row := range rows {
		ms, _ := store.ExecutionDurationMS(row.Status, row.CreatedAt, row.UpdatedAt, row.FinishedAt)
		if ms != nil {
			sample.DurationsMS = append(sample.DurationsMS, *ms)
		}
	}
	return sample, nil
}

// CountActiveWorkflows counts DISTINCT workflow ids in one statement. The
// `workflow_id <> ”` predicate keeps unattributed rows from counting as a
// workflow, matching the per-workflow aggregations.
func (r *executionRepo) CountActiveWorkflows(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (int64, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return 0, err
	}
	if err := filter.Validate(); err != nil {
		return 0, err
	}
	var count int64
	err := r.executionQuery(ctx, ns, filter).
		Where("workflow_id <> ''").
		Distinct("workflow_id").
		Count(&count).Error
	if err := wrapDBErr("count active workflows in scope", err); err != nil {
		return 0, err
	}
	return count, nil
}

// TopFailingWorkflows runs two bounded statements: one GROUP BY for the ranked
// counts (at most `limit` rows), then one windowed read for the newest failure
// detail of exactly those workflows. The second statement is keyed by the ids
// the first returned, so it can never read a workflow the ranking excluded.
func (r *executionRepo) TopFailingWorkflows(ctx context.Context, ns namespace.Namespace, since time.Time, limit int) ([]store.FailingWorkflow, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	limit = store.NormalizeTopFailingLimit(limit)
	if limit == 0 {
		return []store.FailingWorkflow{}, nil
	}
	failedArgs := []any{string(ns), statusStrings(store.ExecutionFailureStatuses())}
	countSQL := `
SELECT workflow_id, COUNT(*) AS failed
FROM xflow_executions
WHERE namespace = ? AND workflow_id <> '' AND status IN ?`
	if !since.IsZero() {
		countSQL += ` AND created_at > ?`
		failedArgs = append(failedArgs, since)
	}
	countSQL += `
GROUP BY workflow_id
ORDER BY failed DESC, workflow_id ASC
LIMIT ?`
	failedArgs = append(failedArgs, limit)
	var top []struct {
		WorkflowID string
		Failed     int64
	}
	if err := wrapDBErr("top failing workflows in scope",
		r.db.WithContext(ctx).Raw(countSQL, failedArgs...).Scan(&top).Error); err != nil {
		return nil, err
	}
	if len(top) == 0 {
		return []store.FailingWorkflow{}, nil
	}
	ids := make([]string, len(top))
	out := make([]store.FailingWorkflow, len(top))
	for i, row := range top {
		ids[i] = row.WorkflowID
		out[i] = store.FailingWorkflow{WorkflowID: types.WorkflowID(row.WorkflowID), Failed: row.Failed}
	}

	detailArgs := []any{string(ns), ids, statusStrings(store.ExecutionFailureStatuses())}
	detailSQL := `
SELECT workflow_id, error_msg, created_at, updated_at, finished_at
FROM (
    SELECT workflow_id, error_msg, created_at, updated_at, finished_at,
           ROW_NUMBER() OVER (
               PARTITION BY workflow_id
               ORDER BY created_at DESC, execution_id ASC
           ) AS rn
    FROM xflow_executions
    WHERE namespace = ? AND workflow_id IN ? AND status IN ?`
	if !since.IsZero() {
		detailSQL += ` AND created_at > ?`
		detailArgs = append(detailArgs, since)
	}
	detailSQL += `
) AS ranked
WHERE rn = 1`
	var details []struct {
		WorkflowID string
		ErrorMsg   string
		CreatedAt  time.Time
		UpdatedAt  time.Time
		FinishedAt *time.Time
	}
	if err := wrapDBErr("latest failing workflow rows in scope",
		r.db.WithContext(ctx).Raw(detailSQL, detailArgs...).Scan(&details).Error); err != nil {
		return nil, err
	}
	byID := make(map[types.WorkflowID]int, len(out))
	for i, row := range out {
		byID[row.WorkflowID] = i
	}
	for _, row := range details {
		i, ok := byID[types.WorkflowID(row.WorkflowID)]
		if !ok {
			continue
		}
		out[i].LastError = row.ErrorMsg
		out[i].LastFailedAt = row.UpdatedAt
		if row.FinishedAt != nil {
			out[i].LastFailedAt = *row.FinishedAt
		}
	}
	return out, nil
}

// terminalNodeStatuses is the node-side analogue of
// store.TerminalExecutionStatuses, taken from types.IsTerminalNodeStatus's set.
// It is a literal list rather than a loop over types because the batch read
// needs the values as SQL bind parameters.
func terminalNodeStatuses() []types.NodeStatus {
	return []types.NodeStatus{
		types.NodeStatusSuccess,
		types.NodeStatusFailed,
		types.NodeStatusSkipped,
		types.NodeStatusCanceled,
		types.NodeStatusContinued,
		types.NodeStatusPinned,
	}
}

// terminalNodeStatusStrings converts terminalNodeStatuses to the strings the
// persisted node status column holds.
func terminalNodeStatusStrings() []string {
	statuses := terminalNodeStatuses()
	out := make([]string, len(statuses))
	for i, s := range statuses {
		out[i] = string(s)
	}
	return out
}

// normalizeExecutionIDs drops empty/oversized ids and deduplicates the rest,
// mirroring store.NormalizeExecutionWorkflowIDs for the execution-id side.
func normalizeExecutionIDs(ids []types.ExecutionID) []types.ExecutionID {
	out := make([]types.ExecutionID, 0, len(ids))
	seen := make(map[types.ExecutionID]struct{}, len(ids))
	for _, id := range ids {
		if id == "" || store.ValidateExecutionID(string(id)) != nil {
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

// executionIDStrings converts validated execution ids to the string slice the
// SQL `IN ?` expansion binds.
func executionIDStrings(ids []types.ExecutionID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

// toStrings converts workflow ids to the string slice the SQL `IN ?` expansion
// binds. It is a value conversion only; the ids have already been validated by
// store.NormalizeExecutionWorkflowIDs.
func toStrings(ids []types.WorkflowID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

// statusStrings converts statuses to the strings the persisted column holds.
func statusStrings(statuses []types.ExecutionStatus) []string {
	out := make([]string, len(statuses))
	for i, s := range statuses {
		out[i] = string(s)
	}
	return out
}
