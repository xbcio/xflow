package memstore

import (
	"context"
	"sort"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// The in-memory backend runs the same contract as the SQL one
// (store/storetest.ExecutionStatsContract). It scans the whole map under the
// store lock, exactly as ListExecutions/LatestExecutions do: this backend is
// for dev and tests, and a second in-memory index would be cost with no
// benefit at the sizes involved.
var _ store.ExecutionStatsReader = (*Store)(nil)

// ExecutionNodeProgress aggregates the requested executions' node rows. The
// execution record is consulted first so the scope check is against the
// execution itself, not merely the requested id: a forged id from another
// namespace has no record in this scope and is dropped.
func (s *Store) ExecutionNodeProgress(_ context.Context, ns namespace.Namespace, ids []types.ExecutionID) (map[types.ExecutionID]store.ExecutionNodeProgress, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.ExecutionID]store.ExecutionNodeProgress, len(ids))
	wanted := make(map[types.ExecutionID]struct{}, len(ids))
	for _, id := range ids {
		if id == "" || store.ValidateExecutionID(string(id)) != nil {
			continue
		}
		wanted[id] = struct{}{}
	}
	if len(wanted) == 0 {
		return out, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	allowed := make(map[types.ExecutionID]struct{}, len(wanted))
	for id := range wanted {
		if rec, ok := s.executions[id]; ok && rec.Namespace == string(ns) {
			allowed[id] = struct{}{}
		}
	}
	for _, node := range s.nodes {
		if _, ok := allowed[node.ExecutionID]; !ok {
			continue
		}
		p := out[node.ExecutionID]
		if node.Attempt > 1 {
			p.Retries += node.Attempt - 1
		}
		if types.IsTerminalNodeStatus(node.Status) {
			p.Done++
		}
		out[node.ExecutionID] = p
	}
	return out, nil
}

// ExecutionDurations returns the terminal durations of ns's matching rows,
// newest first, capped at limit with one extra row read to set Sampled. The
// duration definition is store.ExecutionDurationMS, shared with the SQL
// backend.
func (s *Store) ExecutionDurations(_ context.Context, ns namespace.Namespace, filter store.ExecutionFilter, limit int) (store.ExecutionDurationSample, error) {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var recs []*store.ExecutionRecord
	for _, rec := range s.executions {
		if !executionMatchesFilter(rec, ns, filter) {
			continue
		}
		if !store.IsTerminalExecutionStatus(rec.Status) {
			continue
		}
		recs = append(recs, rec)
	}
	sortExecutionsNewestFirst(recs)
	sample := store.ExecutionDurationSample{DurationsMS: make([]int64, 0, min(len(recs), limit))}
	if len(recs) > limit {
		sample.Sampled = true
		recs = recs[:limit]
	}
	for _, rec := range recs {
		if ms, _ := store.ExecutionDurationMS(rec.Status, rec.CreatedAt, rec.UpdatedAt, rec.FinishedAt); ms != nil {
			sample.DurationsMS = append(sample.DurationsMS, *ms)
		}
	}
	return sample, nil
}

// CountActiveWorkflows counts the DISTINCT non-empty workflow ids in ns that
// match filter.
func (s *Store) CountActiveWorkflows(_ context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (int64, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return 0, err
	}
	if err := filter.Validate(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[types.WorkflowID]struct{})
	for _, rec := range s.executions {
		if rec.WorkflowID == "" || !executionMatchesFilter(rec, ns, filter) {
			continue
		}
		seen[rec.WorkflowID] = struct{}{}
	}
	return int64(len(seen)), nil
}

// TopFailingWorkflows ranks the workflows with the most failed/timeout
// executions created strictly after since, then reads the newest failure's
// error for each. The ordering is failed descending, workflow id ascending, so
// equally-failing workflows have a deterministic order.
func (s *Store) TopFailingWorkflows(_ context.Context, ns namespace.Namespace, since time.Time, limit int) ([]store.FailingWorkflow, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	limit = store.NormalizeTopFailingLimit(limit)
	if limit == 0 {
		return []store.FailingWorkflow{}, nil
	}
	failures := make(map[types.ExecutionStatus]struct{})
	for _, status := range store.ExecutionFailureStatuses() {
		failures[status] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[types.WorkflowID]int64)
	newest := make(map[types.WorkflowID]*store.ExecutionRecord)
	for _, rec := range s.executions {
		if rec.Namespace != string(ns) || rec.WorkflowID == "" {
			continue
		}
		if _, ok := failures[rec.Status]; !ok {
			continue
		}
		if !since.IsZero() && !rec.CreatedAt.After(since) {
			continue
		}
		counts[rec.WorkflowID]++
		if cur, ok := newest[rec.WorkflowID]; !ok || executionIsNewer(rec, cur) {
			newest[rec.WorkflowID] = rec
		}
	}
	out := make([]store.FailingWorkflow, 0, min(len(counts), limit))
	for id, failed := range counts {
		out = append(out, store.FailingWorkflow{WorkflowID: id, Failed: failed})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Failed != out[j].Failed {
			return out[i].Failed > out[j].Failed
		}
		return out[i].WorkflowID < out[j].WorkflowID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	for i := range out {
		if rec := newest[out[i].WorkflowID]; rec != nil {
			out[i].LastError = rec.Error
			out[i].LastFailedAt = rec.UpdatedAt
			if rec.FinishedAt != nil {
				out[i].LastFailedAt = *rec.FinishedAt
			}
		}
	}
	return out, nil
}

// CountExecutionsByStatus returns the status counts of ns's executions matching
// filter. See store.ExecutionStatsReader for the scope and zero-fill rules.
func (s *Store) CountExecutionsByStatus(_ context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (map[types.ExecutionStatus]int64, error) {
	scope, err := s.scopedExecutions(ns, filter)
	if err != nil {
		return nil, err
	}
	out := store.ZeroedExecutionStatusCounts()
	for _, rec := range scope {
		out[rec.Status]++
	}
	return out, nil
}

// ExecutionHistogram groups ns's executions in filter's window into UTC-aligned
// buckets. Bucket generation (alignment, empty buckets, the 168 cap) is
// store.ExecutionHistogramBuckets, shared with the SQL backend so the two
// cannot disagree about which buckets exist.
func (s *Store) ExecutionHistogram(_ context.Context, ns namespace.Namespace, filter store.ExecutionFilter, bucket time.Duration) ([]store.StatusBucket, error) {
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
	scope, err := s.scopedExecutions(ns, filter)
	if err != nil {
		return nil, err
	}
	out := make([]store.StatusBucket, len(starts))
	byStart := make(map[int64]int, len(starts))
	for i, start := range starts {
		out[i] = store.StatusBucket{Start: start, Counts: store.ZeroedExecutionStatusCounts()}
		byStart[start.Unix()] = i
	}
	for _, rec := range scope {
		idx := store.ExecutionStatsBucketIndex(rec.CreatedAt, bucket)
		if i, ok := byStart[idx*int64(bucket/time.Second)]; ok {
			out[i].Counts[rec.Status]++
		}
	}
	return out, nil
}

// WorkflowExecutionStats aggregates the requested workflows' executions created
// strictly after since. See store.ExecutionStatsReader for the percentile
// sample rules; the sample is sorted here by the same total order
// LatestExecutions uses (CreatedAt descending, ExecutionID ascending).
func (s *Store) WorkflowExecutionStats(_ context.Context, ns namespace.Namespace, ids []types.WorkflowID, since time.Time, sampleCap int) (map[types.WorkflowID]store.WorkflowStats, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.WorkflowID]store.WorkflowStats, len(ids))
	wanted := store.NormalizeExecutionWorkflowIDs(ids)
	if len(wanted) == 0 {
		return out, nil
	}
	cap := store.NormalizeExecutionStatsSampleCap(sampleCap)
	wantedSet := make(map[types.WorkflowID]struct{}, len(wanted))
	for _, id := range wanted {
		wantedSet[id] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[types.WorkflowID]map[types.ExecutionStatus]int64, len(wanted))
	samples := make(map[types.WorkflowID][]*store.ExecutionRecord, len(wanted))
	for _, rec := range s.executions {
		// Unattributed rows (empty namespace or empty workflow id) are never
		// part of a per-workflow aggregation.
		if rec.Namespace != string(ns) || rec.WorkflowID == "" {
			continue
		}
		if _, ok := wantedSet[rec.WorkflowID]; !ok {
			continue
		}
		if !since.IsZero() && !rec.CreatedAt.After(since) {
			continue
		}
		c, ok := counts[rec.WorkflowID]
		if !ok {
			c = make(map[types.ExecutionStatus]int64)
			counts[rec.WorkflowID] = c
		}
		c[rec.Status]++
		if cap > 0 && store.IsTerminalExecutionStatus(rec.Status) {
			samples[rec.WorkflowID] = append(samples[rec.WorkflowID], rec)
		}
	}
	for id, c := range counts {
		recs := samples[id]
		sort.Slice(recs, func(i, j int) bool { return executionIsNewer(recs[i], recs[j]) })
		if len(recs) > cap {
			recs = recs[:cap]
		}
		durations := make([]time.Duration, 0, len(recs))
		for _, rec := range recs {
			if ms, _ := store.ExecutionDurationMS(rec.Status, rec.CreatedAt, rec.UpdatedAt, rec.FinishedAt); ms != nil {
				durations = append(durations, time.Duration(*ms)*time.Millisecond)
			}
		}
		out[id] = store.BuildWorkflowStats(c, durations)
	}
	return out, nil
}

// RecentExecutions returns each requested workflow's newest executions in ns.
// See store.ExecutionStatsReader for the order and limit rules.
func (s *Store) RecentExecutions(_ context.Context, ns namespace.Namespace, ids []types.WorkflowID, perWorkflow int) (map[types.WorkflowID][]store.ExecutionSummary, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.WorkflowID][]store.ExecutionSummary, len(ids))
	wanted := store.NormalizeExecutionWorkflowIDs(ids)
	limit := store.NormalizeRecentExecutionsPerWorkflow(perWorkflow)
	if len(wanted) == 0 || limit == 0 {
		return out, nil
	}
	wantedSet := make(map[types.WorkflowID]struct{}, len(wanted))
	for _, id := range wanted {
		wantedSet[id] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	grouped := make(map[types.WorkflowID][]*store.ExecutionRecord, len(wanted))
	for _, rec := range s.executions {
		if rec.Namespace != string(ns) || rec.WorkflowID == "" {
			continue
		}
		if _, ok := wantedSet[rec.WorkflowID]; !ok {
			continue
		}
		grouped[rec.WorkflowID] = append(grouped[rec.WorkflowID], rec)
	}
	for id, recs := range grouped {
		sort.Slice(recs, func(i, j int) bool { return executionIsNewer(recs[i], recs[j]) })
		if len(recs) > limit {
			recs = recs[:limit]
		}
		summaries := make([]store.ExecutionSummary, len(recs))
		for i, rec := range recs {
			summaries[i] = store.ExecutionSummary{
				ExecutionID: rec.ExecutionID,
				Status:      rec.Status,
				CreatedAt:   rec.CreatedAt,
				UpdatedAt:   rec.UpdatedAt,
				FinishedAt:  rec.FinishedAt,
			}
		}
		out[id] = summaries
	}
	return out, nil
}
