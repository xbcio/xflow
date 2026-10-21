package sqlstore

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// executionRepo implements store.Executions.
type executionRepo struct {
	db *gorm.DB
}

var _ store.Executions = (*executionRepo)(nil)
var _ store.ExecutionDriftScanner = (*executionRepo)(nil)

func (r *executionRepo) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	if err := store.ValidateExecutionWorkflowID(string(rec.WorkflowID)); err != nil {
		return err
	}
	d := toDBExecution(rec)
	if err := wrapDBErr(fmt.Sprintf("create execution %q", rec.ExecutionID), r.db.WithContext(ctx).Create(d).Error); err != nil {
		return err
	}
	rec.ID = d.ID
	return nil
}

func (r *executionRepo) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	result := r.db.WithContext(ctx).
		Model(&dbExecution{}).
		Where("execution_id = ?", string(id)).
		Updates(map[string]any{
			"status":    string(status),
			"error_msg": errMsg,
		})
	if err := wrapDBErr(fmt.Sprintf("update execution status %q", id), result.Error); err != nil {
		return err
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("update execution status %q: %w", id, store.ErrNotFound)
	}
	return nil
}

func (r *executionRepo) GetExecution(ctx context.Context, id types.ExecutionID) (*store.ExecutionRecord, error) {
	var d dbExecution
	err := r.db.WithContext(ctx).
		Where("execution_id = ?", string(id)).
		First(&d).Error
	if err := wrapDBErr(fmt.Sprintf("get execution %q", id), err); err != nil {
		return nil, err
	}
	return fromDBExecution(&d), nil
}

// executionQuery builds the scoped, filtered query shared by List and Count.
//
// The namespace predicate is a plain equality test, never an "is the scope
// empty OR does it match" disjunction and never a LIKE: an unscoped read is a
// cross-tenant enumeration, so an unusable scope is refused before this is
// reached (see the validate call at each entry point) and the SQL that does go
// out can only ever name one namespace. A row whose namespace is stored empty
// therefore matches no query this function can build — including the one for
// namespace "", because every query is built from a scope that validate already
// refused if empty.
func (r *executionRepo) executionQuery(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) *gorm.DB {
	q := r.db.WithContext(ctx).
		Model(&dbExecution{}).
		Where("namespace = ?", string(ns))
	if filter.Status != "" {
		q = q.Where("status = ?", string(filter.Status))
	}
	// Exact equality, never LIKE: a workflow filter is an identity lookup, and a
	// row stored with an empty workflow_id (unattributed) matches no non-empty
	// value — the same fail-closed rule the namespace predicate above applies.
	if filter.WorkflowID != "" {
		q = q.Where("workflow_id = ?", string(filter.WorkflowID))
	}
	// Exclusive on both sides, so adjacent windows tile without overlapping.
	if !filter.CreatedAfter.IsZero() {
		q = q.Where("created_at > ?", filter.CreatedAfter)
	}
	if !filter.CreatedBefore.IsZero() {
		q = q.Where("created_at < ?", filter.CreatedBefore)
	}
	return q
}

func (r *executionRepo) ListExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter, opts store.ListOptions) ([]*store.ExecutionRecord, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	var ds []*dbExecution
	q := r.executionQuery(ctx, ns, filter).Order(store.ExecutionOrder)
	err := applyPagination(q, opts).Find(&ds).Error
	if err := wrapDBErr(fmt.Sprintf("list executions in %q", ns), err); err != nil {
		return nil, err
	}
	// A fresh, non-nil, zero-length slice for no rows: this is the payload of a
	// list endpoint, and the API spec requires `list: []` rather than null.
	return fromDBExecutions(ds), nil
}

func (r *executionRepo) CountExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (int64, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return 0, err
	}
	if err := filter.Validate(); err != nil {
		return 0, err
	}
	var count int64
	err := r.executionQuery(ctx, ns, filter).Count(&count).Error
	if err := wrapDBErr(fmt.Sprintf("count executions in %q", ns), err); err != nil {
		return 0, err
	}
	return count, nil
}

// ListStaleRunningExecutions implements store.ExecutionDriftScanner: the
// durable projection's "running" rows whose updated_at has not moved for the
// caller's staleness window. Only the three columns the repair needs are read;
// the rest of the row (definition, params, runtime) is not.
//
// Two fail-closed choices, both about not widening a repair into a sweep:
// a non-positive limit is refused instead of being read as "no limit", and a
// malformed owned entry is refused before any SQL is built. An empty owned set
// is the legacy cross-namespace scan the interface documents.
func (r *executionRepo) ListStaleRunningExecutions(ctx context.Context, owned []namespace.Namespace, updatedBefore time.Time, limit int) ([]store.ExecutionDriftCandidate, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("list stale running executions: limit must be positive, got %d", limit)
	}
	q := r.db.WithContext(ctx).
		Model(&dbExecution{}).
		Select("execution_id", "namespace", "updated_at").
		Where("status = ?", string(types.ExecutionStatusRunning)).
		Where("updated_at < ?", updatedBefore)
	if len(owned) > 0 {
		values := make([]string, 0, len(owned))
		for _, ns := range owned {
			if err := store.ValidateNamespaceScope(ns); err != nil {
				return nil, err
			}
			values = append(values, string(ns))
		}
		q = q.Where("namespace IN ?", values)
	}
	var rows []dbExecution
	err := q.Order("updated_at ASC").Order("id ASC").Limit(limit).Find(&rows).Error
	if err := wrapDBErr("list stale running executions", err); err != nil {
		return nil, err
	}
	out := make([]store.ExecutionDriftCandidate, 0, len(rows))
	for i := range rows {
		out = append(out, store.ExecutionDriftCandidate{
			ID:        rows[i].ExecutionID,
			Namespace: namespace.Namespace(rows[i].Namespace),
			UpdatedAt: rows[i].UpdatedAt,
		})
	}
	return out, nil
}
