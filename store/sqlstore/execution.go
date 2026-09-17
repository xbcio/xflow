package sqlstore

import (
	"context"
	"fmt"

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

func (r *executionRepo) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
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
