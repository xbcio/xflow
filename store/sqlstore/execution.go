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
var _ store.LatestExecutionReader = (*executionRepo)(nil)

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
	updates := map[string]any{
		"status":    string(status),
		"error_msg": errMsg,
	}
	// finished_at is write-once, exactly like a node's: the COALESCE keeps the
	// FIRST terminal instant when a duplicate terminal write (a retry, a
	// cancel/complete race) arrives later. A non-terminal transition never
	// touches it, so a canceling execution that resumes cannot carry a
	// completion time it did not reach.
	if store.IsTerminalExecutionStatus(status) {
		updates["finished_at"] = gorm.Expr("COALESCE(finished_at, ?)", time.Now().UTC())
	}
	result := r.db.WithContext(ctx).
		Model(&dbExecution{}).
		Where("execution_id = ?", string(id)).
		Updates(updates)
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
	// StatusSet collapses the single and multi-select forms into one predicate.
	// A single value keeps the equality form (it is the same plan, and existing
	// callers/tests read that shape); several values expand to IN with every
	// member bound as a parameter.
	switch statuses := filter.StatusSet(); len(statuses) {
	case 0:
	case 1:
		q = q.Where("status = ?", string(statuses[0]))
	default:
		q = q.Where("status IN ?", statusStrings(statuses))
	}
	// A search prefix matches EITHER identity column: the execution id or the
	// workflow id it was submitted under. The same escaped pattern is bound to
	// both sides, and the escape character is declared explicitly because MySQL
	// and SQLite disagree about a backslash in a string literal, but both honor
	// ESCAPE '!'. Validate() has already refused anything outside the id
	// alphabet, so this is the second line of defense.
	if filter.Query != "" {
		pattern := store.ExecutionQueryLikePattern(filter.Query)
		q = q.Where("(execution_id LIKE ? ESCAPE '!' OR workflow_id LIKE ? ESCAPE '!')", pattern, pattern)
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

// LatestExecutions implements store.LatestExecutionReader with one statement.
//
// ROW_NUMBER() over PARTITION BY workflow_id ORDER BY created_at DESC,
// execution_id ASC, filtered to rn = 1, is the same total order the memstore
// applies: a single read cannot disagree with itself about which row is newest,
// and a per-id loop (up to one query per workflow id) is avoided. The predicate
// is exactly idx_namespace_workflow_id_created_at's leading columns, so the
// scan is scoped to one namespace and the requested ids; execution_id is not in
// that index, so a group whose rows share created_at still sorts by it, but
// such groups are bounded by executions created in the same millisecond for one
// workflow. The outer rn = 1 bounds the result to one row per requested id, so
// a workflow list page of at most 200 rows costs one statement and at most 200
// returned rows.
//
// The projection is deliberately narrow. The workflow-list caller reads only
// the identity (execution_id, namespace, workflow_id), the status and the two
// timestamps; workflow_def, params and runtime are JSON blobs proportional to
// the workflow and are never consulted by this capability. Selecting them at
// either level of the derived table made one page of 200 workflows read 200
// full definitions, so both the inner and the outer SELECT list only these six
// columns. The contract's callers MUST NOT read any field this projection
// leaves zero.
//
// Both `IN ?` expansion and window functions are supported by every dialect
// this package is used with (audit_repo.go already relies on the former; MySQL
// 8 and SQLite 3.25+ support the latter), so the single statement is portable
// across the supported dialects.
//
// The scope and the ids are bind parameters; nothing about the request is
// concatenated into the SQL. An id that cannot name a workflow (empty, or
// longer than the column) is dropped before the statement is built, and an
// empty id list sends no statement at all. On error the operation name is the
// only context added: the namespace, the SQL and the row contents stay out of
// the error and out of any log it reaches.
func (r *executionRepo) LatestExecutions(ctx context.Context, ns namespace.Namespace, workflowIDs []types.WorkflowID) (map[types.WorkflowID]*store.ExecutionRecord, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	out := make(map[types.WorkflowID]*store.ExecutionRecord, len(workflowIDs))
	if len(workflowIDs) == 0 {
		return out, nil
	}
	ids := make([]string, 0, len(workflowIDs))
	seen := make(map[string]struct{}, len(workflowIDs))
	for _, id := range workflowIDs {
		if id == "" || store.ValidateExecutionWorkflowID(string(id)) != nil {
			continue
		}
		if _, dup := seen[string(id)]; dup {
			continue
		}
		seen[string(id)] = struct{}{}
		ids = append(ids, string(id))
	}
	if len(ids) == 0 {
		return out, nil
	}

	var rows []dbExecution
	err := r.db.WithContext(ctx).Raw(`
SELECT execution_id, namespace, workflow_id, status, created_at, updated_at
FROM (
    SELECT execution_id, namespace, workflow_id, status, created_at, updated_at,
           ROW_NUMBER() OVER (
               PARTITION BY workflow_id
               ORDER BY created_at DESC, execution_id ASC
           ) AS rn
    FROM xflow_executions
    WHERE namespace = ? AND workflow_id IN ?
) AS ranked
WHERE rn = 1`, string(ns), ids).Find(&rows).Error
	if err := wrapDBErr("latest executions in scope", err); err != nil {
		return nil, err
	}
	for i := range rows {
		rec := fromDBExecution(&rows[i])
		out[rec.WorkflowID] = rec
	}
	return out, nil
}
