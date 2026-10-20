package sqlstore

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// nodeRepo implements store.Nodes.
type nodeRepo struct {
	db *gorm.DB
}

var _ store.Nodes = (*nodeRepo)(nil)

// upsertNodeUpdateColumns is the ON CONFLICT DO UPDATE column set for
// UpsertNode: every column store.UpsertNodeUpdateFields names, plus updated_at,
// which the contract deliberately leaves out because each backend refreshes it
// on its own.
//
// It is a package var rather than a literal inside the clause so
// node_upsert_contract_test.go can read it and check it against the contract.
// While it was inline, nothing compared it to anything — the cross-backend
// "field set contract" was a comment pointing at a list that only its own test
// read, and dropping a column here failed nothing.
var upsertNodeUpdateColumns = []string{
	"node_type", "status", "lease_id", "lease_token", "attempt", "output", "port",
	"signal_name", "signal_config", "timeout_at", "updated_at",
}

// upsertNodeWriteOnceColumns are the timing columns an upsert writes only when
// the stored value is still NULL. They are deliberately NOT part of
// store.UpsertNodeUpdateFields: that contract means "refresh on every
// conflict", and refreshing these would move the start line on a retry and the
// end line on a duplicate terminal write.
//
// The COALESCE keeps this a single statement — the audit statement-count
// contract pins one SQL statement per node transition.
var upsertNodeWriteOnceColumns = []string{"started_at", "finished_at"}

// conflictRowExpr names the value the conflicting INSERT proposed for col, in
// the dialect's own ON CONFLICT spelling. MySQL spells it VALUES(col);
// PostgreSQL and SQLite expose the would-be row as the excluded pseudo-table.
// sqlstore.New takes an already-configured *gorm.DB and the caller chooses the
// dialector (MySQL/Postgres/SQLite), so hard-coding either spelling makes every
// node upsert a syntax error on the other.
func conflictRowExpr(dialect, col string) string {
	if dialect == "mysql" {
		return "VALUES(" + col + ")"
	}
	return "excluded." + col
}

func nodeUpsertAssignments(dialect string) clause.Set {
	updates := clause.AssignmentColumns(upsertNodeUpdateColumns)
	for _, col := range upsertNodeWriteOnceColumns {
		updates = append(updates, clause.Assignment{
			Column: clause.Column{Name: col},
			Value:  gorm.Expr("COALESCE(" + col + ", " + conflictRowExpr(dialect, col) + ")"),
		})
	}
	return updates
}

// UpsertNode updates the same field set as the memstore implementation; see
// store.UpsertNodeUpdateFields for the contract and the tests that bind both
// backends to it.
func (r *nodeRepo) UpsertNode(ctx context.Context, rec *store.NodeRecord) error {
	now := time.Now()
	rec.CreatedAt = now
	rec.UpdatedAt = now

	d := toDBNode(rec)
	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "execution_id"},
				{Name: "node_name"},
			},
			DoUpdates: nodeUpsertAssignments(r.db.Dialector.Name()),
		}).
		Create(d).Error
	if err == nil {
		rec.ID = d.ID
	}
	return wrapDBErr(fmt.Sprintf("upsert node %q/%q", rec.ExecutionID, rec.NodeName), err)
}

func (r *nodeRepo) GetNode(ctx context.Context, id types.ExecutionID, name string) (*store.NodeRecord, error) {
	var d dbNode
	err := r.db.WithContext(ctx).
		Where("execution_id = ? AND node_name = ?", string(id), name).
		First(&d).Error
	if err := wrapDBErr(fmt.Sprintf("get node %q/%q", id, name), err); err != nil {
		return nil, err
	}
	return fromDBNode(&d), nil
}

func (r *nodeRepo) ListNodes(ctx context.Context, id types.ExecutionID, opts store.ListOptions) ([]*store.NodeRecord, error) {
	var ds []*dbNode
	q := r.db.WithContext(ctx).
		Where("execution_id = ?", string(id)).
		Order("id")
	err := applyPagination(q, opts).Find(&ds).Error
	if err := wrapDBErr(fmt.Sprintf("list nodes %q", id), err); err != nil {
		return nil, err
	}
	return fromDBNodes(ds), nil
}

func (r *nodeRepo) ListSuspendedBySignal(ctx context.Context, id types.ExecutionID, signal string) ([]*store.NodeRecord, error) {
	var ds []*dbNode
	err := r.db.WithContext(ctx).
		Where("execution_id = ? AND status = ? AND signal_name = ?", string(id), string(types.NodeStatusSuspended), signal).
		Find(&ds).Error
	if err := wrapDBErr(fmt.Sprintf("list suspended by signal %q/%q", id, signal), err); err != nil {
		return nil, err
	}
	return fromDBNodes(ds), nil
}

func (r *nodeRepo) ListExpiredSuspensions(ctx context.Context, now time.Time, opts store.ListOptions) ([]*store.NodeRecord, error) {
	var ds []*dbNode
	q := r.db.WithContext(ctx).
		Where("status = ? AND timeout_at IS NOT NULL AND timeout_at <= ?", string(types.NodeStatusSuspended), now).
		Order("id")
	err := applyPagination(q, opts).Find(&ds).Error
	if err := wrapDBErr("list expired suspensions", err); err != nil {
		return nil, err
	}
	return fromDBNodes(ds), nil
}
