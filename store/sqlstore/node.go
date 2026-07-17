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

// UpsertNode updates the same column set as the memstore implementation
// (see store/memstore UpsertNode): node_type, status, lease_id, lease_token,
// attempt, output, port, signal_name, signal_config, timeout_at, updated_at.
// Keep these in sync; store/memstore/contract_test.go guards the field set.
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
			DoUpdates: clause.AssignmentColumns([]string{
				"node_type", "status", "lease_id", "lease_token", "attempt", "output", "port",
				"signal_name", "signal_config", "timeout_at", "updated_at",
			}),
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
	opts = opts.Normalized()
	var ds []*dbNode
	err := r.db.WithContext(ctx).
		Where("execution_id = ?", string(id)).
		Order("id").
		Limit(opts.Limit).
		Offset(opts.Offset).
		Find(&ds).Error
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
	opts = opts.Normalized()
	var ds []*dbNode
	err := r.db.WithContext(ctx).
		Where("status = ? AND timeout_at IS NOT NULL AND timeout_at <= ?", string(types.NodeStatusSuspended), now).
		Order("id").
		Limit(opts.Limit).
		Offset(opts.Offset).
		Find(&ds).Error
	if err := wrapDBErr("list expired suspensions", err); err != nil {
		return nil, err
	}
	return fromDBNodes(ds), nil
}
