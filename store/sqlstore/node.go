package sqlstore

import (
	"context"
	"errors"
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

func (r *nodeRepo) UpsertNode(ctx context.Context, rec *store.NodeRecord) error {
	now := time.Now()
	rec.CreatedAt = now
	rec.UpdatedAt = now

	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "execution_id"},
				{Name: "node_name"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"node_type", "status", "output", "port",
				"signal_name", "signal_config", "timeout_at", "updated_at",
			}),
		}).
		Create(rec).Error
}

func (r *nodeRepo) GetNode(ctx context.Context, id types.ExecutionID, name string) (*store.NodeRecord, error) {
	var rec store.NodeRecord
	err := r.db.WithContext(ctx).
		Where("execution_id = ? AND node_name = ?", string(id), name).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get node %q/%q: %w", id, name, err)
	}
	return &rec, nil
}

func (r *nodeRepo) ListNodes(ctx context.Context, id types.ExecutionID, opts store.ListOptions) ([]*store.NodeRecord, error) {
	var records []*store.NodeRecord
	err := r.db.WithContext(ctx).
		Where("execution_id = ?", string(id)).
		Order("id").
		Limit(opts.Limit).
		Offset(opts.Offset).
		Find(&records).Error
	return records, err
}

func (r *nodeRepo) ListSuspendedBySignal(ctx context.Context, id types.ExecutionID, signal string) ([]*store.NodeRecord, error) {
	var records []*store.NodeRecord
	err := r.db.WithContext(ctx).
		Where("execution_id = ? AND status = 'suspended' AND signal_name = ?", string(id), signal).
		Find(&records).Error
	return records, err
}

func (r *nodeRepo) ListExpiredSuspensions(ctx context.Context, now time.Time, opts store.ListOptions) ([]*store.NodeRecord, error) {
	var records []*store.NodeRecord
	err := r.db.WithContext(ctx).
		Where("status = 'suspended' AND timeout_at IS NOT NULL AND timeout_at <= ?", now).
		Order("id").
		Limit(opts.Limit).
		Offset(opts.Offset).
		Find(&records).Error
	return records, err
}
