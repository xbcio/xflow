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

// signalRepo implements store.Signals.
type signalRepo struct {
	db *gorm.DB
}

var _ store.Signals = (*signalRepo)(nil)

func (r *signalRepo) SaveSignal(ctx context.Context, rec *store.SignalRecord) error {
	now := time.Now()
	rec.Status = types.SignalStatusActive
	rec.CreatedAt = now
	rec.UpdatedAt = now

	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "execution_id"},
				{Name: "signal_name"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"payload":    rec.Payload,
				"status":     types.SignalStatusActive,
				"updated_at": now,
			}),
		}).
		Create(rec).Error
	return wrapDBErr(fmt.Sprintf("save signal %q/%q", rec.ExecutionID, rec.SignalName), err)
}

func (r *signalRepo) ConsumeSignal(ctx context.Context, id types.ExecutionID, name string) (*store.SignalRecord, error) {
	var rec store.SignalRecord

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("execution_id = ? AND signal_name = ? AND status = ?", string(id), name, types.SignalStatusActive).
			First(&rec).Error; err != nil {
			return err
		}

		return tx.
			Model(&store.SignalRecord{}).
			Where("execution_id = ? AND signal_name = ?", string(id), name).
			Update("status", types.SignalStatusConsumed).Error
	})

	if err := wrapDBErr(fmt.Sprintf("consume signal %q/%q", id, name), err); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (r *signalRepo) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&store.SignalRecord{}).
		Where("execution_id = ? AND signal_name = ? AND status = ?", string(id), name, types.SignalStatusActive).
		Update("status", types.SignalStatusRevoked)
	if err := wrapDBErr(fmt.Sprintf("revoke signal %q/%q", id, name), result.Error); err != nil {
		return false, err
	}
	return result.RowsAffected > 0, nil
}

func (r *signalRepo) CountSignalsByNames(ctx context.Context, id types.ExecutionID, names []string) (int, error) {
	if len(names) == 0 {
		return 0, nil
	}
	var count int64
	err := r.db.WithContext(ctx).
		Model(&store.SignalRecord{}).
		Where("execution_id = ? AND status = ? AND signal_name IN ?", string(id), types.SignalStatusActive, names).
		Count(&count).Error
	if err := wrapDBErr(fmt.Sprintf("count signals %q", id), err); err != nil {
		return 0, err
	}
	return int(count), nil
}

func (r *signalRepo) ListSignalsByNames(ctx context.Context, id types.ExecutionID, names []string, opts store.ListOptions) ([]*store.SignalRecord, error) {
	if len(names) == 0 {
		return nil, nil
	}
	opts = opts.Normalized()
	var records []*store.SignalRecord
	err := r.db.WithContext(ctx).
		Where("execution_id = ? AND status = ? AND signal_name IN ?", string(id), types.SignalStatusActive, names).
		Order("id").
		Limit(opts.Limit).
		Offset(opts.Offset).
		Find(&records).Error
	if err := wrapDBErr(fmt.Sprintf("list signals %q", id), err); err != nil {
		return nil, err
	}
	return records, nil
}
