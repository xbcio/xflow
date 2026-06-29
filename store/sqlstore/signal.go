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

// signalRepo implements store.Signals.
type signalRepo struct {
	db *gorm.DB
}

var _ store.Signals = (*signalRepo)(nil)

func (r *signalRepo) SaveSignal(ctx context.Context, rec *store.SignalRecord) error {
	now := time.Now()
	rec.Status = "active"
	rec.CreatedAt = now
	rec.UpdatedAt = now

	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "execution_id"},
				{Name: "signal_name"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"payload":    rec.Payload,
				"status":     "active",
				"updated_at": now,
			}),
		}).
		Create(rec).Error
}

func (r *signalRepo) ConsumeSignal(ctx context.Context, id types.ExecutionID, name string) (*store.SignalRecord, error) {
	var rec store.SignalRecord

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("execution_id = ? AND signal_name = ? AND status = 'active'", string(id), name).
			First(&rec).Error; err != nil {
			return err
		}

		return tx.
			Model(&store.SignalRecord{}).
			Where("execution_id = ? AND signal_name = ?", string(id), name).
			Update("status", "consumed").Error
	})

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("consume signal %q/%q: %w", id, name, err)
	}
	return &rec, nil
}

func (r *signalRepo) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&store.SignalRecord{}).
		Where("execution_id = ? AND signal_name = ? AND status = 'active'", string(id), name).
		Update("status", "revoked")
	if result.Error != nil {
		return false, fmt.Errorf("revoke signal %q/%q: %w", id, name, result.Error)
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
		Where("execution_id = ? AND status = 'active' AND signal_name IN ?", string(id), names).
		Count(&count).Error
	return int(count), err
}

func (r *signalRepo) ListSignalsByNames(ctx context.Context, id types.ExecutionID, names []string, opts store.ListOptions) ([]*store.SignalRecord, error) {
	if len(names) == 0 {
		return nil, nil
	}
	var records []*store.SignalRecord
	err := r.db.WithContext(ctx).
		Where("execution_id = ? AND status = 'active' AND signal_name IN ?", string(id), names).
		Order("id").
		Limit(opts.Limit).
		Offset(opts.Offset).
		Find(&records).Error
	return records, err
}
