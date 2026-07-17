package sqlstore

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// executionRepo implements store.Executions.
type executionRepo struct {
	db *gorm.DB
}

var _ store.Executions = (*executionRepo)(nil)

func (r *executionRepo) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	return wrapDBErr(fmt.Sprintf("create execution %q", rec.ExecutionID), r.db.WithContext(ctx).Create(rec).Error)
}

func (r *executionRepo) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	result := r.db.WithContext(ctx).
		Model(&store.ExecutionRecord{}).
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
	var rec store.ExecutionRecord
	err := r.db.WithContext(ctx).
		Where("execution_id = ?", string(id)).
		First(&rec).Error
	if err := wrapDBErr(fmt.Sprintf("get execution %q", id), err); err != nil {
		return nil, err
	}
	return &rec, nil
}
