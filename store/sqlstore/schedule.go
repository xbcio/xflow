package sqlstore

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// scheduleOverrideRepo implements store.ScheduleOverrideStore.
type scheduleOverrideRepo struct {
	db *gorm.DB
}

var _ store.ScheduleOverrideStore = (*scheduleOverrideRepo)(nil)

func (r *scheduleOverrideRepo) GetScheduleOverride(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) (*store.ScheduleOverride, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	var d dbScheduleOverride
	err := r.db.WithContext(ctx).
		Where("namespace = ? AND workflow_id = ? AND node = ?", string(ns), string(workflowID), node).
		First(&d).Error
	if err := wrapDBErr("get schedule override", err); err != nil {
		return nil, err
	}
	return fromDBScheduleOverride(&d), nil
}

func (r *scheduleOverrideRepo) ListScheduleOverrides(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID) ([]*store.ScheduleOverride, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	var rows []dbScheduleOverride
	// Ordered by node so a listing is stable across calls and agrees with the
	// in-memory backend's, which sorts for the same reason.
	if err := r.db.WithContext(ctx).
		Where("namespace = ? AND workflow_id = ?", string(ns), string(workflowID)).
		Order("node ASC").
		Find(&rows).Error; err != nil {
		return nil, wrapDBErr("list schedule overrides", err)
	}
	out := make([]*store.ScheduleOverride, 0, len(rows))
	for i := range rows {
		out = append(out, fromDBScheduleOverride(&rows[i]))
	}
	return out, nil
}

// PutScheduleOverride performs the compare-and-set inside a transaction with a
// row lock, exactly as PutSupply does: two operators PATCHing the same schedule
// with the same If-Match must not both succeed, and the second must see the
// conflict rather than overwrite the first.
func (r *scheduleOverrideRepo) PutScheduleOverride(ctx context.Context, rec *store.ScheduleOverride, ifMatch *uint64) (*store.ScheduleOverride, error) {
	if err := store.ValidateNamespaceScope(rec.Namespace); err != nil {
		return nil, err
	}
	ns, wf, node := string(rec.Namespace), string(rec.WorkflowID), rec.Node

	var out *store.ScheduleOverride
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cur dbScheduleOverride
		found := true
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("namespace = ? AND workflow_id = ? AND node = ?", ns, wf, node).
			First(&cur).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			found = false
		} else if err != nil {
			return err
		}
		var curRev uint64
		if found {
			curRev = cur.Revision
		}
		if ifMatch != nil && *ifMatch != curRev {
			return store.ErrRevisionConflict
		}

		next := dbScheduleOverride{
			Namespace:  ns,
			WorkflowID: rec.WorkflowID,
			Node:       node,
			Expression: cloneString(rec.Expression),
			Timezone:   cloneString(rec.Timezone),
			Misfire:    cloneString(rec.Misfire),
			Overlap:    cloneString(rec.Overlap),
			Paused:     cloneBool(rec.Paused),
			Revision:   curRev + 1,
			UpdatedBy:  rec.UpdatedBy,
		}
		if found {
			next.ID = cur.ID
			// Updates(map) rather than Save/Updates(struct): a nil pointer field
			// must clear the column, and GORM's struct updates skip zero values,
			// which would make "stop overriding this field" a silent no-op.
			if err := tx.Model(&dbScheduleOverride{}).Where("id = ?", cur.ID).Updates(map[string]any{
				"expression": next.Expression,
				"timezone":   next.Timezone,
				"misfire":    next.Misfire,
				"overlap":    next.Overlap,
				"paused":     next.Paused,
				"revision":   next.Revision,
				"updated_by": next.UpdatedBy,
			}).Error; err != nil {
				return err
			}
		} else if err := tx.Create(&next).Error; err != nil {
			return err
		}

		var stored dbScheduleOverride
		if err := tx.Where("namespace = ? AND workflow_id = ? AND node = ?", ns, wf, node).
			First(&stored).Error; err != nil {
			return err
		}
		out = fromDBScheduleOverride(&stored)
		return nil
	})
	if errors.Is(err, store.ErrRevisionConflict) {
		return nil, err
	}
	if err := wrapDBErr("put schedule override", err); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *scheduleOverrideRepo) DeleteScheduleOverride(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string, ifMatch *uint64) error {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return err
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cur dbScheduleOverride
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("namespace = ? AND workflow_id = ? AND node = ?", string(ns), string(workflowID), node).
			First(&cur).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Absent is the state the caller asked for; only an unexpected
			// revision is a conflict.
			return nil
		} else if err != nil {
			return err
		}
		if ifMatch != nil && *ifMatch != cur.Revision {
			return store.ErrRevisionConflict
		}
		return tx.Where("id = ?", cur.ID).Delete(&dbScheduleOverride{}).Error
	})
	if errors.Is(err, store.ErrRevisionConflict) {
		return err
	}
	return wrapDBErr("delete schedule override", err)
}

func (r *scheduleOverrideRepo) DeleteScheduleOverridesForWorkflow(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID) (int, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return 0, err
	}
	res := r.db.WithContext(ctx).
		Where("namespace = ? AND workflow_id = ?", string(ns), string(workflowID)).
		Delete(&dbScheduleOverride{})
	if res.Error != nil {
		return 0, wrapDBErr("delete schedule overrides for workflow", res.Error)
	}
	return int(res.RowsAffected), nil
}
