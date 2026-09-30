package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type runnerPoolRepo struct{ db *gorm.DB }

var _ store.RunnerPoolStore = (*runnerPoolRepo)(nil)

// NewRunnerPoolStore returns the SQL-backed runner pool store.
func NewRunnerPoolStore(db *gorm.DB) store.RunnerPoolStore {
	return &runnerPoolRepo{db: db}
}

func encodeLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	b, err := json.Marshal(labels)
	if err != nil {
		// json.Marshal cannot fail for map[string]string. Keep this defensive
		// fallback aligned with encodeList rather than panic on future widening.
		return "{}"
	}
	return string(b)
}

func decodeLabels(raw string) (map[string]string, error) {
	if raw == "" {
		return nil, nil
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(raw), &labels); err != nil {
		return nil, fmt.Errorf("decode runner pool labels: %w", err)
	}
	return labels, nil
}

func poolToRow(pool store.RunnerPool) dbRunnerPool {
	var deletedAt *time.Time
	if !pool.DeletedAt.IsZero() {
		t := pool.DeletedAt.UTC()
		deletedAt = &t
	}
	return dbRunnerPool{
		ID: pool.ID, Name: pool.Name, OwnerKind: pool.OwnerKind,
		OwnerNamespace: pool.OwnerNamespace, AllowedNamespaces: encodeList(pool.AllowedNamespaces),
		AllowedNodeTypes: encodeList(pool.AllowedNodeTypes), Labels: encodeLabels(pool.Labels),
		MaxInstances: pool.MaxInstances, InheritNamespaces: pool.InheritNamespaces,
		Paused: pool.Paused, CreatedAt: pool.CreatedAt, DeletedAt: deletedAt,
	}
}

func rowToPool(row dbRunnerPool) (store.RunnerPool, error) {
	namespaces, err := decodeList(row.AllowedNamespaces)
	if err != nil {
		return store.RunnerPool{}, fmt.Errorf("runner pool %s: allowed_namespaces: %w", row.ID, err)
	}
	nodeTypes, err := decodeList(row.AllowedNodeTypes)
	if err != nil {
		return store.RunnerPool{}, fmt.Errorf("runner pool %s: allowed_node_types: %w", row.ID, err)
	}
	labels, err := decodeLabels(row.Labels)
	if err != nil {
		return store.RunnerPool{}, fmt.Errorf("runner pool %s: %w", row.ID, err)
	}
	pool := store.RunnerPool{
		ID: row.ID, Name: row.Name, OwnerKind: row.OwnerKind,
		OwnerNamespace: row.OwnerNamespace, AllowedNamespaces: namespaces,
		AllowedNodeTypes: nodeTypes, Labels: labels, MaxInstances: row.MaxInstances,
		InheritNamespaces: row.InheritNamespaces, Paused: row.Paused,
		CreatedAt: row.CreatedAt,
	}
	if row.DeletedAt != nil {
		pool.DeletedAt = row.DeletedAt.UTC()
	}
	return pool, nil
}

func visiblePoolQuery(db *gorm.DB, scope store.OwnerScope) (*gorm.DB, bool) {
	if scope.Validate() != nil {
		return db, false
	}
	q := db.Where("deleted_at IS NULL")
	if !scope.All {
		q = q.Where("owner_kind = ? AND owner_namespace = ?", store.PoolOwnerTenant, scope.Namespace)
	}
	return q, true
}

func (r *runnerPoolRepo) CreatePool(ctx context.Context, pool store.RunnerPool) error {
	row := poolToRow(pool)
	return r.db.WithContext(ctx).Create(&row).Error
}

func (r *runnerPoolRepo) GetPool(ctx context.Context, id string, scope store.OwnerScope) (store.RunnerPool, error) {
	q, valid := visiblePoolQuery(r.db.WithContext(ctx), scope)
	if !valid {
		return store.RunnerPool{}, store.ErrRunnerPoolNotFound
	}
	var row dbRunnerPool
	err := q.Where("id = ?", id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.RunnerPool{}, store.ErrRunnerPoolNotFound
	}
	if err != nil {
		return store.RunnerPool{}, err
	}
	return rowToPool(row)
}

func (r *runnerPoolRepo) ListPools(ctx context.Context, scope store.OwnerScope) ([]store.RunnerPool, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	q, _ := visiblePoolQuery(r.db.WithContext(ctx), scope)
	var rows []dbRunnerPool
	if err := q.Order("created_at ASC, id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]store.RunnerPool, 0, len(rows))
	for _, row := range rows {
		pool, err := rowToPool(row)
		if err != nil {
			return nil, err
		}
		out = append(out, pool)
	}
	return out, nil
}

func (r *runnerPoolRepo) UpdatePool(ctx context.Context, replacement store.RunnerPool, scope store.OwnerScope) error {
	q, valid := visiblePoolQuery(r.db.WithContext(ctx).Model(&dbRunnerPool{}), scope)
	if !valid {
		return store.ErrRunnerPoolNotFound
	}
	res := q.Where("id = ?", replacement.ID).Updates(map[string]any{
		"name": replacement.Name, "allowed_namespaces": encodeList(replacement.AllowedNamespaces),
		"allowed_node_types": encodeList(replacement.AllowedNodeTypes), "labels": encodeLabels(replacement.Labels),
		"max_instances": replacement.MaxInstances, "inherit_namespaces": replacement.InheritNamespaces,
		"paused": replacement.Paused,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 0 {
		return nil
	}
	// MySQL may report zero affected rows for an idempotent update. Re-read
	// visibility before deciding that the pool was absent.
	if _, err := r.GetPool(ctx, replacement.ID, scope); err != nil {
		return err
	}
	return nil
}

func (r *runnerPoolRepo) DeletePool(ctx context.Context, id string, scope store.OwnerScope) error {
	q, valid := visiblePoolQuery(r.db.WithContext(ctx).Model(&dbRunnerPool{}), scope)
	if !valid {
		return store.ErrRunnerPoolNotFound
	}
	res := q.Where("id = ?", id).Update("deleted_at", time.Now().UTC())
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return store.ErrRunnerPoolNotFound
	}
	return nil
}

func rowToRunnerInstance(row dbRunnerInstance) store.RunnerInstance {
	return store.RunnerInstance{
		PoolID: row.PoolID, SystemID: row.SystemID, RunnerID: row.RunnerID,
		InstanceUID: row.InstanceUID, State: row.State, CreatedAt: row.CreatedAt,
		LastEnrolledAt: row.LastEnrolledAt, StateChangedAt: row.StateChangedAt,
	}
}

func (r *runnerPoolRepo) EnrollInstance(ctx context.Context, req store.EnrollInstanceRequest) (store.EnrollInstanceResult, error) {
	var out store.EnrollInstanceResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pool dbRunnerPool
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND deleted_at IS NULL", req.PoolID).
			Take(&pool).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return store.ErrRunnerPoolNotFound
		}
		if err != nil {
			return err
		}

		var instance dbRunnerInstance
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("pool_id = ? AND system_id = ?", req.PoolID, req.SystemID).
			Take(&instance).Error
		if err == nil {
			switch instance.State {
			case store.InstanceActive:
				if err := tx.Model(&dbRunnerInstance{}).
					Where("pool_id = ? AND system_id = ?", req.PoolID, req.SystemID).
					Updates(map[string]any{"instance_uid": req.InstanceUID, "last_enrolled_at": req.Now}).Error; err != nil {
					return err
				}
				instance.InstanceUID = req.InstanceUID
				instance.LastEnrolledAt = req.Now
				out = store.EnrollInstanceResult{Instance: rowToRunnerInstance(instance)}
				return nil
			case store.InstanceDraining:
				return store.ErrRunnerInstanceNotActive
			case store.InstancePruned:
				if err := checkSQLRunnerInstanceLimit(tx, pool); err != nil {
					return err
				}
				if req.CandidateRunnerID == instance.RunnerID {
					return fmt.Errorf("sqlstore: pruned runner ID %q cannot be reused", req.CandidateRunnerID)
				}
				res := tx.Model(&dbRunnerInstance{}).
					Where("pool_id = ? AND system_id = ? AND state = ?", req.PoolID, req.SystemID, store.InstancePruned).
					Updates(map[string]any{
						"runner_id": req.CandidateRunnerID, "instance_uid": req.InstanceUID,
						"state": store.InstanceActive, "created_at": req.Now,
						"last_enrolled_at": req.Now, "state_changed_at": req.Now,
					})
				if res.Error != nil {
					return res.Error
				}
				if res.RowsAffected != 1 {
					return store.ErrRunnerInstanceStateConflict
				}
				instance.RunnerID = req.CandidateRunnerID
				instance.InstanceUID = req.InstanceUID
				instance.State = store.InstanceActive
				instance.CreatedAt = req.Now
				instance.LastEnrolledAt = req.Now
				instance.StateChangedAt = req.Now
				out = store.EnrollInstanceResult{Instance: rowToRunnerInstance(instance), Created: true}
				return nil
			default:
				return store.ErrRunnerInstanceNotActive
			}
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if err := checkSQLRunnerInstanceLimit(tx, pool); err != nil {
			return err
		}
		instance = dbRunnerInstance{
			PoolID: req.PoolID, SystemID: req.SystemID, RunnerID: req.CandidateRunnerID,
			InstanceUID: req.InstanceUID, State: store.InstanceActive,
			CreatedAt: req.Now, LastEnrolledAt: req.Now, StateChangedAt: req.Now,
		}
		if err := tx.Create(&instance).Error; err != nil {
			return err
		}
		out = store.EnrollInstanceResult{Instance: rowToRunnerInstance(instance), Created: true}
		return nil
	})
	if err != nil {
		return store.EnrollInstanceResult{}, err
	}
	return out, nil
}

func checkSQLRunnerInstanceLimit(tx *gorm.DB, pool dbRunnerPool) error {
	if pool.MaxInstances <= 0 {
		return nil
	}
	var active int64
	if err := tx.Model(&dbRunnerInstance{}).
		Where("pool_id = ? AND state = ?", pool.ID, store.InstanceActive).
		Count(&active).Error; err != nil {
		return err
	}
	if active >= int64(pool.MaxInstances) {
		return store.ErrRunnerPoolInstanceLimit
	}
	return nil
}

func (r *runnerPoolRepo) ListInstances(ctx context.Context, poolID string, scope store.OwnerScope) ([]store.RunnerInstance, error) {
	if _, err := r.GetPool(ctx, poolID, scope); err != nil {
		return nil, err
	}
	var rows []dbRunnerInstance
	if err := r.db.WithContext(ctx).Where("pool_id = ?", poolID).
		Order("created_at ASC, runner_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]store.RunnerInstance, 0, len(rows))
	for _, row := range rows {
		out = append(out, rowToRunnerInstance(row))
	}
	return out, nil
}

// ListInstancesByState implements store.RunnerPoolStore.
func (r *runnerPoolRepo) ListInstancesByState(ctx context.Context, state store.InstanceState) ([]store.RunnerInstance, error) {
	var rows []dbRunnerInstance
	if err := r.db.WithContext(ctx).Where("state = ?", state).
		Order("state_changed_at ASC, runner_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]store.RunnerInstance, 0, len(rows))
	for _, row := range rows {
		out = append(out, rowToRunnerInstance(row))
	}
	return out, nil
}

// TransitionInstance implements store.RunnerPoolStore.
func (r *runnerPoolRepo) TransitionInstance(ctx context.Context, runnerID string, from, to store.InstanceState, now time.Time) error {
	res := r.db.WithContext(ctx).Model(&dbRunnerInstance{}).
		Where("runner_id = ? AND state = ?", runnerID, from).
		Updates(map[string]any{"state": to, "state_changed_at": now})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 1 {
		return nil
	}
	var row dbRunnerInstance
	err := r.db.WithContext(ctx).Select("runner_id", "state").Where("runner_id = ?", runnerID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.ErrRunnerInstanceNotFound
	}
	if err != nil {
		return err
	}
	return store.ErrRunnerInstanceStateConflict
}
