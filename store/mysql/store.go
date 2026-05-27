package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Store is the MySQL implementation of engine/store.ClusterStore.
// It uses database/sql directly with parameterized queries — no ORM.
type Store struct {
	db *sql.DB
}

// New opens a MySQL connection pool and returns a ready-to-use Store.
// The dsn must include parseTime=true for correct time.Time scanning.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the connection pool.
func (s *Store) Close() error { return s.db.Close() }

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

func (s *Store) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO xflow_executions
		 (execution_id, workflow_name, workflow_def, params, status, error_msg, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(rec.ExecutionID), rec.WorkflowName, rec.WorkflowDef, rec.Params,
		string(rec.Status), rec.Error, rec.CreatedAt, rec.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create execution %q: %w", rec.ExecutionID, err)
	}
	return nil
}

func (s *Store) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.Status, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE xflow_executions SET status = ?, error_msg = ?, updated_at = ? WHERE execution_id = ?`,
		string(status), errMsg, time.Now(), string(id),
	)
	if err != nil {
		return fmt.Errorf("update execution status %q: %w", id, err)
	}
	return nil
}

func (s *Store) GetExecution(ctx context.Context, id types.ExecutionID) (*store.ExecutionRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, execution_id, workflow_name, workflow_def, params, status, error_msg, created_at, updated_at
		 FROM xflow_executions WHERE execution_id = ?`,
		string(id),
	)
	rec := &store.ExecutionRecord{}
	var eid, status string
	var errMsg sql.NullString
	if err := row.Scan(
		&rec.ID, &eid, &rec.WorkflowName, &rec.WorkflowDef, &rec.Params,
		&status, &errMsg, &rec.CreatedAt, &rec.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get execution %q: %w", id, err)
	}
	rec.ExecutionID = types.ExecutionID(eid)
	rec.Status = types.Status(status)
	if errMsg.Valid {
		rec.Error = errMsg.String
	}
	return rec, nil
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

func (s *Store) UpsertNode(ctx context.Context, rec *store.NodeRecord) error {
	now := time.Now()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO xflow_nodes
		 (execution_id, node_name, node_type, status, output, port, signal_name, signal_config, timeout_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   node_type     = VALUES(node_type),
		   status        = VALUES(status),
		   output        = VALUES(output),
		   port          = VALUES(port),
		   signal_name   = VALUES(signal_name),
		   signal_config = VALUES(signal_config),
		   timeout_at    = VALUES(timeout_at),
		   updated_at    = VALUES(updated_at)`,
		string(rec.ExecutionID), rec.NodeName, rec.NodeType,
		rec.Status, rec.Output, rec.Port,
		rec.SignalName, rec.SignalConfig, rec.Timeout,
		now, now,
	)
	if err != nil {
		return fmt.Errorf("upsert node %q/%q: %w", rec.ExecutionID, rec.NodeName, err)
	}
	return nil
}

func (s *Store) GetNode(ctx context.Context, id types.ExecutionID, name string) (*store.NodeRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, execution_id, node_name, node_type, status, output, port,
		        signal_name, signal_config, timeout_at, created_at, updated_at
		 FROM xflow_nodes WHERE execution_id = ? AND node_name = ?`,
		string(id), name,
	)
	return scanNodeRecord(row)
}

func (s *Store) ListNodes(ctx context.Context, id types.ExecutionID) ([]*store.NodeRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, execution_id, node_name, node_type, status, output, port,
		        signal_name, signal_config, timeout_at, created_at, updated_at
		 FROM xflow_nodes WHERE execution_id = ?`,
		string(id),
	)
	if err != nil {
		return nil, fmt.Errorf("list nodes %q: %w", id, err)
	}
	defer rows.Close()
	return scanNodeRecords(rows)
}

func (s *Store) ListSuspendedBySignal(ctx context.Context, id types.ExecutionID, signal string) ([]*store.NodeRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, execution_id, node_name, node_type, status, output, port,
		        signal_name, signal_config, timeout_at, created_at, updated_at
		 FROM xflow_nodes WHERE execution_id = ? AND status = 'suspended' AND signal_name = ?`,
		string(id), signal,
	)
	if err != nil {
		return nil, fmt.Errorf("list suspended by signal %q/%q: %w", id, signal, err)
	}
	defer rows.Close()
	return scanNodeRecords(rows)
}

func (s *Store) ListExpiredSuspensions(ctx context.Context, now time.Time) ([]*store.NodeRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, execution_id, node_name, node_type, status, output, port,
		        signal_name, signal_config, timeout_at, created_at, updated_at
		 FROM xflow_nodes WHERE status = 'suspended' AND timeout_at IS NOT NULL AND timeout_at <= ?`,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("list expired suspensions: %w", err)
	}
	defer rows.Close()
	return scanNodeRecords(rows)
}

// ---------------------------------------------------------------------------
// SignalStore
// ---------------------------------------------------------------------------

func (s *Store) SaveSignal(ctx context.Context, rec *store.SignalRecord) error {
	now := time.Now()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO xflow_signals (execution_id, signal_name, payload, status, created_at, updated_at)
		 VALUES (?, ?, ?, 'active', ?, ?)
		 ON DUPLICATE KEY UPDATE payload = VALUES(payload), status = 'active', updated_at = VALUES(updated_at)`,
		string(rec.ExecutionID), rec.SignalName, rec.Payload, now, now,
	)
	if err != nil {
		return fmt.Errorf("save signal %q/%q: %w", rec.ExecutionID, rec.SignalName, err)
	}
	return nil
}

func (s *Store) ConsumeSignal(ctx context.Context, id types.ExecutionID, name string) (*store.SignalRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx,
		`SELECT id, execution_id, signal_name, payload, created_at, updated_at
		 FROM xflow_signals WHERE execution_id = ? AND signal_name = ? FOR UPDATE`,
		string(id), name,
	)
	rec := &store.SignalRecord{}
	var eid string
	if err := row.Scan(&rec.ID, &eid, &rec.SignalName, &rec.Payload, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("consume signal %q/%q: %w", id, name, err)
	}
	rec.ExecutionID = types.ExecutionID(eid)

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM xflow_signals WHERE execution_id = ? AND signal_name = ?`,
		string(id), name,
	); err != nil {
		return nil, fmt.Errorf("delete signal %q/%q: %w", id, name, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit consume signal: %w", err)
	}
	return rec, nil
}

func (s *Store) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	result, err := s.db.ExecContext(ctx,
		`UPDATE xflow_signals SET status = 'revoked', updated_at = ?
		 WHERE execution_id = ? AND signal_name = ? AND status = 'active'`,
		time.Now(), string(id), name,
	)
	if err != nil {
		return false, fmt.Errorf("revoke signal %q/%q: %w", id, name, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("revoke signal rows affected: %w", err)
	}
	return rows > 0, nil
}

func (s *Store) CountSignalsByNames(ctx context.Context, id types.ExecutionID, names []string) (int, error) {
	if len(names) == 0 {
		return 0, nil
	}
	query := `SELECT COUNT(*) FROM xflow_signals WHERE execution_id = ? AND signal_name IN (?` +
		strings.Repeat(",?", len(names)-1) + `)`
	args := make([]any, 0, 1+len(names))
	args = append(args, string(id))
	for _, n := range names {
		args = append(args, n)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count signals: %w", err)
	}
	return count, nil
}

func (s *Store) ListSignalsByNames(ctx context.Context, id types.ExecutionID, names []string) ([]*store.SignalRecord, error) {
	if len(names) == 0 {
		return nil, nil
	}
	query := `SELECT id, execution_id, signal_name, payload, created_at, updated_at
	          FROM xflow_signals WHERE execution_id = ? AND signal_name IN (?` +
		strings.Repeat(",?", len(names)-1) + `)`
	args := make([]any, 0, 1+len(names))
	args = append(args, string(id))
	for _, n := range names {
		args = append(args, n)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list signals by names: %w", err)
	}
	defer rows.Close()

	var result []*store.SignalRecord
	for rows.Next() {
		rec := &store.SignalRecord{}
		var eid string
		if err := rows.Scan(&rec.ID, &eid, &rec.SignalName, &rec.Payload, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan signal: %w", err)
		}
		rec.ExecutionID = types.ExecutionID(eid)
		result = append(result, rec)
	}
	return result, rows.Err()
}

// ---------------------------------------------------------------------------
// scan helpers
// ---------------------------------------------------------------------------

type scannable interface {
	Scan(dest ...any) error
}

func scanNodeRecord(row scannable) (*store.NodeRecord, error) {
	rec := &store.NodeRecord{}
	var eid string
	var timeoutAt sql.NullTime
	if err := row.Scan(
		&rec.ID, &eid, &rec.NodeName, &rec.NodeType, &rec.Status,
		&rec.Output, &rec.Port, &rec.SignalName, &rec.SignalConfig,
		&timeoutAt, &rec.CreatedAt, &rec.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("scan node record: %w", err)
	}
	rec.ExecutionID = types.ExecutionID(eid)
	if timeoutAt.Valid {
		rec.Timeout = &timeoutAt.Time
	}
	return rec, nil
}

func scanNodeRecords(rows *sql.Rows) ([]*store.NodeRecord, error) {
	var result []*store.NodeRecord
	for rows.Next() {
		rec, err := scanNodeRecord(rows)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			result = append(result, rec)
		}
	}
	return result, rows.Err()
}
