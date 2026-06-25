package memstore

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Store is an in-memory implementation of store.Store and
// store.Transactor for testing.
type Store struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*store.ExecutionRecord
	nodes      map[string]*store.NodeRecord // key: "execID/nodeName"
	signals    map[string]*signalEntry      // key: "execID/signalName"
	nextID     uint64
}

type signalEntry struct {
	record *store.SignalRecord
	status string // "active", "consumed", "revoked"
}

// compile-time interface checks
var (
	_ store.Store      = (*Store)(nil)
	_ store.Transactor = (*Store)(nil)
)

// New creates a new in-memory store.
func New() *Store {
	return &Store{
		executions: make(map[types.ExecutionID]*store.ExecutionRecord),
		nodes:      make(map[string]*store.NodeRecord),
		signals:    make(map[string]*signalEntry),
	}
}

// Transaction runs fn against this store, rolling back all writes if fn returns
// an error. It is a test-grade approximation: state is snapshotted before fn and
// restored on error. Unlike a real DB it does not isolate concurrent callers, so
// tests should not run overlapping transactions on the same Store.
func (s *Store) Transaction(_ context.Context, fn func(st store.Set) error) error {
	s.mu.Lock()
	snapExec := cloneExecutions(s.executions)
	snapNodes := cloneNodes(s.nodes)
	snapSignals := cloneSignals(s.signals)
	snapID := s.nextID
	s.mu.Unlock()

	bundle := store.Set{Execution: s, Node: s, Signal: s}
	if err := fn(bundle); err != nil {
		s.mu.Lock()
		s.executions = snapExec
		s.nodes = snapNodes
		s.signals = snapSignals
		s.nextID = snapID
		s.mu.Unlock()
		return err
	}
	return nil
}

func cloneExecutions(m map[types.ExecutionID]*store.ExecutionRecord) map[types.ExecutionID]*store.ExecutionRecord {
	out := make(map[types.ExecutionID]*store.ExecutionRecord, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}
	return out
}

func cloneNodes(m map[string]*store.NodeRecord) map[string]*store.NodeRecord {
	out := make(map[string]*store.NodeRecord, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}
	return out
}

func cloneSignals(m map[string]*signalEntry) map[string]*signalEntry {
	out := make(map[string]*signalEntry, len(m))
	for k, v := range m {
		recCp := *v.record
		out[k] = &signalEntry{record: &recCp, status: v.status}
	}
	return out
}

func (s *Store) nextAutoID() uint64 {
	s.nextID++
	return s.nextID
}

func nodeKey(id types.ExecutionID, name string) string {
	return string(id) + "/" + name
}

func signalKey(id types.ExecutionID, name string) string {
	return string(id) + "/" + name
}

// ---------------------------------------------------------------------------
// Executions
// ---------------------------------------------------------------------------

func (s *Store) CreateExecution(_ context.Context, rec *store.ExecutionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.ID = s.nextAutoID()
	cp := *rec
	s.executions[rec.ExecutionID] = &cp
	return nil
}

func (s *Store) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.executions[id]
	if !ok {
		return store.ErrNotFound
	}
	rec.Status = status
	rec.Error = errMsg
	rec.UpdatedAt = time.Now()
	return nil
}

func (s *Store) GetExecution(_ context.Context, id types.ExecutionID) (*store.ExecutionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.executions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	return &cp, nil
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (s *Store) UpsertNode(_ context.Context, rec *store.NodeRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := nodeKey(rec.ExecutionID, rec.NodeName)
	existing, ok := s.nodes[key]
	if ok {
		existing.NodeType = rec.NodeType
		existing.Status = rec.Status
		existing.Output = rec.Output
		existing.Port = rec.Port
		existing.SignalName = rec.SignalName
		existing.SignalConfig = rec.SignalConfig
		existing.Timeout = rec.Timeout
		existing.UpdatedAt = time.Now()
	} else {
		rec.ID = s.nextAutoID()
		now := time.Now()
		rec.CreatedAt = now
		rec.UpdatedAt = now
		cp := *rec
		s.nodes[key] = &cp
	}
	return nil
}

func (s *Store) GetNode(_ context.Context, id types.ExecutionID, name string) (*store.NodeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.nodes[nodeKey(id, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	return &cp, nil
}

func (s *Store) ListNodes(_ context.Context, id types.ExecutionID, opts store.ListOptions) ([]*store.NodeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*store.NodeRecord
	for _, rec := range s.nodes {
		if rec.ExecutionID == id {
			cp := *rec
			all = append(all, &cp)
		}
	}
	return applyPagination(all, opts), nil
}

func (s *Store) ListSuspendedBySignal(_ context.Context, id types.ExecutionID, signal string) ([]*store.NodeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*store.NodeRecord
	for _, rec := range s.nodes {
		if rec.ExecutionID == id && rec.Status == types.NodeStatusSuspended && rec.SignalName == signal {
			cp := *rec
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (s *Store) ListExpiredSuspensions(_ context.Context, now time.Time, opts store.ListOptions) ([]*store.NodeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*store.NodeRecord
	for _, rec := range s.nodes {
		if rec.Status == types.NodeStatusSuspended && rec.Timeout != nil && !rec.Timeout.After(now) {
			cp := *rec
			all = append(all, &cp)
		}
	}
	return applyPagination(all, opts), nil
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

func (s *Store) SaveSignal(_ context.Context, rec *store.SignalRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := signalKey(rec.ExecutionID, rec.SignalName)
	now := time.Now()
	if existing, ok := s.signals[key]; ok {
		existing.record.Payload = rec.Payload
		existing.record.UpdatedAt = now
		existing.status = "active"
	} else {
		rec.ID = s.nextAutoID()
		rec.CreatedAt = now
		rec.UpdatedAt = now
		cp := *rec
		s.signals[key] = &signalEntry{record: &cp, status: "active"}
	}
	return nil
}

func (s *Store) ConsumeSignal(_ context.Context, id types.ExecutionID, name string) (*store.SignalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := signalKey(id, name)
	entry, ok := s.signals[key]
	if !ok || entry.status != "active" {
		return nil, store.ErrNotFound
	}
	entry.status = "consumed"
	entry.record.UpdatedAt = time.Now()
	cp := *entry.record
	return &cp, nil
}

func (s *Store) RevokeSignal(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := signalKey(id, name)
	entry, ok := s.signals[key]
	if !ok || entry.status != "active" {
		return false, nil
	}
	entry.status = "revoked"
	entry.record.UpdatedAt = time.Now()
	return true, nil
}

func (s *Store) CountSignalsByNames(_ context.Context, id types.ExecutionID, names []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nameSet := make(map[string]struct{}, len(names))
	for _, n := range names {
		nameSet[n] = struct{}{}
	}
	count := 0
	for _, entry := range s.signals {
		if entry.record.ExecutionID == id && entry.status == "active" {
			if _, ok := nameSet[entry.record.SignalName]; ok {
				count++
			}
		}
	}
	return count, nil
}

func (s *Store) ListSignalsByNames(_ context.Context, id types.ExecutionID, names []string, opts store.ListOptions) ([]*store.SignalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(names) == 0 {
		return nil, nil
	}
	nameSet := make(map[string]struct{}, len(names))
	for _, n := range names {
		nameSet[n] = struct{}{}
	}
	var all []*store.SignalRecord
	for _, entry := range s.signals {
		if entry.record.ExecutionID == id && entry.status == "active" {
			if _, ok := nameSet[entry.record.SignalName]; ok {
				cp := *entry.record
				all = append(all, &cp)
			}
		}
	}
	return applySignalPagination(all, opts), nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func applyPagination(records []*store.NodeRecord, opts store.ListOptions) []*store.NodeRecord {
	if opts.Offset >= len(records) {
		return nil
	}
	records = records[opts.Offset:]
	if opts.Limit > 0 && opts.Limit < len(records) {
		records = records[:opts.Limit]
	}
	return records
}

func applySignalPagination(records []*store.SignalRecord, opts store.ListOptions) []*store.SignalRecord {
	if opts.Offset >= len(records) {
		return nil
	}
	records = records[opts.Offset:]
	if opts.Limit > 0 && opts.Limit < len(records) {
		records = records[:opts.Limit]
	}
	return records
}
