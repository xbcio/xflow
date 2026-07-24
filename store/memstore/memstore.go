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
	audit      []*store.AuditRecord
	nextID     uint64
}

type signalEntry struct {
	record *store.SignalRecord
	status types.SignalStatus // "active", "consumed", "revoked"
}

// compile-time interface checks
var (
	_ store.Store          = (*Store)(nil)
	_ store.Transactor     = (*Store)(nil)
	_ store.AuditReconciler = (*Store)(nil)
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
	snapAudit := cloneAudit(s.audit)
	snapID := s.nextID
	s.mu.Unlock()

	bundle := store.Set{Execution: s, Node: s, Signal: s, Audit: s}
	if err := fn(bundle); err != nil {
		s.mu.Lock()
		s.executions = snapExec
		s.nodes = snapNodes
		s.signals = snapSignals
		s.audit = snapAudit
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

func cloneAudit(in []*store.AuditRecord) []*store.AuditRecord {
	if in == nil {
		return nil
	}
	out := make([]*store.AuditRecord, len(in))
	for i, v := range in {
		cp := *v
		out[i] = &cp
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
		// Match sqlstore's ON CONFLICT update list: lease and attempt fields
		// must be refreshed so the memstore reflects the same lifecycle as the
		// SQL projection. Previously these were skipped, masking lease/attempt
		// bugs in tests that run against the memstore.
		existing.LeaseID = rec.LeaseID
		existing.LeaseToken = rec.LeaseToken
		existing.Attempt = rec.Attempt
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
	return paginate(all, opts), nil
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
	return paginate(all, opts), nil
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
		existing.status = types.SignalStatusActive
	} else {
		rec.ID = s.nextAutoID()
		rec.CreatedAt = now
		rec.UpdatedAt = now
		cp := *rec
		s.signals[key] = &signalEntry{record: &cp, status: types.SignalStatusActive}
	}
	return nil
}

func (s *Store) ConsumeSignal(_ context.Context, id types.ExecutionID, name string) (*store.SignalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := signalKey(id, name)
	entry, ok := s.signals[key]
	if !ok || entry.status != types.SignalStatusActive {
		return nil, store.ErrNotFound
	}
	entry.status = types.SignalStatusConsumed
	entry.record.UpdatedAt = time.Now()
	cp := *entry.record
	return &cp, nil
}

func (s *Store) RevokeSignal(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := signalKey(id, name)
	entry, ok := s.signals[key]
	if !ok || entry.status != types.SignalStatusActive {
		return false, nil
	}
	entry.status = types.SignalStatusRevoked
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
		if entry.record.ExecutionID == id && entry.status == types.SignalStatusActive {
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
		if entry.record.ExecutionID == id && entry.status == types.SignalStatusActive {
			if _, ok := nameSet[entry.record.SignalName]; ok {
				cp := *entry.record
				all = append(all, &cp)
			}
		}
	}
	return paginate(all, opts), nil
}

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

// AppendAudit records one append-only audit event. The in-memory store is a
// test-grade projection; production uses the SQL sink.
func (s *Store) AppendAudit(_ context.Context, rec *store.AuditRecord) error {
	if rec == nil {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.ID = s.nextAutoID()
	rec.SeqID = rec.ID
	cp := *rec
	s.audit = append(s.audit, &cp)
	return nil
}

// ListUnreconciledAdmissions returns admitted mutation audit rows (phase=
// "admission", outcome="admitted") older than `before` for which no
// outcome-phase row exists for the same (tenant, request_id). In-memory
// mirror of the SQL provider's pending scan; used by the T9 reconcile worker
// unit tests. Rows are returned oldest-first. When afterSeqID > 0 only rows
// with ID > afterSeqID are considered (cursor pagination).
func (s *Store) ListUnreconciledAdmissions(_ context.Context, before time.Time, afterSeqID uint64, limit int) ([]*store.AuditRecord, error) {
	if limit <= 0 {
		limit = 256
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Index existing outcome rows by (tenant, request_id) for the NOT EXISTS
	// filter.
	hasOutcome := make(map[string]bool, len(s.audit))
	for _, r := range s.audit {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.TenantID+"|"+r.RequestID] = true
		}
	}
	var out []*store.AuditRecord
	for _, r := range s.audit {
		if afterSeqID > 0 && r.ID <= afterSeqID {
			continue
		}
		if r.Phase != store.AuditPhaseAdmission || r.Outcome != store.AuditOutcomeAdmitted {
			continue
		}
		// The in-memory store has no separate created_at; use the event
		// timestamp. A zero timestamp (never set) is treated as old enough so
		// tests that build records directly can reconcile immediately.
		if !r.Timestamp.IsZero() && !r.Timestamp.Before(before) {
			continue
		}
		if r.RequestID != "" && hasOutcome[r.TenantID+"|"+r.RequestID] {
			continue
		}
		cp := *r
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// AppendOutcomeIfAbsent idempotently appends an outcome-phase row. In-memory
// mirror of the SQL provider's check-then-append + unique phase_key index:
// the mutex makes the check-then-append atomic, so a concurrent worker or a
// leader switch can never append a duplicate outcome. Returns appended=false
// when an outcome row already exists for the same (tenant, request_id).
func (s *Store) AppendOutcomeIfAbsent(_ context.Context, rec *store.AuditRecord) (bool, error) {
	if rec == nil {
		return false, store.ErrNotFound
	}
	rec.Phase = store.AuditPhaseOutcome
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec.RequestID != "" && rec.TenantID != "" {
		key := rec.TenantID + "|" + rec.RequestID
		for _, r := range s.audit {
			if r.Phase == store.AuditPhaseOutcome && r.TenantID+"|"+r.RequestID == key {
				return false, nil
			}
		}
	}
	rec.ID = s.nextAutoID()
	rec.SeqID = rec.ID
	cp := *rec
	s.audit = append(s.audit, &cp)
	return true, nil
}

// CountUnreconciledAdmissions returns the total count of pending admissions
// older than `before` and the timestamp of the oldest one (full-table backlog
// metrics, independent of the cursor). When no pending rows exist, pending=0
// and oldest is the zero time.
func (s *Store) CountUnreconciledAdmissions(_ context.Context, before time.Time) (int, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hasOutcome := make(map[string]bool, len(s.audit))
	for _, r := range s.audit {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.TenantID+"|"+r.RequestID] = true
		}
	}
	var count int
	var oldest time.Time
	for _, r := range s.audit {
		if r.Phase != store.AuditPhaseAdmission || r.Outcome != store.AuditOutcomeAdmitted {
			continue
		}
		if !r.Timestamp.IsZero() && !r.Timestamp.Before(before) {
			continue
		}
		if r.RequestID != "" && hasOutcome[r.TenantID+"|"+r.RequestID] {
			continue
		}
		count++
		if oldest.IsZero() || (!r.Timestamp.IsZero() && r.Timestamp.Before(oldest)) {
			oldest = r.Timestamp
		}
	}
	return count, oldest, nil
}

// AuditRecords returns a copy of the recorded audit events (test helper).
func (s *Store) AuditRecords() []*store.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*store.AuditRecord, len(s.audit))
	for i, v := range s.audit {
		cp := *v
		out[i] = &cp
	}
	return out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// paginate applies the ListOptions offset/limit to a slice of records. It
// normalizes opts first so a negative offset or limit never panics. It is
// generic over the record type so both node and signal listings share one
// implementation.
func paginate[T any](records []T, opts store.ListOptions) []T {
	opts = opts.Normalized()
	if opts.Offset >= len(records) {
		return nil
	}
	records = records[opts.Offset:]
	if opts.Limit > 0 && opts.Limit < len(records) {
		records = records[:opts.Limit]
	}
	return records
}
