package rstate

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/types"
)

// This file pins the SQL-statement cost of the state provider's best-effort
// audit projection, in the one unit that can be compared against a saturated
// instance's statement counters: statements per logical operation. It also
// pins the boundary of that projection — the state provider writes the
// execution/node/signal projection tables and NOTHING to xflow_audit_events,
// whose only writers are the apiserver authz audit sink, the dead-letter
// receipt projector and the crash-safe audit reconcile worker.
//
// The statements are counted with GORM's DryRun mode over the real sqlstore
// repositories, so the SQL shape (and therefore the statement count) is the
// production one and no MySQL server is needed.

// sqlStatementLog records every statement GORM builds.
type sqlStatementLog struct {
	stmts []string
}

func (l *sqlStatementLog) LogMode(gormlogger.LogLevel) gormlogger.Interface { return l }
func (l *sqlStatementLog) Info(context.Context, string, ...interface{})     {}
func (l *sqlStatementLog) Warn(context.Context, string, ...interface{})     {}
func (l *sqlStatementLog) Error(context.Context, string, ...interface{})    {}

func (l *sqlStatementLog) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	if strings.TrimSpace(sql) == "" {
		return
	}
	l.stmts = append(l.stmts, sql)
}

func (l *sqlStatementLog) snapshot() []string {
	out := make([]string, len(l.stmts))
	copy(out, l.stmts)
	return out
}

func (l *sqlStatementLog) forTable(table string) []string {
	var out []string
	for _, s := range l.stmts {
		if strings.Contains(s, table) {
			out = append(out, strings.Join(strings.Fields(s), " "))
		}
	}
	return out
}

// sqlCountingStore sends every projection write through the real sqlstore
// repositories on a DryRun connection and mirrors it into memory, so reads
// answer as they would against a real server. Reads must be in-memory rather
// than DryRun: a DryRun query answers "no row" for every execution, which
// isTransient reads as "transient" — and a transient execution skips the
// projection this test exists to measure.
type sqlCountingStore struct {
	store.Store // reads (memstore)
	sql         store.Store
	log         *sqlStatementLog

	auditAppends int
}

func (s *sqlCountingStore) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	if err := s.sql.CreateExecution(ctx, rec); err != nil {
		return err
	}
	return s.Store.CreateExecution(ctx, rec)
}

func (s *sqlCountingStore) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	if err := s.sql.UpdateExecutionStatus(ctx, id, status, errMsg); err != nil {
		return err
	}
	return s.Store.UpdateExecutionStatus(ctx, id, status, errMsg)
}

func (s *sqlCountingStore) UpsertNode(ctx context.Context, rec *store.NodeRecord) error {
	if err := s.sql.UpsertNode(ctx, rec); err != nil {
		return err
	}
	return s.Store.UpsertNode(ctx, rec)
}

func (s *sqlCountingStore) SaveSignal(ctx context.Context, rec *store.SignalRecord) error {
	if err := s.sql.SaveSignal(ctx, rec); err != nil {
		return err
	}
	return s.Store.SaveSignal(ctx, rec)
}

func (s *sqlCountingStore) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	if _, err := s.sql.RevokeSignal(ctx, id, name); err != nil {
		return false, err
	}
	return s.Store.RevokeSignal(ctx, id, name)
}

func (s *sqlCountingStore) AppendAudit(ctx context.Context, rec *store.AuditRecord) error {
	s.auditAppends++
	return s.sql.AppendAudit(ctx, rec)
}

func newSQLCountingState(t *testing.T) (*Store, *sqlCountingStore) {
	t.Helper()
	log := &sqlStatementLog{}
	db, err := gorm.Open(gormmysql.New(gormmysql.Config{
		DSN:                       "u:p@tcp(127.0.0.1:1)/xflow",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:                 true,
		DisableAutomaticPing:   true,
		SkipDefaultTransaction: true,
		Logger:                 log,
	})
	if err != nil {
		t.Fatalf("gorm.Open (dry-run): %v", err)
	}
	mem := memstore.New()
	counting := &sqlCountingStore{Store: mem, sql: sqlstore.New(db), log: log}
	state := New(newRedisStateTestClient(t), counting, time.Hour)
	return state, counting
}

func (s *sqlCountingStore) reset() { s.log.stmts = nil }

// TestAuditProjectionNeverWritesTheAuditLedger pins the boundary that a
// statement-count investigation depends on: no state-provider operation writes
// xflow_audit_events. One accepted entry seed, taken through the whole node
// lifecycle, issues its statements against xflow_executions and xflow_nodes
// only.
func TestAuditProjectionNeverWritesTheAuditLedger(t *testing.T) {
	state, db := newSQLCountingState(t)
	tenant := namespace.Namespace("ledger-boundary")
	ctx := namespace.WithNamespace(context.Background(), tenant)

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "ledger-boundary",
		Nodes: []types.NodeDef{{Name: "only", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("ledger-boundary-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}
	idx, _ := g.NodeIndex("only")
	lease := &engine.TaskLease{
		LeaseID: "l1", LeaseToken: "t1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "only", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess, StoreOutput: true, Port: "main",
	}); err != nil {
		t.Fatal(err)
	}

	if db.auditAppends != 0 {
		t.Fatalf("state provider called AppendAudit %d times, want 0", db.auditAppends)
	}
	if ledger := db.log.forTable("xflow_audit_events"); len(ledger) != 0 {
		t.Fatalf("state provider issued %d statements against xflow_audit_events, want 0: %v", len(ledger), ledger)
	}
	if execs := db.log.forTable("xflow_executions"); len(execs) == 0 {
		t.Fatal("no statements against xflow_executions: the projection did not run")
	}
	if nodes := db.log.forTable("xflow_nodes"); len(nodes) == 0 {
		t.Fatal("no statements against xflow_nodes: the projection did not run")
	}
}

// TestAuditProjectionStatementCountPerOperation records the multiplicity that
// turns a request rate into a statement rate: how many SQL statements one
// logical state transition costs. Counts are asserted so a change that adds a
// write (or silently drops one) fails here rather than on a saturated instance.
func TestAuditProjectionStatementCountPerOperation(t *testing.T) {
	t.Run("create_execution", func(t *testing.T) {
		state, db := newSQLCountingState(t)
		ctx := namespace.WithNamespace(context.Background(), "stmt-count-create")
		db.reset()
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: "stmt-count-create-1", Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatal(err)
		}
		assertTableStatements(t, db, "xflow_executions", 1)
	})

	t.Run("entry_seed", func(t *testing.T) {
		state, db := newSQLCountingState(t)
		ctx := namespace.WithNamespace(context.Background(), "stmt-count-seed")
		req := seedRequestFor(t, "stmt-count-seed", "stmt-count-seed-key")
		db.reset()
		resp, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("admission state = %q, want accepted", resp.State)
		}
		// One seeded execution = exactly one INSERT, because the Lua creates
		// the execution and this projection is its only SQL row.
		assertTableStatements(t, db, "xflow_executions", 1)
	})

	t.Run("duplicate_entry_seed", func(t *testing.T) {
		state, db := newSQLCountingState(t)
		ctx := namespace.WithNamespace(context.Background(), "stmt-count-seed-dup")
		req := seedRequestFor(t, "stmt-count-seed-dup", "stmt-count-seed-dup-key")
		if _, err := state.SeedExecutionFromEntry(ctx, req); err != nil {
			t.Fatal(err)
		}
		db.reset()
		resp, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.Duplicate {
			t.Fatal("second admission must be a duplicate")
		}
		assertTableStatements(t, db, "xflow_executions", 0)
	})

	t.Run("acquire_task_lease", func(t *testing.T) {
		state, db := newSQLCountingState(t)
		ctx, lease := leaseFixture(t, state, "stmt-count-lease")
		db.reset()
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
		}
		assertTableStatements(t, db, "xflow_nodes", 1)
	})

	t.Run("commit_terminal_node", func(t *testing.T) {
		state, db := newSQLCountingState(t)
		ctx, lease := leaseFixture(t, state, "stmt-count-commit")
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
		}
		lease.Attempt = 1
		db.reset()
		if _, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
			ExecutionID: lease.Task.ExecutionID, NodeName: lease.Task.NodeName,
			NodeIdx: lease.Task.NodeIdx, ActivationID: lease.Task.ActivationID,
			LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
			Status: types.NodeStatusSuccess, StoreOutput: true, Port: "main",
		}); err != nil {
			t.Fatal(err)
		}
		// The node row, plus the execution's terminal status: the commit
		// finalizes the execution inside its Lua, so this is the only place the
		// terminal state can reach SQL.
		assertTableStatements(t, db, "xflow_nodes", 1)
		assertTableStatements(t, db, "xflow_executions", 1)
	})

	t.Run("deliver_signal_without_waiter", func(t *testing.T) {
		state, db := newSQLCountingState(t)
		ctx := namespace.WithNamespace(context.Background(), "stmt-count-signal")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: "stmt-count-signal-1", Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatal(err)
		}
		db.reset()
		if _, _, err := state.DeliverSignal(ctx, "stmt-count-signal-1", "go", map[string]any{"ok": true}); err != nil {
			t.Fatal(err)
		}
		assertTableStatements(t, db, "xflow_signals", 1)
	})
}

func assertTableStatements(t *testing.T, db *sqlCountingStore, table string, want int) {
	t.Helper()
	got := db.log.forTable(table)
	if len(got) != want {
		t.Fatalf("%s statements = %d, want %d: %v", table, len(got), want, got)
	}
	for _, stmt := range got {
		if len(stmt) > 120 {
			stmt = stmt[:120] + "..."
		}
		t.Logf("%s: %s", table, stmt)
	}
}

// leaseFixture creates a running one-node execution and returns its context and
// the lease the commit path expects. AcquireTaskLease is NOT called: each caller
// decides where its measurement window starts.
func leaseFixture(t *testing.T, state *Store, tenant string) (context.Context, *engine.TaskLease) {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace(tenant))
	g := testGraphOneNode()
	id := types.ExecutionID(tenant + "-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}
	idx, _ := g.NodeIndex("start")
	return ctx, &engine.TaskLease{
		LeaseID: engine.LeaseID("l-" + tenant), LeaseToken: engine.LeaseToken("t-" + tenant),
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
}

// seedRequestFor builds an entry-seed request whose whole trigger group is
// carried by the seed itself, so the admission also finalizes the execution.
func seedRequestFor(t *testing.T, name, admissionKey string) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	return engine.SeedExecutionFromEntryRequest{
		AdmissionKey: engine.AdmissionKey(admissionKey),
		WorkflowID:   types.WorkflowID("wf"), WorkflowVersion: "v1",
		EntryUnitID: gm.Name, EntryUnitIdx: gm.UnitIdx, Graph: g,
		Outcome:    engine.GroupOutcomeSuccess,
		Exits:      exits,
		ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	}
}

// TestAuditProjectionBlocksItsCaller pins the latency property that matters
// more than the statement count: the projection is awaited inline. No site
// detaches it into a goroutine, so a state transition cannot return until its
// SQL write has completed (or failed) — which is what makes a slow store show
// up as slow admission/commit rather than as a queue that hides it.
func TestAuditProjectionBlocksItsCaller(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := &blockingUpsertStore{
		countingStore: &countingStore{},
		entered:       entered,
		release:       release,
	}
	state := New(newRedisStateTestClient(t), blocking, time.Hour)

	done := make(chan error, 1)
	go func() {
		done <- state.UpsertNode(context.Background(), &engine.NodeSnapshot{
			ExecutionID: "blocking-1", Name: "n", Status: types.NodeStatusSuccess,
		})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("projection never reached the store")
	}
	select {
	case err := <-done:
		t.Fatalf("state transition returned (err=%v) while its SQL projection was still blocked: the write is detached", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
}

type blockingUpsertStore struct {
	*countingStore
	entered chan struct{}
	release chan struct{}
}

func (b *blockingUpsertStore) UpsertNode(ctx context.Context, rec *store.NodeRecord) error {
	close(b.entered)
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.countingStore.UpsertNode(ctx, rec)
}

// TestSeedProjectionKeepsTheRowWhenAFieldIsUnencodable: the seeded-execution
// projection is the execution's only SQL record, so a params/runtime value that
// cannot be marshalled must cost the row that FIELD, not the whole row (both
// JSON columns are nullable). It must also not be reported as an audit-store
// failure — the audit counters answer "did the projection diverge from Redis?",
// and an encode failure is not divergence.
func TestSeedProjectionKeepsTheRowWhenAFieldIsUnencodable(t *testing.T) {
	state, db := newSQLCountingState(t)
	mem := db.Store
	obs := newRecordingObserver()
	state.audit = obs

	tenant := namespace.Namespace("encode-failure")
	ctx := namespace.WithNamespace(context.Background(), tenant)
	req := seedRequestFor(t, "encode-failure", "encode-failure-key")
	req.Params = map[string]any{"nan": math.NaN()}
	req.Runtime = &types.Runtime{Vars: map[string]any{"inf": math.Inf(1)}}

	resp, err := state.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("admission state = %q, want accepted", resp.State)
	}

	rec, err := mem.GetExecution(ctx, resp.ExecutionID)
	if err != nil {
		t.Fatalf("admitted execution has no SQL row: %v", err)
	}
	if len(rec.Params) != 0 || len(rec.Runtime) != 0 {
		t.Fatalf("unencodable fields were persisted: params=%s runtime=%s", rec.Params, rec.Runtime)
	}
	stats := state.auditCounters.snapshot()
	if stats.OK["create_seeded_execution"] != 1 || stats.Failed["create_seeded_execution"] != 0 {
		t.Fatalf("audit counters ok=%d failed=%d, want 1/0",
			stats.OK["create_seeded_execution"], stats.Failed["create_seeded_execution"])
	}
	if obs.failed["create_seeded_execution"] != 0 {
		t.Fatalf("observer saw %d audit failures for an encode failure, want 0",
			obs.failed["create_seeded_execution"])
	}
}

// TestSeedProjectionSkipsTransientExecution guards the exclusion that this
// change must not disturb: a transient execution gets no SQL row at all.
func TestSeedProjectionSkipsTransientExecution(t *testing.T) {
	state, db := newSQLCountingState(t)
	ctx := namespace.WithNamespace(context.Background(), "transient-seed")
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "transient-seed",
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second
	db.reset()
	if _, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: engine.AdmissionKey("transient-seed-key"),
		WorkflowID:   types.WorkflowID("wf"), WorkflowVersion: "v1",
		EntryUnitID: gm.Name, EntryUnitIdx: gm.UnitIdx, Graph: g,
		Outcome:    engine.GroupOutcomeSuccess,
		Exits:      exits,
		ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	}); err != nil {
		t.Fatal(err)
	}
	assertTableStatements(t, db, "xflow_executions", 0)
}
