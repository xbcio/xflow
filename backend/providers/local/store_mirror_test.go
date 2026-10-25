package local

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// mirrorFailStore wraps a real store and injects failures for the write paths
// whose error semantics the tests pin. Embedding store.Store means every
// method the test does not override behaves exactly like the memstore.
type mirrorFailStore struct {
	store.Store
	createErrFor map[types.ExecutionID]error
	statusErr    error
	nodeErr      error
	signalErr    error
}

func (m *mirrorFailStore) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	if err := m.createErrFor[rec.ExecutionID]; err != nil {
		return err
	}
	return m.Store.CreateExecution(ctx, rec)
}

func (m *mirrorFailStore) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	if m.statusErr != nil {
		return m.statusErr
	}
	return m.Store.UpdateExecutionStatus(ctx, id, status, errMsg)
}

func (m *mirrorFailStore) UpsertNode(ctx context.Context, rec *store.NodeRecord) error {
	if m.nodeErr != nil {
		return m.nodeErr
	}
	return m.Store.UpsertNode(ctx, rec)
}

func (m *mirrorFailStore) SaveSignal(ctx context.Context, rec *store.SignalRecord) error {
	if m.signalErr != nil {
		return m.signalErr
	}
	return m.Store.SaveSignal(ctx, rec)
}

func mirrorState(st store.Store) *memoryState {
	state := newMemoryState()
	state.db = st
	return state
}

func mirrorSingleNodeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "mirror-single",
		Nodes: []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func triggerGroupGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "mirror-group",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("Compile(group graph) error = %v", err)
	}
	return g
}

// TestStoreMirrorLifecycle walks the projection points the Redis backend also
// projects: creation, status transitions, node upserts, lease acquisition,
// terminal commit, pre-delivered signals, revocation, and suspended-node
// cancellation.
func TestStoreMirrorLifecycle(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	g := mirrorSingleNodeGraph(t)
	id := types.ExecutionID("mirror-lifecycle")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:         id,
		Graph:      g,
		Status:     types.ExecutionStatusRunning,
		Params:     map[string]any{"ticket": "T-1"},
		TraceID:    "trace-1",
		SpanID:     "span-1",
		WorkflowID: "wf-mirror",
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	rec, err := st.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() after create error = %v", err)
	}
	if rec.Status != types.ExecutionStatusRunning {
		t.Fatalf("mirrored status = %q, want running", rec.Status)
	}
	if rec.Namespace != string(namespace.Default) {
		t.Fatalf("mirrored namespace = %q, want %q", rec.Namespace, namespace.Default)
	}
	if rec.WorkflowID != "wf-mirror" {
		t.Fatalf("mirrored workflow id = %q, want wf-mirror", rec.WorkflowID)
	}
	var params map[string]any
	if err := json.Unmarshal(rec.Params, &params); err != nil {
		t.Fatalf("mirrored params are not JSON: %v", err)
	}
	if params["ticket"] != "T-1" {
		t.Fatalf("mirrored params = %v, want ticket T-1", params)
	}
	if rec.TraceID != "trace-1" || rec.SpanID != "span-1" {
		t.Fatalf("mirrored trace = %q/%q, want trace-1/span-1", rec.TraceID, rec.SpanID)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusPaused, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(paused) error = %v", err)
	}
	rec, err = st.GetExecution(ctx, id)
	if err != nil || rec.Status != types.ExecutionStatusPaused {
		t.Fatalf("mirrored paused status = %v, err = %v", rec, err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusRunning, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(running) error = %v", err)
	}

	started := time.Now().UTC().Add(-time.Second)
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "seeded",
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"seeded": true},
		StartedAt:   started,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	node, err := st.GetNode(ctx, id, "seeded")
	if err != nil {
		t.Fatalf("GetNode() after upsert error = %v", err)
	}
	if node.Status != types.NodeStatusSuccess || node.StartedAt == nil || node.FinishedAt == nil {
		t.Fatalf("mirrored node = %+v, want success with start and finish times", node)
	}

	nodeIdx, _ := g.NodeIndex("start")
	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     nodeIdx,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	node, err = st.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetNode() after acquire error = %v", err)
	}
	if node.Status != types.NodeStatusRunning || node.LeaseID != "lease-1" || node.LeaseToken != "token-1" || node.Attempt != 1 {
		t.Fatalf("mirrored acquired node = %+v", node)
	}

	// The single unit completing finalizes the execution inside CommitNode, so
	// this is the only path that can mirror the terminal status.
	res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "start",
		NodeIdx:     nodeIdx,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
		StoreOutput: true,
		Output:      map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !res.ExecutionDone {
		t.Fatalf("CommitNode() ExecutionDone = false, want true")
	}
	rec, err = st.GetExecution(ctx, id)
	if err != nil || rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("mirrored terminal status = %v, err = %v", rec, err)
	}
	node, err = st.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetNode() after commit error = %v", err)
	}
	var output map[string]any
	if err := json.Unmarshal(node.Output, &output); err != nil {
		t.Fatalf("mirrored output is not JSON: %v", err)
	}
	if output["ok"] != true || node.Status != types.NodeStatusSuccess || node.FinishedAt == nil {
		t.Fatalf("mirrored committed node = %+v output=%v", node, output)
	}

	// A signal with no waiter is pre-delivered and mirrored.
	if name, _, err := state.DeliverSignal(ctx, id, "approval", map[string]any{"by": "alice"}); err != nil || name != "" {
		t.Fatalf("DeliverSignal() name=%q err=%v", name, err)
	}
	signals, err := st.ListSignalsByNames(ctx, id, []string{"approval"}, store.ListOptions{})
	if err != nil || len(signals) != 1 {
		t.Fatalf("ListSignalsByNames() = %v, err = %v", signals, err)
	}
	if revoked, err := state.RevokeSignal(ctx, id, "approval"); err != nil || !revoked {
		t.Fatalf("RevokeSignal() revoked=%v err=%v", revoked, err)
	}
	signals, err = st.ListSignalsByNames(ctx, id, []string{"approval"}, store.ListOptions{})
	if err != nil {
		t.Fatalf("ListSignalsByNames() after revoke error = %v", err)
	}
	if len(signals) != 0 {
		t.Fatalf("signal still active after revoke: %v", signals)
	}

	// CancelSuspendedNode mirrors the canceled snapshot.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "waiter",
		Status:      types.NodeStatusSuspended,
		StartedAt:   started,
	}); err != nil {
		t.Fatalf("UpsertNode(suspended) error = %v", err)
	}
	canceled, err := state.CancelSuspendedNode(ctx, id, "waiter")
	if err != nil || !canceled {
		t.Fatalf("CancelSuspendedNode() canceled=%v err=%v", canceled, err)
	}
	node, err = st.GetNode(ctx, id, "waiter")
	if err != nil || node.Status != types.NodeStatusCanceled || node.FinishedAt == nil {
		t.Fatalf("mirrored canceled node = %+v, err = %v", node, err)
	}
}

// TestStoreMirrorSignalConsumptionIsNotProjected pins the rstate rule that a
// signal which resumes a parked node is not written to the signal table.
func TestStoreMirrorSignalConsumptionIsNotProjected(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-signal-consume")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if _, err := state.SuspendOrConsume(ctx, id, "waiter", &types.SuspendSpec{Signals: []string{"go"}}); err != nil {
		t.Fatalf("SuspendOrConsume() error = %v", err)
	}
	name, _, err := state.DeliverSignal(ctx, id, "go", map[string]any{"ok": true})
	if err != nil || name != "waiter" {
		t.Fatalf("DeliverSignal() name=%q err=%v, want waiter", name, err)
	}
	signals, err := st.ListSignalsByNames(ctx, id, []string{"go"}, store.ListOptions{})
	if err != nil {
		t.Fatalf("ListSignalsByNames() error = %v", err)
	}
	if len(signals) != 0 {
		t.Fatalf("resumed signal was projected: %v", signals)
	}
}

// TestStoreMirrorSeedPaths pins the two creation paths that do not go through
// CreateExecution: the rerun seed and the entry admission.
func TestStoreMirrorSeedPaths(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	g := triggerGroupGraph(t)
	group := g.Groups()[0]

	rerunID := types.ExecutionID("mirror-rerun")
	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: &engine.ExecutionSnapshot{
			ID:         rerunID,
			Graph:      g,
			Status:     types.ExecutionStatusRunning,
			WorkflowID: "wf-rerun",
			TraceID:    "trace-rerun",
		},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	rec, err := st.GetExecution(ctx, rerunID)
	if err != nil {
		t.Fatalf("GetExecution() after rerun seed error = %v", err)
	}
	if rec.Status != types.ExecutionStatusRunning || rec.WorkflowID != "wf-rerun" || rec.WorkflowName != g.Name() {
		t.Fatalf("mirrored rerun row = %+v", rec)
	}

	admissionKey := engine.AdmissionKey("mirror-entry")
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	resp, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: admissionKey,
		Namespace:    namespace.Default,
		WorkflowID:   "wf-entry",
		EntryUnitID:  group.Name,
		EntryUnitIdx: group.UnitIdx,
		Graph:        g,
		Outcome:      engine.GroupOutcomeSuccess,
		Exits:        exits,
		ResultHash:   engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry() error = %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry() state = %q, want accepted", resp.State)
	}
	rec, err = st.GetExecution(ctx, resp.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution() after entry seed error = %v", err)
	}
	// The only unit is the admitted group, so the admission finalizes the
	// execution; the mirrored row must carry the terminal status.
	if rec.Status != types.ExecutionStatusSuccess || rec.WorkflowID != "wf-entry" {
		t.Fatalf("mirrored entry row = %+v", rec)
	}

	// A duplicate admission writes no second row: the first one stays.
	dup, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: admissionKey,
		Namespace:    namespace.Default,
		WorkflowID:   "wf-entry",
		EntryUnitID:  group.Name,
		EntryUnitIdx: group.UnitIdx,
		Graph:        g,
		Outcome:      engine.GroupOutcomeSuccess,
		Exits:        exits,
		ResultHash:   engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil || !dup.Duplicate {
		t.Fatalf("duplicate SeedExecutionFromEntry() = %+v, err = %v", dup, err)
	}
}

// TestStoreMirrorGroupCommitTerminalStatus pins the group commit's terminal
// projection: the group finalizes the execution inside the transition, so
// CommitGroup is the only writer that can reach the row.
func TestStoreMirrorGroupCommitTerminalStatus(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	g := triggerGroupGraph(t)
	id := types.ExecutionID("mirror-group-commit")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	group := g.Groups()[0]
	lease := &engine.GroupLease{
		LeaseID: "group-lease", LeaseToken: "group-token", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: group.UnitIdx, GroupID: group.Name,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	if acquired, err := state.AcquireGroupLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireGroupLease() acquired=%v err=%v", acquired, err)
	}
	res, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID: id, GroupUnitIdx: group.UnitIdx, GroupID: group.Name,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: lease.Attempt,
		Outcome: engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	if !res.ExecutionDone {
		t.Fatalf("CommitGroup() ExecutionDone = false, want true")
	}
	rec, err := st.GetExecution(ctx, id)
	if err != nil || rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("mirrored group terminal status = %v, err = %v", rec, err)
	}
}

// TestStoreMirrorCreateIsRequired pins the one non-best-effort projection:
// rstate rolls the Redis execution back and fails the call when the SQL insert
// fails, so the local backend must roll its memory state back and return the
// error. All three creation entry points are covered.
func TestStoreMirrorCreateIsRequired(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("mirror unavailable")

	t.Run("CreateExecution", func(t *testing.T) {
		st := &mirrorFailStore{
			Store:        memstore.New(),
			createErrFor: map[types.ExecutionID]error{"create-1": boom},
		}
		state := mirrorState(st)
		err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: "create-1", Status: types.ExecutionStatusRunning,
		})
		if !errors.Is(err, boom) {
			t.Fatalf("CreateExecution() error = %v, want %v", err, boom)
		}
		if snap, err := state.GetExecution(ctx, "create-1"); err != nil || snap != nil {
			t.Fatalf("rolled-back execution still readable: %v, err = %v", snap, err)
		}
	})

	t.Run("CreateExecutionWithOutbox", func(t *testing.T) {
		st := &mirrorFailStore{
			Store:        memstore.New(),
			createErrFor: map[types.ExecutionID]error{"create-outbox": boom},
		}
		state := mirrorState(st)
		entry := engine.OutboxEntry{ID: "entry-1", Task: engine.Task{ExecutionID: "create-outbox", NodeName: "start"}}
		err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: "create-outbox", Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{entry})
		if !errors.Is(err, boom) {
			t.Fatalf("CreateExecutionWithOutbox() error = %v, want %v", err, boom)
		}
		if snap, err := state.GetExecution(ctx, "create-outbox"); err != nil || snap != nil {
			t.Fatalf("rolled-back execution still readable: %v, err = %v", snap, err)
		}
		entries, err := state.ListOutbox(ctx, "create-outbox", time.Now().Add(time.Hour), 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("rolled-back outbox = %v, err = %v", entries, err)
		}
	})

	t.Run("CreateExecutionWithConcurrencyReleasesSlot", func(t *testing.T) {
		st := &mirrorFailStore{
			Store:        memstore.New(),
			createErrFor: map[types.ExecutionID]error{"limited-1": boom},
		}
		state := mirrorState(st)
		admission := engine.ConcurrencyAdmission{
			Namespace: string(namespace.Default),
			Workflow:  "mirror-limited",
			Limit:     1,
			Mode:      string(types.ConcurrencyModeReject),
		}
		_, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
			ID: "limited-1", Status: types.ExecutionStatusRunning,
		}, nil, admission)
		if !errors.Is(err, boom) {
			t.Fatalf("CreateExecutionWithConcurrency() error = %v, want %v", err, boom)
		}
		// The failed create's slot must be free again: a second admission under
		// Limit=1 has to succeed, and the row it writes must reach the store.
		if _, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
			ID: "limited-2", Status: types.ExecutionStatusRunning,
		}, nil, admission); err != nil {
			t.Fatalf("second admission error = %v, want the released slot", err)
		}
		if _, err := st.GetExecution(ctx, "limited-2"); err != nil {
			t.Fatalf("second admission not mirrored: %v", err)
		}
	})
}

// TestStoreMirrorBestEffortErrors pins the other half of the contract: once
// the in-memory transition has committed, a failed projection is logged and
// dropped, never returned, and never rolls the transition back.
func TestStoreMirrorBestEffortErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("mirror unavailable")
	st := &mirrorFailStore{
		Store:     memstore.New(),
		statusErr: boom,
		nodeErr:   boom,
		signalErr: boom,
	}
	state := mirrorState(st)
	g := mirrorSingleNodeGraph(t)
	id := types.ExecutionID("mirror-best-effort")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusPaused, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v; best-effort mirror must not fail the transition", err)
	}
	if status, found, err := state.GetExecutionStatus(ctx, id); err != nil || !found || status != types.ExecutionStatusPaused {
		t.Fatalf("local status = %q found=%v err=%v, want paused", status, found, err)
	}
	rec, err := st.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if rec.Status != types.ExecutionStatusRunning {
		t.Fatalf("mirrored status = %q, want the unprojected running value", rec.Status)
	}

	lease := &engine.TaskLease{
		LeaseID: "lease-1", LeaseToken: "token-1",
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v; best-effort mirror must not refuse the lease", acquired, err)
	}
	res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "start",
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess, StoreOutput: true, Output: map[string]any{"ok": true},
	})
	if err != nil || !res.Applied {
		t.Fatalf("CommitNode() applied=%v err=%v; best-effort mirror must not fail the commit", res.Applied, err)
	}

	if name, _, err := state.DeliverSignal(ctx, id, "parked", map[string]any{"k": "v"}); err != nil || name != "" {
		t.Fatalf("DeliverSignal() name=%q err=%v; best-effort mirror must not fail delivery", name, err)
	}
	state.mu.Lock()
	_, stored := state.signals[string(id)+"/parked"]
	state.mu.Unlock()
	if !stored {
		t.Fatal("signal was not stored in memory")
	}
}

// TestStoreMirrorNilStoreUnchanged pins the compatibility promise: without a
// store the backend holds no store handle and every transition still succeeds.
// The exhaustive no-store behavior remains covered by the contract suite in
// state_store_contract_test.go.
func TestStoreMirrorNilStoreUnchanged(t *testing.T) {
	ctx := context.Background()
	backend := New()
	state := backend.State().(*memoryState)
	if state.db != nil {
		t.Fatalf("New() installed a store handle: %v", state.db)
	}
	id := types.ExecutionID("mirror-nil-store")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, "boom"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	status, found, err := state.GetExecutionStatus(ctx, id)
	if err != nil || !found || status != types.ExecutionStatusFailed {
		t.Fatalf("local status = %q found=%v err=%v, want failed", status, found, err)
	}
}
