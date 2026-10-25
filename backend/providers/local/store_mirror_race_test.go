package local

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Test stores
// ---------------------------------------------------------------------------

// blockingCreateStore holds CreateExecution until the test releases it, so a
// completion can be driven deterministically between the memory commit and the
// SQL insert.
type blockingCreateStore struct {
	store.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingCreateStore() *blockingCreateStore {
	return &blockingCreateStore{
		Store:   memstore.New(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingCreateStore) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return b.Store.CreateExecution(ctx, rec)
}

// blockingStatusStore holds the first UpdateExecutionStatus until released.
// It is installed after an execution was created, so the create-time
// reconciliation does not consume the block.
type blockingStatusStore struct {
	store.Store
	mu      sync.Mutex
	blocked bool
	entered chan struct{}
	release chan struct{}
}

func newBlockingStatusStore(underlying store.Store) *blockingStatusStore {
	return &blockingStatusStore{
		Store:   underlying,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingStatusStore) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	b.mu.Lock()
	block := !b.blocked
	b.blocked = true
	b.mu.Unlock()
	if block {
		close(b.entered)
		<-b.release
	}
	return b.Store.UpdateExecutionStatus(ctx, id, status, errMsg)
}

// existingRowStore simulates a durable row left by a previous process: creates
// for the marked IDs fail the way a unique-key violation does, and reads still
// return the old row.
type existingRowStore struct {
	store.Store
	mu       sync.Mutex
	existing map[types.ExecutionID]bool
	creates  int
	updates  int
}

func newExistingRowStore() *existingRowStore {
	return &existingRowStore{Store: memstore.New(), existing: make(map[types.ExecutionID]bool)}
}

func (e *existingRowStore) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	e.mu.Lock()
	e.creates++
	duplicate := e.existing[rec.ExecutionID]
	e.mu.Unlock()
	if duplicate {
		return errors.New("duplicate key value violates unique constraint uk_execution_id")
	}
	return e.Store.CreateExecution(ctx, rec)
}

func (e *existingRowStore) GetExecution(_ context.Context, id types.ExecutionID) (*store.ExecutionRecord, error) {
	e.mu.Lock()
	duplicate := e.existing[id]
	e.mu.Unlock()
	if duplicate {
		return &store.ExecutionRecord{ExecutionID: id, Status: types.ExecutionStatusSuccess}, nil
	}
	return e.Store.GetExecution(context.Background(), id)
}

func (e *existingRowStore) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	e.mu.Lock()
	e.updates++
	e.mu.Unlock()
	return e.Store.UpdateExecutionStatus(ctx, id, status, errMsg)
}

func (e *existingRowStore) counts() (creates, updates int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.creates, e.updates
}

// blockingIDStore blocks CreateExecution for one execution ID until released.
type blockingIDStore struct {
	store.Store
	blockID types.ExecutionID
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingIDStore(blockID types.ExecutionID) *blockingIDStore {
	return &blockingIDStore{
		Store:   memstore.New(),
		blockID: blockID,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingIDStore) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	if rec.ExecutionID == b.blockID {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.Store.CreateExecution(ctx, rec)
}

// blockingExistingStore is existingRowStore with one blocked insert.
type blockingExistingStore struct {
	*existingRowStore
	blockID types.ExecutionID
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingExistingStore(blockID types.ExecutionID) *blockingExistingStore {
	return &blockingExistingStore{
		existingRowStore: newExistingRowStore(),
		blockID:          blockID,
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}
}

func (b *blockingExistingStore) CreateExecution(ctx context.Context, rec *store.ExecutionRecord) error {
	if rec.ExecutionID == b.blockID {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.existingRowStore.CreateExecution(ctx, rec)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func transientMirrorGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "mirror-transient",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
		Options: &types.WorkflowOptions{
			Transient:              true,
			TransientTTL:           time.Minute,
			TransientCompletionTTL: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("Compile(transient) error = %v", err)
	}
	return g
}

func waitForMemoryStatus(t *testing.T, state *memoryState, id types.ExecutionID, want types.ExecutionStatus) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, ok, err := state.GetExecutionStatus(context.Background(), id)
		if err == nil && ok && status == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	status, ok, _ := state.GetExecutionStatus(context.Background(), id)
	t.Fatalf("memory status = %q found=%v, want %q", status, ok, want)
}

func mustCreateDurable(t *testing.T, state *memoryState, id types.ExecutionID) {
	t.Helper()
	if err := state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
}

// ---------------------------------------------------------------------------
// Transient workflows
// ---------------------------------------------------------------------------

// TestStoreMirrorSkipsTransientExecution mirrors rstate's
// TestPerWorkflowTransient_SkipsSQLAudit: a transient workflow and a durable
// workflow run against the same store, and only the durable one produces rows.
func TestStoreMirrorSkipsTransientExecution(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	g := transientMirrorGraph(t)
	transientID := types.ExecutionID("mirror-transient-exec")
	durableID := types.ExecutionID("mirror-durable-exec")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     transientID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: map[string]any{"secret": "must-not-persist"},
	}); err != nil {
		t.Fatalf("CreateExecution(transient) error = %v", err)
	}
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     durableID,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(durable) error = %v", err)
	}

	// Exercise every projection point for the transient execution.
	if err := state.UpdateExecutionStatus(ctx, transientID, types.ExecutionStatusPaused, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(transient) error = %v", err)
	}
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: transientID, Name: "start", Status: types.NodeStatusSuccess,
		Output: map[string]any{"secret": "must-not-persist"},
	}); err != nil {
		t.Fatalf("UpsertNode(transient) error = %v", err)
	}
	if _, _, err := state.DeliverSignal(ctx, transientID, "approval", map[string]any{"secret": "must-not-persist"}); err != nil {
		t.Fatalf("DeliverSignal(transient) error = %v", err)
	}
	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: &engine.ExecutionSnapshot{
			ID:     "mirror-transient-seed",
			Graph:  g,
			Status: types.ExecutionStatusRunning,
		},
	}); err != nil {
		t.Fatalf("SeedExecution(transient) error = %v", err)
	}

	// Entry admission: the graph carried by the admission request is transient
	// too, so its deterministic execution row must not be written.
	tg, err := graph.Compile(&types.WorkflowDef{
		Name: "mirror-transient-entry",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups:  []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
		Options: &types.WorkflowOptions{Transient: true, TransientTTL: time.Minute},
	})
	if err != nil {
		t.Fatalf("Compile(transient group) error = %v", err)
	}
	group := tg.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"secret": "must-not-persist"}}}
	resp, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: "mirror-transient-entry",
		Namespace:    namespace.Default,
		EntryUnitID:  group.Name,
		EntryUnitIdx: group.UnitIdx,
		Graph:        tg,
		Outcome:      engine.GroupOutcomeSuccess,
		Exits:        exits,
		ResultHash:   engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry(transient) error = %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry(transient) state = %q, want accepted", resp.State)
	}
	if rec, err := st.GetExecution(ctx, resp.ExecutionID); err == nil {
		t.Fatalf("transient entry admission was projected: %+v", rec)
	}

	if rec, err := st.GetExecution(ctx, transientID); err == nil {
		t.Fatalf("transient execution was projected: %+v", rec)
	}
	if rec, err := st.GetExecution(ctx, "mirror-transient-seed"); err == nil {
		t.Fatalf("transient seed was projected: %+v", rec)
	}
	if node, err := st.GetNode(ctx, transientID, "start"); err == nil {
		t.Fatalf("transient node was projected: %+v", node)
	}
	if signals, err := st.ListSignalsByNames(ctx, transientID, []string{"approval"}, store.ListOptions{}); err != nil || len(signals) != 0 {
		t.Fatalf("transient signal rows = %v, err = %v, want none", signals, err)
	}

	// The durable execution in the same state must still be projected, so the
	// test cannot pass by disabling the mirror.
	if rec, err := st.GetExecution(ctx, durableID); err != nil || rec.Status != types.ExecutionStatusRunning {
		t.Fatalf("durable execution was not projected: %+v, err = %v", rec, err)
	}
}

// TestStoreMirrorSkipsTransientContextHint covers the per-execution hint on a
// graph that is not marked transient itself (rstate's per-execution override).
func TestStoreMirrorSkipsTransientContextHint(t *testing.T) {
	ctx := engine.WithExecutionTransient(context.Background(), engine.TransientHint{
		TTL: time.Minute, CompletionTTL: time.Second,
	})
	st := memstore.New()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-transient-hint")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: mirrorSingleNodeGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if rec, err := st.GetExecution(ctx, id); err == nil {
		t.Fatalf("hint-only transient execution was projected: %+v", rec)
	}
}

// ---------------------------------------------------------------------------
// Create-vs-completion and status ordering
// ---------------------------------------------------------------------------

// TestStoreMirrorCreateReconcilesRacingCompletion pins the create/completion
// race: the execution reaches a terminal state while the required insert is
// blocked, and the durable row must still end terminal.
func TestStoreMirrorCreateReconcilesRacingCompletion(t *testing.T) {
	ctx := context.Background()
	st := newBlockingCreateStore()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-racing-completion")

	createErr := make(chan error, 1)
	go func() {
		createErr <- state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
		})
	}()
	<-st.entered

	// The memory transition lands while the insert is still blocked; its own
	// projection queues behind the create's mirror lock.
	statusErr := make(chan error, 1)
	go func() {
		statusErr <- state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, "")
	}()
	waitForMemoryStatus(t, state, id, types.ExecutionStatusSuccess)

	close(st.release)
	if err := <-createErr; err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := <-statusErr; err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	rec, err := st.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("durable status = %q, want success (insert must be reconciled)", rec.Status)
	}
}

// TestStoreMirrorSeedReconcilesRacingCompletion is the seeded-path variant.
func TestStoreMirrorSeedReconcilesRacingCompletion(t *testing.T) {
	ctx := context.Background()
	st := newBlockingCreateStore()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-racing-seed")

	seedErr := make(chan error, 1)
	go func() {
		_, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
			Execution: &engine.ExecutionSnapshot{
				ID: id, Graph: mirrorSingleNodeGraph(t), Status: types.ExecutionStatusRunning,
			},
		})
		seedErr <- err
	}()
	<-st.entered

	statusErr := make(chan error, 1)
	go func() {
		statusErr <- state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, "")
	}()
	waitForMemoryStatus(t, state, id, types.ExecutionStatusSuccess)

	close(st.release)
	if err := <-seedErr; err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	if err := <-statusErr; err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	rec, err := st.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("durable status = %q, want success (seed insert must be reconciled)", rec.Status)
	}
}

// TestStoreMirrorLateStatusDoesNotOverwriteTerminal pins the other ordering
// direction: a paused projection held inside the store must not overwrite a
// terminal transition that landed after it read its value.
func TestStoreMirrorLateStatusDoesNotOverwriteTerminal(t *testing.T) {
	ctx := context.Background()
	underlying := memstore.New()
	state := mirrorState(underlying)
	id := types.ExecutionID("mirror-status-ordering")
	mustCreateDurable(t, state, id)

	// Install the blocking wrapper after creation so only the paused projection
	// is held; it writes into the same underlying store the assertions read.
	st := newBlockingStatusStore(underlying)
	state.mu.Lock()
	state.db = st
	state.mu.Unlock()

	pauseErr := make(chan error, 1)
	go func() {
		pauseErr <- state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusPaused, "")
	}()
	<-st.entered

	// This commits in memory and queues behind the held paused write.
	successErr := make(chan error, 1)
	go func() {
		successErr <- state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, "")
	}()
	waitForMemoryStatus(t, state, id, types.ExecutionStatusSuccess)

	close(st.release)
	if err := <-pauseErr; err != nil {
		t.Fatalf("paused projection error = %v", err)
	}
	if err := <-successErr; err != nil {
		t.Fatalf("terminal projection error = %v", err)
	}

	rec, err := st.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("durable status = %q, want the newer terminal success", rec.Status)
	}
}

// ---------------------------------------------------------------------------
// Restart collision
// ---------------------------------------------------------------------------

// TestStoreMirrorRequiredCreateReportsExistingRow pins the required-create
// restart case: a deterministic ID that already has a durable row must fail
// with the typed error and leave no in-memory execution behind.
func TestStoreMirrorRequiredCreateReportsExistingRow(t *testing.T) {
	ctx := context.Background()
	st := newExistingRowStore()
	st.existing["mirror-existing"] = true
	state := mirrorState(st)

	err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: "mirror-existing", Status: types.ExecutionStatusRunning,
	})
	if !errors.Is(err, ErrStoreExecutionExists) {
		t.Fatalf("CreateExecution() error = %v, want ErrStoreExecutionExists", err)
	}
	if snap, _ := state.GetExecution(ctx, "mirror-existing"); snap != nil {
		t.Fatalf("execution was not rolled back: %+v", snap)
	}
}

// TestStoreMirrorSeedConflictStopsProjection pins the seed half: a collision
// with a row from a previous process is logged and disables later projections,
// so the old execution's history is never overwritten.
func TestStoreMirrorSeedConflictStopsProjection(t *testing.T) {
	ctx := context.Background()
	st := newExistingRowStore()
	id := types.ExecutionID("mirror-existing-seed")
	st.existing[id] = true
	state := mirrorState(st)

	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: &engine.ExecutionSnapshot{
			ID: id, Graph: mirrorSingleNodeGraph(t), Status: types.ExecutionStatusRunning,
		},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, "boom"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id, Name: "start", Status: types.NodeStatusSuccess,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	if _, updates := st.counts(); updates != 0 {
		t.Fatalf("UpdateExecutionStatus calls after a seed conflict = %d, want 0", updates)
	}
	if node, err := st.GetNode(ctx, id, "start"); err == nil {
		t.Fatalf("node row written for a conflicting seed: %+v", node)
	}
}

// ---------------------------------------------------------------------------
// Create hold and rollback
// ---------------------------------------------------------------------------

// TestStoreMirrorCreateHoldsIntentsUntilProjected pins the rollback race: an
// outbox intent must not be deliverable while the required insert is in
// flight, and it becomes deliverable only after the row lands.
func TestStoreMirrorCreateHoldsIntentsUntilProjected(t *testing.T) {
	ctx := context.Background()
	st := newBlockingCreateStore()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-held-intents")
	entry := engine.OutboxEntry{
		ID:   "root-intent",
		Task: engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
	}

	createErr := make(chan error, 1)
	go func() {
		createErr <- state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{entry})
	}()
	<-st.entered

	held, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ListOutbox() while held error = %v", err)
	}
	if len(held) != 0 {
		t.Fatalf("intents were deliverable before the insert landed: %+v", held)
	}

	close(st.release)
	if err := <-createErr; err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	released, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ListOutbox() after create error = %v", err)
	}
	if len(released) != 1 || !released[0].AvailableAt.IsZero() {
		t.Fatalf("intents after create = %+v, want the single released intent", released)
	}
}

// TestStoreMirrorRollbackRefusesLateLease pins the late-task guard: once a
// failed create rolls the execution back, a lease for it must be refused
// rather than recreating node state.
func TestStoreMirrorRollbackRefusesLateLease(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("mirror unavailable")
	st := &mirrorFailStore{
		Store:        memstore.New(),
		createErrFor: map[types.ExecutionID]error{"mirror-rolled-back": boom},
	}
	state := mirrorState(st)
	id := types.ExecutionID("mirror-rolled-back")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); !errors.Is(err, boom) {
		t.Fatalf("CreateExecution() error = %v, want %v", err, boom)
	}
	lease := &engine.TaskLease{
		LeaseID: "lease", LeaseToken: "token", IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want refused", acquired, err)
	}
	if node, _ := state.GetNode(ctx, id, "start"); node != nil {
		t.Fatalf("late acquire recreated node state: %+v", node)
	}
}

// TestStoreMirrorRollbackClosesWatchers pins the waiter cleanup: Stream
// callers must observe a closed channel when a create is rolled back.
func TestStoreMirrorRollbackClosesWatchers(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("mirror unavailable")
	st := &mirrorFailStore{
		Store:        memstore.New(),
		createErrFor: map[types.ExecutionID]error{"mirror-watched": boom},
	}
	state := mirrorState(st)
	id := types.ExecutionID("mirror-watched")
	watcher, err := state.WatchExecution(context.Background(), id)
	if err != nil {
		t.Fatalf("WatchExecution() error = %v", err)
	}
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); !errors.Is(err, boom) {
		t.Fatalf("CreateExecution() error = %v, want %v", err, boom)
	}
	select {
	case _, open := <-watcher:
		if open {
			t.Fatal("watcher received an event, want a closed channel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watcher was not closed by the rollback")
	}
}

// TestStoreMirrorTransientCreateDoesNotHoldIntents guards the nil-store
// compatibility promise from the other side: a transient execution is not
// mirrored, so its intents must be immediately deliverable even with a store
// attached.
func TestStoreMirrorTransientCreateDoesNotHoldIntents(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-transient-intents")
	entry := engine.OutboxEntry{
		ID:   "root-intent",
		Task: engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: transientMirrorGraph(t), Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
	if err != nil || len(entries) != 1 || !entries[0].AvailableAt.IsZero() {
		t.Fatalf("transient intents = %+v, err = %v, want one deliverable intent", entries, err)
	}
	if _, err := st.GetExecution(ctx, id); err == nil {
		t.Fatal("transient execution was projected")
	}
}

// ---------------------------------------------------------------------------
// Create-pending gate
// ---------------------------------------------------------------------------

// TestStoreMirrorPromotionDuringPendingCreateKeepsIntentsHeld pins the
// promotion half of the create-pending gate: a queued execution whose required
// insert is still blocked may be promoted in memory, but its intents must stay
// parked until the row exists.
func TestStoreMirrorPromotionDuringPendingCreateKeepsIntentsHeld(t *testing.T) {
	ctx := context.Background()
	promotedID := types.ExecutionID("mirror-promoted")
	st := newBlockingIDStore(promotedID)
	state := mirrorState(st)
	admission := engine.ConcurrencyAdmission{
		Namespace: string(namespace.Default),
		Workflow:  "mirror-promotion",
		Limit:     1,
		Mode:      types.ConcurrencyModeQueue,
		MaxQueued: 10,
	}

	holderID := types.ExecutionID("mirror-holder")
	if _, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: holderID, Status: types.ExecutionStatusRunning,
	}, nil, admission); err != nil {
		t.Fatalf("holder CreateExecutionWithConcurrency() error = %v", err)
	}

	entry := engine.OutboxEntry{
		ID:   "promoted-intent",
		Task: engine.Task{ExecutionID: promotedID, NodeName: "start", Type: engine.TaskTypeNodeExec},
	}
	createErr := make(chan error, 1)
	go func() {
		_, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
			ID: promotedID, Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{entry}, admission)
		createErr <- err
	}()
	<-st.entered

	// Freeing the holder promotes the queued execution while its insert is
	// still blocked. The status flips, the intents must not.
	if err := state.UpdateExecutionStatus(ctx, holderID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("holder terminal UpdateExecutionStatus() error = %v", err)
	}
	waitForMemoryStatus(t, state, promotedID, types.ExecutionStatusRunning)
	if held, err := state.ListOutbox(ctx, promotedID, time.Now().Add(time.Hour), 10); err != nil || len(held) != 0 {
		t.Fatalf("intents dispatched during the pending insert: %+v, err = %v", held, err)
	}

	close(st.release)
	if err := <-createErr; err != nil {
		t.Fatalf("promoted CreateExecutionWithConcurrency() error = %v", err)
	}
	released, err := state.ListOutbox(ctx, promotedID, time.Now().Add(time.Hour), 10)
	if err != nil || len(released) != 1 || !released[0].AvailableAt.IsZero() {
		t.Fatalf("promoted intents after the insert = %+v, err = %v, want one deliverable intent", released, err)
	}
}

// TestStoreMirrorSeedConflictDoesNotTouchOldRowDuringInsert pins the seed
// half: while the insert is blocked, status and node projections for the
// colliding ID must not reach the previous process's row.
func TestStoreMirrorSeedConflictDoesNotTouchOldRowDuringInsert(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("mirror-seed-old")
	st := newBlockingExistingStore(id)
	st.existing[id] = true
	state := mirrorState(st)
	g := mirrorSingleNodeGraph(t)

	seedErr := make(chan error, 1)
	go func() {
		_, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
			Execution: &engine.ExecutionSnapshot{
				ID: id, Graph: g, Status: types.ExecutionStatusRunning,
			},
		})
		seedErr <- err
	}()
	<-st.entered

	// Ordinary projections land while the insert is in flight.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id, Name: "start", Status: types.NodeStatusSuccess,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	close(st.release)
	if err := <-seedErr; err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	if _, updates := st.counts(); updates != 0 {
		t.Fatalf("old row status updated %d times during a pending seed", updates)
	}
	if node, err := st.GetNode(ctx, id, "start"); err == nil {
		t.Fatalf("node row written into the old execution: %+v", node)
	}

	// The conflict verdict persists after the seed: a late node projection is
	// still not allowed to touch the old row.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id, Name: "late", Status: types.NodeStatusSuccess,
	}); err != nil {
		t.Fatalf("late UpsertNode() error = %v", err)
	}
	if node, err := st.GetNode(ctx, id, "late"); err == nil {
		t.Fatalf("late node row written into the old execution: %+v", node)
	}
}

// TestStoreMirrorCancelDuringPendingCreateDoesNotTouchOldRow pins the
// pre-insert cancellation case: a deterministic ID that will be refused as a
// duplicate must not have its old row updated by the canceling transitions.
func TestStoreMirrorCancelDuringPendingCreateDoesNotTouchOldRow(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("mirror-cancel-old")
	st := newBlockingExistingStore(id)
	st.existing[id] = true
	state := mirrorState(st)

	createErr := make(chan error, 1)
	go func() {
		createErr <- state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
		})
	}()
	<-st.entered

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}

	close(st.release)
	if err := <-createErr; !errors.Is(err, ErrStoreExecutionExists) {
		t.Fatalf("CreateExecution() error = %v, want ErrStoreExecutionExists", err)
	}
	if _, updates := st.counts(); updates != 0 {
		t.Fatalf("old row status updated %d times during the pending create", updates)
	}
	if snap, _ := state.GetExecution(ctx, id); snap != nil {
		t.Fatalf("execution was not rolled back: %+v", snap)
	}
}

// ---------------------------------------------------------------------------
// Nil store and lock/skip lifecycle
// ---------------------------------------------------------------------------

// TestStoreMirrorSignalDuringPendingCreateIsReconciled pins the one projection
// the create reconciliation cannot derive from the execution row: a signal
// pre-delivered while the insert was blocked must be written once it lands.
func TestStoreMirrorSignalDuringPendingCreateIsReconciled(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("mirror-pending-signal")
	st := newBlockingIDStore(id)
	state := mirrorState(st)

	createErr := make(chan error, 1)
	go func() {
		createErr <- state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
		})
	}()
	<-st.entered

	if name, _, err := state.DeliverSignal(ctx, id, "approval", map[string]any{"by": "alice"}); err != nil || name != "" {
		t.Fatalf("DeliverSignal() name=%q err=%v", name, err)
	}

	close(st.release)
	if err := <-createErr; err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	signals, err := st.ListSignalsByNames(ctx, id, []string{"approval"}, store.ListOptions{})
	if err != nil || len(signals) != 1 {
		t.Fatalf("reconciled signals = %v, err = %v, want one", signals, err)
	}
}

// TestStoreMirrorNilStoreKeepsNoMirrorState pins the nil-store promise: no
// mirror bookkeeping is written at all when no store is attached.
func TestStoreMirrorNilStoreKeepsNoMirrorState(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: "nil-store-transient", Graph: transientMirrorGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(transient) error = %v", err)
	}
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: "nil-store-durable", Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(durable) error = %v", err)
	}
	state.mu.Lock()
	total := len(state.mirrorLocks) + len(state.mirrorSkip) + len(state.mirrorCreatePending) +
		len(state.mirrorConflict) + len(state.rolledBack)
	state.mu.Unlock()
	if total != 0 {
		t.Fatalf("nil store left %d mirror bookkeeping entries, want 0", total)
	}
}

// TestStoreMirrorTerminalRetiresExecutionLock pins the refcounted lifecycle:
// the per-execution lock is retained while the execution is live and dropped
// once it is terminal, including when only node projections ran.
func TestStoreMirrorTerminalRetiresExecutionLock(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-lock-retire")
	mustCreateDurable(t, state, id)

	state.mu.Lock()
	live := len(state.mirrorLocks)
	state.mu.Unlock()
	if live != 1 {
		t.Fatalf("live execution locks = %d, want 1", live)
	}

	lease := &engine.TaskLease{
		LeaseID: "lock-lease", LeaseToken: "lock-token", IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	state.mu.Lock()
	remaining := len(state.mirrorLocks)
	state.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("terminal execution retained %d mirror locks, want 0", remaining)
	}
}

// TestStoreMirrorTransientSkipRetiredAtTerminal pins the bounded transient
// bookkeeping: once a transient execution is over and its last status
// projection has run, the skip verdict is dropped.
func TestStoreMirrorTransientSkipRetiredAtTerminal(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	state := mirrorState(st)
	id := types.ExecutionID("mirror-transient-retire")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: transientMirrorGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(transient) error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	state.mu.Lock()
	_, skipped := state.mirrorSkip[id]
	state.mu.Unlock()
	if skipped {
		t.Fatal("transient skip verdict was retained after the terminal projection")
	}
	if _, err := st.GetExecution(ctx, id); err == nil {
		t.Fatal("transient execution was projected")
	}
}
