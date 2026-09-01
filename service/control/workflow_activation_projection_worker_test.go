package control

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const workflowProjectionTestNamespace namespace.Namespace = "projection-test"

type workflowProjectionTestRegistry struct {
	registry backend.WorkflowRegistry
	outbox   backend.DurableWorkflowReplaceCapability
}

func newWorkflowProjectionTestRegistry(t *testing.T) workflowProjectionTestRegistry {
	t.Helper()

	registry := local.New().WorkflowRegistry()
	outbox, ok := registry.(backend.DurableWorkflowReplaceCapability)
	if !ok {
		t.Fatalf("local workflow registry type %T does not implement DurableWorkflowReplaceCapability", registry)
	}
	return workflowProjectionTestRegistry{registry: registry, outbox: outbox}
}

func commitWorkflowProjectionTestIntent(
	t *testing.T,
	registry backend.WorkflowRegistry,
	mutationID string,
	g *graph.Graph,
) backend.WorkflowReplaceResult {
	t.Helper()

	workflowID := types.WorkflowID("workflow-" + mutationID)
	name := "workflow-" + mutationID
	original, err := registry.AddWorkflow(context.Background(), backend.WorkflowRecord{
		ID:             workflowID,
		Key:            string(workflowProjectionTestNamespace) + "/" + name + "@v1",
		Namespace:      string(workflowProjectionTestNamespace),
		Name:           name,
		Version:        "v1",
		DefinitionHash: "hash:before:" + mutationID,
	})
	if err != nil {
		t.Fatalf("AddWorkflow(%q): %v", mutationID, err)
	}

	replacement := original
	replacement.DefinitionHash = "hash:after:" + mutationID
	replacement.RegistryRevision = 0
	replacement.Graph = g
	capability, ok := registry.(backend.WorkflowReplaceCapability)
	if !ok {
		t.Fatalf("workflow registry type %T does not implement WorkflowReplaceCapability", registry)
	}
	result, err := capability.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	})
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow(%q): %v", mutationID, err)
	}
	if result.Status != backend.WorkflowReplaceReplaced {
		t.Fatalf("CompareAndReplaceWorkflow(%q) status = %q, want %q", mutationID, result.Status, backend.WorkflowReplaceReplaced)
	}
	return result
}

func newWorkflowProjectionTestWorker(
	outbox backend.WorkflowActivationProjectionOutbox,
	store engine.EntryActivationStore,
	leader LeaderGate,
	token func() string,
) *WorkflowActivationProjectionWorker {
	if leader == nil {
		leader = backend.AlwaysLeader{}
	}
	return NewWorkflowActivationProjectionWorker(WorkflowActivationProjectionWorkerConfig{
		Outbox:            outbox,
		Projector:         NewWorkflowActivationProjector(NewEntryActivationManager(store)),
		Leader:            leader,
		Period:            time.Hour,
		Batch:             32,
		ProjectionTimeout: 10 * time.Millisecond,
		ClaimTTL:          50 * time.Millisecond,
		Token:             token,
	})
}

func workflowProjectionTestTokenSequence(prefix string) func() string {
	var sequence atomic.Uint64
	return func() string {
		return fmt.Sprintf("%s-%d", prefix, sequence.Add(1))
	}
}

func pendingWorkflowProjectionMutationIDs(
	t *testing.T,
	outbox backend.WorkflowActivationProjectionOutbox,
) []string {
	t.Helper()

	refs, next, err := outbox.ListPendingWorkflowActivationProjections(
		context.Background(),
		workflowProjectionTestNamespace,
		0,
		128,
	)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections: %v", err)
	}
	if next != 0 {
		t.Fatalf("ListPendingWorkflowActivationProjections next cursor = %d, want 0 for complete test page", next)
	}
	mutationIDs := make([]string, len(refs))
	for i, ref := range refs {
		mutationIDs[i] = ref.MutationID
	}
	sort.Strings(mutationIDs)
	return mutationIDs
}

func requireWorkflowProjectionActivation(
	t *testing.T,
	store engine.EntryActivationStore,
	current backend.WorkflowRecord,
) engine.EntryActivation {
	t.Helper()

	activations, err := store.List(context.Background(), workflowProjectionTestNamespace)
	if err != nil {
		t.Fatalf("List entry activations: %v", err)
	}
	var matches []engine.EntryActivation
	for _, activation := range activations {
		if activation.WorkflowID == current.ID && activation.WorkflowVersion == current.Version {
			matches = append(matches, activation)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("activations for %s/%s = %+v, want exactly one", current.ID, current.Version, matches)
	}
	activation := matches[0]
	if !activation.Desired {
		t.Fatalf("activation for %s/%s is not desired: %+v", current.ID, current.Version, activation)
	}
	if activation.RegistryRevision != current.RegistryRevision {
		t.Fatalf("activation registry revision = %d, want %d", activation.RegistryRevision, current.RegistryRevision)
	}
	return activation
}

func requireNoWorkflowProjectionActivation(
	t *testing.T,
	store engine.EntryActivationStore,
	workflowID types.WorkflowID,
) {
	t.Helper()

	activations, err := store.List(context.Background(), workflowProjectionTestNamespace)
	if err != nil {
		t.Fatalf("List entry activations: %v", err)
	}
	for _, activation := range activations {
		if activation.WorkflowID == workflowID {
			t.Fatalf("unexpected activation for workflow %s: %+v", workflowID, activation)
		}
	}
}

type workflowProjectionFaultStore struct {
	engine.EntryActivationStore
	revisionStore engine.EntryActivationRevisionStore

	mu         sync.Mutex
	upserts    map[types.WorkflowID]int
	failUpsert func(engine.EntryActivation, int) error
}

var (
	_ engine.EntryActivationStore         = (*workflowProjectionFaultStore)(nil)
	_ engine.EntryActivationRevisionStore = (*workflowProjectionFaultStore)(nil)
)

func newWorkflowProjectionFaultStore(store *MemoryEntryActivationStore) *workflowProjectionFaultStore {
	return &workflowProjectionFaultStore{
		EntryActivationStore: store,
		revisionStore:        store,
		upserts:              make(map[types.WorkflowID]int),
	}
}

func (s *workflowProjectionFaultStore) AdvanceWorkflowRevision(
	ctx context.Context,
	ns namespace.Namespace,
	workflowID types.WorkflowID,
	revision uint64,
) error {
	return s.revisionStore.AdvanceWorkflowRevision(ctx, ns, workflowID, revision)
}

func (s *workflowProjectionFaultStore) Upsert(ctx context.Context, activation engine.EntryActivation) error {
	s.mu.Lock()
	s.upserts[activation.WorkflowID]++
	attempt := s.upserts[activation.WorkflowID]
	fail := s.failUpsert
	s.mu.Unlock()

	if fail != nil {
		if err := fail(activation, attempt); err != nil {
			return err
		}
	}
	return s.EntryActivationStore.Upsert(ctx, activation)
}

func (s *workflowProjectionFaultStore) upsertCount(workflowID types.WorkflowID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upserts[workflowID]
}

type workflowProjectionFaultOutbox struct {
	delegate backend.WorkflowActivationProjectionOutbox

	mu                    sync.Mutex
	namespaceListCalls    int
	ackFailuresAfterApply int
	ackCalls              int
	ackFailure            error
	acknowledged          chan struct{}
	acknowledgedOnce      sync.Once
	listStarted           chan struct{}
	listStartedOnce       sync.Once
	blockNamespaceList    bool
}

var _ backend.WorkflowActivationProjectionOutbox = (*workflowProjectionFaultOutbox)(nil)

func (o *workflowProjectionFaultOutbox) ListWorkflowActivationProjectionNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	o.mu.Lock()
	o.namespaceListCalls++
	block := o.blockNamespaceList
	o.mu.Unlock()

	if o.listStarted != nil {
		o.listStartedOnce.Do(func() { close(o.listStarted) })
	}
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return o.delegate.ListWorkflowActivationProjectionNamespaces(ctx)
}

func (o *workflowProjectionFaultOutbox) ListPendingWorkflowActivationProjections(
	ctx context.Context,
	ns namespace.Namespace,
	cursor uint64,
	limit int,
) ([]backend.WorkflowActivationProjectionRef, uint64, error) {
	return o.delegate.ListPendingWorkflowActivationProjections(ctx, ns, cursor, limit)
}

func (o *workflowProjectionFaultOutbox) ClaimWorkflowActivationProjection(
	ctx context.Context,
	ns namespace.Namespace,
	mutationID string,
	token string,
	ttl time.Duration,
) (backend.WorkflowActivationProjectionClaim, error) {
	return o.delegate.ClaimWorkflowActivationProjection(ctx, ns, mutationID, token, ttl)
}

func (o *workflowProjectionFaultOutbox) AckWorkflowActivationProjection(
	ctx context.Context,
	ns namespace.Namespace,
	mutationID string,
	token string,
) (bool, error) {
	acked, err := o.delegate.AckWorkflowActivationProjection(ctx, ns, mutationID, token)

	o.mu.Lock()
	o.ackCalls++
	fail := err == nil && acked && o.ackFailuresAfterApply > 0
	if fail {
		o.ackFailuresAfterApply--
	}
	failure := o.ackFailure
	o.mu.Unlock()

	if err == nil && acked && o.acknowledged != nil {
		o.acknowledgedOnce.Do(func() { close(o.acknowledged) })
	}
	if fail {
		return false, failure
	}
	return acked, err
}

func (o *workflowProjectionFaultOutbox) namespaceLists() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.namespaceListCalls
}

func (o *workflowProjectionFaultOutbox) acknowledgements() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ackCalls
}

type workflowProjectionLeaderGate bool

func (g workflowProjectionLeaderGate) IsLeader() bool { return bool(g) }

func TestWorkflowActivationProjectionWorkerRunRecoversImmediately(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	result := commitWorkflowProjectionTestIntent(t, registry.registry, "startup", singleTriggerGraph(t, nil))
	store := NewMemoryEntryActivationStore()
	acked := make(chan struct{})
	outbox := &workflowProjectionFaultOutbox{
		delegate:     registry.outbox,
		acknowledged: acked,
	}
	worker := newWorkflowProjectionTestWorker(outbox, store, backend.AlwaysLeader{}, func() string { return "startup-token" })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()

	select {
	case <-acked:
		// The period is one hour, so an acknowledgement within this deadline can
		// only have come from Run's immediate startup sweep.
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("Run did not recover the pending projection during its startup sweep")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after startup recovery was canceled")
	}

	requireWorkflowProjectionActivation(t, store, result.Current)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); len(pending) != 0 {
		t.Fatalf("pending projections after startup recovery = %v, want none", pending)
	}
}

func TestWorkflowActivationProjectionWorkerProjectionFailureRemainsPendingAndRetries(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	result := commitWorkflowProjectionTestIntent(t, registry.registry, "projection-retry", singleTriggerGraph(t, nil))
	memoryStore := NewMemoryEntryActivationStore()
	store := newWorkflowProjectionFaultStore(memoryStore)
	projectionFailure := errors.New("injected activation upsert failure")
	store.failUpsert = func(activation engine.EntryActivation, attempt int) error {
		if activation.WorkflowID == result.Current.ID && attempt == 1 {
			return projectionFailure
		}
		return nil
	}
	worker := newWorkflowProjectionTestWorker(
		registry.outbox,
		store,
		backend.AlwaysLeader{},
		workflowProjectionTestTokenSequence("projection-retry"),
	)

	if applied := worker.ReconcileOnce(context.Background()); applied != 0 {
		t.Fatalf("first ReconcileOnce applied = %d, want 0 after projection failure", applied)
	}
	if got := store.upsertCount(result.Current.ID); got != 1 {
		t.Fatalf("upsert attempts after failed projection = %d, want 1", got)
	}
	requireNoWorkflowProjectionActivation(t, memoryStore, result.Current.ID)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); !reflect.DeepEqual(pending, []string{"projection-retry"}) {
		t.Fatalf("pending projections after projection failure = %v, want [projection-retry]", pending)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if worker.ReconcileOnce(context.Background()) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending projection was not retried after its failed claim lease expired")
		}
		time.Sleep(10 * time.Millisecond)
	}

	requireWorkflowProjectionActivation(t, memoryStore, result.Current)
	if got := store.upsertCount(result.Current.ID); got != 2 {
		t.Fatalf("upsert attempts after successful retry = %d, want 2", got)
	}
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); len(pending) != 0 {
		t.Fatalf("pending projections after successful retry = %v, want none", pending)
	}
}

func TestWorkflowActivationProjectionWorkerAckResponseLossAllowsSafeDuplicateDelivery(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	result := commitWorkflowProjectionTestIntent(t, registry.registry, "ack-response-loss", singleTriggerGraph(t, nil))
	memoryStore := NewMemoryEntryActivationStore()
	store := newWorkflowProjectionFaultStore(memoryStore)
	ackFailure := errors.New("injected ack response loss")
	outbox := &workflowProjectionFaultOutbox{
		delegate:              registry.outbox,
		ackFailuresAfterApply: 1,
		ackFailure:            ackFailure,
	}
	worker := newWorkflowProjectionTestWorker(
		outbox,
		store,
		backend.AlwaysLeader{},
		workflowProjectionTestTokenSequence("ack-response-loss"),
	)

	applied, err := worker.ProjectPending(context.Background(), workflowProjectionTestNamespace, "ack-response-loss", 1)
	if applied || !errors.Is(err, ackFailure) {
		t.Fatalf("first ProjectPending = applied %v, err %v; want false and injected ack response loss", applied, err)
	}
	requireWorkflowProjectionActivation(t, memoryStore, result.Current)
	if got := store.upsertCount(result.Current.ID); got != 1 {
		t.Fatalf("upserts before duplicate delivery = %d, want 1", got)
	}

	applied, err = worker.ProjectPending(context.Background(), workflowProjectionTestNamespace, "ack-response-loss", 1)
	if err != nil || !applied {
		t.Fatalf("duplicate ProjectPending = applied %v, err %v; want true, nil", applied, err)
	}
	if got := store.upsertCount(result.Current.ID); got != 1 {
		t.Fatalf("duplicate delivery reprojected activation: upserts = %d, want 1", got)
	}
	if got := outbox.acknowledgements(); got != 2 {
		t.Fatalf("ack attempts = %d, want 2", got)
	}
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); len(pending) != 0 {
		t.Fatalf("pending projections after duplicate delivery = %v, want none", pending)
	}
}

func TestWorkflowActivationProjectionWorkerPoisonIntentDoesNotBlockBatch(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	poison := commitWorkflowProjectionTestIntent(t, registry.registry, "poison", singleTriggerGraph(t, nil))
	healthy := commitWorkflowProjectionTestIntent(t, registry.registry, "healthy", singleTriggerGraph(t, nil))
	memoryStore := NewMemoryEntryActivationStore()
	store := newWorkflowProjectionFaultStore(memoryStore)
	poisonFailure := errors.New("injected poison projection")
	store.failUpsert = func(activation engine.EntryActivation, _ int) error {
		if activation.WorkflowID == poison.Current.ID {
			return poisonFailure
		}
		return nil
	}
	worker := newWorkflowProjectionTestWorker(
		registry.outbox,
		store,
		backend.AlwaysLeader{},
		workflowProjectionTestTokenSequence("batch"),
	)

	if applied := worker.ReconcileOnce(context.Background()); applied != 1 {
		t.Fatalf("ReconcileOnce applied = %d, want 1 healthy intent from the same batch", applied)
	}
	requireNoWorkflowProjectionActivation(t, memoryStore, poison.Current.ID)
	requireWorkflowProjectionActivation(t, memoryStore, healthy.Current)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); !reflect.DeepEqual(pending, []string{"poison"}) {
		t.Fatalf("pending projections after batch = %v, want only poison intent", pending)
	}
}

func TestWorkflowActivationProjectionWorkerNonLeaderDoesNotProcess(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	result := commitWorkflowProjectionTestIntent(t, registry.registry, "non-leader", singleTriggerGraph(t, nil))
	store := NewMemoryEntryActivationStore()
	outbox := &workflowProjectionFaultOutbox{delegate: registry.outbox}
	worker := newWorkflowProjectionTestWorker(
		outbox,
		store,
		workflowProjectionLeaderGate(false),
		func() string { return "must-not-be-used" },
	)

	if applied := worker.ReconcileOnce(context.Background()); applied != 0 {
		t.Fatalf("non-leader ReconcileOnce applied = %d, want 0", applied)
	}
	if got := outbox.namespaceLists(); got != 0 {
		t.Fatalf("non-leader listed projection namespaces %d times, want 0", got)
	}
	requireNoWorkflowProjectionActivation(t, store, result.Current.ID)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); !reflect.DeepEqual(pending, []string{"non-leader"}) {
		t.Fatalf("pending projections after non-leader pass = %v, want [non-leader]", pending)
	}
}

func TestWorkflowActivationProjectionWorkerRunCancelExitsPromptly(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	started := make(chan struct{})
	outbox := &workflowProjectionFaultOutbox{
		delegate:           registry.outbox,
		listStarted:        started,
		blockNamespaceList: true,
	}
	worker := newWorkflowProjectionTestWorker(outbox, NewMemoryEntryActivationStore(), backend.AlwaysLeader{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("Run did not enter its startup reconciliation")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit promptly after its context was canceled")
	}
}

func TestWorkflowActivationProjectionWorkerRecoversCrashAfterCASBeforeProjection(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	result := commitWorkflowProjectionTestIntent(t, registry.registry, "crash-after-cas", singleTriggerGraph(t, nil))
	store := NewMemoryEntryActivationStore()

	committed, err := registry.registry.GetWorkflow(context.Background(), result.Current.ID)
	if err != nil {
		t.Fatalf("GetWorkflow after CAS: %v", err)
	}
	if committed.DefinitionHash != result.Current.DefinitionHash || committed.RegistryRevision != result.Current.RegistryRevision {
		t.Fatalf("committed registry record = %+v, want CAS result %+v", committed, result.Current)
	}
	requireNoWorkflowProjectionActivation(t, store, result.Current.ID)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); !reflect.DeepEqual(pending, []string{"crash-after-cas"}) {
		t.Fatalf("pending projections before recovery = %v, want [crash-after-cas]", pending)
	}

	// Constructing the worker only after the authoritative CAS models a process
	// dying before it could issue any activation-store write.
	worker := newWorkflowProjectionTestWorker(
		registry.outbox,
		store,
		backend.AlwaysLeader{},
		func() string { return "post-crash-worker" },
	)
	if applied := worker.ReconcileOnce(context.Background()); applied != 1 {
		t.Fatalf("post-crash ReconcileOnce applied = %d, want 1", applied)
	}
	requireWorkflowProjectionActivation(t, store, result.Current)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); len(pending) != 0 {
		t.Fatalf("pending projections after post-crash recovery = %v, want none", pending)
	}
}
