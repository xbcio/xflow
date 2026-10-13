package statestoretest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ConcurrencyStoreFactory returns a fresh StateStore for one sub-test. The
// factory must register a cleanup with t.Cleanup so the underlying resource
// (in-memory state, miniredis instance, etc.) is released.
type ConcurrencyStoreFactory func(t *testing.T) engine.StateStore

// RunConcurrencyAdmissionContract exercises engine.ConcurrencyLimitedStateStore
// against a concrete backend.
//
// Every backend that implements the capability must pass this suite, so the
// two implementations cannot drift on what a slot means. The cases are chosen
// around the two failure modes that are invisible from the engine: an
// admission that persists before it checks the limit (a rejection that leaks a
// zero-node execution), and a release that is wired at one engine call site
// instead of into the store's terminal writers (slots that leak until restart,
// silently lowering the effective limit).
func RunConcurrencyAdmissionContract(t *testing.T, newStore ConcurrencyStoreFactory) {
	t.Helper()
	t.Run("AdmitsUntilTheLimit", func(t *testing.T) {
		admissionAdmitsUntilTheLimit(t, newStore(t))
	})
	t.Run("RefusesWithoutPersisting", func(t *testing.T) {
		admissionRefusesWithoutPersisting(t, newStore(t))
	})
	t.Run("QueueModeRefusalIsHonest", func(t *testing.T) {
		admissionQueueModeRefusalIsHonest(t, newStore(t))
	})
	t.Run("KeyedByNameNotVersion", func(t *testing.T) {
		admissionKeyedByNameNotVersion(t, newStore(t))
	})
	t.Run("KeyedByNamespace", func(t *testing.T) {
		admissionKeyedByNamespace(t, newStore(t))
	})
	t.Run("ReleaseOnTerminalStatus", func(t *testing.T) {
		admissionReleasesOnTerminalStatus(t, newStore(t))
	})
	t.Run("ReleaseOnEveryTerminalStatus", func(t *testing.T) {
		admissionReleasesOnEveryTerminalStatus(t, newStore(t))
	})
	t.Run("KeepsSlotWhileCanceling", func(t *testing.T) {
		admissionKeepsSlotWhileCanceling(t, newStore(t))
	})
	t.Run("ReleaseOnAtomicCompletion", func(t *testing.T) {
		admissionReleasesOnAtomicCompletion(t, newStore(t))
	})
	t.Run("LimitOfOneSerializes", func(t *testing.T) {
		admissionLimitOfOneSerializes(t, newStore(t))
	})
}

// concurrencyGraph compiles the one-root workflow the admission cases submit.
// settings.concurrency is not read by the store — the admission carries the
// policy — so each case states the limit on the admission itself and the graph
// only has to be a real, compilable definition with one root.
func concurrencyGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// concurrencyAdmissionFor builds the admission one submission contends for, as
// the engine builds it from a compiled graph.
//
// The namespace is a parameter rather than read from ctx so the key-scope cases
// can vary it; concurrencyAdmissionContext is paired with it below, because in
// production the engine derives both from the same context and a store is
// entitled to read either.
func concurrencyAdmissionFor(t *testing.T, g *graph.Graph, limit int) engine.ConcurrencyAdmission {
	t.Helper()
	return engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     limit,
		Mode:      types.ConcurrencyModeReject,
	}
}

// concurrencyAdmissionContext binds an admission's namespace to the context the
// submission runs under, which is the relationship the engine maintains
// (concurrencyAdmissionFor reads the namespace off the very context it hands
// the store).
func concurrencyAdmissionContext(ctx context.Context, admission engine.ConcurrencyAdmission) context.Context {
	if admission.Namespace == "" {
		return ctx
	}
	return namespace.WithNamespace(ctx, namespace.Namespace(admission.Namespace))
}

// concurrencySnapshot builds one submission as the engine would: a fresh ID, a
// running snapshot carrying the compiled graph, and the one root delivery
// intent that makes the admission atomic with. The task mirrors
// engine.submitInitialTasks exactly — notably it carries no ActivationID, as
// acyclic roots do not — so a backend that only satisfied this suite for
// hand-built tasks would still be caught here.
func concurrencySnapshot(t *testing.T, g *graph.Graph) (*engine.ExecutionSnapshot, []engine.OutboxEntry) {
	t.Helper()
	id := engine.NewExecutionID()
	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	entries := []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}}
	return snap, entries
}

func concurrencyCapability(t *testing.T, state engine.StateStore) engine.ConcurrencyLimitedStateStore {
	t.Helper()
	limited, ok := state.(engine.ConcurrencyLimitedStateStore)
	if !ok {
		t.Fatalf("state store %T does not implement engine.ConcurrencyLimitedStateStore", state)
	}
	return limited
}

func admissionAdmitsUntilTheLimit(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-admit")
	admission := concurrencyAdmissionFor(t, g, 3)

	for i := 0; i < admission.Limit; i++ {
		snap, entries := concurrencySnapshot(t, g)
		res, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission)
		if err != nil {
			t.Fatalf("submission %d error = %v, want admission", i, err)
		}
		if res.Queued {
			t.Fatalf("submission %d reported Queued=%v in reject mode", i, res.Queued)
		}
		loaded, err := state.GetExecution(ctx, snap.ID)
		if err != nil {
			t.Fatalf("GetExecution(%q) error = %v", snap.ID, err)
		}
		if loaded == nil {
			t.Fatalf("admitted execution %q was not persisted", snap.ID)
		}
	}
}

func admissionRefusesWithoutPersisting(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-refuse")
	admission := concurrencyAdmissionFor(t, g, 1)

	first, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, first, entries, admission); err != nil {
		t.Fatalf("first submission error = %v", err)
	}

	refused, refusedEntries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, refused, refusedEntries, admission); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("over-limit submission error = %v, want ErrConcurrencyLimit", err)
	}
	loaded, err := state.GetExecution(ctx, refused.ID)
	if err != nil {
		t.Fatalf("GetExecution(refused) error = %v", err)
	}
	// GetExecution reports "no such execution" with a nil snapshot on both
	// backends; the refusal must leave exactly that, not a zero-node running
	// execution the list API would show.
	if loaded != nil {
		t.Fatalf("refused execution was persisted: %+v", loaded)
	}
}

// admissionQueueModeRefusalIsHonest is the one thing every store must agree on
// about QUEUE mode, whether or not it can queue: an over-limit submission is
// either parked or refused, and a refusal leaves nothing behind.
//
// The third possibility — reporting success while admitting the submission over
// the limit — is the silent limit break, and it is what makes this case worth
// keeping in the shared suite even though both in-tree backends queue. A store
// that refuses must use the sentinel, because a caller that cannot tell "this
// store has no queue" from "this submission was rejected" cannot retry the
// right way, and the HTTP layer maps the two to different statuses.
//
// The behaviour of a store that does queue is described by the queue contract
// (RunConcurrencyQueueContract), which such a store runs in addition.
func admissionQueueModeRefusalIsHonest(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue")
	admission := concurrencyAdmissionFor(t, g, 1)
	admission.Mode = types.ConcurrencyModeQueue

	// Fill the only slot with a REJECT-mode submission, so the next one is over
	// the limit whichever way this store handles QUEUE mode. The mode of the
	// submission that holds a slot says nothing about the slot: it is the same
	// set either way.
	holder := concurrencyAdmissionFor(t, g, 1)
	holderSnap, holderEntries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, holderSnap, holderEntries, holder); err != nil {
		t.Fatalf("filling the only slot error = %v", err)
	}

	snap, entries := concurrencySnapshot(t, g)
	res, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission)
	switch {
	case err == nil && res.Queued:
		// A store that parks submissions: the execution must be visible and
		// must not be running, or it would be work over the limit wearing a
		// queue's label.
		loaded, loadErr := state.GetExecution(ctx, snap.ID)
		if loadErr != nil {
			t.Fatalf("GetExecution(queued) error = %v", loadErr)
		}
		if loaded == nil || loaded.Status != types.ExecutionStatusQueued {
			t.Fatalf("queued submission persisted as %+v, want status %q", loaded, types.ExecutionStatusQueued)
		}
	case errors.Is(err, engine.ErrConcurrencyQueueUnsupported):
		loaded, loadErr := state.GetExecution(ctx, snap.ID)
		if loadErr != nil {
			t.Fatalf("GetExecution(refused) error = %v", loadErr)
		}
		if loaded != nil {
			t.Fatalf("queue-mode refusal persisted an execution: %+v", loaded)
		}
	case err == nil:
		t.Fatalf("queue-mode submission reported %+v with no error, want it parked or refused: admitting it over the limit breaks the limit silently", res)
	default:
		t.Fatalf("queue-mode submission error = %v, want ErrConcurrencyQueueUnsupported or a parked execution", err)
	}
}

// admissionKeyedByNameNotVersion pins the key scope: two versions of the same
// workflow name share one slot set. Keying by version would let a re-published
// definition run limit x versions concurrently and orphan the slots the
// previous version's executions still hold.
func admissionKeyedByNameNotVersion(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	v1 := concurrencyGraph(t, "conc-version")
	v2, err := graph.Compile(&types.WorkflowDef{
		Name:    "conc-version",
		Version: "2",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(v2) error = %v", err)
	}

	admission := concurrencyAdmissionFor(t, v1, 1)
	snap, entries := concurrencySnapshot(t, v1)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission); err != nil {
		t.Fatalf("v1 submission error = %v", err)
	}

	snap2, entries2 := concurrencySnapshot(t, v2)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, snap2, entries2, admission); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("v2 submission while v1 holds the only slot error = %v, want ErrConcurrencyLimit", err)
	}
}

// admissionKeyedByNamespace pins tenant isolation: the same workflow name in
// another namespace gets its own limit, or one tenant's backlog would throttle
// another's.
func admissionKeyedByNamespace(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-ns")

	first := concurrencyAdmissionFor(t, g, 1)
	snap, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(concurrencyAdmissionContext(ctx, first), snap, entries, first); err != nil {
		t.Fatalf("default-namespace submission error = %v", err)
	}

	other := first
	other.Namespace = "other-tenant"
	snap2, entries2 := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(concurrencyAdmissionContext(ctx, other), snap2, entries2, other); err != nil {
		t.Fatalf("other-namespace submission error = %v, want its own slot", err)
	}
}

// admissionReleasesOnTerminalStatus is the release half of the contract, driven
// through UpdateExecutionStatus: whatever moves an execution to a terminal
// status has to free its slot, or the effective limit decays with every
// completed execution until a restart clears the leak.
func admissionReleasesOnTerminalStatus(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-release")
	admission := concurrencyAdmissionFor(t, g, 1)

	snap, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission); err != nil {
		t.Fatalf("first submission error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, snap.ID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(success) error = %v", err)
	}

	second, entries2 := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, second, entries2, admission); err != nil {
		t.Fatalf("submission after the slot was released error = %v, want admission", err)
	}
}

// admissionReleasesOnEveryTerminalStatus widens the release case from "a
// terminal status releases" to every one of them. A store that hooked one
// writer, or one status, leaks a slot for every execution that finishes the
// other way — and timeout and canceled are the two that arrive through paths
// nobody exercises by hand.
func admissionReleasesOnEveryTerminalStatus(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-terminal-statuses")
	admission := concurrencyAdmissionFor(t, g, 1)

	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout,
	} {
		snap, entries := concurrencySnapshot(t, g)
		if _, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission); err != nil {
			t.Fatalf("submission before %s error = %v, want admission", status, err)
		}
		if err := state.UpdateExecutionStatus(ctx, snap.ID, status, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(%s) error = %v", status, err)
		}
		// The next iteration's submission is the probe: it can only be admitted
		// if this status released the slot.
	}
	probe, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, probe, entries, admission); err != nil {
		t.Fatalf("submission after every terminal status error = %v, want the slot released", err)
	}
}

// admissionKeepsSlotWhileCanceling pins the other half of the terminal rule:
// canceling is not terminal. An execution that a cancel has asked to stop is
// still running its handler, and freeing its slot would admit a replacement
// that overlaps it — exactly what the limit exists to prevent.
func admissionKeepsSlotWhileCanceling(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-canceling")
	admission := concurrencyAdmissionFor(t, g, 1)

	snap, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission); err != nil {
		t.Fatalf("first submission error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, snap.ID, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}

	second, entries2 := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, second, entries2, admission); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("submission while the holder is canceling error = %v, want ErrConcurrencyLimit", err)
	}
}

// admissionReleasesOnAtomicCompletion drives the release through the atomic
// commit path instead of UpdateExecutionStatus. The two are separate writers in
// every backend, and a release wired into only one of them leaks a slot for
// every execution that finishes through the other — which is the common case.
func admissionReleasesOnAtomicCompletion(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Skipf("%T does not implement AtomicStateStore", state)
	}
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-atomic-release")
	admission := concurrencyAdmissionFor(t, g, 1)

	snap, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission); err != nil {
		t.Fatalf("first submission error = %v", err)
	}

	// Lease the root as the dispatcher would, then commit it: a one-unit graph
	// finishes on that commit, which is what must release the slot. Attempt is
	// stated on the lease because the commit fences on it.
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("contract-lease"),
		LeaseToken: engine.LeaseToken("contract-token"),
		Attempt:    1,
		Task:       entries[0].Task,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	res, err := atomic.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: snap.ID,
		NodeName:    "start",
		NodeIdx:     0,
		UnitIdx:     0,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !res.ExecutionDone {
		t.Fatalf("CommitNode() ExecutionDone = false, want the one-unit graph to finish")
	}

	second, entries2 := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, second, entries2, admission); err != nil {
		t.Fatalf("submission after atomic completion error = %v, want the slot released", err)
	}
}

// admissionLimitOfOneSerializes is the concurrent half: N submitters race for
// one slot. Exactly one may be admitted, and the store must report the losers
// as refusals rather than over-admitting.
func admissionLimitOfOneSerializes(t *testing.T, state engine.StateStore) {
	limited := concurrencyCapability(t, state)
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-serialize")
	admission := concurrencyAdmissionFor(t, g, 1)

	const submitters = 8
	type outcome struct {
		id  types.ExecutionID
		err error
	}
	results := make(chan outcome, submitters)
	for i := 0; i < submitters; i++ {
		go func() {
			snap, entries := concurrencySnapshot(t, g)
			_, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission)
			results <- outcome{id: snap.ID, err: err}
		}()
	}

	var admitted []types.ExecutionID
	for i := 0; i < submitters; i++ {
		res := <-results
		switch {
		case res.err == nil:
			admitted = append(admitted, res.id)
		case errors.Is(res.err, engine.ErrConcurrencyLimit):
			// expected for every loser
		default:
			t.Fatalf("unexpected submission error = %v", res.err)
		}
	}
	if len(admitted) != 1 {
		t.Fatalf("admitted %d submissions, want exactly 1", len(admitted))
	}
}
