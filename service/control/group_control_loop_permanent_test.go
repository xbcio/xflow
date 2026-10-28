package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// A batch whose stored items can never be read back fails BuildSubgraphLease
// with a permanent error. These tests pin what the control plane does with it:
// settle the batch as failed and drop the claim, instead of requeueing the claim
// and returning the error -- which the runner's pollLoop treats as fatal for the
// whole session, so the same poison claim would be handed back on every
// reconnect.
//
// "Dropped" and "requeued" are observed on the directory, not inferred from the
// return value: a requeue after a dispatch failure is MakeClaimHandoffRecoverable
// (the claim stays as a handoff debt), a drop is SettleClaimHandoff(Drop).

// settleRecordingDirectory is a real MemoryRunnerDirectory that records how the
// claim handoff was resolved, and can fail the settlement.
type settleRecordingDirectory struct {
	*MemoryRunnerDirectory

	mu          sync.Mutex
	settled     []HandoffDisposition
	recoverable int
	settleErr   error
}

func (d *settleRecordingDirectory) SettleClaimHandoff(ctx context.Context, id ClaimID, disposition HandoffDisposition) error {
	d.mu.Lock()
	d.settled = append(d.settled, disposition)
	err := d.settleErr
	d.mu.Unlock()
	if err != nil {
		return err
	}
	return d.MemoryRunnerDirectory.SettleClaimHandoff(ctx, id, disposition)
}

func (d *settleRecordingDirectory) MakeClaimHandoffRecoverable(ctx context.Context, id ClaimID) error {
	d.mu.Lock()
	d.recoverable++
	d.mu.Unlock()
	return d.MemoryRunnerDirectory.MakeClaimHandoffRecoverable(ctx, id)
}

func (d *settleRecordingDirectory) outcome() (settled []HandoffDisposition, recoverable int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]HandoffDisposition(nil), d.settled...), d.recoverable
}

// failBatchCall is one FailBatchTask invocation as the fake saw it.
type failBatchCall struct {
	task  engine.Task
	cause error
}

// failBatchFakeEngine adds the failBatchEngine capability to the batch fake.
type failBatchFakeEngine struct {
	subgraphFakeEngine

	mu      sync.Mutex
	failErr error
	calls   []failBatchCall
}

func (f *failBatchFakeEngine) FailBatchTask(_ context.Context, t *engine.Task, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, failBatchCall{task: *t, cause: cause})
	return f.failErr
}

func (f *failBatchFakeEngine) failCalls() []failBatchCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]failBatchCall(nil), f.calls...)
}

// permanentBatchFixture is a core wired to a recording directory with one batch
// assignment already claimed, i.e. the state dispatchSubgraphLease is called in.
type permanentBatchFixture struct {
	core    *Core
	dir     *settleRecordingDirectory
	session RunnerSession
	claim   Claim
	logger  *recordingLogger
}

func newPermanentBatchFixture(t *testing.T, eng EngineFacade) *permanentBatchFixture {
	t.Helper()
	ctx := context.Background()
	mem := NewMemoryRunnerDirectory()
	caps := []protocol.Capability{{NodeType: "xflow.map"}}
	session := registerCarrierProbeRunner(t, mem, "runner-perm-batch", caps)
	task := engine.Task{ExecutionID: "exec-perm-batch", NodeName: "m/_batch/0", NodeIdx: 0, Type: engine.TaskTypeNodeBatch}
	mustEnqueueAssignment(t, ctx, mem, Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.map"},
	})
	claim := mustClaimAssignment(t, ctx, mem, session)

	dir := &settleRecordingDirectory{MemoryRunnerDirectory: mem}
	logger := &recordingLogger{}
	return &permanentBatchFixture{
		core:    &Core{engine: eng, runners: dir, logger: logger, pollWait: time.Second},
		dir:     dir,
		session: session,
		claim:   claim,
		logger:  logger,
	}
}

func (f *permanentBatchFixture) dispatch() (protocol.PollTaskResponse, error) {
	return f.core.dispatchSubgraphLease(context.Background(), f.claim)
}

// assertDropped checks the claim was dropped: settled as Drop, never made
// recoverable, and the assignment is gone from the queue.
func (f *permanentBatchFixture) assertDropped(t *testing.T) {
	t.Helper()
	settled, recoverable := f.dir.outcome()
	if len(settled) != 1 || settled[0] != HandoffDispositionDrop || recoverable != 0 {
		t.Fatalf("settled = %v, recoverable = %d; want exactly one Drop and no requeue", settled, recoverable)
	}
	again, ok, err := f.dir.ClaimForRunner(context.Background(), testClaimRequest(f.session, 2))
	if err != nil || ok {
		t.Fatalf("ClaimForRunner after drop = %#v, ok=%v, err=%v; want nothing left to claim", again, ok, err)
	}
}

// assertRequeued checks the claim went back as recoverable debt and was not
// dropped: the next claim for the session is this same claim, as a handoff.
func (f *permanentBatchFixture) assertRequeued(t *testing.T, wantSettleAttempts int) {
	t.Helper()
	settled, recoverable := f.dir.outcome()
	if len(settled) != wantSettleAttempts || recoverable != 1 {
		t.Fatalf("settled = %v, recoverable = %d; want %d settle attempt(s) and one requeue", settled, recoverable, wantSettleAttempts)
	}
	again, ok, err := f.dir.ClaimForRunner(context.Background(), testClaimRequest(f.session, 2))
	if err != nil || !ok || again.ClaimID != f.claim.ClaimID || again.Handoff == nil {
		t.Fatalf("ClaimForRunner after requeue = %#v, ok=%v, err=%v; want the same claim back as a handoff", again, ok, err)
	}
}

func permanentBatchBuildError() error {
	return fmt.Errorf("build subgraph lease: %w", types.NewPermanentError("batch.items_unavailable", "items blob missing"))
}

// TestDispatchSubgraphLeasePermanentErrorFailsBatchAndDropsClaim: a permanent
// build error settles the batch as failed and drops the claim, and the runner
// gets "no task" rather than an error that would end its session.
func TestDispatchSubgraphLeasePermanentErrorFailsBatchAndDropsClaim(t *testing.T) {
	buildErr := permanentBatchBuildError()
	eng := &failBatchFakeEngine{subgraphFakeEngine: subgraphFakeEngine{err: buildErr}}
	f := newPermanentBatchFixture(t, eng)

	resp, err := f.dispatch()
	if err != nil {
		t.Fatalf("dispatchSubgraphLease() error = %v, want nil so the runner session survives", err)
	}
	if resp.Lease != nil || resp.Wait != 0 {
		t.Fatalf("dispatchSubgraphLease() = %+v, want an empty response", resp)
	}
	calls := eng.failCalls()
	if len(calls) != 1 {
		t.Fatalf("FailBatchTask calls = %d, want exactly 1", len(calls))
	}
	if calls[0].task.ExecutionID != f.claim.Assignment.Task.ExecutionID ||
		calls[0].task.NodeName != f.claim.Assignment.Task.NodeName ||
		calls[0].task.Type != engine.TaskTypeNodeBatch {
		t.Fatalf("FailBatchTask task = %+v, want the claim's batch task %+v", calls[0].task, f.claim.Assignment.Task)
	}
	if !errors.Is(calls[0].cause, types.ErrPermanent) || !errors.Is(calls[0].cause, buildErr) {
		t.Fatalf("FailBatchTask cause = %v, want the original permanent build error", calls[0].cause)
	}
	f.assertDropped(t)

	logs := f.logger.withMsg("batch lease permanently unbuildable; failing the batch")
	if len(logs) != 1 || logs[0].level != "warn" {
		t.Fatalf("log records = %+v, want exactly one warn", logs)
	}
	for _, key := range []string{"execution_id", "node_name", "claim", "err"} {
		if logs[0].field(key) == nil {
			t.Errorf("log record lacks field %q: %+v", key, logs[0].args)
		}
	}
	if len(logs[0].args) != 8 {
		t.Errorf("log record args = %v, want only the four identifying fields", logs[0].args)
	}
}

// TestDispatchSubgraphLeasePermanentErrorKeepsClaimWhenSettlementFails: when the
// failed-batch commit itself fails the batch is still unsettled, so the claim
// must stay claimable and the error must reach the caller, not be swallowed.
func TestDispatchSubgraphLeasePermanentErrorKeepsClaimWhenSettlementFails(t *testing.T) {
	eng := &failBatchFakeEngine{
		subgraphFakeEngine: subgraphFakeEngine{err: permanentBatchBuildError()},
		failErr:            errors.New("redis: connection reset"),
	}
	f := newPermanentBatchFixture(t, eng)

	_, err := f.dispatch()
	if err == nil {
		t.Fatal("dispatchSubgraphLease() error = nil, want the transient settlement failure")
	}
	if !errors.Is(err, ErrInternalServer) {
		t.Fatalf("dispatchSubgraphLease() error = %v, want it normalized to ErrInternalServer", err)
	}
	if len(eng.failCalls()) != 1 {
		t.Fatalf("FailBatchTask calls = %d, want 1", len(eng.failCalls()))
	}
	f.assertRequeued(t, 0)
}

// TestDispatchSubgraphLeasePermanentErrorWithoutFailBatchEngineKeepsOldBehavior:
// an engine that cannot fail a batch gets the unchanged default branch.
func TestDispatchSubgraphLeasePermanentErrorWithoutFailBatchEngineKeepsOldBehavior(t *testing.T) {
	buildErr := permanentBatchBuildError()
	eng := &subgraphFakeEngine{err: buildErr}
	if _, ok := any(eng).(failBatchEngine); ok {
		t.Fatal("premise broken: subgraphFakeEngine must not implement failBatchEngine")
	}
	f := newPermanentBatchFixture(t, eng)

	_, err := f.dispatch()
	if !errors.Is(err, buildErr) {
		t.Fatalf("dispatchSubgraphLease() error = %v, want the original build error returned as is", err)
	}
	f.assertRequeued(t, 0)
}

// TestDispatchSubgraphLeaseTransientErrorDoesNotFailBatch: only a permanent
// error may settle the batch; anything else is retried by redelivery.
func TestDispatchSubgraphLeaseTransientErrorDoesNotFailBatch(t *testing.T) {
	buildErr := errors.New("redis: i/o timeout")
	eng := &failBatchFakeEngine{subgraphFakeEngine: subgraphFakeEngine{err: buildErr}}
	f := newPermanentBatchFixture(t, eng)

	_, err := f.dispatch()
	if !errors.Is(err, buildErr) {
		t.Fatalf("dispatchSubgraphLease() error = %v, want the original build error returned as is", err)
	}
	if n := len(eng.failCalls()); n != 0 {
		t.Fatalf("FailBatchTask calls = %d, want 0 for a non-permanent error", n)
	}
	f.assertRequeued(t, 0)
}

// TestDispatchSubgraphLeaseInactiveExecutionDropsWithoutFailingBatch: an
// inactive execution has nothing to settle, so it is dropped and the engine is
// never asked to fail the batch.
func TestDispatchSubgraphLeaseInactiveExecutionDropsWithoutFailingBatch(t *testing.T) {
	eng := &failBatchFakeEngine{subgraphFakeEngine: subgraphFakeEngine{
		err: fmt.Errorf("build subgraph lease: %w", engine.ErrExecutionInactive),
	}}
	f := newPermanentBatchFixture(t, eng)

	resp, err := f.dispatch()
	if err != nil || resp.Lease != nil {
		t.Fatalf("dispatchSubgraphLease() = %+v, %v; want an empty response and no error", resp, err)
	}
	if n := len(eng.failCalls()); n != 0 {
		t.Fatalf("FailBatchTask calls = %d, want 0 for an inactive execution", n)
	}
	f.assertDropped(t)
}

// TestDispatchSubgraphLeasePermanentErrorRequeuesWhenDropFails: the batch is
// settled but the claim cannot be dropped, so it is requeued (the settlement is
// idempotent on redelivery) and the normalized error is returned.
func TestDispatchSubgraphLeasePermanentErrorRequeuesWhenDropFails(t *testing.T) {
	eng := &failBatchFakeEngine{subgraphFakeEngine: subgraphFakeEngine{err: permanentBatchBuildError()}}
	f := newPermanentBatchFixture(t, eng)
	f.dir.settleErr = errors.New("redis: settle failed")

	_, err := f.dispatch()
	if !errors.Is(err, ErrInternalServer) {
		t.Fatalf("dispatchSubgraphLease() error = %v, want ErrInternalServer", err)
	}
	if n := len(eng.failCalls()); n != 1 {
		t.Fatalf("FailBatchTask calls = %d, want 1", n)
	}
	f.assertRequeued(t, 1)
}
