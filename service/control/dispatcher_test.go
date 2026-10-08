package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

var _ func(Router, RunnerDirectory, ...DispatcherOption) *Dispatcher = NewDispatcher

func TestDispatcherEnqueuesRoutedAssignmentWithoutBuildingLease(t *testing.T) {
	ctx := context.Background()
	dispatchEngine := &fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
	}
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(dispatchEngine, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if claim.Assignment.Task.NodeName != "node-a" {
		t.Fatalf("claimed task = %+v, want node-a", claim.Assignment.Task)
	}
	if claim.Assignment.AssignmentID != BuildAssignmentID(task) {
		t.Fatalf("assignment id = %q, want %q", claim.Assignment.AssignmentID, BuildAssignmentID(task))
	}
}

func TestDispatcherDeduplicatesAssignmentByDeterministicTaskIdentity(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(&fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
	}, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("first HandleTask() error = %v", err)
	}
	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("second HandleTask() error = %v", err)
	}

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil || !ok {
		t.Fatalf("first ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if _, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil {
		t.Fatalf("second ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("second ClaimForRunner() ok=true, want deduplicated queue")
	}
}

func TestDispatcherReturnsTransientErrorWithoutRunnerDirectory(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	dispatcher := NewDispatcher(&fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
	}, nil)

	err := dispatcher.HandleTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected error without runner directory")
	}
	if !IsTransient(err) {
		t.Fatalf("err = %v, want transient", err)
	}
}

type fakeDispatchEngine struct {
	routing    engine.TaskRouting
	routingErr error
}

func (e *fakeDispatchEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return e.routing, e.routingErr
}

func (e *fakeDispatchEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	panic("dispatcher must not call BuildTaskLease")
}

func TestDispatcherAssignmentCarriesNamespaceFromContext(t *testing.T) {
	ctx := context.Background()
	ctx = namespace.WithNamespace(ctx, "namespace-acme")
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(&fakeDispatchEngine{routing: engine.TaskRouting{NodeType: "xflow.function"}}, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
		Namespaces:   []namespace.Namespace{"namespace-acme"},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if claim.Assignment.Namespace != "namespace-acme" {
		t.Fatalf("assignment namespace = %q, want namespace-acme", claim.Assignment.Namespace)
	}
}

func TestDispatcherTreatsExecutionInactiveAsNoOp(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	leaseEngine := &fakeDispatchEngine{routingErr: engine.ErrExecutionInactive}
	dir := NewMemoryRunnerDirectory()
	obs := &fakeDispatcherObserver{}

	if err := NewDispatcher(leaseEngine, dir, WithDispatcherObserver(obs)).HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v, want nil", err)
	}
	// Backward compatibility: the bare sentinel keeps its nil disposition, but
	// it is no longer silent — it is counted as a node-level stale route.
	if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropNodeStale {
		t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropNodeStale)
	}

	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, ok, err := dir.ClaimForRunner(context.Background(), ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want no queued assignment")
	}
}

// fakeDispatcherObserver records every optional observer callback so a test
// can assert what the dispatcher reported. It implements the transient
// observer and both optional extensions.
type fakeDispatcherObserver struct {
	transient []string
	dropped   []string
	lags      []time.Duration
}

func (o *fakeDispatcherObserver) OnDispatchTransient(_ context.Context, reason string) {
	o.transient = append(o.transient, reason)
}

func (o *fakeDispatcherObserver) OnDispatchDropped(_ context.Context, reason string) {
	o.dropped = append(o.dropped, reason)
}

func (o *fakeDispatcherObserver) OnTaskDeliveryLag(_ context.Context, lag time.Duration) {
	o.lags = append(o.lags, lag)
}

func assertNoAssignmentQueued(t *testing.T, dir *MemoryRunnerDirectory) {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     "runner-claim-probe",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, ok, err := dir.ClaimForRunner(context.Background(), ClaimRequest{
		RunnerID:     "runner-claim-probe",
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want no queued assignment")
	}
}

// TestDispatcherGoneExecutionIsCountedAndNotSuccessful pins deliverable (1b):
// a task for an execution that no longer exists must not be acked like a
// success. It is counted as execution_gone, logged, and returned as a
// permanent error so the transport archives rather than retries it.
func TestDispatcherGoneExecutionIsCountedAndNotSuccessful(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-gone", NodeName: "start", NodeIdx: 0}
	routingErr := &engine.ExecutionInactiveError{
		ExecutionID: "exec-gone",
		Kind:        engine.ExecutionInactiveGone,
	}
	dir := NewMemoryRunnerDirectory()
	obs := &fakeDispatcherObserver{}
	logger := &recordingLogger{}
	err := NewDispatcher(&fakeDispatchEngine{routingErr: routingErr}, dir,
		WithDispatcherObserver(obs), WithDispatcherLogger(logger)).
		HandleTask(context.Background(), task)

	if err == nil {
		t.Fatal("HandleTask() error = nil: a task whose execution is gone must not be a success")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("HandleTask() error = %v, want a permanent error so the transport archives it", err)
	}
	if IsTransient(err) {
		t.Fatalf("HandleTask() error = %v, must not be transient (retrying a gone execution is futile)", err)
	}
	if !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("HandleTask() error = %v, want it to still carry ErrExecutionInactive", err)
	}
	if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropExecutionGone {
		t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropExecutionGone)
	}
	if got := logger.entries; len(got) != 1 {
		t.Fatalf("log entries = %d, want 1 (the gone drop must be logged)", len(got))
	} else {
		if got[0].msg != dispatchDroppedLogMessage {
			t.Fatalf("log message = %q, want %q", got[0].msg, dispatchDroppedLogMessage)
		}
		if got[0].field("execution_id") != "exec-gone" {
			t.Fatalf("log execution_id = %v, want exec-gone", got[0].field("execution_id"))
		}
		if got[0].field("node_name") != "start" {
			t.Fatalf("log node_name = %v, want start", got[0].field("node_name"))
		}
	}
	assertNoAssignmentQueued(t, dir)
}

// TestDispatcherTerminalExecutionIsCountedButBenign pins deliverable (1a): a
// late delivery for an execution that already reached a terminal state is a
// benign drop — nil, not an error — but it is counted, not silent.
func TestDispatcherTerminalExecutionIsCountedButBenign(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-done", NodeName: "start", NodeIdx: 0}
	routingErr := &engine.ExecutionInactiveError{
		ExecutionID: "exec-done",
		Kind:        engine.ExecutionInactiveTerminal,
		Status:      types.ExecutionStatusCanceled,
	}
	dir := NewMemoryRunnerDirectory()
	obs := &fakeDispatcherObserver{}
	logger := &recordingLogger{}
	err := NewDispatcher(&fakeDispatchEngine{routingErr: routingErr}, dir,
		WithDispatcherObserver(obs), WithDispatcherLogger(logger)).
		HandleTask(context.Background(), task)

	if err != nil {
		t.Fatalf("HandleTask() error = %v, want nil for a terminal late delivery", err)
	}
	if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropExecutionTerminal {
		t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropExecutionTerminal)
	}
	if len(logger.entries) != 0 {
		t.Fatalf("log entries = %d, want 0: terminal late deliveries must not warn", len(logger.entries))
	}
	assertNoAssignmentQueued(t, dir)
}

// TestDispatcherClassifyErrorIsCountedAndRetried pins the failure mode a read
// error produces: counted as classify_error, returned unmarked so a
// retry-capable transport retries it, and never reported as data loss.
func TestDispatcherClassifyErrorIsCountedAndRetried(t *testing.T) {
	readErr := errors.New("redis: connection refused")
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	routingErr := &engine.ExecutionInactiveClassifyError{ExecutionID: "exec-1", Err: readErr}
	obs := &fakeDispatcherObserver{}

	err := NewDispatcher(&fakeDispatchEngine{routingErr: routingErr}, NewMemoryRunnerDirectory(),
		WithDispatcherObserver(obs)).
		HandleTask(context.Background(), task)

	if !errors.Is(err, readErr) {
		t.Fatalf("HandleTask() error = %v, want it to wrap the read error", err)
	}
	if types.IsPermanent(err) {
		t.Fatalf("HandleTask() error = %v, a classification read failure must stay retryable", err)
	}
	if IsTransient(err) {
		t.Fatalf("HandleTask() error = %v, a store failure is not dispatch backpressure", err)
	}
	if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropClassifyError {
		t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropClassifyError)
	}
}

// TestDispatcherGoneExecutionLogsAtMostOncePerInterval pins the rate limit on
// the lost-task log: the counter stays exact, the log is a bounded sample that
// carries how many records it suppressed.
func TestDispatcherGoneExecutionLogsAtMostOncePerInterval(t *testing.T) {
	routingErr := &engine.ExecutionInactiveError{ExecutionID: "exec-gone", Kind: engine.ExecutionInactiveGone}
	dir := NewMemoryRunnerDirectory()
	obs := &fakeDispatcherObserver{}
	logger := &recordingLogger{}
	dispatcher := NewDispatcher(&fakeDispatchEngine{routingErr: routingErr}, dir,
		WithDispatcherObserver(obs), WithDispatcherLogger(logger))

	task := &engine.Task{
		ExecutionID: "exec-gone",
		NodeName:    "start",
		NodeIdx:     0,
		Payload:     &types.SignalPayload{Name: "resume", Data: map[string]any{"credential": "secret-value"}},
	}
	for i := 0; i < 3; i++ {
		if err := dispatcher.HandleTask(context.Background(), task); err == nil {
			t.Fatal("HandleTask() error = nil, want permanent")
		}
	}
	if got := len(obs.dropped); got != 3 {
		t.Fatalf("drop count = %d, want 3 (the counter is exact)", got)
	}
	if got := len(logger.entries); got != 1 {
		t.Fatalf("log entries = %d, want 1 (rate-limited)", got)
	}
	// Move the throttle window into the past: the next drop logs again and
	// reports the two suppressed records.
	dispatcher.dropLogMu.Lock()
	dispatcher.dropLogLast = time.Now().Add(-2 * dispatcherDropLogInterval)
	dispatcher.dropLogMu.Unlock()
	if err := dispatcher.HandleTask(context.Background(), task); err == nil {
		t.Fatal("HandleTask() error = nil, want permanent")
	}
	if got := len(logger.entries); got != 2 {
		t.Fatalf("log entries = %d, want 2 after the interval elapsed", got)
	}
	if got := logger.entries[1].field("suppressed"); got != 2 {
		t.Fatalf("suppressed = %v, want 2", got)
	}
	for _, entry := range logger.entries {
		for _, arg := range entry.args {
			if s, ok := arg.(string); ok && s == "secret-value" {
				t.Fatal("log line leaked the task payload; resume payloads can carry credentials")
			}
		}
	}
}

// TestDispatcherGoneWithoutObserverOrLoggerDoesNotPanic pins the nil-safe
// options: detection still works, the disposition is still non-success.
func TestDispatcherGoneWithoutObserverOrLoggerDoesNotPanic(t *testing.T) {
	routingErr := &engine.ExecutionInactiveError{ExecutionID: "exec-gone", Kind: engine.ExecutionInactiveGone}
	err := NewDispatcher(&fakeDispatchEngine{routingErr: routingErr}, NewMemoryRunnerDirectory()).
		HandleTask(context.Background(), &engine.Task{ExecutionID: "exec-gone", NodeName: "start"})
	if !types.IsPermanent(err) {
		t.Fatalf("HandleTask() error = %v, want permanent", err)
	}
}

// TestDispatcherObservesDeliveryLag pins the pre-loss signal: a consumed task
// carrying its deliverable stamp reports how long it waited, and an unstamped
// task reports nothing rather than a fabricated age.
func TestDispatcherObservesDeliveryLag(t *testing.T) {
	obs := &fakeDispatcherObserver{}
	dispatcher := NewDispatcher(&fakeDispatchEngine{routing: engine.TaskRouting{NodeType: "xflow.function"}},
		NewMemoryRunnerDirectory(), WithDispatcherObserver(obs))

	task := &engine.Task{
		ExecutionID:   "exec-lag",
		NodeName:      "start",
		NodeIdx:       0,
		Type:          engine.TaskTypeNodeExec,
		DeliverableAt: time.Now().Add(-2 * time.Second),
	}
	if err := dispatcher.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if got := len(obs.lags); got != 1 {
		t.Fatalf("lag observations = %d, want 1", got)
	} else if obs.lags[0] < 1500*time.Millisecond || obs.lags[0] > 5*time.Second {
		t.Fatalf("lag = %v, want ~2s", obs.lags[0])
	}

	unstamped := &engine.Task{ExecutionID: "exec-lag", NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec}
	if err := dispatcher.HandleTask(context.Background(), unstamped); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if got := len(obs.lags); got != 1 {
		t.Fatalf("lag observations = %d, want 1 (no stamp, no observation)", got)
	}
}

func TestDispatcherPropagatesUnexpectedRoutingError(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	want := errors.New("boom")
	leaseEngine := &fakeDispatchEngine{routingErr: want}

	err := NewDispatcher(leaseEngine, NewMemoryRunnerDirectory()).HandleTask(context.Background(), task)
	if !errors.Is(err, want) {
		t.Fatalf("HandleTask() error = %v, want %v", err, want)
	}
}

// TestDispatcherUnattributedDropIsCountedButNotLoss pins the tri-state at the
// sink: a no-evidence drop whose wait exceeded the retention window is counted
// as execution_unattributed — not execution_gone — logged at warning level
// with its own message, and still never acked as a success.
func TestDispatcherUnattributedDropIsCountedButNotLoss(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-unattributed", NodeName: "start", NodeIdx: 0}
	routingErr := &engine.ExecutionInactiveError{
		ExecutionID: "exec-unattributed",
		Kind:        engine.ExecutionInactiveUnattributed,
	}
	dir := NewMemoryRunnerDirectory()
	obs := &fakeDispatcherObserver{}
	logger := &recordingLogger{}
	err := NewDispatcher(&fakeDispatchEngine{routingErr: routingErr}, dir,
		WithDispatcherObserver(obs), WithDispatcherLogger(logger)).
		HandleTask(context.Background(), task)

	if err == nil {
		t.Fatal("HandleTask() error = nil: an unattributable drop must not be acked as a success")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("HandleTask() error = %v, want permanent so the transport archives instead of retrying", err)
	}
	if IsTransient(err) {
		t.Fatalf("HandleTask() error = %v, must not be transient", err)
	}
	if !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("HandleTask() error = %v, want it to carry ErrExecutionInactive", err)
	}
	if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropExecutionUnattributed {
		t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropExecutionUnattributed)
	}
	if got := logger.entries; len(got) != 1 {
		t.Fatalf("log entries = %d, want 1 (the unattributed drop must be visible)", len(got))
	} else {
		if got[0].msg != dispatchUnattributedLogMessage {
			t.Fatalf("log message = %q, want %q", got[0].msg, dispatchUnattributedLogMessage)
		}
		if got[0].level != "warn" {
			t.Fatalf("log level = %q, want warn: this bucket holds benign late duplicates too, "+
				"so it must not read as the lost-task error line", got[0].level)
		}
		if got[0].field("reason") != dispatchDropExecutionUnattributed {
			t.Fatalf("log reason = %v, want %s", got[0].field("reason"), dispatchDropExecutionUnattributed)
		}
	}
	assertNoAssignmentQueued(t, dir)
}

// fakeSystemTaskEngine is a Router whose system-task handler is scripted, so a
// test can drive HandleTask's system-task branch without a real engine.
type fakeSystemTaskEngine struct {
	*fakeDispatchEngine
	handled bool
	err     error
}

func (e *fakeSystemTaskEngine) HandleSystemTask(context.Context, *engine.Task) (bool, error) {
	return e.handled, e.err
}

// TestDispatcherSystemTaskDropIsClassified pins the second half of the silent
// path: an advance/skip intent the engine owns but could not process must take
// the same classification as a routing drop (counted, logged, not a success),
// while a genuinely processed system task ((true, nil)) stays a success and is
// never counted. The two (true, ...) shapes must not be conflated.
func TestDispatcherSystemTaskDropIsClassified(t *testing.T) {
	routingErr := errors.New("routing must not be consulted for a handled system task")
	dropErr := &engine.ExecutionInactiveError{
		ExecutionID: "exec-sys-drop",
		Kind:        engine.ExecutionInactiveGone,
	}

	t.Run("inactive execution is a counted drop", func(t *testing.T) {
		eng := &fakeSystemTaskEngine{
			fakeDispatchEngine: &fakeDispatchEngine{routingErr: routingErr},
			handled:            true,
			err:                dropErr,
		}
		dir := NewMemoryRunnerDirectory()
		obs := &fakeDispatcherObserver{}
		logger := &recordingLogger{}
		err := NewDispatcher(eng, dir, WithDispatcherObserver(obs), WithDispatcherLogger(logger)).
			HandleTask(context.Background(), &engine.Task{
				ExecutionID: "exec-sys-drop", NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeAdvance,
			})

		if err == nil {
			t.Fatal("HandleTask() error = nil: a system task for an inactive execution must not be a success")
		}
		if !types.IsPermanent(err) {
			t.Fatalf("HandleTask() error = %v, want permanent", err)
		}
		if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropExecutionGone {
			t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropExecutionGone)
		}
		if got := logger.entries; len(got) != 1 {
			t.Fatalf("log entries = %d, want 1", len(got))
		}
		assertNoAssignmentQueued(t, dir)
	})

	t.Run("processed system task stays a silent success", func(t *testing.T) {
		eng := &fakeSystemTaskEngine{
			fakeDispatchEngine: &fakeDispatchEngine{routingErr: routingErr},
			handled:            true,
		}
		obs := &fakeDispatcherObserver{}
		logger := &recordingLogger{}
		err := NewDispatcher(eng, NewMemoryRunnerDirectory(),
			WithDispatcherObserver(obs), WithDispatcherLogger(logger)).
			HandleTask(context.Background(), &engine.Task{
				ExecutionID: "exec-sys-ok", NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeAdvance,
			})

		if err != nil {
			t.Fatalf("HandleTask() error = %v, want nil for a system task that was actually processed", err)
		}
		if len(obs.dropped) != 0 {
			t.Fatalf("drop observer reasons = %v, want none: a processed system task is not a drop", obs.dropped)
		}
		if len(logger.entries) != 0 {
			t.Fatalf("log entries = %d, want 0", len(logger.entries))
		}
	})

	t.Run("non-inactive system task error propagates unchanged", func(t *testing.T) {
		want := errors.New("advance commit failed")
		eng := &fakeSystemTaskEngine{
			fakeDispatchEngine: &fakeDispatchEngine{routingErr: routingErr},
			handled:            true,
			err:                want,
		}
		obs := &fakeDispatcherObserver{}
		err := NewDispatcher(eng, NewMemoryRunnerDirectory(), WithDispatcherObserver(obs)).
			HandleTask(context.Background(), &engine.Task{
				ExecutionID: "exec-sys-err", NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeAdvance,
			})

		if !errors.Is(err, want) {
			t.Fatalf("HandleTask() error = %v, want the original failure %v", err, want)
		}
		if len(obs.dropped) != 0 {
			t.Fatalf("drop observer reasons = %v, want none: a real processing failure is not a drop", obs.dropped)
		}
	})
}

// A batch the directory refuses as a duplicate is still acked, but counted:
// a refused batch that was not a redelivery leaves its barrier short.
func TestDispatcherCountsDuplicateBatchEnqueue(t *testing.T) {
	ctx := context.Background()
	obs := &fakeDispatcherObserver{}
	dispatcher := NewDispatcher(&fakeDispatchEngine{routing: engine.TaskRouting{NodeType: "xflow.function"}},
		NewMemoryRunnerDirectory(), WithDispatcherObserver(obs))
	batch := mapBatchTask("L1", 0)
	node := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", Type: engine.TaskTypeNodeExec, ActivationID: 1}
	for _, task := range []*engine.Task{&batch, &batch, node, node} {
		if err := dispatcher.HandleTask(ctx, task); err != nil {
			t.Fatalf("HandleTask(%s) error = %v", task.NodeName, err)
		}
	}
	if got := obs.dropped; len(got) != 1 || got[0] != dispatchDropBatchDuplicate {
		t.Fatalf("drop observer reasons = %v, want [%s]", got, dispatchDropBatchDuplicate)
	}
}
