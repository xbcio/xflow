package trigger

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// --- test infrastructure for trigger-group mode ---

// mockTriggerGroupRuntime implements types.TriggerGroupRuntime for unit tests.
type mockTriggerGroupRuntime struct {
	mu           sync.Mutex
	calls        []types.TriggerGroupAdmissionRequest
	response     types.TriggerGroupAdmissionResponse
	err          error
	callCount    atomic.Int32
	blockUntil   chan struct{} // if non-nil, blocks until closed
	failOnce     sync.Once
	failFirstErr error // if set, first call returns this error
}

func (m *mockTriggerGroupRuntime) SeedTriggeredGroupResult(ctx context.Context, req types.TriggerGroupAdmissionRequest) (types.TriggerGroupAdmissionResponse, error) {
	m.callCount.Add(1)
	if m.blockUntil != nil {
		select {
		case <-m.blockUntil:
		case <-ctx.Done():
			return types.TriggerGroupAdmissionResponse{}, ctx.Err()
		}
	}
	if m.failFirstErr != nil {
		var shouldFail bool
		m.failOnce.Do(func() { shouldFail = true })
		if shouldFail {
			return types.TriggerGroupAdmissionResponse{}, m.failFirstErr
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, req)
	m.mu.Unlock()
	if m.err != nil {
		return types.TriggerGroupAdmissionResponse{}, m.err
	}
	return m.response, nil
}

func (m *mockTriggerGroupRuntime) getCalls() []types.TriggerGroupAdmissionRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]types.TriggerGroupAdmissionRequest, len(m.calls))
	copy(cp, m.calls)
	return cp
}

// commitRecordingConsumer wraps a KafkaConsumer and records all commits.
type commitRecordingConsumer struct {
	inner      KafkaConsumer
	commits    [][]KafkaMessage
	mu         sync.Mutex
	commitErr  error // if set, CommitMessages returns this
	commitOnce sync.Once
	failFirst  error // first commit fails, rest succeed
}

func (c *commitRecordingConsumer) Messages() <-chan KafkaMessage { return c.inner.Messages() }
func (c *commitRecordingConsumer) Close() error                 { return c.inner.Close() }
func (c *commitRecordingConsumer) CommitMessages(_ context.Context, msgs ...KafkaMessage) error {
	if c.failFirst != nil {
		var shouldFail bool
		c.commitOnce.Do(func() { shouldFail = true })
		if shouldFail {
			return c.failFirst
		}
	}
	if c.commitErr != nil {
		return c.commitErr
	}
	c.mu.Lock()
	c.commits = append(c.commits, msgs)
	c.mu.Unlock()
	return nil
}

func (c *commitRecordingConsumer) getCommits() [][]KafkaMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([][]KafkaMessage, len(c.commits))
	copy(cp, c.commits)
	return cp
}

// scriptedConsumer sends messages from a pre-defined slice, one at a time.
type scriptedConsumer struct {
	ch     chan KafkaMessage
	closed atomic.Bool
}

func newScriptedConsumer(msgs []KafkaMessage) *scriptedConsumer {
	ch := make(chan KafkaMessage, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	return &scriptedConsumer{ch: ch}
}

func (s *scriptedConsumer) Messages() <-chan KafkaMessage { return s.ch }
func (s *scriptedConsumer) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
	return nil
}

// --- trigger-group mode tests ---

// TestKafkaTriggerGroup_AdmissionAccepted_CommitsOffset verifies that when
// SeedTriggeredGroupResult returns accepted, the Kafka offset is committed.
func TestKafkaTriggerGroup_AdmissionAccepted_CommitsOffset(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 100, Value: []byte("hello")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockTriggerGroupRuntime{
		response: types.TriggerGroupAdmissionResponse{Accepted: true, ExecutionID: "exec-1"},
	}

	rt := &triggerGroupTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"group_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := emitKafkaTriggerGroupMessage(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("emitKafkaTriggerGroupMessage returned false, want true")
	}

	commits := recorder.getCommits()
	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1", len(commits))
	}
	if commits[0][0].Offset != 100 {
		t.Fatalf("committed offset = %d, want 100", commits[0][0].Offset)
	}
	if admitter.callCount.Load() != 1 {
		t.Fatalf("admission calls = %d, want 1", admitter.callCount.Load())
	}
}

// TestKafkaTriggerGroup_AdmissionError_NoCommit verifies that when
// SeedTriggeredGroupResult returns a transient error, the offset is NOT
// committed — allowing Kafka redelivery.
func TestKafkaTriggerGroup_AdmissionError_NoCommit(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 200, Value: []byte("world")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockTriggerGroupRuntime{
		err: context.DeadlineExceeded, // transient network timeout
	}

	rt := &triggerGroupTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"group_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := emitKafkaTriggerGroupMessage(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("emitKafkaTriggerGroupMessage returned true, want false (transient error)")
	}

	commits := recorder.getCommits()
	if len(commits) != 0 {
		t.Fatalf("commit count = %d, want 0 (no commit on transient error)", len(commits))
	}
}

// TestKafkaTriggerGroup_DuplicateAccepted_CommitsOffset verifies that a
// duplicate-accepted response (same key, same hash replayed) still commits the
// Kafka offset — this is the idempotent recovery path.
func TestKafkaTriggerGroup_DuplicateAccepted_CommitsOffset(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 300, Value: []byte("dup")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockTriggerGroupRuntime{
		response: types.TriggerGroupAdmissionResponse{Accepted: true, Duplicate: true, ExecutionID: "exec-dup"},
	}

	rt := &triggerGroupTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"group_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := emitKafkaTriggerGroupMessage(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("emitKafkaTriggerGroupMessage returned false, want true (duplicate accepted)")
	}

	commits := recorder.getCommits()
	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1", len(commits))
	}
}

// TestKafkaTriggerGroup_Conflict_CommitsOffset verifies that a conflict response
// (same key, different hash — another runner won) still commits the Kafka offset
// since the admission was already handled by the winning runner.
func TestKafkaTriggerGroup_Conflict_CommitsOffset(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 400, Value: []byte("conflict")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockTriggerGroupRuntime{
		response: types.TriggerGroupAdmissionResponse{Conflict: true, ExecutionID: "exec-other"},
	}

	rt := &triggerGroupTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"group_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := emitKafkaTriggerGroupMessage(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("emitKafkaTriggerGroupMessage returned false, want true (conflict = admission handled)")
	}

	commits := recorder.getCommits()
	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1 (conflict still commits offset)", len(commits))
	}
}

// TestKafkaTriggerGroup_CommitFailure_Safe verifies that when admission
// succeeds but the Kafka commit fails, the message will be redelivered and
// the admission returns duplicate-accepted (safe, no data loss).
func TestKafkaTriggerGroup_CommitFailure_Safe(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 500, Value: []byte("commit-fail")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{
		inner:     consumer,
		failFirst: context.DeadlineExceeded,
	}

	admitter := &mockTriggerGroupRuntime{
		response: types.TriggerGroupAdmissionResponse{Accepted: true, ExecutionID: "exec-cf"},
	}

	rt := &triggerGroupTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"group_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	// First call: admission succeeds, commit fails → returns false (message redelivered).
	ok := emitKafkaTriggerGroupMessage(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("first call should return false when commit fails")
	}

	// Simulate redelivery: admission returns duplicate, commit succeeds.
	admitter.response = types.TriggerGroupAdmissionResponse{Accepted: true, Duplicate: true, ExecutionID: "exec-cf"}
	ok = emitKafkaTriggerGroupMessage(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("second call should succeed (duplicate accepted + commit succeeds)")
	}

	// Verify admission was called twice (once per delivery).
	if admitter.callCount.Load() != 2 {
		t.Fatalf("admission calls = %d, want 2", admitter.callCount.Load())
	}
}

// --- trigger-group test runtime ---

type triggerGroupTestRuntime struct {
	admitter types.TriggerGroupRuntime
	dedup    func(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

func (r *triggerGroupTestRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
	return "", nil
}
func (r *triggerGroupTestRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if r.dedup != nil {
		return r.dedup(ctx, key, ttl)
	}
	return true, nil
}
func (r *triggerGroupTestRuntime) TryLock(_ context.Context, _ string, _ time.Duration) (types.TriggerLock, bool, error) {
	return nil, false, nil
}
func (r *triggerGroupTestRuntime) State(_ context.Context, _ string) types.TriggerState { return nil }

// SeedTriggeredGroupResult delegates to the embedded admitter.
func (r *triggerGroupTestRuntime) SeedTriggeredGroupResult(ctx context.Context, req types.TriggerGroupAdmissionRequest) (types.TriggerGroupAdmissionResponse, error) {
	return r.admitter.SeedTriggeredGroupResult(ctx, req)
}
