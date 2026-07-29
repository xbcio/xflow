package trigger

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// --- test infrastructure for trigger-group mode ---

// mockEntrySeedRuntime implements types.EntrySeedRuntime for unit tests.
type mockEntrySeedRuntime struct {
	mu           sync.Mutex
	calls        []types.EntrySeedRequest
	response     types.EntrySeedResponse
	err          error
	callCount    atomic.Int32
	blockUntil   chan struct{} // if non-nil, blocks until closed
	failOnce     sync.Once
	failFirstErr error // if set, first call returns this error
}

func (m *mockEntrySeedRuntime) SeedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	m.callCount.Add(1)
	if m.blockUntil != nil {
		select {
		case <-m.blockUntil:
		case <-ctx.Done():
			return types.EntrySeedResponse{}, ctx.Err()
		}
	}
	if m.failFirstErr != nil {
		var shouldFail bool
		m.failOnce.Do(func() { shouldFail = true })
		if shouldFail {
			return types.EntrySeedResponse{}, m.failFirstErr
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, req)
	m.mu.Unlock()
	if m.err != nil {
		return types.EntrySeedResponse{}, m.err
	}
	return m.response, nil
}

func (m *mockEntrySeedRuntime) getCalls() []types.EntrySeedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]types.EntrySeedRequest, len(m.calls))
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
func (c *commitRecordingConsumer) Close() error                  { return c.inner.Close() }
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

// TestKafkaEntrySeed_AdmissionAccepted_CommitsOffset verifies that when
// SeedExecutionFromEntry returns accepted, the Kafka offset is committed.
func TestKafkaEntrySeed_AdmissionAccepted_CommitsOffset(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 100, Value: []byte("hello")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockEntrySeedRuntime{
		response: types.EntrySeedResponse{Accepted: true, ExecutionID: "exec-1"},
	}

	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("seedKafkaEntryBatch returned false, want true")
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

// TestKafkaEntrySeed_AdmissionError_NoCommit verifies that when
// SeedExecutionFromEntry returns a transient error, the offset is NOT
// committed — allowing Kafka redelivery.
func TestKafkaEntrySeed_AdmissionError_NoCommit(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 200, Value: []byte("world")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockEntrySeedRuntime{
		err: context.DeadlineExceeded, // transient network timeout
	}

	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("seedKafkaEntryBatch returned true, want false (transient error)")
	}

	commits := recorder.getCommits()
	if len(commits) != 0 {
		t.Fatalf("commit count = %d, want 0 (no commit on transient error)", len(commits))
	}
}

// TestKafkaEntrySeed_DuplicateAccepted_CommitsOffset verifies that a
// duplicate-accepted response (same key, same hash replayed) still commits the
// Kafka offset — this is the idempotent recovery path.
func TestKafkaEntrySeed_DuplicateAccepted_CommitsOffset(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 300, Value: []byte("dup")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockEntrySeedRuntime{
		response: types.EntrySeedResponse{Accepted: true, Duplicate: true, ExecutionID: "exec-dup"},
	}

	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("seedKafkaEntryBatch returned false, want true (duplicate accepted)")
	}

	commits := recorder.getCommits()
	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1", len(commits))
	}
}

// TestKafkaEntrySeed_Conflict_CommitsOffset verifies that a conflict response
// (same key, different hash — another runner won) still commits the Kafka offset
// since the admission was already handled by the winning runner.
func TestKafkaEntrySeed_Conflict_CommitsOffset(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 400, Value: []byte("conflict")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockEntrySeedRuntime{
		response: types.EntrySeedResponse{Conflict: true, ExecutionID: "exec-other"},
	}

	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("seedKafkaEntryBatch returned false, want true (conflict = admission handled)")
	}

	commits := recorder.getCommits()
	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1 (conflict still commits offset)", len(commits))
	}
}

// TestKafkaEntrySeed_StaleGeneration409_NoCommit is the end-to-end offset-safety
// guard: it wires the REAL HTTPEntrySeedRuntime against a control plane that
// returns 409 {"error":"stale_generation"} (a generation fence rejection for a
// NEW admission key), and asserts seedKafkaEntryBatch does NOT commit the Kafka
// offset. If it committed, Kafka would never redeliver and the current-generation
// owner would never process the message — silent message loss during a
// generation upgrade / reassignment.
func TestKafkaEntrySeed_StaleGeneration409_NoCommit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mimic apiserver writeError(w, 409, "stale_generation").
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"stale_generation"}`))
	}))
	defer srv.Close()

	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 700, Value: []byte("stale")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	// A superseded runtime at an old generation talking to the real endpoint.
	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client(), Generation: 1}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	ok := seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("seedKafkaEntryBatch returned true, want false (stale-generation fence must not commit)")
	}
	if commits := recorder.getCommits(); len(commits) != 0 {
		t.Fatalf("commit count = %d, want 0 (stale-generation fence must NOT commit offset)", len(commits))
	}
}

// TestKafkaEntrySeed_CommitFailure_Safe verifies that when admission
// succeeds but the Kafka commit fails, the message will be redelivered and
// the admission returns duplicate-accepted (safe, no data loss).
func TestKafkaEntrySeed_CommitFailure_Safe(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 500, Value: []byte("commit-fail")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{
		inner:     consumer,
		failFirst: context.DeadlineExceeded,
	}

	admitter := &mockEntrySeedRuntime{
		response: types.EntrySeedResponse{Accepted: true, ExecutionID: "exec-cf"},
	}

	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	// First call: admission succeeds, commit fails → returns false (message redelivered).
	ok := seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("first call should return false when commit fails")
	}

	// Simulate redelivery: admission returns duplicate, commit succeeds.
	admitter.response = types.EntrySeedResponse{Accepted: true, Duplicate: true, ExecutionID: "exec-cf"}
	ok = seedKafkaEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("second call should succeed (duplicate accepted + commit succeeds)")
	}

	// Verify admission was called twice (once per delivery).
	if admitter.callCount.Load() != 2 {
		t.Fatalf("admission calls = %d, want 2", admitter.callCount.Load())
	}
}

// TestEntrySeedDispatch_PerMessageWorker verifies that when the runtime supports
// EntrySeedRuntime and the trigger is configured with entry_seed=true, a
// delivered message drives SeedExecutionFromEntry (not Emit) and the offset is
// committed only after an accepted response.
func TestEntrySeedDispatch_PerMessageWorker(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 700, Value: []byte("seed-me")},
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockEntrySeedRuntime{
		response: types.EntrySeedResponse{Accepted: true, ExecutionID: "exec-seed"},
	}
	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(ctx context.Context, key string, ttl time.Duration) (bool, error) { return true, nil },
	}

	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_seed": true, "entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	cfg := KafkaConsumerConfig{MaxInflight: 4}
	sub := activateKafkaPerMessage(context.Background(), in, cfg, recorder)
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	// Wait for the message to be admitted + committed.
	deadline := time.After(2 * time.Second)
	for {
		if len(recorder.getCommits()) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for commit; admission calls=%d commits=%d",
				admitter.callCount.Load(), len(recorder.getCommits()))
		case <-time.After(5 * time.Millisecond):
		}
	}

	if admitter.callCount.Load() != 1 {
		t.Fatalf("admission calls = %d, want 1 (should drive SeedExecutionFromEntry)", admitter.callCount.Load())
	}
	if got := rt.emitCount.Load(); got != 0 {
		t.Fatalf("Emit calls = %d, want 0 (entry-seed must not use legacy Emit path)", got)
	}
	commits := recorder.getCommits()
	if len(commits) != 1 || commits[0][0].Offset != 700 {
		t.Fatalf("commits = %+v, want single commit at offset 700", commits)
	}
}

// durableSeedAdmitter models the control-plane admission store: it keeps a
// durable record keyed by AdmissionKey. The first admission for a key is
// Accepted (a new durable execution seed); every subsequent admission for the
// same key is Duplicate (accepted, but no new seed). This is the offset-safe
// invariant used to prove no-loss under crash-between-accept-and-commit.
type durableSeedAdmitter struct {
	mu        sync.Mutex
	seeds     map[string]types.ExecutionID // admissionKey -> execID (durable)
	lastReq   types.EntrySeedRequest
	callCount atomic.Int32
}

func newDurableSeedAdmitter() *durableSeedAdmitter {
	return &durableSeedAdmitter{seeds: map[string]types.ExecutionID{}}
}

func (d *durableSeedAdmitter) SeedExecutionFromEntry(_ context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	d.callCount.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastReq = req
	if execID, ok := d.seeds[req.AdmissionKey]; ok {
		// Already durable — idempotent replay.
		return types.EntrySeedResponse{Accepted: true, Duplicate: true, ExecutionID: execID}, nil
	}
	execID := types.ExecutionID("exec-" + req.AdmissionKey)
	d.seeds[req.AdmissionKey] = execID
	return types.EntrySeedResponse{Accepted: true, ExecutionID: execID}, nil
}

func (d *durableSeedAdmitter) seedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seeds)
}

func (d *durableSeedAdmitter) last() types.EntrySeedRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastReq
}

// TestKafkaSingleNodeSeedNoLoss proves the P0-1 fix for a single-node trigger:
// a crash between a durable accept and the Kafka commit does NOT lose the event.
// First delivery: admission accepts (durable seed) but the committer errors →
// seedKafkaEntryBatch returns false and the offset is NOT committed, so Kafka
// redelivers. Redelivery of the same offset returns Duplicate (no new seed) and
// commits. Exactly one durable execution seed exists; no event is lost.
//
// The trigger is a single node: no entry_unit_id param, so the entry unit ID
// falls back to the node name (spec §11.5). The single BoundaryExit carries the
// message data on port "main", and the admission key is derived from
// topic/partition/offset (BuildAdmissionKeySingle semantics).
func TestKafkaSingleNodeSeedNoLoss(t *testing.T) {
	msg := KafkaMessage{Topic: "orders", Partition: 2, Offset: 900, Key: []byte("k9"), Value: []byte("payload")}
	consumer := newScriptedConsumer([]KafkaMessage{msg})
	// First commit fails (crash between accept and commit); redelivery commit succeeds.
	recorder := &commitRecordingConsumer{inner: consumer, failFirst: context.DeadlineExceeded}

	admitter := newDurableSeedAdmitter()
	rt := &entrySeedTestRuntime{admitter: admitter}

	// Single node: NO entry_unit_id param — entry unit ID must fall back to NodeName.
	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "single-node",
		Params:     map[string]any{"workflow_version": "v1"},
		Runtime:    rt,
	}

	// First delivery: accept durable, commit fails → false, offset NOT committed.
	if ok := seedKafkaEntryBatch(context.Background(), in, recorder, msg); ok {
		t.Fatal("first delivery: seedKafkaEntryBatch returned true, want false (commit failed → redeliver)")
	}
	if got := len(recorder.getCommits()); got != 0 {
		t.Fatalf("first delivery: commits = %d, want 0 (offset must NOT be committed)", got)
	}

	// Redelivery of the SAME offset: admission is Duplicate, commit succeeds → true.
	if ok := seedKafkaEntryBatch(context.Background(), in, recorder, msg); !ok {
		t.Fatal("redelivery: seedKafkaEntryBatch returned false, want true (duplicate accepted + commit)")
	}

	// Exactly one durable execution seed — no loss, no double-seed.
	if got := admitter.seedCount(); got != 1 {
		t.Fatalf("durable execution seeds = %d, want exactly 1", got)
	}
	// The offset was committed exactly once (on redelivery).
	commits := recorder.getCommits()
	if len(commits) != 1 || commits[0][0].Offset != 900 {
		t.Fatalf("commits = %+v, want single commit at offset 900", commits)
	}
	// Admission attempted twice (once per delivery).
	if got := admitter.callCount.Load(); got != 2 {
		t.Fatalf("admission calls = %d, want 2", got)
	}

	// Single-node specifics: entry unit ID = node name; one exit on port "main".
	req := admitter.last()
	if req.EntryUnitID != "single-node" {
		t.Fatalf("EntryUnitID = %q, want node name %q (single-node fallback)", req.EntryUnitID, "single-node")
	}
	if len(req.Exits) != 1 {
		t.Fatalf("exits = %d, want 1 (cardinality-1 seed)", len(req.Exits))
	}
	exit := req.Exits[0]
	if exit.NodeName != "single-node" || exit.Port != "main" {
		t.Fatalf("exit = %+v, want NodeName=single-node Port=main", exit)
	}
	if exit.Data["offset"] != int64(900) || exit.Data["value"] != "payload" {
		t.Fatalf("exit data = %+v, want offset=900 value=payload", exit.Data)
	}
	// Admission key must derive from topic/partition/offset so redelivery collides.
	if req.AdmissionKey == "" {
		t.Fatal("admission key is empty, want topic/partition/offset-derived key")
	}
}

// --- trigger-group test runtime ---
type entrySeedTestRuntime struct {
	admitter  types.EntrySeedRuntime
	dedup     func(ctx context.Context, key string, ttl time.Duration) (bool, error)
	emitCount atomic.Int32
}

func (r *entrySeedTestRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
	r.emitCount.Add(1)
	return "", nil
}
func (r *entrySeedTestRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if r.dedup != nil {
		return r.dedup(ctx, key, ttl)
	}
	return true, nil
}
func (r *entrySeedTestRuntime) TryLock(_ context.Context, _ string, _ time.Duration) (types.TriggerLock, bool, error) {
	return nil, false, nil
}
func (r *entrySeedTestRuntime) State(_ context.Context, _ string) types.TriggerState { return nil }

// SeedExecutionFromEntry delegates to the embedded admitter.
func (r *entrySeedTestRuntime) SeedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	return r.admitter.SeedExecutionFromEntry(ctx, req)
}
