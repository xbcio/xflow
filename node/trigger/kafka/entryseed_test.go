package kafka

import (
	"context"
	"reflect"
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

// commitRecordingConsumer wraps a Consumer and records all commits.
type commitRecordingConsumer struct {
	inner      Consumer
	commits    [][]Message
	mu         sync.Mutex
	commitErr  error // if set, CommitMessages returns this
	commitOnce sync.Once
	failFirst  error // first commit fails, rest succeed
}

func (c *commitRecordingConsumer) Messages() <-chan Message { return c.inner.Messages() }
func (c *commitRecordingConsumer) Close() error             { return c.inner.Close() }
func (c *commitRecordingConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
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

func (c *commitRecordingConsumer) getCommits() [][]Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([][]Message, len(c.commits))
	copy(cp, c.commits)
	return cp
}

// scriptedConsumer sends messages from a pre-defined slice, one at a time.
type scriptedConsumer struct {
	ch     chan Message
	closed atomic.Bool
}

func newScriptedConsumer(msgs []Message) *scriptedConsumer {
	ch := make(chan Message, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	return &scriptedConsumer{ch: ch}
}

func (s *scriptedConsumer) Messages() <-chan Message { return s.ch }
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
	msgs := []Message{
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

	ok := seedEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("seedEntryBatch returned false, want true")
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

// NOTE: the companion case that drives a REAL HTTPEntrySeedRuntime against a
// 409 {"error":"stale_generation"} control plane lives in
// test/integration/kafka_entry_seed_fence_real_test.go — it cannot live here
// because HTTPEntrySeedRuntime is in service/protocol and node must not import
// service/. This test covers only the withhold half (admission error -> no
// commit); the integration case covers that a stale-generation 409 maps to an
// error rather than to Conflict.
// TestKafkaEntrySeed_AdmissionError_NoCommit verifies that when
// SeedExecutionFromEntry returns a transient error, the offset is NOT
// committed — allowing Kafka redelivery.
func TestKafkaEntrySeed_AdmissionError_NoCommit(t *testing.T) {
	msgs := []Message{
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

	ok := seedEntryBatch(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("seedEntryBatch returned true, want false (transient error)")
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
	msgs := []Message{
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

	ok := seedEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("seedEntryBatch returned false, want true (duplicate accepted)")
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
	msgs := []Message{
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

	ok := seedEntryBatch(context.Background(), in, recorder, msgs[0])
	if !ok {
		t.Fatal("seedEntryBatch returned false, want true (conflict = admission handled)")
	}

	commits := recorder.getCommits()
	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1 (conflict still commits offset)", len(commits))
	}
}

// TestKafkaEntrySeed_CommitFailure_Safe verifies that when admission
// succeeds but the Kafka commit fails, the message will be redelivered and
// the admission returns duplicate-accepted (safe, no data loss).
func TestKafkaEntrySeed_CommitFailure_Safe(t *testing.T) {
	msgs := []Message{
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
	ok := seedEntryBatch(context.Background(), in, recorder, msgs[0])
	if ok {
		t.Fatal("first call should return false when commit fails")
	}

	// Simulate redelivery: admission returns duplicate, commit succeeds.
	admitter.response = types.EntrySeedResponse{Accepted: true, Duplicate: true, ExecutionID: "exec-cf"}
	ok = seedEntryBatch(context.Background(), in, recorder, msgs[0])
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
	msgs := []Message{
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

	cfg := ConsumerConfig{MaxInflight: 4}
	sub := activatePerMessage(context.Background(), in, cfg, recorder, nil)
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
// seedEntryBatch returns false and the offset is NOT committed, so Kafka
// redelivers. Redelivery of the same offset returns Duplicate (no new seed) and
// commits. Exactly one durable execution seed exists; no event is lost.
//
// The trigger is a single node: no entry_unit_id param, so the entry unit ID
// falls back to the node name (spec §11.5). The single BoundaryExit carries the
// message data on port "main", and the admission key is derived from
// topic/partition/offset (BuildAdmissionKeySingle semantics).
func TestKafkaSingleNodeSeedNoLoss(t *testing.T) {
	msg := Message{Topic: "orders", Partition: 2, Offset: 900, Key: []byte("k9"), Value: []byte("payload")}
	consumer := newScriptedConsumer([]Message{msg})
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
	if ok := seedEntryBatch(context.Background(), in, recorder, msg); ok {
		t.Fatal("first delivery: seedEntryBatch returned true, want false (commit failed → redeliver)")
	}
	if got := len(recorder.getCommits()); got != 0 {
		t.Fatalf("first delivery: commits = %d, want 0 (offset must NOT be committed)", got)
	}

	// Redelivery of the SAME offset: admission is Duplicate, commit succeeds → true.
	if ok := seedEntryBatch(context.Background(), in, recorder, msg); !ok {
		t.Fatal("redelivery: seedEntryBatch returned false, want true (duplicate accepted + commit)")
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
	// The admission key is what makes redelivery collide, so it has to be the
	// exact key — not merely non-empty. "AdmissionKey != \"\"" could not fail:
	// the key is built with fmt.Sprintf from a format string containing literal
	// "/" and "-" separators, so every possible combination of inputs, including
	// all-empty and all-zero, produces a non-empty string. Dropping the
	// partition, or swapping partition and offset, left that check green while
	// silently changing which messages collide.
	//
	// Spelled out as a literal rather than re-running production's Sprintf: a
	// test that mirrors the format call changes shape together with the bug.
	// Fields in order: namespace (server-side, empty) / workflow / version /
	// entry unit / topic / partition / offset-offset.
	const wantKey = "/wf1/v1/single-node/orders/2/900-900"
	if req.AdmissionKey != wantKey {
		t.Fatalf("admission key = %q, want %q (topic/partition/offset-derived)",
			req.AdmissionKey, wantKey)
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

// --- concurrency refusal on the per-record path ---

// refusingAdmitter refuses the first refuseFor admissions and accepts every one
// after that, standing in for a REJECT-mode workflow whose slot frees.
//
// It records the arrival time and admission key of every attempt, which is what
// lets a test read the retry cadence and prove a retry is a retry of the SAME
// record rather than a fresh admission.
//
// onCall, when set, runs inside the admission call before the response is
// returned, so a test can assert on the world as it stands at that attempt.
// refuseFor and onCall are read-only once the activation starts.
type refusingAdmitter struct {
	refuseFor int32
	onCall    func(attempt int)
	calls     atomic.Int32

	mu   sync.Mutex
	at   []time.Time
	keys []string
}

func (a *refusingAdmitter) SeedExecutionFromEntry(_ context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	n := int(a.calls.Add(1))
	a.mu.Lock()
	a.at = append(a.at, time.Now())
	a.keys = append(a.keys, req.AdmissionKey)
	a.mu.Unlock()
	if a.onCall != nil {
		a.onCall(n)
	}
	if int32(n) <= a.refuseFor {
		return types.EntrySeedResponse{Refused: true}, nil
	}
	return types.EntrySeedResponse{Accepted: true, ExecutionID: "exec-admitted"}, nil
}

func (a *refusingAdmitter) snapshot() ([]time.Time, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]time.Time(nil), a.at...), append([]string(nil), a.keys...)
}

// TestKafkaEntrySeed_RefusedRecordIsRetriedNotSkipped is the offset half of the
// concurrency refusal on the per-record path, and the property the retry loop
// exists for.
//
// This path is the DEFAULT entry-seed shape — aggregation is opt-in — and its
// worker is serial per partition. Returning on a refusal would let the worker
// take the NEXT record, and that record's commit would advance the partition's
// offset past one nothing persisted: Kafka redelivers strictly past the
// committed offset, so nothing would ever bring the refused record back. The
// test drives two records through a real per-message activation, has the
// control plane refuse the first record twice, and asserts that
//
//   - the refused record is attempted again: three admissions for offset 800,
//     two refusals then an acceptance;
//   - every one of those attempts carries the identical admission key, so the
//     retry is that record's retry and not a new admission;
//   - nothing is committed at all while the refusal stands, so no commit can
//     have skipped the held record;
//   - the record behind it is admitted and committed only afterwards, in order
//     (800 then 801); and
//   - the wait between attempts is the batch path's backoff rather than a spin,
//     and the partition reports itself blocked for exactly that window.
func TestKafkaEntrySeed_RefusedRecordIsRetriedNotSkipped(t *testing.T) {
	msgs := []Message{
		{Topic: "orders", Partition: 3, Offset: 800, Value: []byte("held")},
		{Topic: "orders", Partition: 3, Offset: 801, Value: []byte("behind")},
	}
	consumer := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}

	admitter := &refusingAdmitter{refuseFor: 2}
	admitter.onCall = func(attempt int) {
		// The first three attempts are the held record's (two refusals and the
		// acceptance). A commit visible at any of them would mean an offset
		// advanced while the record in front of it was still unpersisted.
		if attempt <= 3 {
			if commits := consumer.getCommits(); len(commits) != 0 {
				t.Errorf("attempt %d: %v committed while offset 800 was still held; "+
					"a commit behind a held record skips it permanently", attempt, commits)
			}
		}
	}

	rt := &entrySeedTestRuntime{admitter: admitter}
	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	o := &recordingBatchObserver{}
	SetObserver(o)
	defer SetObserver(nil)

	sub := activatePerMessage(context.Background(), in, ConsumerConfig{}, consumer, nil)
	defer func() { _ = sub.Close(context.Background()) }()

	// Generous: the test's own subject is a two-second refusal, and a cadence
	// that regressed past this bound would fail here rather than hang.
	deadline := time.Now().Add(15 * time.Second)
	for len(consumer.getCommits()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	commits := consumer.getCommits()
	if len(commits) != 2 {
		at, keys := admitter.snapshot()
		t.Fatalf("commit count = %d, want 2 ([800] then [801]); attempts=%v gaps=%v",
			len(commits), keys, gaps(at))
	}
	if commits[0][0].Offset != 800 || commits[1][0].Offset != 801 {
		t.Fatalf("commit order = %d then %d, want 800 then 801: a commit behind the "+
			"held record would have skipped it", commits[0][0].Offset, commits[1][0].Offset)
	}

	at, keys := admitter.snapshot()
	wantKeys := []string{
		"/wf1/v1/g1/orders/3/800-800",
		"/wf1/v1/g1/orders/3/800-800",
		"/wf1/v1/g1/orders/3/800-800",
		"/wf1/v1/g1/orders/3/801-801",
	}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("admission keys = %v, want %v — the retry must repeat the same "+
			"admission, and the second record must not be admitted before the first",
			keys, wantKeys)
	}

	// The cadence: aggregateRetryDelay's flat window, one second in entry-seed
	// mode (defaultEntrySeedFlushInterval). Only the lower bound is asserted —
	// "it waited" is the property that distinguishes a backoff from a spin, and
	// the attempt count above already rules the spin out. No upper bound: a
	// loaded machine can stretch a 1s timer, and the total wait above is the
	// bound that matters.
	for i := 1; i < 3; i++ {
		if gap := at[i].Sub(at[i-1]); gap < 900*time.Millisecond {
			t.Errorf("gap between attempt %d and %d = %v, want >= the 1s flat retry "+
				"window: a refusal must be backed off, not spun on", i, i+1, gap)
		}
	}

	if pairs := o.admissionPairs(); !reflect.DeepEqual(pairs, []string{
		"refused/concurrency_limit", "refused/concurrency_limit",
		"accepted/none", "accepted/none",
	}) {
		t.Errorf("admissions = %v, want each attempt counted, refusals included: "+
			"an attempt only the retry loop saw would make a throttled partition "+
			"read as an idle one", pairs)
	}
	// The gauge says the partition STOPPED, which is the half the refusal
	// counter cannot report — and it is cleared on the way out, so a resumed
	// partition does not stay blocked in the metric forever.
	if got := o.blockedTransitions(); !reflect.DeepEqual(got, []string{"orders/3=true", "orders/3=false"}) {
		t.Errorf("blocked transitions = %v, want [orders/3=true orders/3=false]", got)
	}
}

// gaps renders the intervals between attempts, for a failure message that says
// whether the loop was retrying or spinning.
func gaps(at []time.Time) []time.Duration {
	out := make([]time.Duration, 0, len(at))
	for i := 1; i < len(at); i++ {
		out = append(out, at[i].Sub(at[i-1]))
	}
	return out
}
