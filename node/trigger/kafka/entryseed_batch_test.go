package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestBuildKafkaBatchAdmissionKey_UsesActualRange(t *testing.T) {
	msgs := []Message{
		{Topic: "t", Partition: 3, Offset: 100},
		{Topic: "t", Partition: 3, Offset: 101},
		{Topic: "t", Partition: 3, Offset: 147},
	}
	got := buildBatchAdmissionKey("wf-1", "v2", "unit-a", msgs)
	want := "/wf-1/v2/unit-a/t/3/100-147"
	if got != want {
		t.Fatalf("admission key = %q, want %q", got, want)
	}
}

func TestBuildKafkaBatchAdmissionKey_SingleMessage(t *testing.T) {
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 7}}
	got := buildBatchAdmissionKey("wf-1", "v1", "unit-a", msgs)
	want := "/wf-1/v1/unit-a/t/0/7-7"
	if got != want {
		t.Fatalf("admission key = %q, want %q", got, want)
	}
}

func TestBuildKafkaBatchExits_CarriesAllMessages(t *testing.T) {
	msgs := []Message{
		{Topic: "t", Partition: 1, Offset: 10, Key: []byte("k1"), Value: []byte(`{"a":1}`)},
		{Topic: "t", Partition: 1, Offset: 11, Key: []byte("k2"), Value: []byte(`{"a":2}`)},
	}
	exits := buildBatchExits("kafka-node", msgs)
	if len(exits) != 1 {
		t.Fatalf("exits len = %d, want 1", len(exits))
	}
	if exits[0].NodeName != "kafka-node" || exits[0].Port != "main" {
		t.Fatalf("exit identity = %q/%q", exits[0].NodeName, exits[0].Port)
	}
	if got := exits[0].Data["count"]; got != 2 {
		t.Fatalf("count = %v, want 2", got)
	}
	if got := exits[0].Data["start_offset"]; got != int64(10) {
		t.Fatalf("start_offset = %v, want 10", got)
	}
	if got := exits[0].Data["end_offset"]; got != int64(11) {
		t.Fatalf("end_offset = %v, want 11", got)
	}
	list, ok := exits[0].Data["messages"].([]map[string]any)
	if !ok || len(list) != 2 {
		t.Fatalf("messages = %#v, want 2 entries", exits[0].Data["messages"])
	}
	if list[1]["value"] != `{"a":2}` {
		t.Fatalf("second message value = %v", list[1]["value"])
	}
}

// 空批不该产生 key —— 调用方永远不该传空，但静默返回一个畸形 key 比 panic 更难查。
func TestBuildKafkaBatchAdmissionKey_EmptyReturnsEmpty(t *testing.T) {
	if got := buildBatchAdmissionKey("wf", "v1", "u", nil); got != "" {
		t.Fatalf("empty batch key = %q, want empty", got)
	}
}

var _ = types.BoundaryExit{}

func TestSeedKafkaEntryBatchMessages_AcceptedAllowsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	in := &types.TriggerActivateInput{
		NodeName:   "kafka-node",
		WorkflowID: "wf-1",
		Params:     map[string]any{"entry_unit_id": "unit-a", "workflow_version": "v2"},
	}
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "t", Partition: 0, Offset: 9},
	}
	if !seedEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("accepted admission must allow commit")
	}
	calls := rt.getCalls()
	if len(calls) != 1 {
		t.Fatalf("seed calls = %d, want 1", len(calls))
	}
	if calls[0].AdmissionKey != "/wf-1/v2/unit-a/t/0/5-9" {
		t.Fatalf("admission key = %q", calls[0].AdmissionKey)
	}
	if calls[0].Outcome != "success" {
		t.Fatalf("outcome = %q, want success", calls[0].Outcome)
	}
}

func TestSeedKafkaEntryBatchMessages_DuplicateAllowsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Duplicate: true}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 1}}
	if !seedEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("duplicate admission must allow commit")
	}
}

func TestSeedKafkaEntryBatchMessages_ConflictAllowsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Conflict: true}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 1}}
	if !seedEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("conflict means another runner admitted it; commit is correct")
	}
}

// 这条是 offset 安全的关键：stale_generation 经由 error 返回，绝不能提交。
func TestSeedKafkaEntryBatchMessages_ErrorWithholdsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{err: errors.New("entry-seed: rejected by generation fence: stale_generation")}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 1}}
	if seedEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("a fence rejection must NOT commit the offset — Kafka must redeliver")
	}
}

// 全 false 的响应是未知状态，防御性地不提交。
func TestSeedKafkaEntryBatchMessages_UnknownStateWithholdsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 1}}
	if seedEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("unknown admission state must not commit")
	}
}

func TestSeedKafkaEntryBatchMessages_EmptyBatchIsNoop(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	if !seedEntryBatchMessages(context.Background(), in, rt, nil) {
		t.Fatal("empty batch is trivially done")
	}
	if len(rt.getCalls()) != 0 {
		t.Fatal("empty batch must not call the control plane")
	}
}

// ---------------------------------------------------------------------------
// Aggregate + entry-seed wiring (Task 3): flush routes a flushed batch through
// seedEntryBatchMessages instead of the legacy Emit path when the
// activation is entry-seed. The three tests below drive this through the real
// KafkaTriggerNode.Activate + aggregator, not by calling flush directly, so
// they also prove the ban in Activate has been lifted.
//
// entrySeedTestRuntime (kafka_entry_seed_test.go) is used as the Runtime
// rather than a bare mockEntrySeedRuntime: Activate requires a full
// types.TriggerRuntime (Emit/Dedup/TryLock/State), which mockEntrySeedRuntime
// alone does not implement. entrySeedTestRuntime implements both TriggerRuntime
// and types.EntrySeedRuntime by delegating admission calls to an embedded
// admitter, so the mockEntrySeedRuntime's callCount/getCalls stay available by
// asserting on the admitter directly.
// ---------------------------------------------------------------------------

func stubNewConsumer(c Consumer) func() {
	prev := newConsumer
	newConsumer = func(ConsumerConfig) (Consumer, error) { return c, nil }
	return func() { newConsumer = prev }
}

func entrySeedAggregateInput(t *testing.T, maxSize int) *types.TriggerActivateInput {
	t.Helper()
	return &types.TriggerActivateInput{
		NodeName:   "kafka-node",
		WorkflowID: "wf-1",
		Params: map[string]any{
			"brokers":       []string{"localhost:9092"},
			"topic":         "t",
			"group":         "g",
			"entry_seed":    true,
			"entry_unit_id": "unit-a",
			"aggregate": map[string]any{
				"enabled": true, "by": "partition", "dedup": "message", "max_size": maxSize,
			},
		},
	}
}

func waitForSeedCalls(t *testing.T, rt *mockEntrySeedRuntime, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rt.callCount.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d seed calls, got %d", want, rt.callCount.Load())
}

func waitForCommitCount(t *testing.T, c *commitRecordingConsumer, want int) {
	t.Helper()
	if !waitForCommitCountUpTo(c, want, 2*time.Second) {
		t.Fatalf("timed out waiting for %d commits, got %d", want, len(c.getCommits()))
	}
}

// waitForCommitCountUpTo reports whether the commit count reached want within
// the window. Used for BOTH positive assertions and negative ones — a negative
// assertion that reads the count immediately is a fake probe, because the
// commit it claims must not happen simply has not had time to happen yet.
func waitForCommitCountUpTo(c *commitRecordingConsumer, want int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(c.getCommits()) >= want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// entry-seed 下 aggregate 不再被拒绝。
func TestKafkaAggregate_EntrySeedActivates(t *testing.T) {
	admitter := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	rt := &entrySeedTestRuntime{admitter: admitter}
	in := &types.TriggerActivateInput{
		NodeName:   "kafka-node",
		WorkflowID: "wf-1",
		Params: map[string]any{
			"brokers":       []string{"localhost:9092"},
			"topic":         "t",
			"group":         "g",
			"entry_seed":    true,
			"entry_unit_id": "unit-a",
			"aggregate": map[string]any{
				"enabled": true, "by": "partition", "dedup": "message", "max_size": 2,
			},
		},
		Runtime: rt,
	}

	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 1},
		{Topic: "t", Partition: 0, Offset: 2},
	}
	consumer := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}
	restore := stubNewConsumer(consumer)
	defer restore()

	sub, err := (&Node{}).Activate(context.Background(), in)
	if err != nil {
		t.Fatalf("entry-seed aggregate activation must succeed, got: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	waitForSeedCalls(t, admitter, 1)

	calls := admitter.getCalls()
	if calls[0].AdmissionKey != "/wf-1//unit-a/t/0/1-2" {
		t.Fatalf("batch admission key = %q, want a 1-2 range", calls[0].AdmissionKey)
	}
	exitData := calls[0].Exits[0].Data
	if exitData["count"] != 2 {
		t.Fatalf("batch count = %v, want 2", exitData["count"])
	}
}

// seed 成功后才提交 offset —— 顺序不能反。
func TestKafkaAggregate_EntrySeedCommitsAfterSeed(t *testing.T) {
	admitter := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	rt := &entrySeedTestRuntime{admitter: admitter}
	in := entrySeedAggregateInput(t, 2)
	in.Runtime = rt

	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 1},
		{Topic: "t", Partition: 0, Offset: 2},
	}
	consumer := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}
	restore := stubNewConsumer(consumer)
	defer restore()

	sub, err := (&Node{}).Activate(context.Background(), in)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	waitForCommitCount(t, consumer, 1)
	if admitter.callCount.Load() == 0 {
		t.Fatal("offset committed without any seed call — ordering is inverted")
	}
}

// seed 失败绝不提交。这是「不该发生」的断言：必须反向等待，不能立刻读。
func TestKafkaAggregate_EntrySeedFailureWithholdsCommit(t *testing.T) {
	admitter := &mockEntrySeedRuntime{err: errors.New("entry-seed: rejected by generation fence: stale_generation")}
	rt := &entrySeedTestRuntime{admitter: admitter}
	in := entrySeedAggregateInput(t, 2)
	in.Runtime = rt

	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 1},
		{Topic: "t", Partition: 0, Offset: 2},
	}
	consumer := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}
	restore := stubNewConsumer(consumer)
	defer restore()

	sub, err := (&Node{}).Activate(context.Background(), in)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	// 先等 seed 真的发生过，否则下面的「无提交」断言是恒真的假探针。
	waitForSeedCalls(t, admitter, 1)
	// 再给提交足够时间发生 —— 反向等待，见 negative-assertion-needs-time。
	if waitForCommitCountUpTo(consumer, 1, 300*time.Millisecond) {
		t.Fatal("a failed seed must never commit the offset")
	}
}

// ---------------------------------------------------------------------------
// Task 4: batch flush + admission metrics.
// ---------------------------------------------------------------------------

type recordingBatchObserver struct {
	noopObserver
	mu         sync.Mutex
	flushes    []string // trigger reasons
	sizes      []int
	admissions []string
	reasons    []string // parallel to admissions
}

func (o *recordingBatchObserver) OnBatchFlushed(_ context.Context, _, trigger string, size int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushes = append(o.flushes, trigger)
	o.sizes = append(o.sizes, size)
}

func (o *recordingBatchObserver) OnBatchAdmission(_ context.Context, _, state, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.admissions = append(o.admissions, state)
	o.reasons = append(o.reasons, reason)
}

// admissionPairs returns state+reason together. Asserting on state alone was
// exactly the blind spot the reason label was added to close, so a helper that
// keeps them apart would reintroduce it in the tests.
func (o *recordingBatchObserver) admissionPairs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	pairs := make([]string, 0, len(o.admissions))
	for i, state := range o.admissions {
		pairs = append(pairs, state+"/"+o.reasons[i])
	}
	return pairs
}

func (o *recordingBatchObserver) snapshot() ([]string, []int, []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.flushes...),
		append([]int(nil), o.sizes...),
		append([]string(nil), o.admissions...)
}

func TestKafkaAggregate_EntrySeedRecordsMetrics(t *testing.T) {
	o := &recordingBatchObserver{}
	SetObserver(o)
	defer SetObserver(nil)

	admitter := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	rt := &entrySeedTestRuntime{admitter: admitter}
	in := entrySeedAggregateInput(t, 2)
	in.Runtime = rt

	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 1},
		{Topic: "t", Partition: 0, Offset: 2},
	}
	consumer := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}
	restore := stubNewConsumer(consumer)
	defer restore()

	sub, err := (&Node{}).Activate(context.Background(), in)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	waitForCommitCount(t, consumer, 1)

	flushes, sizes, admissions := o.snapshot()
	if len(flushes) == 0 || flushes[0] != "size" {
		t.Fatalf("flush triggers = %v, want first to be \"size\"", flushes)
	}
	if len(sizes) == 0 || sizes[0] != 2 {
		t.Fatalf("flush sizes = %v, want first to be 2", sizes)
	}
	if len(admissions) == 0 || admissions[0] != "accepted" {
		t.Fatalf("admissions = %v, want first to be \"accepted\"", admissions)
	}
}

// ---------------------------------------------------------------------------
// Task 8: entry-seed batch flush interval defaults to 1s.
// ---------------------------------------------------------------------------

func TestKafkaAggregateConfig_EntrySeedDefaultsToOneSecond(t *testing.T) {
	raw := map[string]any{
		"enabled": true, "by": "partition", "dedup": "message",
	}
	cfg, err := aggregateConfigFromParamForMode(raw, true /* entrySeed */)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if cfg.FlushInterval != time.Second {
		t.Fatalf("entry-seed flush interval = %v, want 1s", cfg.FlushInterval)
	}
}

// 既有部署的行为不能被静默改变。
func TestKafkaAggregateConfig_LegacyKeeps100ms(t *testing.T) {
	raw := map[string]any{
		"enabled": true, "by": "partition", "dedup": "message",
	}
	cfg, err := aggregateConfigFromParamForMode(raw, false /* entrySeed */)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if cfg.FlushInterval != 100*time.Millisecond {
		t.Fatalf("legacy flush interval = %v, want 100ms", cfg.FlushInterval)
	}
}

// 显式配置在两种模式下都优先于默认。
func TestKafkaAggregateConfig_ExplicitIntervalWinsInBothModes(t *testing.T) {
	for _, entrySeed := range []bool{true, false} {
		raw := map[string]any{
			"enabled": true, "by": "partition", "dedup": "message",
			"flush_interval": "250ms",
		}
		cfg, err := aggregateConfigFromParamForMode(raw, entrySeed)
		if err != nil {
			t.Fatalf("entrySeed=%v config: %v", entrySeed, err)
		}
		if cfg.FlushInterval != 250*time.Millisecond {
			t.Fatalf("entrySeed=%v flush interval = %v, want 250ms", entrySeed, cfg.FlushInterval)
		}
	}
}

// ---------------------------------------------------------------------------
// Task 7: batch flush routes through GroupExecRuntime when the runtime
// implements it (trigger-group local execution).
// ---------------------------------------------------------------------------

// mockGroupExecRuntime is a mockEntrySeedRuntime that ALSO implements
// types.GroupExecRuntime, simulating a trigger-group's local execution
// capability (groupExecTriggerRuntime in production).
type mockGroupExecRuntime struct {
	mockEntrySeedRuntime
	execResult types.GroupExecResult
	execErr    error
	gotInput   map[string]any
}

func (m *mockGroupExecRuntime) ExecuteGroup(_ context.Context, input map[string]any) (types.GroupExecResult, error) {
	m.gotInput = input
	return m.execResult, m.execErr
}

// TestSeedKafkaEntryBatchViaGroupExec_UsesRealExitsNotRawMessages proves the
// group-exec batch path admits the exits ExecuteGroup RETURNED, not exits
// synthesized from the raw Kafka messages (buildBatchExits) — the
// defining difference from seedEntryBatchMessages and the whole point of
// this feature.
func TestSeedKafkaEntryBatchViaGroupExec_UsesRealExitsNotRawMessages(t *testing.T) {
	realExits := []types.BoundaryExit{{NodeName: "member", Port: "main", Data: map[string]any{"processed": true}}}
	rt := &mockGroupExecRuntime{
		mockEntrySeedRuntime: mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}},
		execResult:           types.GroupExecResult{Outcome: "success", Exits: realExits},
	}
	in := &types.TriggerActivateInput{
		NodeName: "trig", WorkflowID: "wf-1",
		Params: map[string]any{"entry_unit_id": "g", "workflow_version": "v2"},
	}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 5}, {Topic: "t", Partition: 0, Offset: 9}}

	if !seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, false) {
		t.Fatal("accepted admission must allow commit")
	}
	calls := rt.getCalls()
	if len(calls) != 1 {
		t.Fatalf("seed calls = %d, want 1", len(calls))
	}
	if len(calls[0].Exits) != 1 || calls[0].Exits[0].NodeName != "member" || calls[0].Exits[0].Data["processed"] != true {
		t.Fatalf("admitted exits = %+v, want the REAL exits ExecuteGroup returned, not raw-message exits", calls[0].Exits)
	}
	if calls[0].AdmissionKey != "/wf-1/v2/g/t/0/5-9" {
		t.Fatalf("admission key = %q", calls[0].AdmissionKey)
	}
	if rt.gotInput["count"] != 2 {
		t.Fatalf("ExecuteGroup input = %+v, want count=2", rt.gotInput)
	}
}

// TestSeedKafkaEntryBatchViaGroupExec_GroupFailureWithholdsCommit verifies a
// non-success group outcome withholds the offset commit and never calls
// SeedExecutionFromEntry at all (a failed member execution must not be
// admitted as if it succeeded).
func TestSeedKafkaEntryBatchViaGroupExec_GroupFailureWithholdsCommit(t *testing.T) {
	rt := &mockGroupExecRuntime{
		mockEntrySeedRuntime: mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}},
		execResult:           types.GroupExecResult{Outcome: "failed", Error: "member node error"},
	}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 1}}

	if seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, false) {
		t.Fatal("a failed group execution must not commit the offset")
	}
	if len(rt.getCalls()) != 0 {
		t.Fatal("a failed group execution must never reach SeedExecutionFromEntry")
	}
}

// TestSeedKafkaEntryBatchViaGroupExec_ExecuteGroupErrorWithholdsCommit
// verifies an infrastructure-level ExecuteGroup error (package resolution
// failure, etc.) also withholds the commit.
func TestSeedKafkaEntryBatchViaGroupExec_ExecuteGroupErrorWithholdsCommit(t *testing.T) {
	rt := &mockGroupExecRuntime{execErr: errors.New("package validation failed")}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 1}}

	if seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, false) {
		t.Fatal("an ExecuteGroup error must not commit the offset")
	}
}

// captureAdmissionLog redirects the default slog logger into a buffer for the
// duration of the test and returns it. It also gives the test a fresh
// discardLogger: the throttle is a package global keyed by topic+state, so a
// second test using the same key within 30s would find its line suppressed and
// fail for a reason that has nothing to do with the code under test.
func captureAdmissionLog(t *testing.T) *strings.Builder {
	t.Helper()
	buf := &strings.Builder{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	prevThrottle := discardLog
	discardLog = &discardLogger{last: map[string]time.Time{}, suppressed: map[string]int{}}
	t.Cleanup(func() {
		slog.SetDefault(prevLogger)
		discardLog = prevThrottle
	})
	return buf
}

// TestSeedKafkaEntryBatchViaGroupExec_FailureCauseIsLogged is the regression
// test for a real production blind spot: a scenario-A run showed 599 of 634
// batch admissions failing, and the cause was unavailable anywhere. The
// OnBatchAdmission observer takes only a state enum — an error string cannot be
// a metric label — so unless the cause reaches a log it is destroyed at the
// point of failure and the only way to learn it is to reproduce the run.
//
// Asserted on the transient and deterministic branches. Neither commits: a
// deterministic failure stalls the partition until the workflow is repaired
// rather than silently discarding a whole batch of production traffic.
func TestSeedKafkaEntryBatchViaGroupExec_FailureCauseIsLogged(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deterministic bool
		wantState     string
		wantCommitted string
	}{
		{"transient", false, "error", "committed=false"},
		{"deterministic", true, "deterministic_error", "committed=false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureAdmissionLog(t)
			rt := &mockGroupExecRuntime{execResult: types.GroupExecResult{
				Outcome:       "failed",
				Error:         "wasm trap: unreachable in clean guest",
				Deterministic: tc.deterministic,
			}}
			in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}
			msgs := []Message{
				{Topic: "cause-" + tc.name, Partition: 4, Offset: 100},
				{Topic: "cause-" + tc.name, Partition: 4, Offset: 137},
			}

			seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, false)

			got := buf.String()
			for _, want := range []string{
				"wasm trap: unreachable in clean guest", // the cause itself
				"outcome=failed",                        // and what the group reported
				"state=" + tc.wantState,
				tc.wantCommitted,
				"partition=4", "start_offset=100", "end_offset=137", "count=2",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("admission failure log is missing %q; without it an "+
						"operator sees only a counter increment.\nlog: %s", want, got)
				}
			}
		})
	}
}

// TestSeedKafkaEntryBatchViaGroupExec_AdmissionErrorCauseIsLogged covers the
// second failure surface: the group succeeded but the admission round trip to
// the control plane failed. Same blind spot, different call site — a fix that
// only covered ExecuteGroup would leave transport failures mute.
func TestSeedKafkaEntryBatchViaGroupExec_AdmissionErrorCauseIsLogged(t *testing.T) {
	buf := captureAdmissionLog(t)
	rt := &mockGroupExecRuntime{
		mockEntrySeedRuntime: mockEntrySeedRuntime{err: errors.New("dial control plane: i/o timeout")},
		execResult:           types.GroupExecResult{Outcome: "success"},
	}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}
	msgs := []Message{{Topic: "admit-err", Partition: 1, Offset: 7}}

	if seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, false) {
		t.Fatal("a failed admission must not commit the offset")
	}
	if got := buf.String(); !strings.Contains(got, "dial control plane: i/o timeout") ||
		!strings.Contains(got, "SeedExecutionFromEntry") {
		t.Errorf("admission transport failure did not name its cause in the log: %s", got)
	}
}

// TestLogBatchAdmission_TruncatesCause bounds the log line. The cause is the
// engine's error text, and a guest that quotes its input into an error message
// would otherwise put an unbounded fragment of production traffic into the log.
func TestLogBatchAdmission_TruncatesCause(t *testing.T) {
	buf := captureAdmissionLog(t)
	logBatchAdmission("trunc", 0, 1, 2, 1, "error", strings.Repeat("x", admissionCauseLimit*3))
	got := buf.String()
	if !strings.Contains(got, "(truncated)") {
		t.Errorf("an oversized cause was not truncated: %d bytes logged", len(got))
	}
	if strings.Count(got, "x") > admissionCauseLimit {
		t.Errorf("logged %d cause bytes, want at most %d", strings.Count(got, "x"), admissionCauseLimit)
	}
}

// entrySeedGroupExecTestRuntime is entrySeedTestRuntime PLUS ExecuteGroup, so
// it satisfies types.TriggerRuntime, types.EntrySeedRuntime AND
// types.GroupExecRuntime all at once — exactly the shape of the production
// groupExecTriggerRuntime (service/runner). Used to drive a batch through the
// REAL kafkaPartitionAggregator.flush (via KafkaTriggerNode.Activate) and
// prove the combined-interface type assertion in flush actually selects the
// group-exec branch, not just that seedEntryBatchViaGroupExec works in
// isolation.
type entrySeedGroupExecTestRuntime struct {
	entrySeedTestRuntime
	execResult types.GroupExecResult
	execErr    error
	execCalls  atomic.Int32
	gotInput   map[string]any
}

func (r *entrySeedGroupExecTestRuntime) ExecuteGroup(_ context.Context, input map[string]any) (types.GroupExecResult, error) {
	r.execCalls.Add(1)
	r.gotInput = input
	return r.execResult, r.execErr
}

// TestKafkaAggregate_EntrySeedGroupExec_RoutesThroughFlushGroupBranch drives a
// batch through the real Activate -> aggregator -> flush path (not calling
// seedEntryBatchViaGroupExec directly) with a runtime implementing BOTH
// types.EntrySeedRuntime and types.GroupExecRuntime, and asserts:
//  1. ExecuteGroup was actually invoked by flush's combined-interface branch.
//  2. The exits admitted to the control plane are the REAL exits ExecuteGroup
//     returned, not the raw-message exits buildBatchExits would
//     synthesize on the plain-EntrySeedRuntime branch.
//
// This is the load-bearing guard for the type assertion added at
// kafka.go:838-841 in kafkaPartitionAggregator.flush: if that branch is ever
// dropped, reordered behind the plain-EntrySeedRuntime check, or the combined
// interface literal is broken, this test fails because the admitted exits
// revert to the raw-message shape (no "processed" key) and execCalls stays 0.
func TestKafkaAggregate_EntrySeedGroupExec_RoutesThroughFlushGroupBranch(t *testing.T) {
	admitter := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	realExits := []types.BoundaryExit{{NodeName: "member", Port: "main", Data: map[string]any{"processed": true}}}
	rt := &entrySeedGroupExecTestRuntime{
		entrySeedTestRuntime: entrySeedTestRuntime{admitter: admitter},
		execResult:           types.GroupExecResult{Outcome: "success", Exits: realExits},
	}
	in := entrySeedAggregateInput(t, 2)
	in.Runtime = rt

	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 1},
		{Topic: "t", Partition: 0, Offset: 2},
	}
	consumer := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}
	restore := stubNewConsumer(consumer)
	defer restore()

	sub, err := (&Node{}).Activate(context.Background(), in)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	waitForSeedCalls(t, admitter, 1)

	if rt.execCalls.Load() != 1 {
		t.Fatalf("ExecuteGroup calls = %d, want 1 — flush did not take the group-exec branch", rt.execCalls.Load())
	}
	calls := admitter.getCalls()
	if len(calls) != 1 {
		t.Fatalf("seed calls = %d, want 1", len(calls))
	}
	if len(calls[0].Exits) != 1 || calls[0].Exits[0].NodeName != "member" || calls[0].Exits[0].Data["processed"] != true {
		t.Fatalf("admitted exits = %+v, want the REAL exits ExecuteGroup returned (flush must have taken the group-exec branch, not the raw-message one)", calls[0].Exits)
	}
	if rt.gotInput["count"] != 2 {
		t.Fatalf("ExecuteGroup input = %+v, want count=2", rt.gotInput)
	}
}

// conflict 率是唯一能看出「重投产生重复」实际频率的信号，必须被记录。
func TestKafkaAggregate_EntrySeedRecordsConflict(t *testing.T) {
	o := &recordingBatchObserver{}
	SetObserver(o)
	defer SetObserver(nil)

	admitter := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Conflict: true}}
	rt := &entrySeedTestRuntime{admitter: admitter}
	in := entrySeedAggregateInput(t, 1)
	in.Runtime = rt

	consumer := &commitRecordingConsumer{
		inner: newScriptedConsumer([]Message{{Topic: "t", Partition: 0, Offset: 1}}),
	}
	restore := stubNewConsumer(consumer)
	defer restore()

	sub, err := (&Node{}).Activate(context.Background(), in)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	waitForCommitCount(t, consumer, 1)

	_, _, admissions := o.snapshot()
	if len(admissions) == 0 || admissions[0] != "conflict" {
		t.Fatalf("admissions = %v, want \"conflict\"", admissions)
	}
}
