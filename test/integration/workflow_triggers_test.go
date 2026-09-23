//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
)

// This file fires each of the five trigger kinds through a REAL trigger
// handler, on a REAL backend, and asserts the firing produced an execution
// that ran to success and whose recording node captured the event.
//
// It is the companion to TestWorkflowTriggerCoverage: that test proves every
// registered trigger kind appears in some tier definition, which is a
// statement about declarations. It would still pass if every one of those
// triggers failed to activate, connected to nothing, or emitted into a vacuum.
// The tests here are what make the coverage claim load-bearing — each one
// observes an execution that only a real firing could have created.
//
// What each case does and does not cover:
//
//   - timer, cron: the tier definitions, hosted in-process, with their real
//     handlers driving the scheduler. No external dependency.
//   - webhook: the tier definition, its route mounted on a real listener and
//     driven by real HTTP requests.
//   - redis, kafka: the real handlers against the real brokers, but with the
//     endpoint fixed at build time rather than read from $vars. A trigger's
//     parameters render at REGISTRATION, and the in-process AddWorkflow path
//     exposes no way to supply the compiled graph's vars (the server's submit
//     body is what carries Context). The $vars form itself is exercised where
//     it can be: see test/workflows' definitions and the hosted-trigger suites.

// triggerFiringObserver records the executions a trigger firing created.
//
// It observes through the engine's public hook surface rather than by polling
// a list: the embedded engine exposes no execution listing, and a hook reports
// the execution at the moment it starts, so a firing that creates an execution
// is visible whether or not the run has finished when the assertion is made.
//
// OnNodeStart fires for every node, so the first call for an execution records
// it; later calls for the same execution (the record node, the end node) are
// the same execution and must not be counted twice — the map is the dedupe.
type triggerFiringObserver struct {
	engine.BaseHooks

	mu      sync.Mutex
	created []types.ExecutionID
	seen    map[types.ExecutionID]bool
	status  map[types.ExecutionID]types.ExecutionStatus
}

func newTriggerFiringObserver() *triggerFiringObserver {
	return &triggerFiringObserver{
		seen:   make(map[types.ExecutionID]bool),
		status: make(map[types.ExecutionID]types.ExecutionStatus),
	}
}

func (o *triggerFiringObserver) OnNodeStart(_ context.Context, id types.ExecutionID, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.seen[id] {
		return
	}
	o.seen[id] = true
	o.created = append(o.created, id)
}

func (o *triggerFiringObserver) OnExecutionComplete(_ context.Context, id types.ExecutionID, s types.ExecutionStatus) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status[id] = s
}

// executions returns a copy of the executions seen so far.
func (o *triggerFiringObserver) executions() []types.ExecutionID {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]types.ExecutionID, len(o.created))
	copy(out, o.created)
	return out
}

// awaitExecutions polls until at least want executions have been observed, or
// the timeout elapses.
//
// It polls rather than waiting on a signal because the firings arrive from the
// trigger's own goroutine: a timer's tick, a broker's consumer loop. There is
// no channel to select on that would also be correct for all five kinds.
func (o *triggerFiringObserver) awaitExecutions(t *testing.T, want int, timeout time.Duration) []types.ExecutionID {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := o.executions()
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d execution(s) observed in %s, want at least %d", len(got), timeout, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// triggerEngine starts an embedded engine wired to record every execution its
// triggers create, and stops it on cleanup.
func triggerEngine(t *testing.T) (*xflow.Engine, *triggerFiringObserver) {
	t.Helper()
	obs := newTriggerFiringObserver()
	eng, err := xflow.NewLocal(xflow.WithConcurrency(4), xflow.WithHooks(obs))
	if err != nil {
		t.Fatalf("xflow.NewLocal: %v", err)
	}
	t.Cleanup(eng.Stop)
	return eng, obs
}

// recordedEvent pulls the trigger event out of a recording node's output.
//
// The definition writes it under "event" as whatever `$input.trigger` held: a
// *types.TriggerEvent in-process, a map[string]any once the event has crossed a
// JSON boundary. Both are accepted, so the assertion is about the event's
// content rather than about which topology produced it.
func recordedEvent(t *testing.T, out map[string]any) map[string]any {
	t.Helper()
	switch ev := out["event"].(type) {
	case *types.TriggerEvent:
		return map[string]any{"kind": ev.Kind, "id": ev.ID, "data": ev.Data}
	case map[string]any:
		return ev
	default:
		t.Fatalf("record node event has type %T, want *types.TriggerEvent or map[string]any", out["event"])
		return nil
	}
}

// assertRecordedKind waits for the execution to terminate, asserts it succeeded,
// and returns the trigger event its recording node captured after checking the
// event's kind.
//
// The recording node is the same node every tier trigger workflow carries
// (see test/workflows.triggerWorkflow), so reading it is reading the tier
// definition's own output, not a test-only instrument.
func assertRecordedKind(t *testing.T, eng *xflow.Engine, id types.ExecutionID, wantKind string) map[string]any {
	t.Helper()
	ctx := context.Background()
	waitCtx, cancel := context.WithTimeout(ctx, workflowRunTimeout)
	defer cancel()
	res, err := eng.Wait(waitCtx, id)
	if err != nil {
		t.Fatalf("Wait(%s): %v", id, err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution %s status = %s, want success (error %q)", id, res.Status, res.Error)
	}
	detail, err := eng.Inspect(ctx, id, "record")
	if err != nil {
		t.Fatalf("Inspect(%s, record): %v", id, err)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Inspect(%s, record) returned %d node details, want 1", id, len(detail.Nodes))
	}
	node := detail.Nodes[0]
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("record node status = %s, want success (error %q)", node.Status, node.Error)
	}
	event := recordedEvent(t, node.Output)
	if got, _ := event["kind"].(string); got != wantKind {
		t.Fatalf("recorded event kind = %q, want %q (event %v)", got, wantKind, event)
	}
	return event
}

// eventData returns the event's data map, which is where each trigger's own
// payload lives.
func eventData(t *testing.T, event map[string]any) map[string]any {
	t.Helper()
	data, _ := event["data"].(map[string]any)
	if data == nil {
		t.Fatalf("recorded event has no data map (event %v)", event)
	}
	return data
}

// TestWorkflowTriggerTimerFiresAndExecutes fires the timer tier on an embedded
// engine and asserts the interval produced repeated executions, each recording
// the trigger kind the definition writes.
func TestWorkflowTriggerTimerFiresAndExecutes(t *testing.T) {
	eng, obs := triggerEngine(t)
	ctx := context.Background()

	const interval = 150 * time.Millisecond
	if _, err := eng.AddWorkflow(ctx, workflows.TimerTriggerWorkflow(interval)); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	ids := obs.awaitExecutions(t, 2, 10*time.Second)
	for _, id := range ids {
		assertRecordedKind(t, eng, id, "timer")
	}
}

// TestWorkflowTriggerCronFiresAndExecutes is the cron arm. The expression is
// "@every 1s" rather than a five-field schedule: a five-field schedule fires at
// minute granularity, which is longer than this suite should wait to observe a
// real event.
func TestWorkflowTriggerCronFiresAndExecutes(t *testing.T) {
	eng, obs := triggerEngine(t)
	ctx := context.Background()

	if _, err := eng.AddWorkflow(ctx, workflows.CronTriggerWorkflow("@every 1s")); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	ids := obs.awaitExecutions(t, 1, 10*time.Second)
	for _, id := range ids {
		assertRecordedKind(t, eng, id, "cron")
	}
}

// TestWorkflowTriggerWebhookFiresOneExecutionPerRequest mounts the tier
// webhook workflow's route on a real listener and drives it with real HTTP
// requests.
//
// Unlike the timed triggers, the firings here are entirely under the test's
// control, so the assertion is exact: N distinct requests must produce exactly
// N executions — no fewer (a request that reached the route but emitted
// nothing) and no more (a duplicate delivery).
//
// The bodies differ per request on purpose. The trigger derives its event ID
// from the body when no event-id header is configured, and dedups on that ID
// for a day, so identical requests would collapse into one execution and the
// test would be asserting the dedup window rather than the firing.
//
// The workflow's own handler is what serves the route: the engine's trigger
// runtime owns the route table, and nothing mounts it automatically, which is
// why the test must mount it. Without that mounting step an embedded webhook
// trigger activates successfully and then never receives anything.
func TestWorkflowTriggerWebhookFiresOneExecutionPerRequest(t *testing.T) {
	eng, obs := triggerEngine(t)
	ctx := context.Background()

	if _, err := eng.AddWorkflow(ctx, workflows.WebhookTriggerWorkflow()); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	handler := eng.WebhookHandler()
	if handler == nil {
		t.Fatal("WebhookHandler() is nil; the tier workflow registers a webhook route, " +
			"so an embedded engine must serve it")
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	const requests = 3
	bodySeen := make([]string, 0, requests)
	for i := 0; i < requests; i++ {
		body := fmt.Sprintf(`{"seq":%d}`, i)
		bodySeen = append(bodySeen, body)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+workflows.WebhookPath, strings.NewReader(body))
		if err != nil {
			t.Fatalf("build request %d: %v", i, err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("post request %d: %v", i, err)
		}
		// The webhook runtime answers 202 for an accepted request; a 4xx or 5xx
		// means the route rejected it, and asserting the status here keeps a
		// rejection from being misread as "the trigger never fired".
		if resp.StatusCode != http.StatusAccepted {
			resp.Body.Close()
			t.Fatalf("request %d status = %d, want 202", i, resp.StatusCode)
		}
		resp.Body.Close()
	}

	ids := obs.awaitExecutions(t, requests, 10*time.Second)
	gotBodies := make(map[string]bool, requests)
	for _, id := range ids {
		event := assertRecordedKind(t, eng, id, "webhook")
		body, _ := eventData(t, event)["body"].(string)
		gotBodies[body] = true
	}
	// The events must carry the requests' own bodies, not just be the right
	// count: an execution created by some other path would otherwise satisfy
	// the count alone.
	for _, want := range bodySeen {
		if !gotBodies[want] {
			t.Fatalf("no execution recorded request body %q (recorded: %v)", want, gotBodies)
		}
	}
	// Exactly N, checked after the executions settle: an extra one would mean a
	// request emitted twice, which awaitExecutions alone cannot catch.
	for _, id := range ids {
		waitCtx, cancel := context.WithTimeout(ctx, workflowRunTimeout)
		if _, err := eng.Wait(waitCtx, id); err != nil {
			cancel()
			t.Fatalf("Wait(%s): %v", id, err)
		}
		cancel()
	}
	if got := len(obs.executions()); got != requests {
		t.Fatalf("observed %d executions for %d requests, want exactly %d", got, requests, requests)
	}
}

// TestWorkflowTriggerRedisFiresOneExecutionPerMessage publishes to a real Redis
// stream and asserts each message produced an execution.
//
// Redis is required, not optional: a skipped run would report the firing path
// as covered while nothing was ever consumed. requireRedis fails under
// XFLOW_REQUIRE_REDIS_INTEGRATION=1 and skips otherwise, which is the
// repository's convention for a dependency a suite cannot substitute.
func TestWorkflowTriggerRedisFiresOneExecutionPerMessage(t *testing.T) {
	addr := requireRedis(t)
	eng, obs := triggerEngine(t)
	ctx := context.Background()

	// A fresh stream and consumer group per run. A group remembers its
	// last-delivered ID, so reusing a shared name would make this test resume
	// from a previous run's offsets rather than consume the messages it
	// publishes — and would then be asserting on someone else's messages.
	stream := fmt.Sprintf("xflow:qa:stream:%d", time.Now().UnixNano())
	group := fmt.Sprintf("xflow-qa-group:%d", time.Now().UnixNano())
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	defer client.Del(context.Background(), stream)

	if _, err := eng.AddWorkflow(ctx, workflows.RedisTriggerWorkflow(addr, stream, group)); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	const messages = 3
	for i := 0; i < messages; i++ {
		// The fields are the trigger's own payload convention.
		values := map[string]any{"seq": i, "body": fmt.Sprintf(`{"seq":%d}`, i)}
		if err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: values,
		}).Err(); err != nil {
			t.Fatalf("XADD %d: %v", i, err)
		}
	}

	ids := obs.awaitExecutions(t, messages, 15*time.Second)
	gotStreams := make(map[string]bool, messages)
	for _, id := range ids {
		event := assertRecordedKind(t, eng, id, "redis")
		values, _ := eventData(t, event)["values"].(map[string]any)
		if values == nil {
			t.Fatalf("redis event carried no values map (event %v)", event)
		}
		// seq was published as an int; a round trip through Redis and JSON can
		// land it as a string, so compare on the rendered form.
		gotStreams[fmt.Sprint(values["seq"])] = true
	}
	for i := 0; i < messages; i++ {
		if !gotStreams[fmt.Sprint(i)] {
			t.Fatalf("no execution recorded stream entry seq=%d (recorded: %v)", i, gotStreams)
		}
	}
}

// TestWorkflowTriggerKafkaFiresAndExecutes produces to a real Kafka topic and
// asserts the consumer fired an execution carrying the produced value.
//
// It does not assert an exact execution count: a consumer group may rebalance
// or redeliver within the window, and an exact-count assertion would then be
// testing broker timing rather than the trigger. What it does assert is
// stronger than a count — the recording node's captured value is compared to
// the value that was produced, so a firing that fired on the wrong record
// fails even though an execution happened.
func TestWorkflowTriggerKafkaFiresAndExecutes(t *testing.T) {
	brokers := requireKafka(t)
	eng, obs := triggerEngine(t)
	ctx := context.Background()

	topic := uniqueTopic("xflow-qa-trigger")
	group := uniqueTopic("xflow-qa-group")
	newKafkaTopic(t, brokers, topic, 1)
	if _, err := eng.AddWorkflow(ctx, workflows.KafkaTriggerWorkflow(brokers, topic, group)); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	// A plain (non-JSON) payload: the trigger carries the raw message under
	// data.value, so a bare token is the unambiguous value to compare against.
	const want = "kafka-trigger-payload"
	writeKafkaMessages(t, brokers, topic, []kafka.Message{{Value: []byte(want)}})

	ids := obs.awaitExecutions(t, 1, 30*time.Second)
	found := false
	for _, id := range ids {
		event := assertRecordedKind(t, eng, id, "kafka")
		if value, _ := eventData(t, event)["value"].(string); value == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no execution recorded value %q under kind kafka (executions: %v)", want, ids)
	}
}
