//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Generation-fence 409 must not commit
//
// Replaces the unit-level TestKafkaEntrySeed_StaleGeneration409_NoCommit that
// lived in node/internal/trigger. That test combined the REAL
// HTTPEntrySeedRuntime with kafka's unexported seedKafkaEntryBatch; after the
// package split the runtime lives in service/protocol (node must not import
// service/) and the seed function is unexported, so neither half can reach the
// other from a unit test.
//
// What only this test proves: the two halves AGREE. seedEntryBatch commits on
// (Accepted || Duplicate || Conflict) and withholds on err != nil — so it
// depends on exactly one property of the runtime, that a generation-fence
// rejection surfaces as an ERROR and not as Conflict. The control plane
// overloads HTTP 409 for both:
//
//   - {"state":"conflict",...}       → handled by another runner → COMMIT
//   - {"error":"stale_generation"}   → this runner is superseded → MUST NOT COMMIT
//
// Committing the second case would mean Kafka never redelivers and the
// current-generation owner never sees the message: silent message loss during a
// generation upgrade.
//
// Failure condition: if HTTPEntrySeedRuntime mapped the fence body to
// Conflict:true, pass 1 would commit offsets and pass 2 would observe zero
// redelivered messages — the final assertion below would find the values
// missing.
// ---------------------------------------------------------------------------

func TestKafkaEntrySeed_StaleGeneration409_DoesNotCommit(t *testing.T) {
	brokers := requireKafka(t)
	_ = requireRedis(t)

	topic := uniqueTopic("xflow-seed-fence-409")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 1)

	const totalMessages = 6
	const batchSize = 2

	msgs := make([]kafka.Message, 0, totalMessages)
	for i := 0; i < totalMessages; i++ {
		msgs = append(msgs, kafka.Message{
			Key:   []byte("k"),
			Value: []byte(fmt.Sprintf("fence-%03d", i)),
		})
	}
	writeKafkaMessages(t, brokers, topic, msgs)

	// --- Pass 1: a control plane that ALWAYS fences. ---
	//
	// Body mimics apiserver writeError(w, 409, "stale_generation") exactly:
	// an errorResponse with NO "state" field. That absence is the whole
	// discriminator inside HTTPEntrySeedRuntime.
	var fenced atomic.Int64
	fenceSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fenced.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"stale_generation"}`))
	}))
	defer fenceSrv.Close()

	// Generation 1 is the superseded generation in this scenario.
	rt1 := &protocol.HTTPEntrySeedRuntime{
		BaseURL:    fenceSrv.URL,
		Client:     fenceSrv.Client(),
		Generation: 1,
	}

	tr1 := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		AggregateByPartition(batchSize, 200*time.Millisecond).
		BlockOnOverflow()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()

	sub1, err := tr1.Activate(ctx1, &types.TriggerActivateInput{
		WorkflowID: "wf-seed-fence",
		NodeName:   "kafka-fence",
		Params:     fenceParams(brokers, topic, group, batchSize),
		Runtime:    rt1,
	})
	if err != nil {
		t.Fatalf("activate pass 1: %v", err)
	}

	// Wait until the control plane has actually been hit — otherwise a
	// still-committed offset of -1 would "pass" for the wrong reason (nothing
	// was ever consumed).
	deadline := time.After(20 * time.Second)
	for fenced.Load() < 1 {
		select {
		case <-deadline:
			t.Fatalf("pass 1 timeout: control plane never received a seed request")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Reverse wait: the assertion is that the offset does NOT advance. Reading
	// it once immediately after the first 409 would be a fake probe (the commit
	// may simply not have happened YET). Poll for a window and require that it
	// never covers any message.
	const observeWindow = 3 * time.Second
	const pollInterval = 100 * time.Millisecond
	samples := 0
	for start := time.Now(); time.Since(start) < observeWindow; {
		if off := fetchCommittedOffset(t, brokers, group, topic, 0); off > 0 {
			t.Fatalf("committed offset = %d during fence window, want <= 0 (a stale-generation 409 must not commit)", off)
		}
		samples++
		time.Sleep(pollInterval)
	}
	if samples < 10 {
		t.Fatalf("only %d offset samples in %v, want >= 10 (the reverse wait degenerated)", samples, observeWindow)
	}
	t.Logf("pass 1: %d fence responses served, committed offset stayed <= 0 across %d samples",
		fenced.Load(), samples)

	if err := sub1.Close(context.Background()); err != nil {
		t.Logf("sub1 close: %v", err)
	}
	cancel1()

	// --- Pass 2: the current-generation owner. All seeds accepted. ---
	//
	// This is the half that turns "offset not committed" into the property that
	// actually matters: the messages are still THERE. An implementation that
	// discarded them without committing would pass pass 1 and fail here.
	var seeded []string
	var seedMu sync.Mutex
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req protocol.SeedExecutionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		seedMu.Lock()
		for _, ex := range req.Exits {
			for _, m := range messageValuesFromExit(ex) {
				seeded = append(seeded, m)
			}
		}
		n := len(seeded)
		seedMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.SeedExecutionResponse{
			State:       "accepted",
			ExecutionID: fmt.Sprintf("exec-%d", n),
		})
	}))
	defer okSrv.Close()

	rt2 := &protocol.HTTPEntrySeedRuntime{
		BaseURL:    okSrv.URL,
		Client:     okSrv.Client(),
		Generation: 2, // the new owner
	}

	tr2 := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		AggregateByPartition(batchSize, 200*time.Millisecond).
		BlockOnOverflow()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	sub2, err := tr2.Activate(ctx2, &types.TriggerActivateInput{
		WorkflowID: "wf-seed-fence",
		NodeName:   "kafka-fence",
		Params:     fenceParams(brokers, topic, group, batchSize),
		Runtime:    rt2,
	})
	if err != nil {
		t.Fatalf("activate pass 2: %v", err)
	}

	deadline2 := time.After(20 * time.Second)
	for {
		seedMu.Lock()
		got := len(seeded)
		seedMu.Unlock()
		if got >= totalMessages {
			break
		}
		select {
		case <-deadline2:
			t.Fatalf("pass 2 timeout: %d message values seeded, want %d", got, totalMessages)
		case <-time.After(100 * time.Millisecond):
		}
	}

	time.Sleep(500 * time.Millisecond) // grace for the final commit
	if err := sub2.Close(context.Background()); err != nil {
		t.Logf("sub2 close: %v", err)
	}
	cancel2()

	// CRITICAL: every produced value must have reached the new generation.
	seedMu.Lock()
	got := map[string]bool{}
	for _, v := range seeded {
		got[v] = true
	}
	seedMu.Unlock()
	for i := 0; i < totalMessages; i++ {
		want := fmt.Sprintf("fence-%03d", i)
		if !got[want] {
			t.Errorf("MISSING message value %q after the fence; the stale-generation 409 lost it", want)
		}
	}
	if t.Failed() {
		t.Fatalf("stale-generation fence lost messages: see MISSING errors above")
	}

	final := fetchCommittedOffset(t, brokers, group, topic, 0)
	if final < int64(totalMessages) {
		t.Fatalf("final committed offset = %d, want >= %d", final, totalMessages)
	}
	t.Logf("all %d values redelivered to generation 2; final committed offset = %d",
		totalMessages, final)
}

// fenceParams builds the entry-seed activation params. Written out rather than
// taken from RawParams() because the entry-seed selecting keys (entry_seed,
// entry_unit_id, workflow_version) are added by the runner's
// withEntrySeedParams, not by the builder — see
// service/runner/trigger_activation_handler.go.
func fenceParams(brokers []string, topic, group string, batchSize int) map[string]any {
	return map[string]any{
		"brokers":          brokers,
		"topic":            topic,
		"group":            group,
		"start_offset":     "earliest",
		"entry_seed":       true,
		"entry_unit_id":    "kafka-fence",
		"workflow_version": "v1",
		"aggregate": map[string]any{
			"enabled":        true,
			"by":             "partition",
			"max_size":       batchSize,
			"flush_interval": "200ms",
			"dedup":          "message",
			"on_overflow":    "block",
		},
	}
}

// messageValuesFromExit pulls the Kafka message values out of one boundary
// exit's Data. In batch entry-seed mode the exit carries the batch under
// "messages" (see buildBatchExits in node/trigger/kafka/entryseed.go); a
// single-message exit carries "value" directly.
func messageValuesFromExit(ex protocol.BoundaryExit) []string {
	if ex.Data == nil {
		return nil
	}
	if v, ok := ex.Data["value"].(string); ok {
		return []string{v}
	}
	raw, ok := ex.Data["messages"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if v, ok := m["value"].(string); ok {
			out = append(out, v)
		}
	}
	return out
}
