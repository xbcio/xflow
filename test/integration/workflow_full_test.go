//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
)

// This file runs workflows.FullWorkflow — the one definition that carries every
// registered node type and trigger kind — end to end.
//
//   - TestWorkflowFullDistributed drives the main lane through the production
//     server+runner topology (HTTP invoke, Redis/Asynq, a real runner, MySQL, a
//     live HTTP and gRPC endpoint, signals from an authenticated caller), once
//     per gate outcome and switch arm.
//   - TestWorkflowFullTriggersFire hosts the same definition in-process and
//     fires the trigger lane from every kind this environment can reach.
//
// Both also assert the other lane resolved as skipped: an execution starts from
// one of six entries, and the other five must not hold it open.

// TestWorkflowFullDistributed runs the main lane on two paths that between
// them take every branch arm the definition has: bulk+approve+notify and
// single+reject+quiet.
func TestWorkflowFullDistributed(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	table := uniqueTableName(t, "qa_full")
	createHighTierTable(t, dsn, table)
	t.Cleanup(func() { dropTable(t, dsn, table) })

	grpcHost := startNotFoundGRPCServer(t)
	enrich := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/enrich" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"enriched": true}`))
	}))
	t.Cleanup(enrich.Close)

	pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Close(ctx)
	})
	// Never log the resolved value: the DSN embeds the MySQL password.
	resolver := func(_ namespace.Namespace, name string) map[string]any {
		if name != workflows.CredentialDB {
			return nil
		}
		return map[string]any{"driver": "mysql", "dsn": dsn}
	}

	for _, tc := range []struct {
		name         string
		input        map[string]any
		action       string // approval action
		wantDecision string // node carrying the gate outcome, and its decision
		decisionNode string
		ran          []string
		skipped      []string
	}{
		{
			name:         "bulk_approve_notify",
			input:        workflows.AboveThresholdInput(),
			action:       "approve",
			decisionNode: "approved",
			wantDecision: "approved",
			ran:          []string{"fan", "hold", "tick", "approved", "notify"},
			skipped:      []string{"single_lane", "rejected", "timed_out", "quiet"},
		},
		{
			name:         "single_reject_quiet",
			input:        workflows.BelowThresholdInput(),
			action:       "reject",
			decisionNode: "rejected",
			wantDecision: "rejected",
			ran:          []string{"single_lane", "rejected", "quiet"},
			skipped:      []string{"fan", "hold", "tick", "approved", "timed_out", "notify"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def, err := workflows.FullWorkflow(workflows.DefaultFullTriggerConfig()).Definition()
			if err != nil {
				t.Fatalf("Definition: %v", err)
			}
			rowID := "row-" + uniqueSuffix(t)
			workflows.WithVars(def, workflows.FullVars(enrich.URL, "qa@example.test", grpcHost, table, rowID))

			h := newServerRunnerHarnessAs(t, addr, workflows.FullApprover)
			startWorkflowRunner(t, h, "runner-full-"+tc.name, def, pool, resolver)

			// Invoke, not Submit: Submit schedules every root, and five of this
			// definition's roots are triggers that only a trigger firing starts.
			execID := invokeWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, workflows.FullEntryStart, tc.input)

			// The approver identity is the authenticated principal the API
			// server injects; a caller may not claim it in the payload.
			waitNodeSuspendedDumping(t, h, execID, "gate")
			postSignal(t, h, execID, workflows.FullApprovalSignal, map[string]any{
				"action": tc.action,
			})
			if tc.action == "approve" {
				waitNodeSuspendedDumping(t, h, execID, "hold")
				postSignal(t, h, execID, workflows.FullReleaseSignal, map[string]any{"release": true})
			}

			result := waitDistributedCompletionDumping(t, h, execID, workflows.FullMainNodeNames(),
				"renamed", "script", "brief", tc.decisionNode)
			if result.Status != types.ExecutionStatusSuccess {
				dumpNodes(t, h, execID, workflows.FullMainNodeNames()...)
				t.Fatalf("status = %s, want success", result.Status)
			}

			// Transform chain: five seeded rows, one zero-qty dropped, one
			// duplicate id removed, capped at three.
			if count := numericField(t, result.Output["renamed"], "count"); count != 3 {
				t.Fatalf("renamed count = %v, want 3", count)
			}
			if ok, _ := result.Output["script"].(map[string]any)["script_ok"].(bool); !ok {
				t.Fatalf("script output = %#v, want script_ok true", result.Output["script"])
			}

			// brief is the pick over the summary, so every field below crossed
			// set → pick and proves the upstream node produced it.
			brief := result.Output["brief"]
			if rows, ok := summarizeField(t, brief, "rows_loaded").([]any); !ok || len(rows) != 1 {
				t.Fatalf("brief rows_loaded = %#v, want the one inserted row", summarizeField(t, brief, "rows_loaded"))
			}
			if scored, ok := summarizeField(t, brief, "risk_ok").(bool); !ok || scored {
				t.Fatalf("brief risk_ok = %#v, want false (gRPC must take the error port)", summarizeField(t, brief, "risk_ok"))
			}
			if denied, ok := summarizeField(t, brief, "browser_denied").(bool); !ok || !denied {
				t.Fatalf("brief browser_denied = %#v, want true (browser.cdp must fail closed)", summarizeField(t, brief, "browser_denied"))
			}
			if _, leaked := brief.(map[string]any)["tier"]; leaked {
				t.Fatalf("brief = %#v, pick must drop fields it was not asked for", brief)
			}

			if got := stringField(t, result.Output[tc.decisionNode], "decision"); got != tc.wantDecision {
				t.Fatalf("%s decision = %q, want %q", tc.decisionNode, got, tc.wantDecision)
			}

			for _, name := range tc.ran {
				if st := nodeStatus(t, h, execID, name); st != types.NodeStatusSuccess {
					dumpNodes(t, h, execID, workflows.FullMainNodeNames()...)
					t.Fatalf("node %q status = %s, want success", name, st)
				}
			}
			// The trigger lane was not selected, so it is skipped end to end.
			for _, name := range append(append([]string{}, tc.skipped...), workflows.FullTriggerNodeNames()...) {
				if st := nodeStatus(t, h, execID, name); st != types.NodeStatusSkipped {
					dumpNodes(t, h, execID, workflows.FullMainNodeNames()...)
					t.Fatalf("node %q status = %s, want skipped", name, st)
				}
			}
		})
	}
}

// TestWorkflowFullTriggersFire hosts FullWorkflow on an embedded engine and
// fires its trigger lane from every kind this environment can reach: timer and
// cron by their own schedules, webhook by a real HTTP request, redis by a real
// stream message, and kafka by a real topic message when a broker is present.
//
// Redis is required, as elsewhere in this suite. Kafka is optional because the
// definition is still registered with the kafka entry armed: an absent broker
// is that trigger's own retry loop and does not affect the other four. Under
// XFLOW_REQUIRE_KAFKA_INTEGRATION=1 its absence fails the test instead.
func TestWorkflowFullTriggersFire(t *testing.T) {
	redisAddr := requireRedis(t)
	brokers := kafkaBrokers(t)
	kafkaUp := kafkaUsable(brokers)
	if !kafkaUp && os.Getenv("XFLOW_REQUIRE_KAFKA_INTEGRATION") == "1" {
		t.Fatalf("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: kafka unavailable at %v", brokers)
	}

	suffix := uniqueSuffix(t)
	cfg := workflows.FullTriggerConfig{
		TimerInterval:  time.Second,
		CronExpression: "@every 1s",
		RedisAddr:      redisAddr,
		RedisStream:    "xflow:qa:full:" + suffix,
		RedisGroup:     "xflow-qa-full-" + suffix,
		KafkaBrokers:   brokers,
		KafkaTopic:     uniqueTopic("xflow-qa-full"),
		KafkaGroup:     uniqueTopic("xflow-qa-full-group"),
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() {
		rdb.Del(context.Background(), cfg.RedisStream)
		_ = rdb.Close()
	})
	if kafkaUp {
		newKafkaTopic(t, brokers, cfg.KafkaTopic, 1)
	}

	eng, obs := triggerEngine(t)
	ctx := context.Background()
	if _, err := eng.AddWorkflow(ctx, workflows.FullWorkflow(cfg)); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	srv := httptest.NewServer(eng.WebhookHandler())
	t.Cleanup(srv.Close)
	resp, err := srv.Client().Post(srv.URL+workflows.FullWebhookPath, "application/json",
		strings.NewReader(`{"source":"full-webhook"}`))
	if err != nil {
		t.Fatalf("webhook: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status = %d, want 202", resp.StatusCode)
	}
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: cfg.RedisStream, Values: map[string]any{"body": `{"source":"full-redis"}`}}).Err(); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	if kafkaUp {
		writeKafkaMessages(t, brokers, cfg.KafkaTopic, []kafka.Message{{Value: []byte("full-kafka")}})
	}

	want := map[string]string{"timer": workflows.FullEntryTimer, "cron": workflows.FullEntryCron,
		"webhook": workflows.FullEntryWebhook, "redis": workflows.FullEntryRedis}
	if kafkaUp {
		want["kafka"] = workflows.FullEntryKafka
	} else {
		t.Logf("kafka unavailable at %v: the kafka entry is armed but not fired", brokers)
	}

	fired := map[string]types.ExecutionID{}
	checked := map[types.ExecutionID]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for len(fired) < len(want) {
		if time.Now().After(deadline) {
			t.Fatalf("trigger kinds fired = %v, want all of %v", fired, want)
		}
		for _, id := range obs.executions() {
			if checked[id] {
				continue
			}
			checked[id] = true
			kind := fullTriggerKind(t, eng, id)
			if _, ok := fired[kind]; !ok {
				fired[kind] = id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Per kind: its own entry ran, the four siblings and the whole main lane
	// resolved as skipped.
	for kind, entry := range want {
		id := fired[kind]
		for _, name := range workflows.FullTriggerNodeNames() {
			wantStatus := types.NodeStatusSkipped
			switch name {
			case entry, "trigger_join", "trigger_record", "trigger_done":
				wantStatus = types.NodeStatusSuccess
			}
			if got := embeddedNodeStatus(t, eng, id, name); got != wantStatus {
				t.Errorf("%s run: node %s = %s, want %s", kind, name, got, wantStatus)
			}
		}
		for _, name := range []string{"start", "seed", "gate", "summarize", "done"} {
			if got := embeddedNodeStatus(t, eng, id, name); got != types.NodeStatusSkipped {
				t.Errorf("%s run: main-lane node %s = %s, want skipped", kind, name, got)
			}
		}
	}
}

// fullTriggerKind waits for one trigger-lane execution, asserts it succeeded,
// and returns the kind its recording node captured.
func fullTriggerKind(t *testing.T, eng *xflow.Engine, id types.ExecutionID) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), workflowRunTimeout)
	defer cancel()
	res, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait(%s): %v", id, err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution %s status = %s (%s), want success", id, res.Status, res.Error)
	}
	detail, err := eng.Inspect(context.Background(), id, "trigger_record")
	if err != nil || len(detail.Nodes) != 1 {
		t.Fatalf("Inspect(trigger_record): %v", err)
	}
	out := detail.Nodes[0].Output
	if lane, _ := out["lane"].(string); lane != "trigger" {
		t.Fatalf("trigger_record output = %#v, want lane=trigger", detail.Nodes[0].Output)
	}
	kind, _ := recordedEvent(t, out)["kind"].(string)
	return kind
}

func embeddedNodeStatus(t *testing.T, eng *xflow.Engine, id types.ExecutionID, name string) types.NodeStatus {
	t.Helper()
	d, err := eng.Inspect(context.Background(), id, name)
	if err != nil || len(d.Nodes) != 1 {
		t.Fatalf("Inspect(%s, %s): %v", id, name, err)
	}
	return d.Nodes[0].Status
}

// waitNodeSuspendedDumping is waitNodeSuspended that, on timeout, first dumps
// every main-lane node so the failure names the node the run stuck on.
func waitNodeSuspendedDumping(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, nodeName string) {
	t.Helper()
	deadline := time.Now().Add(workflowRunTimeout)
	for time.Now().Before(deadline) {
		if snap, err := h.state.GetNode(context.Background(), id, nodeName); err == nil && snap != nil &&
			snap.Status == types.NodeStatusSuspended {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	dumpNodes(t, h, id, workflows.FullMainNodeNames()...)
	t.Fatalf("timeout waiting for node %q to suspend", nodeName)
}

// kafkaUsable reports whether brokers answer a metadata request, not merely a
// TCP dial: a proxy or a half-started broker can accept the connection and
// then reset it, which pingKafka alone reports as reachable.
func kafkaUsable(brokers []string) bool {
	if pingKafka(brokers) != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return false
	}
	defer conn.Close()
	_, err = conn.Controller()
	return err == nil
}
