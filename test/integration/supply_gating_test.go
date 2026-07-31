//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// This file covers the second highest-priority regression the design flags:
// the supply readiness gate must sit at the ACTIVATION layer, not the
// execution layer. If a node checked readiness per message instead, the Kafka
// consumer group would already be live and committing offsets by the time the
// check ran — a permanent per-message failure with no retry policy then
// evaporates that traffic silently (no dead letter, no trace, offset already
// advanced). Declining the ACTIVATION instead means the Kafka reader /
// consumer group is never even constructed while not ready: messages stay in
// the topic, consumer lag is the visible signal, and a later reconnect picks
// them all up once content appears.
//
// IMPORTANT correction versus the brief's assumed self-heal mechanism: the
// brief describes "decline and retry next reconcile round" as if periodic
// background Reconcile alone recovers a declined activation. I verified this
// experimentally (see the report) and it is FALSE as currently implemented:
// EntryActivationReconciler.reconcileOne renews an assigned activation's lease
// indefinitely as long as its runner keeps heartbeating and stays selector-
// matching — it never checks whether the runner's Activate actually
// succeeded. A runner whose SupplyGate declines therefore holds the
// assignment (silently, forever) under continuous heartbeats; nothing in the
// current wiring re-triggers Activate. The ONLY mechanism that recovers a
// declined activation today is a runner RESTART: a fresh Register call
// reports an empty (or non-matching) inventory, which ReconcileRunnerInventory
// turns into a Fence+Deactivate, and the next Reconcile pass reassigns with a
// fresh Activate directive. protocol.ActivationAck /
// protocol.ActivationAckPath exist in the wire types but are never sent by
// the runner or consumed by the server — there is no per-activation
// success/failure signal today. This is exactly the gap Task 19 ("两层分发
// 心跳 hint 与 observed 上报" — heartbeat hint + observed reporting) is
// scoped to close; it is correctly still pending, not a defect introduced by
// this task. The tests below assert what production actually does today
// (restart recovers; continuous background reconcile alone does not), rather
// than asserting a self-heal path that does not exist yet.
//
// Coverage note (per the task-18 brief addendum §5's fallback instruction):
// this harness wires real Kafka + real Redis end-to-end through the same
// production code path cmd/runner uses (TriggerActivationHandler +
// SupplyGate + ActivationTracker + EntryActivationReconciler), so the four
// "no message loss" assertions in the brief are exercised, adapted to the
// self-heal mechanism verified above. What is NOT exercised: cmd/runner's own
// process-level reconnect/backoff loop (runWithReconnect in cmd/runner/run.go)
// — this test drives Runner.Run/ActivationTracker/reconciler directly, same
// as the existing i_remote_trigger_hosting_e2e_test.go, and constructs a
// second Runner instance to stand in for a restarted process.

// gatingTriggerBodyType is the downstream action node the gated trigger's
// boundary output fans out to. Registered once per process (idempotent via
// registry.Register).
const gatingTriggerBodyType = "test.supplygating.body"

// gatingBodyHandler counts successful executions so the test can assert
// sent == processed.
type gatingBodyHandler struct{ counter *gatingCounter }

func (h gatingBodyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: gatingTriggerBodyType, Kind: types.NodeKindAction}
}

func (h gatingBodyHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.counter.add(1)
	return &types.Output{Data: map[string]any{"handled": true}}, nil
}

// gatingCounter is a tiny concurrency-safe counter — a package-level handler
// instance is shared across all executions the runner dispatches, and the
// runner's worker pool may call Execute concurrently.
type gatingCounter struct {
	mu    chanMutex
	value int64
}

type chanMutex chan struct{}

func newGatingCounter() *gatingCounter {
	c := &gatingCounter{mu: make(chanMutex, 1)}
	c.mu <- struct{}{}
	return c
}

func (c *gatingCounter) add(n int64) {
	<-c.mu
	c.value += n
	c.mu <- struct{}{}
}

func (c *gatingCounter) load() int64 {
	<-c.mu
	v := c.value
	c.mu <- struct{}{}
	return v
}

// registryTriggerLookupGating adapts the global node registry to the runner's
// TriggerHandlerLookup interface — the same adapter
// i_remote_trigger_hosting_e2e_test.go uses under a different name (types are
// unexported per-file so there is no collision).
type registryTriggerLookupGating struct{}

func (registryTriggerLookupGating) Trigger(nodeType string) (types.TriggerHandler, bool) {
	return registry.LookupTrigger(nodeType)
}

// authedTransport sets a bearer token on every outbound request. Used both for
// this test's direct HTTP calls (register workflow, PUT supply content) and for
// the runner's protocol client and HTTPSupplyFetcher, mirroring how a real
// deployment attaches one static token to every call a runner makes.
type authedTransport struct {
	token string
	base  http.RoundTripper
}

func (a authedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+a.token)
	return a.base.RoundTrip(req)
}

func authedClient(token string) *http.Client {
	return &http.Client{Transport: authedTransport{token: token, base: http.DefaultTransport}}
}

// putSupplyContent PUTs content to /v1/supplies/{resource}, failing the test
// on any non-200.
func putSupplyContent(t *testing.T, baseURL string, client *http.Client, resource string, content []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, baseURL+"/v1/supplies/"+resource, strings.NewReader(string(content)))
	if err != nil {
		t.Fatalf("build PUT /v1/supplies/%s: %v", resource, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT /v1/supplies/%s: %v", resource, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT /v1/supplies/%s status = %d", resource, resp.StatusCode)
	}
}

// newSupplyGatingControlPlane wires a real Redis-backed EntryActivationStore
// (via distributed.New, mirroring TestRemoteTriggerHosting_Redis) plus the
// in-memory supply content store and a single bearer-token principal carrying
// every scope this test's HTTP calls need (workflow register, execution seed,
// supply read/write). It flushes asynq + xflow:* keys first: this Redis
// instance is shared across test runs/processes, and a stale runner-directory
// entry or leftover asynq task from a prior run/crash would otherwise race
// this test's own state (observed: flaky "round 1 must assign ..." and
// over-counted processed messages before this flush was added).
func newSupplyGatingControlPlane(t *testing.T, redisAddr string) (*httptest.Server, *control.ControlPlane, string) {
	t.Helper()
	const token = "supply-gating-test-token-0123456789ab"

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	flushAsynqKeys(context.Background(), t, rdb)
	flushXflowKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	be, err := distributed.New(redisAddr, nil, distributed.WithConsumer(true), distributed.WithConcurrency(1))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	entryStore := be.NewEntryActivationStore(time.Minute)

	cp, err := control.NewControlPlane(control.Config{
		Backend:              be,
		EntryActivationStore: entryStore,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	supplies := memstore.New()
	srv, err := apiserver.New(apiserver.Config{
		Supplies:      supplies,
		PrincipalAuth: apiserver.NewBearerPrincipalAuth(token, "gating-test", []string{"workflow", "execution", "supply.write", "supply.read"}),
		Authorizer:    apiserver.ScopeAuthorizer{},
		AuditSink:     apiserver.NewInMemoryAuditSink(),
	}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("apiserver.Start: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	return httpSrv, cp, token
}

// newSupplyGatingRunner builds an ActivationTracker using the PRODUCTION
// construction path (cmd/runner/run.go): TriggerActivationHandler wrapped
// with WithSupplyGate, whose gate is a real HTTPSupplyFetcher hitting the
// test server's /v1/supplies/{name} endpoint. reg is an ISOLATED
// node/supply.Registry (never supply.Default) so this test cannot leak state
// into, or be polluted by, any other test in this build sharing the process.
func newSupplyGatingRunner(baseURL, token string, reg *supply.Registry) *runnersvc.ActivationTracker {
	lookup := registryTriggerLookupGating{}
	fetcher := &runnersvc.HTTPSupplyFetcher{BaseURL: baseURL, Token: token}
	gate := runnersvc.NewSupplyGate(fetcher, reg, slog.Default())
	handler := runnersvc.NewTriggerActivationHandler(baseURL, token, lookup, runnersvc.WithSupplyGate(gate))
	return runnersvc.NewActivationTracker(handler, slog.Default())
}

// restartRunner simulates a full process restart: it cancels the given
// context (stopping the old Runner.Run goroutine and closing its
// subscriptions) and starts a brand-new Runner with a FRESH ActivationTracker
// (empty inventory — a new process remembers nothing) under a new
// cancellable context, using the SAME RunnerID (a real restart keeps its
// identity). It waits for the old Run() goroutine to return and for the new
// runner to complete Register before returning, so the caller can immediately
// call Reconcile and see the effect of ReconcileRunnerInventory's revoke.
func restartRunner(t *testing.T, parent context.Context, cp *control.ControlPlane, oldCancel context.CancelFunc, oldRunErr <-chan error, baseURL, token, runnerID string, labels map[string]string, caps []protocol.Capability, tracker *runnersvc.ActivationTracker) (newCtx context.Context, newCancel context.CancelFunc, newRunErr <-chan error) {
	t.Helper()
	oldCancel()
	select {
	case <-oldRunErr:
	case <-time.After(3 * time.Second):
		t.Fatal("old runner did not stop within 3s of cancellation")
	}

	ctx2, cancel2 := context.WithCancel(parent)
	client := authedClient(token)
	runner2 := runnersvc.New(
		protocol.NewClient(baseURL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:          runnerID,
			Concurrency:       1,
			Labels:            labels,
			Capabilities:      caps,
			HeartbeatInterval: 100 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
		},
	)
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- runner2.Run(ctx2) }()
	waitForE2ERunner(t, cp.RunnerDirectory(), runnerID)
	return ctx2, cancel2, runErr2
}

// assertNoConsumerGroup asserts the given consumer group currently has no
// members on the real broker — i.e. no reader was ever constructed for it. A
// group that was never joined surfaces as an error or an empty member list on
// DescribeGroups; either outcome satisfies "no reader was ever constructed".
func assertNoConsumerGroup(t *testing.T, brokers []string, group string) {
	t.Helper()
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	resp, err := client.DescribeGroups(context.Background(), &kafka.DescribeGroupsRequest{
		GroupIDs: []string{group},
	})
	if err != nil {
		// A never-joined group being unknown to the broker is a legitimate
		// "does not exist" outcome — exactly what this assertion wants.
		return
	}
	for _, g := range resp.Groups {
		if g.GroupID != group {
			continue
		}
		if len(g.Members) != 0 {
			t.Fatalf("consumer group %q has %d member(s); the Kafka reader must never be constructed while the supply gate declines", group, len(g.Members))
		}
	}
}

// TestSupplyGateLosesNoMessages is the highest-priority regression this whole
// design flags in spec §7-3/§2.13 (alongside the $config seam covered in
// wasm_supply_seam_test.go). It asserts the readiness gate lives at the
// ACTIVATION layer, not the execution layer, driving the exact production
// wiring: control-plane EntryActivationReconciler -> heartbeat directive ->
// runner ActivationTracker -> TriggerActivationHandler -> SupplyGate.Admit ->
// (only if admitted) the real Kafka trigger's Activate.
//
// The four assertions, in order (see the file-level comment above for why
// (c) is proven via a runner restart rather than periodic Reconcile alone):
//
//	(a) with require_ready:true and no content, the runner does NOT construct
//	    the Kafka consumer — proven by an empty ActivationTracker.Inventory()
//	    and by no consumer group ever appearing on the broker;
//	(b) messages produced to the topic are NOT consumed while content is
//	    absent: processed stays 0 across two reconcile rounds;
//	(c) after the content is PUT and the runner restarts (the verified
//	    self-heal path today), the fresh session's Activate succeeds and the
//	    runner takes over;
//	(d) every message sent before the takeover is eventually processed:
//	    sent == processed, zero loss.
func TestSupplyGateLosesNoMessages(t *testing.T) {
	brokers := requireKafka(t)
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	counter := newGatingCounter()
	registry.Register(gatingBodyHandler{counter: counter})

	topic := uniqueTopic("xflow-supply-gating")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 1)

	// Produce the backlog BEFORE any activation attempt — this is the
	// production scenario the regression is about: a runner picking up a
	// topic that already has traffic waiting, not a race against messages
	// arriving after the fact.
	const n = 8
	msgs := make([]kafka.Message, 0, n)
	for i := 0; i < n; i++ {
		msgs = append(msgs, kafka.Message{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	writeKafkaMessages(t, brokers, topic, msgs)

	httpSrv, cp, token := newSupplyGatingControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin"
		bodyUnitID  = "body"
		supplyNode  = "rules"
		supplyRes   = "gating-rules"
		zoneLabel   = "gate"
		runnerID    = "runner-gate"
	)

	def := &types.WorkflowDef{
		// Name must be unique per test run: AddWorkflow keys on (namespace, Name,
		// Version) and treats a different DefinitionHash under the same key as a
		// conflict (500) rather than a fresh workflow — and this def embeds a
		// freshly generated topic name, so its hash differs every run.
		Name:           topic,
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: "xflow.trigger.kafka", Parameters: map[string]any{
				"brokers":      brokers,
				"topic":        topic,
				"group":        group,
				"start_offset": "earliest",
			}},
			{Name: bodyUnitID, Kind: types.NodeKindAction, Type: gatingTriggerBodyType},
			{Name: supplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {{Node: bodyUnitID, Input: "main"}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: entryUnitID, Supply: supplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	key := engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: wfID, WorkflowVersion: wfVersion, EntryUnitID: entryUnitID,
	}
	labels := map[string]string{"zone": zoneLabel}
	caps := []protocol.Capability{{NodeType: "xflow.trigger.kafka"}, {NodeType: gatingTriggerBodyType}}

	reg := supply.NewRegistry()
	tracker := newSupplyGatingRunner(httpSrv.URL, token, reg)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:          runnerID,
			Concurrency:       1,
			Labels:            labels,
			Capabilities:      caps,
			HeartbeatInterval: 100 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(runnerCtx) }()
	waitForE2ERunner(t, cp.RunnerDirectory(), runnerID)

	// --- reconcile round 1: assigns runner-gate (assignment always happens
	// before Activate runs), but the runner's Activate declines — SupplyGate.
	// Admit returns *NotReadyError BEFORE the Kafka trigger's Activate (and
	// therefore its reader / consumer group) is ever constructed. ---
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile (round 1): %v", err)
	}
	act, _, err := cp.EntryActivationStore().Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after round 1: %v", err)
	}
	if act.RunnerID != runnerID {
		t.Fatalf("round 1 must assign %s, got %q", runnerID, act.RunnerID)
	}
	gen1 := act.Generation

	// --- (a): give the failed Activate call time to actually run and fail,
	// then assert the runner's inventory does NOT contain this activation.
	// The wait is generous (well past a Kafka consumer group's join+first-poll
	// latency) so a regression that starts consuming before/without the gate
	// has time to show up in (a)/(b) below rather than racing past them. ---
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) != 0 {
		t.Fatalf("(a) runner inventory must be empty (Activate declined, never hosted), got %#v", got)
	}
	assertNoConsumerGroup(t, brokers, group)

	// --- (b): across two more reconcile periods with no content, nothing is
	// consumed — the consumer group never formed, so there is nothing to have
	// committed an offset, and the processed count stays 0. Also confirm
	// continuous background reconciliation alone does NOT resend Activate: the
	// generation must stay exactly gen1 (this is the corrected understanding
	// documented at the top of this file). ---
	for i := 0; i < 2; i++ {
		if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
			t.Fatalf("Reconcile (still not ready, pass %d): %v", i, err)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if got := counter.load(); got != 0 {
		t.Fatalf("(b) processed count = %d, want 0 while supply is not ready — a message was consumed with no rules applied", got)
	}
	assertNoConsumerGroup(t, brokers, group)
	actStillPending, _, _ := cp.EntryActivationStore().Get(ctx, key)
	if actStillPending.Generation != gen1 {
		t.Fatalf("generation changed to %d without a restart — periodic Reconcile alone must not resend Activate (see file header)", actStillPending.Generation)
	}

	// --- PUT the supply content: the gate's next Admit will fetch and apply
	// it. ---
	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[]}`))

	// --- (c): restart the runner (fresh Register -> ReconcileRunnerInventory
	// revokes the stale assignment -> next Reconcile reassigns with a fresh
	// generation and a fresh Activate directive, which now succeeds). This is
	// the verified self-heal mechanism (see file header). ---
	tracker2 := newSupplyGatingRunner(httpSrv.URL, token, reg)
	_, runnerCancel2, runErr2 := restartRunner(t, ctx, cp, runnerCancel, runErr, httpSrv.URL, token, runnerID, labels, caps, tracker2)

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		act, _, _ := cp.EntryActivationStore().Get(ctx, key)
		if act.RunnerID == "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile (round after restart): %v", err)
	}
	act2, _, err := cp.EntryActivationStore().Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after restart reconcile: %v", err)
	}
	if act2.Generation <= gen1 {
		t.Fatalf("(c) expected a fresh generation after restart, got %d (was %d)", act2.Generation, gen1)
	}

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(tracker2.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker2.Inventory(); len(got) == 0 {
		t.Fatal("(c) runner must host the activation after content appears and it restarts")
	}

	// --- (d): all N messages sent before the takeover must still be
	// processed — nothing was lost while the gate declined. ---
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && counter.load() < n {
		time.Sleep(100 * time.Millisecond)
	}
	if got := counter.load(); got != int64(n) {
		t.Fatalf("(d) processed = %d, want %d — sent == processed, zero loss", got, n)
	}

	runnerCancel2()
	select {
	case <-runErr2:
	case <-time.After(3 * time.Second):
	}
}

// TestSupplyGateRecoversOnRestart is a focused version of the recovery
// assertion in TestSupplyGateLosesNoMessages, run standalone so a future
// regression confined to just the retry path (not Kafka message delivery)
// fails on its own without also depending on message accounting. It also
// makes explicit the negative half of the corrected understanding documented
// at the top of this file: periodic background Reconcile alone, with the
// runner continuously live and selector-matching, must NOT advance the
// generation — only a restart does.
func TestSupplyGateRecoversOnRestart(t *testing.T) {
	brokers := requireKafka(t)
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	topic := uniqueTopic("xflow-supply-gating-selfheal")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 1)

	httpSrv, cp, token := newSupplyGatingControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()

	const (
		wfVersion   = "1"
		entryUnitID = "kin2"
		bodyUnitID  = "body2"
		supplyNode  = "rules2"
		supplyRes   = "gating-rules-selfheal"
		zoneLabel   = "gate2"
		runnerID    = "runner-gate2"
	)
	counter := newGatingCounter()
	registry.Register(gatingBodyHandler{counter: counter})

	def := &types.WorkflowDef{
		Name:           topic,
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: "xflow.trigger.kafka", Parameters: map[string]any{
				"brokers": brokers, "topic": topic, "group": group, "start_offset": "earliest",
			}},
			{Name: bodyUnitID, Kind: types.NodeKindAction, Type: gatingTriggerBodyType},
			{Name: supplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {{Node: bodyUnitID, Input: "main"}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: entryUnitID, Supply: supplyNode}},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)
	key := engine.EntryActivationKey{Namespace: namespace.Default, WorkflowID: wfID, WorkflowVersion: wfVersion, EntryUnitID: entryUnitID}
	labels := map[string]string{"zone": zoneLabel}
	caps := []protocol.Capability{{NodeType: "xflow.trigger.kafka"}, {NodeType: gatingTriggerBodyType}}

	reg := supply.NewRegistry()
	tracker := newSupplyGatingRunner(httpSrv.URL, token, reg)
	runnerCtx, runnerCancel := context.WithCancel(ctx)
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:          runnerID,
			Concurrency:       1,
			Labels:            labels,
			Capabilities:      caps,
			HeartbeatInterval: 100 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(runnerCtx) }()
	waitForE2ERunner(t, cp.RunnerDirectory(), runnerID)

	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile (round 1): %v", err)
	}
	act, _, err := cp.EntryActivationStore().Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after round 1: %v", err)
	}
	gen1 := act.Generation
	if act.RunnerID != runnerID {
		t.Fatalf("round 1 must assign %s, got %q", runnerID, act.RunnerID)
	}

	// Confirm the decline before any content or restart.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) != 0 {
		t.Fatalf("expected the activation declined (empty inventory) before content appears, got %#v", got)
	}

	// Negative half: several more reconcile rounds with the SAME session
	// (still live, still selector-matching) must NOT advance the generation —
	// there is no per-activation success signal for the reconciler to act on.
	for i := 0; i < 3; i++ {
		if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
			t.Fatalf("Reconcile (pre-restart pass %d): %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	actUnchanged, _, _ := cp.EntryActivationStore().Get(ctx, key)
	if actUnchanged.Generation != gen1 {
		t.Fatalf("generation advanced to %d under continuous reconcile with no restart — periodic Reconcile alone must not resend Activate", actUnchanged.Generation)
	}

	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[]}`))

	// Positive half: a restart (fresh Register -> revoke -> reassign) DOES
	// recover it, with the runner process's own action (restarting) being the
	// only "manual" step — no operator touches the activation record directly.
	tracker2 := newSupplyGatingRunner(httpSrv.URL, token, reg)
	_, runnerCancel2, runErr2 := restartRunner(t, ctx, cp, runnerCancel, runErr, httpSrv.URL, token, runnerID, labels, caps, tracker2)

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		act, _, _ := cp.EntryActivationStore().Get(ctx, key)
		if act.RunnerID == "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile (round after restart): %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(tracker2.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker2.Inventory(); len(got) == 0 {
		t.Fatal("self-heal failed: the runner never took over the activation after content appeared and it restarted")
	}

	runnerCancel2()
	select {
	case <-runErr2:
	case <-time.After(3 * time.Second):
	}
}
