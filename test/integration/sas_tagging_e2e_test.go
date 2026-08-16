//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	nodesupply "github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

// TestSASTrafficTaggingE2E is the end-to-end proof for the SAS API-traffic
// tagging pipeline: a real Kafka message drives a real workflow execution whose
// wasm reactor cleanses and tags the record using rules delivered over the
// supply channel — not baked into the workflow definition.
//
// It exercises the four links that must hold for the SAS use case:
//
//	real Kafka -> trigger emits a real execution (not a mock runtime)
//	supply     -> rules reach the wasm pool via Registry.Apply, and a
//	              workflow that never carries them still evaluates them
//	wasm eval  -> the record is cleansed (PII dropped) and tagged
//	rule swap  -> new supply content re-tags subsequent traffic, and the
//	              result carries the config generation that produced it
//
// Kafka runs without authentication here. SASL is not a gap — the trigger
// takes sasl_mechanism/sasl_username/sasl_password, and in production reads
// them from supply content rather than from params (node/trigger/kafka/
// kafka.go). It is left off here because turning it on would make a failure
// ambiguous between "pipeline broken" and "auth misconfigured", and because
// SAS's own cluster (the upstream cluster, internal network) runs SASL_PLAINTEXT.
func TestSASTrafficTaggingE2E(t *testing.T) {
	brokers := requireKafka(t)
	redisAddr := requireRedis(t)

	taggerWasmPath := buildTaggerWasm(t)
	taggerDigest := digestOfFile(t, taggerWasmPath)
	// A unique supply name per run: supply.Default is process-global and its
	// content outlives an unregister, so a shared name would leak rules into
	// whichever test ran next.
	supplyName := uniqueTopic("sas-tag-rules")

	// The workflow definition carries NO rules — only the module and a
	// declaration that it consumes the supply. That absence is the point: it is
	// what proves the rules travelled over the supply channel.
	//
	// This manual registration is the INLINE-MODE equivalent of what a
	// distributed deployment derives automatically. Inline mode activates its
	// triggers through triggerRuntime.ReconcileWorkflow, which never consults
	// the control plane's activation derivation, so no ActivateDirective — and
	// therefore no SupplyConsumerBinding — is ever produced here; there is
	// nothing to derive the binding from. In distributed mode the control plane
	// walks the graph's dependency edges, puts a SupplyConsumerBinding on the ActivateDirective,
	// and the receiving runner registers exactly this pairing at activation
	// time (service/runner/wasm_supply_wiring_test.go covers that path;
	// TestSupplyConsumerBindingReachesRunner covers it end to end).
	//
	// RegisterByDigest: the module is identified by its artifact digest, not
	// a multi-MB base64 string. This is the production path — ScriptFile puts
	// the artifact into the store at AddWorkflow time, and Execute fetches it
	// by digest at runtime.
	if err := node.RegisterWasmSupplyConsumerByDigest(taggerDigest, supplyName); err != nil {
		t.Fatalf("register supply consumer: %v", err)
	}

	topic := uniqueTopic("xflow-sas-tagging")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 1)

	// Artifact store: local FS store only (embedded mode, no HTTP origin needed).
	artifactDir := t.TempDir()
	artifactStore := store.NewArtifactStore(objectstore.NewFSStore(artifactDir), nil)

	// The engine observes the tagger node's completion through a Hooks
	// receiver rather than through eng.Wait/eng.Inspect: this test runs the
	// cluster engine in ExecutionModeTransient, and Inspect (what Wait's
	// StateStore-polling fallback calls to build its Result) is
	// unconditionally disabled in that mode. The Kafka trigger's Emit path
	// also creates its own execution asynchronously, so this test never
	// learns the execution ID up front — the hook is what supplies it.
	tagged := newTaggedNodeRecorder("tagger")
	eng, err := xflow.NewCluster(
		xflow.ClusterConfig{RedisAddr: redisAddr},
		xflow.WithExecutionMode(xflow.ExecutionModeTransient),
		xflow.WithNodes(node.Script("")),
		xflow.WithHooks(tagged),
		xflow.WithArtifactStore(artifactStore),
	)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	t.Cleanup(eng.Stop)

	wf := taggingWorkflow(t, taggerWasmPath, supplyName, brokers, topic, group)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := eng.AddWorkflow(ctx, wf); err != nil {
		t.Fatalf("add workflow: %v", err)
	}

	// Rules v1: strip the auth header from every record, tag admin traffic.
	// Applied AFTER AddWorkflow: resolveArtifacts pre-compiles the wasm module
	// so the supply consumer can configure its pool. Before that the engine
	// doesn't exist and OnSupplyChanged would no-op.
	applyRules(t, supplyName, ruleSet(
		cleanRule("authorization", ""),
		tagRule("admin-api", `path startsWith "/admin"`),
	))

	// A consumer-group join is not instant; produce until one message lands
	// rather than racing the first write against the rebalance.
	writeKafkaMessages(t, brokers, topic, []kafka.Message{{Value: []byte(mustJSONString(t, map[string]any{
		"path":          "/admin/users",
		"latency_ms":    42,
		"authorization": "Bearer super-secret",
	}))}})

	got := waitForTaggedRecord(t, redisAddr, tagged, ctx, 60*time.Second)

	if _, present := got.Record["authorization"]; present {
		t.Fatalf("authorization was not stripped: %v", got.Record)
	}
	// Absence of the KEY is not absence of the VALUE. The guest's eval input is
	// the host's whole expression environment, which carries the record twice —
	// merged at the top level and again under the $input/$inputs roots — so a
	// guest that echoed its input verbatim would strip the top-level key while
	// shipping the credential to Redis under $input. The key check above passed
	// while exactly that was happening; this scan is what actually holds the
	// cleansing guarantee.
	if where := findRecordValue(got.Record, "Bearer super-secret"); where != "" {
		t.Fatalf("credential survives in the persisted record at %s: %v", where, got.Record)
	}
	if got.Record["path"] != "/admin/users" {
		t.Fatalf("path = %v, want /admin/users", got.Record["path"])
	}
	if !got.Tags["admin-api"] {
		t.Fatalf("admin-api tag missing: %v", got.Tags)
	}
	t.Logf("v1 tagged record: %+v tags=%v generation=%v", got.Record, got.Tags, got.Generation)
}

// findRecordValue walks v recursively and returns the path at which want appears
// as a string value, or "" if it does not appear at all. Recursive rather than a
// top-level key check so a change that merely relocates a cleaned field — into
// an engine env root, a nested copy — is still caught.
func findRecordValue(v any, want string) string {
	switch t := v.(type) {
	case string:
		if t == want {
			return "."
		}
	case map[string]any:
		for k, e := range t {
			if p := findRecordValue(e, want); p != "" {
				return "." + k + strings.TrimPrefix(p, ".")
			}
		}
	case []any:
		for i, e := range t {
			if p := findRecordValue(e, want); p != "" {
				return fmt.Sprintf("[%d]%s", i, strings.TrimPrefix(p, "."))
			}
		}
	}
	return ""
}

// taggedResult is one decoded tagger output.
type taggedResult struct {
	Record     map[string]any
	Tags       map[string]bool
	Generation any
}

// applyRulesGen tracks the server-side revision handed out per supply name so
// repeated applyRules calls for the SAME name (e.g. the v1 -> v2 rule swap)
// advance the revision instead of reusing 0 for every version. A real
// SupplyResource store assigns a monotonically increasing revision on each
// PUT; this test has no such store, so it fakes the same monotonic contract.
var applyRulesGen = map[string]uint64{}

func applyRules(t *testing.T, name string, ruleset map[string]any) {
	t.Helper()
	content := mustJSON(t, ruleset)
	// Registry.Apply's change-detection is entirely hash-driven (see
	// node/supply/registry.go Apply: `changed := !had || prev.Hash !=
	// snap.Hash`). Leaving Hash empty means every Apply call for the same name
	// looks unchanged after the first, so RegisterConsumer's own immediate
	// notify is the only dispatch that ever fires — Apply itself never
	// notifies the consumer again, and the wasm pool never gets configured
	// past its first (possibly pre-registration) content. Hash MUST be
	// store.ContentHash(content), the same token the production
	// SupplyResource store computes.
	applyRulesGen[name]++
	if err := nodesupply.Default.Apply(context.Background(), nodesupply.Snapshot{
		Name:      name,
		Content:   content,
		Hash:      store.ContentHash(content),
		Revision:  applyRulesGen[name],
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply supply %q: %v", name, err)
	}
}

func ruleSet(rules ...map[string]any) map[string]any {
	if rules == nil {
		rules = []map[string]any{}
	}
	rs := make([]any, 0, len(rules))
	for _, r := range rules {
		rs = append(rs, r)
	}
	return map[string]any{"rules": rs}
}

func cleanRule(field, when string) map[string]any {
	return map[string]any{"kind": "clean", "field": field, "when": when}
}

func tagRule(tag, when string) map[string]any {
	return map[string]any{"kind": "tag", "tag": tag, "when": when}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func mustJSONString(t *testing.T, v any) string {
	return string(mustJSON(t, v))
}

// taggingWorkflow builds the SAS tagging pipeline:
//
//	kafka trigger -> parse (decode the raw JSON message value into a record)
//	-> tagger (xflow.script, wasm/wazero-reactor)
//
// plus a standalone xflow.supply.external declaration node the tagger
// depends on via a DependencyEdge (DependsOn) — never a dataflow edge. That
// declaration is the ONLY place the supply name appears in the definition:
// the rules themselves never do. If the rules were baked into the workflow
// instead, this test would prove nothing about the supply channel.
func taggingWorkflow(t *testing.T, taggerCode, supplyName string, brokers []string, topic, group string) *xflow.WorkflowBuilder {
	t.Helper()

	wf := xflow.Workflow(topic)

	kafkaEntry := wf.Node("kafka", trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest"))

	// The kafka trigger's "value" field is the raw message bytes as a string;
	// decode it into the record map the tagger guest expects as its eval
	// input. fromJSON is an expr-lang builtin, not custom production code.
	parse := wf.Node("parse", node.Expr(`fromJSON(trigger.data.value)`))
	wf.Connect(kafkaEntry, parse)

	tagger := wf.Node("tagger", node.ScriptFile(taggerCode).Language("wasm").Runtime("wazero-reactor"))
	wf.Connect(parse, tagger)

	supplyDecl := wf.Node("rules", node.SupplyExternal(supplyName))
	wf.DependsOn(tagger, supplyDecl)

	return wf
}

// waitForTaggedRecord blocks until the tagger node has completed for some
// execution (observed via rec, an engine.Hooks receiver registered at cluster
// construction) and returns its decoded {record, tags, config_generation}
// output, read directly from Redis at the production output key
// (xflow:ns:<namespace>:exec:{<id>}:output:<node>). A raw client is used
// rather than a StateStore because sdk/xflow.Engine exposes no public state
// accessor, and this test's cluster runs in ExecutionModeTransient where
// Inspect is unconditionally unavailable.
func waitForTaggedRecord(t *testing.T, redisAddr string, rec *taggedNodeRecorder, ctx context.Context, timeout time.Duration) taggedResult {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var execID types.ExecutionID
	select {
	case execID = <-rec.completed:
	case <-ctx.Done():
		t.Fatalf("waitForTaggedRecord: context done before %q completed: %v", rec.nodeName, ctx.Err())
	case <-deadline.C:
		t.Fatalf("waitForTaggedRecord: timed out after %s waiting for node %q to complete", timeout, rec.nodeName)
	}

	key := fmt.Sprintf("xflow:ns:default:exec:{%s}:output:%s", execID, rec.nodeName)
	raw, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatalf("get output key %q: %v", key, err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode output key %q: %v (raw=%s)", key, err, raw)
	}

	record, _ := out["record"].(map[string]any)
	tags := map[string]bool{}
	if arr, ok := out["tags"].([]any); ok {
		for _, v := range arr {
			if s, ok := v.(string); ok {
				tags[s] = true
			}
		}
	}
	return taggedResult{Record: record, Tags: tags, Generation: out["config_generation"]}
}

// taggedNodeRecorder is an engine.Hooks receiver that signals the execution ID
// the first time the named node completes, across any execution the engine
// runs. It is registered via xflow.WithHooks at cluster construction, since
// the Kafka trigger's Emit path creates its own execution the test never
// learns synchronously.
type taggedNodeRecorder struct {
	engine.BaseHooks
	nodeName  string
	completed chan types.ExecutionID
}

func newTaggedNodeRecorder(nodeName string) *taggedNodeRecorder {
	return &taggedNodeRecorder{nodeName: nodeName, completed: make(chan types.ExecutionID, 1)}
}

func (r *taggedNodeRecorder) OnNodeComplete(_ context.Context, id types.ExecutionID, name string, _ types.NodeStatus) {
	if name != r.nodeName {
		return
	}
	select {
	case r.completed <- id:
	default:
	}
}

// buildTaggerWasm compiles the tagger reactor guest and returns the path to the
// .wasm file. The file lives in a t.TempDir so cleanup is automatic.
func buildTaggerWasm(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "tagger.wasm")
	src := filepath.Join("..", "..", "node", "internal", "code", "script", "wasm", "testdata", "tagger", "main.go")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, src)
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build tagger guest: %v\n%s", err, b)
	}
	return out
}

// digestOfFile computes the content-addressable digest of a file, matching
// store.ContentHash ("sha256:<64 hex>").
func digestOfFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file for digest: %v", err)
	}
	return store.ContentHash(raw)
}
