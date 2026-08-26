package exprx

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

func seedSupply(t *testing.T, reg *supply.Registry, name, content string, rev uint64) {
	t.Helper()
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: name, Content: []byte(content), Hash: "h-" + content,
		Revision: rev, FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
}

func TestSuppliesExpressionReadsCurrentContent(t *testing.T) {
	reg := supply.NewRegistry()
	seedSupply(t, reg, "rules", `{"threshold":7}`, 3)

	env := BuildExprEnv(&types.Input{}, SuppliesEnv(reg))
	out, err := EvalExpr(`$supplies.rules.threshold`, env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got, ok := out.(float64); !ok || got != 7 {
		t.Fatalf("result = %#v, want 7", out)
	}
}

// The name says "RevisionAndHash". Only $revision was ever evaluated, and
// $hash appears in no test anywhere in the tree -- dropping the injection at
// registry.go left the whole suite green while every staleness check written
// against the documented DSL root silently read nil.
func TestSuppliesExposesRevisionAndHash(t *testing.T) {
	const content = `{"a":1}`
	reg := supply.NewRegistry()
	seedSupply(t, reg, "rules", content, 42)
	env := BuildExprEnv(&types.Input{}, SuppliesEnv(reg))

	rev, err := EvalExpr(`$supplies.rules["$revision"]`, env, false)
	if err != nil {
		t.Fatalf("eval revision: %v", err)
	}
	if got, ok := rev.(uint64); !ok || got != 42 {
		t.Fatalf("revision = %#v, want uint64 42", rev)
	}

	hash, err := EvalExpr(`$supplies.rules["$hash"]`, env, false)
	if err != nil {
		t.Fatalf("eval hash: %v", err)
	}
	// seedSupply publishes Hash: "h-" + content, so the expected value is a
	// statement about what the registry must carry through, not something the
	// registry computes.
	if want := "h-" + content; hash != want {
		t.Fatalf("hash = %#v, want %q", hash, want)
	}
}

// A missing supply must evaluate to nil, not panic: require_ready:false lets a
// node run before content ever arrived.
func TestSuppliesMissingNameIsNil(t *testing.T) {
	env := BuildExprEnv(&types.Input{}, SuppliesEnv(supply.NewRegistry()))
	out, err := EvalExpr(`$supplies.absent`, env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if out != nil {
		t.Fatalf("missing supply = %#v, want nil", out)
	}
}

// The default env must expose $supplies without any caller wiring.
func TestBuildExprEnvAlwaysExposesSupplies(t *testing.T) {
	env := BuildExprEnv(&types.Input{}, nil)
	v, ok := env["$supplies"]
	if !ok {
		t.Fatal("$supplies must be present in the default env")
	}
	if _, ok := v.(map[string]any); !ok {
		t.Fatalf("$supplies = %T, want map[string]any", v)
	}
}

// TestBuildExprEnvDefaultSuppliesReadsDefaultRegistry pins where the default
// $supplies value comes from.
//
// TestBuildExprEnvAlwaysExposesSupplies above checks only that the key exists
// and holds a map, and every other $supplies test injects an isolated registry
// through SuppliesEnv(reg) into extra -- which the merge loop at the end of
// BuildExprEnv writes over the default with. So replacing
// supply.Default.Decoded() with an empty map satisfies the whole package while
// no supply content ever reaches an expression on the paths that pass extra=nil:
// execution/params.go (every node's parameter expansion) and the transform and
// flow nodes (if/switch/split/map/filter/set). A routing condition written
// against $supplies would quietly evaluate to nil and take the other branch,
// with no error anywhere.
//
// supply.Default is process-global and its content is never dropped, so this
// uses a name no other test can collide with rather than trying to clean up.
func TestBuildExprEnvDefaultSuppliesReadsDefaultRegistry(t *testing.T) {
	const name = "exprx_default_env_probe"
	seedSupply(t, supply.Default, name, `{"threshold":7}`, 3)

	env := BuildExprEnv(&types.Input{}, nil)
	out, err := EvalExpr(`$supplies.exprx_default_env_probe.threshold`, env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got, ok := out.(float64); !ok || got != 7 {
		t.Fatalf("$supplies.%s.threshold = %#v, want 7: with extra=nil the default "+
			"env is the only source of supply content, and every node parameter "+
			"expansion plus every flow/transform node takes that path", name, out)
	}
}

// Content that is not a JSON object stays usable.
func TestSuppliesNonObjectContent(t *testing.T) {
	reg := supply.NewRegistry()
	seedSupply(t, reg, "list", `[1,2,3]`, 1)
	seedSupply(t, reg, "raw", "not json at all", 1)
	env := BuildExprEnv(&types.Input{}, SuppliesEnv(reg))

	out, err := EvalExpr(`len($supplies.list)`, env, false)
	if err != nil {
		t.Fatalf("eval list: %v", err)
	}
	if got, ok := out.(int); !ok || got != 3 {
		t.Fatalf("len = %#v, want 3", out)
	}
	raw, err := EvalExpr(`$supplies.raw`, env, false)
	if err != nil {
		t.Fatalf("eval raw: %v", err)
	}
	if b, ok := raw.([]byte); !ok || string(b) != "not json at all" {
		t.Fatalf("raw = %#v, want the original bytes", raw)
	}
}

// $config must keep meaning workflow-level config. This is the regression for
// the key collision that made the wasm reactor read workflow config as rules.
func TestConfigStaysWorkflowConfig(t *testing.T) {
	reg := supply.NewRegistry()
	seedSupply(t, reg, "rules", `{"from":"supply"}`, 1)
	extra := SuppliesEnv(reg)
	env := BuildExprEnv(&types.Input{
		Config: map[string]any{"env": "prod"},
		Data:   map[string]any{"$config": map[string]any{"from": "upstream"}},
	}, extra)

	got, err := EvalExpr(`$config.env`, env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != "prod" {
		t.Fatalf("$config.env = %#v, want prod", got)
	}
	from, err := EvalExpr(`$supplies.rules.from`, env, false)
	if err != nil {
		t.Fatalf("$supplies must be independent of $config: %v", err)
	}
	if from != "supply" {
		t.Fatalf("$supplies.rules.from = %#v, want supply", from)
	}
}

// The published map must never be mutated in place: concurrent expression
// evaluation reads it without a lock.
func TestDecodedMapIsReplacedNotMutated(t *testing.T) {
	reg := supply.NewRegistry()
	seedSupply(t, reg, "rules", `{"v":1}`, 1)
	first := reg.Decoded()
	seedSupply(t, reg, "rules", `{"v":2}`, 2)
	second := reg.Decoded()

	if firstVal := first["rules"].(map[string]any)["v"]; firstVal != float64(1) {
		t.Fatalf("previously published map was mutated: v=%#v", firstVal)
	}
	if secondVal := second["rules"].(map[string]any)["v"]; secondVal != float64(2) {
		t.Fatalf("new map missing the update: v=%#v", secondVal)
	}
}
