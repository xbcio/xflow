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

func TestSuppliesExposesRevisionAndHash(t *testing.T) {
	reg := supply.NewRegistry()
	seedSupply(t, reg, "rules", `{"a":1}`, 42)
	env := BuildExprEnv(&types.Input{}, SuppliesEnv(reg))

	rev, err := EvalExpr(`$supplies.rules["$revision"]`, env, false)
	if err != nil {
		t.Fatalf("eval revision: %v", err)
	}
	if got, ok := rev.(uint64); !ok || got != 42 {
		t.Fatalf("revision = %#v, want uint64 42", rev)
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
