package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func retryDef(settings, node *types.RetrySettings) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:     "retry",
		Settings: &types.WorkflowSettings{Retry: settings},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "worker", Type: "test.worker", Retry: node},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
		},
	}
}

func workerRetry(t *testing.T, def *types.WorkflowDef) *types.RetrySettings {
	t.Helper()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	idx, _ := g.NodeIndex("worker")
	return g.NodeAt(idx).Retry
}

func boolPtr(v bool) *bool { return &v }

func TestRetryEnabledOmittedStillRetries(t *testing.T) {
	if r := workerRetry(t, retryDef(nil, &types.RetrySettings{MaxAttempts: 3})); r == nil || r.MaxAttempts != 3 {
		t.Fatalf("retry = %+v, want MaxAttempts 3", r)
	}
	if r := workerRetry(t, retryDef(&types.RetrySettings{MaxAttempts: 2}, nil)); r == nil || r.MaxAttempts != 2 {
		t.Fatalf("inherited retry = %+v, want MaxAttempts 2", r)
	}
}

func TestRetryNodeEnabledFalseDoesNotInherit(t *testing.T) {
	def := retryDef(&types.RetrySettings{MaxAttempts: 5}, &types.RetrySettings{Enabled: boolPtr(false), MaxAttempts: 3})
	if r := workerRetry(t, def); r != nil {
		t.Fatalf("retry = %+v, want nil", r)
	}
}

func TestRetrySettingsEnabledFalseDisables(t *testing.T) {
	def := retryDef(&types.RetrySettings{Enabled: boolPtr(false), MaxAttempts: 5}, nil)
	if r := workerRetry(t, def); r != nil {
		t.Fatalf("retry = %+v, want nil", r)
	}
	// A node override with retries enabled still wins.
	def = retryDef(&types.RetrySettings{Enabled: boolPtr(false), MaxAttempts: 5}, &types.RetrySettings{MaxAttempts: 2})
	if r := workerRetry(t, def); r == nil || r.MaxAttempts != 2 {
		t.Fatalf("retry = %+v, want MaxAttempts 2", r)
	}
}

func TestRetryStrategyValidation(t *testing.T) {
	for _, s := range []string{"", "fixed", "exponential"} {
		if _, err := Compile(retryDef(&types.RetrySettings{MaxAttempts: 2, Strategy: s}, &types.RetrySettings{MaxAttempts: 2, Strategy: s})); err != nil {
			t.Fatalf("strategy %q: %v", s, err)
		}
	}
	_, err := Compile(retryDef(nil, &types.RetrySettings{MaxAttempts: 2, Strategy: "linear"}))
	if err == nil || !strings.Contains(err.Error(), `"worker"`) || !strings.Contains(err.Error(), "linear") {
		t.Fatalf("node err = %v, want naming worker and linear", err)
	}
	_, err = Compile(retryDef(&types.RetrySettings{MaxAttempts: 2, Strategy: "linear"}, nil))
	if err == nil || !strings.Contains(err.Error(), "settings") {
		t.Fatalf("settings err = %v, want naming settings", err)
	}
}

func TestRetryEnabledFalseJSONRoundTrip(t *testing.T) {
	var r types.RetrySettings
	if err := json.Unmarshal([]byte(`{"enabled":false,"max_attempts":3}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.Enabled == nil || *r.Enabled {
		t.Fatalf("Enabled = %v, want non-nil false", r.Enabled)
	}
	b, _ := json.Marshal(types.RetrySettings{MaxAttempts: 3})
	if string(b) != `{"max_attempts":3}` {
		t.Fatalf("omitted Enabled marshals as %s", b)
	}
}
