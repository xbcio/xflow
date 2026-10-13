package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// supplyPullDef builds a workflow whose pull supply is actually consumed
// through a dependency edge, so a well-formed source travels the whole compile
// -- registration, body validation, dependency routing -- rather than being
// short-circuited by an earlier failure.
func supplyPullDef(params map[string]any) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: types.SupplyPullNodeType, Kind: types.NodeKindSupply, Parameters: params},
			{Name: "consume", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
		},
		Connections: types.Connections{
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "consume"}},
			}},
		},
	}
}

// TestCompile_AcceptsWellFormedSupplyPullNode is the positive control: without
// it, a rejection that matched every pull node -- or every node -- would pass
// the rejection tests below.
func TestCompile_AcceptsWellFormedSupplyPullNode(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
	}{
		{"url only", map[string]any{
			types.SupplyPullParamURL: "https://config.example.com/rules.json",
		}},
		{"every optional key", map[string]any{
			types.SupplyPullParamURL:            "https://config.example.com/rules.json",
			types.SupplyPullParamAuthentication: "rules_token",
			types.SupplyPullParamPollInterval:   "30s",
			types.SupplyParamRequireReady:       false,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Compile(supplyPullDef(tc.params)); err != nil {
				t.Fatalf("Compile rejected a well-formed pull supply: %v", err)
			}
		})
	}
}

// TestCompile_RejectsMalformedSupplyPullNode pins the failure point: a bad
// source must be rejected on the submission paths that never run the SDK's
// descriptor scan (HTTP register and execute), which means here, in Compile.
func TestCompile_RejectsMalformedSupplyPullNode(t *testing.T) {
	valid := "https://config.example.com/rules.json"
	cases := []struct {
		name    string
		params  map[string]any
		wantErr string
	}{
		{
			name:    "plain http source",
			params:  map[string]any{types.SupplyPullParamURL: "http://config.example.com/rules.json"},
			wantErr: "must be an https URL",
		},
		{
			name: "resource key of the store-backed shape",
			params: map[string]any{
				types.SupplyPullParamURL:  valid,
				types.SupplyParamResource: "shared_rules",
			},
			wantErr: `"resource" is not accepted on a pull node`,
		},
		{
			name:    "poll interval below the floor",
			params:  map[string]any{types.SupplyPullParamURL: valid, types.SupplyPullParamPollInterval: "5s"},
			wantErr: "below the 10s minimum",
		},
		{
			name:    "url missing",
			params:  map[string]any{types.SupplyParamRequireReady: true},
			wantErr: "required",
		},
		{
			name:    "unknown parameter",
			params:  map[string]any{types.SupplyPullParamURL: valid, "max_retries": 3},
			wantErr: "unknown parameter",
		},
		{
			name:    "credentials in the url",
			params:  map[string]any{types.SupplyPullParamURL: "https://user:pass@config.example.com/rules.json"},
			wantErr: "must not embed credentials",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(supplyPullDef(tc.params))
			if err == nil {
				t.Fatal("Compile accepted a malformed pull supply; the HTTP register and " +
					"execute paths never run the SDK's descriptor scan, so this is the last " +
					"point the definition can be caught before a runner tries to fetch it")
			}
			if !strings.Contains(err.Error(), `"rules"`) {
				t.Errorf("error %q does not name the offending node", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// A store-backed supply still accepts `resource`: the pull rule must apply to
// the pull type alone, not to the shared parameter names. Without this check,
// a rule keyed on the parameter rather than the type would reject every
// external supply that overrides its resource name.
func TestCompile_StillAcceptsResourceOnStoreBackedSupply(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{types.SupplyParamResource: "shared_rules"}},
			{Name: "consume", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
		},
		Connections: types.Connections{
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "consume"}},
			}},
		},
	}
	if _, err := Compile(def); err != nil {
		t.Fatalf("Compile rejected an external supply carrying resource: %v", err)
	}
}
