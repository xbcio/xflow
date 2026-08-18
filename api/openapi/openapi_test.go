package openapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

//go:embed xflow-v1.yaml
var specYAML []byte

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	spec, err := loader.LoadFromData(specYAML)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		t.Fatalf("validate spec: %v", err)
	}
	return spec
}

func TestSpecIsValid(t *testing.T) {
	_ = loadSpec(t)
}

// TestSchemasMatchHandlerTypes binds the contract to the implementation.
//
// Fixtures are gone on purpose. A hand-written JSON file checked against a
// hand-written schema only proves the two hand-written things agree with each
// other — the old execution-snapshot.json was camelCase against a snake_case
// Go type and passed for months. What binds the contract to the implementation
// is marshalling the REAL Go value the handler writes and validating THAT
// against the schema. Each value below is a non-zero, fully-populated instance
// of the type the corresponding handler serializes, so an omitempty field or a
// renamed tag surfaces as a real mismatch instead of a vacuous {}.
//
// The apiserver response/request structs (registerWorkflowResponse,
// leaderResponse, deadLetterReplayResponse, ...) are unexported, so the
// apiserver package exposes small Example* constructors (returning any) that
// build the real typed values with non-zero fields. The exported engine/types
// values are constructed inline.
func TestSchemasMatchHandlerTypes(t *testing.T) {
	spec := loadSpec(t)

	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	execID := types.ExecutionID("exec-01H8XG")

	cases := []struct {
		name   string
		schema string
		value  any
	}{
		{
			name:   "envelope",
			schema: "Envelope",
			value:  apiserver.ExampleEnvelope(),
		},
		{
			name:   "error envelope",
			schema: "ErrorEnvelope",
			value:  apiserver.ExampleErrorEnvelope(),
		},
		{
			name:   "execution detail",
			schema: "ExecutionDetail",
			value: engine.ExecutionDetail{
				ExecutionID: execID,
				Status:      types.ExecutionStatusRunning,
				Nodes: []engine.NodeDetail{
					{Name: "Start", Status: types.NodeStatusSuccess, Attempt: 1, Port: "default", Output: map[string]any{"ok": true}},
					{Name: "Fetch", Status: types.NodeStatusRunning, Attempt: 0},
				},
			},
		},
		{
			name:   "register workflow response",
			schema: "RegisterWorkflowResponse",
			value:  apiserver.ExampleRegisterWorkflowResponse("wf-01H8XG", []string{"node \"Fetch\" references unknown output \"missing\""}),
		},
		{
			name:   "execute workflow response",
			schema: "ExecuteWorkflowResponse",
			value:  apiserver.ExampleExecuteWorkflowResponse(execID),
		},
		{
			name:   "signal request",
			schema: "SignalRequest",
			value:  apiserver.ExampleSignalRequest("approve", map[string]any{"ok": true}),
		},
		{
			name:   "execute workflow request",
			schema: "ExecuteWorkflowRequest",
			value:  apiserver.ExampleExecuteWorkflowRequest(),
		},
		{
			name:   "leader response",
			schema: "LeaderResponse",
			value:  apiserver.ExampleLeaderResponse(true),
		},
		{
			name:   "ready response",
			schema: "ReadyResponse",
			value:  apiserver.ExampleReadyResponse(true, true),
		},
		{
			name:   "supply put response",
			schema: "SupplyPutResponse",
			value:  apiserver.ExampleSupplyPutResponse(7, "sha256:deadbeef"),
		},
		{
			name:   "wait timeout response",
			schema: "WaitTimeoutResponse",
			value:  apiserver.ExampleWaitTimeoutResponse(execID, types.ExecutionStatusRunning),
		},
		{
			name:   "runner snapshot",
			schema: "RunnerSnapshot",
			value: control.RunnerSnapshot{
				RunnerID:      "runner-1",
				Capacity:      4,
				InFlight:      2,
				Labels:        map[string]string{"zone": "us-east-1"},
				Capabilities:  []protocol.Capability{{NodeType: "kafka.source", NodeVersion: 1, Runtimes: []string{"wasm"}}},
				Namespaces:    []namespace.Namespace{"default"},
				LastHeartbeat: now,
			},
		},
		{
			name:   "capability",
			schema: "Capability",
			value:  protocol.Capability{NodeType: "kafka.source", NodeVersion: 1, Runtimes: []string{"wasm"}, Features: []string{"group"}},
		},
		{
			name:   "outbox entry",
			schema: "OutboxEntry",
			value: engine.OutboxEntry{
				ID:          "entry-1",
				Task:        engine.Task{ExecutionID: execID, NodeName: "Start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
				AvailableAt: now,
				CreatedAt:   now,
				Attempts:    1,
			},
		},
		{
			name:   "task",
			schema: "Task",
			value:  engine.Task{ExecutionID: execID, NodeName: "Start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		},
		{
			name:   "dead letter list response",
			schema: "DeadLetterListResponse",
			value: apiserver.ExampleDeadLetterListResponse(
				[]engine.OutboxEntry{
					{
						ID:          "entry-1",
						Task:        engine.Task{ExecutionID: execID, NodeName: "Start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
						AvailableAt: now,
						CreatedAt:   now,
						Attempts:    1,
					},
				},
				"cursor-2",
			),
		},
		{
			name:   "dead letter replay request",
			schema: "DeadLetterReplayRequest",
			value:  apiserver.ExampleDeadLetterReplayRequest("entry-1", "req-1", "operator retry"),
		},
		{
			name:   "dead letter replay response",
			schema: "DeadLetterReplayResponse",
			value: apiserver.ExampleDeadLetterReplayResponse(
				"replayed", "audit-1", string(execID), "Start", "activation-1",
			),
		},
		{
			name:   "workflow def",
			schema: "WorkflowDef",
			value: &types.WorkflowDef{
				ID:          "wf-01H8XG",
				Namespace:   "default",
				Name:        "health-check",
				Version:     "v1",
				Description: "example",
				Spec:        "1",
				Nodes:       []types.NodeDef{{ID: "n1", Name: "Start", Type: "http.request", Kind: types.NodeKindAction, Version: 1}},
				Connections: types.Connections{"Start": {"default": types.PortConnections{Type: types.ConnectionTypeData, Targets: []types.Connection{{Node: "n1"}}}}},
				Outputs:     map[string]types.WorkflowOutput{"out": {Value: "ok", DisplayName: "Result"}},
			},
		},
		{
			// Covers the oneOf OBJECT branch of Connections/PortConnections:
			// a dependency port marshals as {"type":"dependency","targets":[...]}
			// via portConnectionsAlias, NOT the array shorthand the data-port
			// case above exercises. Without this, a broken PortConnections
			// schema (renamed/missing field, wrong enum) would never surface in
			// CI — oneOf's permissive matching would let the object fall through
			// to the array branch and still pass.
			name:   "workflow def (dependency port)",
			schema: "WorkflowDef",
			value: &types.WorkflowDef{
				ID:        "wf-dependency-01H8XG",
				Namespace: "default",
				Name:      "supply-consumer",
				Version:   "v1",
				Spec:      "1",
				Nodes:     []types.NodeDef{{ID: "rules", Name: "Rules", Type: "wasm.rules", Kind: types.NodeKindAction, Version: 1}},
				Connections: types.Connections{"supply-src": {"supply": types.PortConnections{
					Type:    types.ConnectionTypeDependency,
					Targets: []types.Connection{{Node: "rules", Input: "main"}},
				}}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schemaRef, ok := spec.Components.Schemas[tc.schema]
			if !ok {
				t.Fatalf("schema %q not found in spec", tc.schema)
			}
			data, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatalf("marshal %s: %v", tc.name, err)
			}
			var v any
			if err := json.Unmarshal(data, &v); err != nil {
				t.Fatalf("unmarshal %s: %v", tc.name, err)
			}
			if err := schemaRef.Value.VisitJSON(v); err != nil {
				t.Fatalf("schema %q does not match marshaled %T: %v\nbody: %s",
					tc.schema, tc.value, err, string(data))
			}
		})
	}
}

// TestContractPathsAreAllRegistered is the §10 guard: every path the contract
// declares must be a registered user-facing path. A contract path with no
// implementation is exactly what shipped /workflow-definitions: CI-green, TS
// types generated, zero routes behind it. The guard is a plain string-set
// comparison against apiserver.UserFacingPaths — the contract stores absolute
// paths with the same {placeholder} spellings as the path constants, so no
// prefix stitching is involved (a prefix stitch would be a bug farm).
//
// The reverse direction (every UserFacingPaths entry has a mux registration) is
// a separate guard belonging to the next task; this test covers only
// "contract ⊆ registered".
func TestContractPathsAreAllRegistered(t *testing.T) {
	spec := loadSpec(t)

	registered := make(map[string]struct{}, len(apiserver.UserFacingPaths))
	for _, p := range apiserver.UserFacingPaths {
		registered[p] = struct{}{}
	}

	for path := range spec.Paths.Map() {
		if _, ok := registered[path]; !ok {
			t.Errorf("contract path %q is not a registered user-facing path; "+
				"a contract path with no implementation is the /workflow-definitions "+
				"defect class (spec §10)", path)
		}
	}
}
