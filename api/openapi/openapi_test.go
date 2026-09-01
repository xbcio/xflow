package openapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"strings"
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
			// PUT /v1/workflows/{id} accepts the real WorkflowDef DTO without
			// either server-authoritative field. The path supplies id and the
			// authenticated principal supplies namespace.
			name:   "workflow def (replace request without server identity)",
			schema: "WorkflowDef",
			value: &types.WorkflowDef{
				Name:    "health-check-renamed",
				Version: "v2",
				Nodes:   []types.NodeDef{{Name: "Start", Type: "http.request"}},
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

// TestReplaceWorkflowContract pins the parts of PUT /v1/workflows/{id} that
// JSON Schema cannot infer from types.WorkflowDef alone: the path selects the
// resource, body identity is optional, and each identity failure has a stable
// HTTP status. The operation description carries the relational constraints
// (body id must equal path id and rename destinations must be unoccupied).
func TestReplaceWorkflowContract(t *testing.T) {
	spec := loadSpec(t)
	path := spec.Paths.Value("/v1/workflows/{id}")
	if path == nil || path.Put == nil {
		t.Fatal("PUT /v1/workflows/{id} is missing")
	}
	operation := path.Put
	if operation.RequestBody == nil || operation.RequestBody.Value == nil {
		t.Fatal("replace workflow request body is missing")
	}
	if !operation.RequestBody.Value.Required {
		t.Fatal("replace workflow request body must be required")
	}
	media := operation.RequestBody.Value.GetMediaType("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		t.Fatal("replace workflow application/json schema is missing")
	}
	if got, want := media.Schema.Ref, "#/components/schemas/WorkflowDef"; got != want {
		t.Fatalf("replace workflow request schema ref = %q, want %q", got, want)
	}
	for _, field := range media.Schema.Value.Required {
		if field == "id" || field == "namespace" {
			t.Fatalf("replace workflow request unexpectedly requires server-authoritative field %q", field)
		}
	}

	for _, status := range []string{"200", "400", "404", "409"} {
		response := operation.Responses.Value(status)
		if response == nil || response.Value == nil {
			t.Errorf("replace workflow response %s is missing", status)
			continue
		}
		header := response.Value.Headers["X-Request-Id"]
		if header == nil {
			t.Errorf("replace workflow response %s does not declare X-Request-Id", status)
			continue
		}
		if got, want := header.Ref, "#/components/headers/XRequestId"; got != want {
			t.Errorf("replace workflow response %s X-Request-Id ref = %q, want %q", status, got, want)
		}
	}
	for status, wantCode := range map[string]string{
		"400": "workflow_id_mismatch",
		"404": "workflow_not_found",
		"409": "workflow_conflict",
	} {
		response := operation.Responses.Value(status)
		if response == nil || response.Value == nil {
			t.Errorf("replace workflow response %s is missing", status)
			continue
		}
		media := response.Value.Content.Get("application/json")
		if media == nil {
			t.Errorf("replace workflow response %s has no application/json content", status)
			continue
		}
		found := false
		for _, example := range media.Examples {
			if example == nil || example.Value == nil {
				continue
			}
			value, ok := example.Value.Value.(map[string]any)
			if ok && value["code"] == wantCode {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("replace workflow response %s has no %q example", status, wantCode)
		}
	}

	description := strings.Join(strings.Fields(strings.ToLower(operation.Description)), " ")
	for _, semantic := range []string{
		"path `id` is the authoritative resource identity",
		"body `id` is optional",
		"including a workflow owned by another namespace",
		"preserving the path id",
		"destination key is free",
		"never deletes or overwrites a different workflow",
	} {
		if !strings.Contains(description, semantic) {
			t.Errorf("replace workflow description does not state %q", semantic)
		}
	}
	if strings.Contains(description, "deregisters a conflicting definition") {
		t.Error("replace workflow description still documents deletion by the body key")
	}
}

func TestReadyzContract(t *testing.T) {
	spec := loadSpec(t)
	path := spec.Paths.Value("/readyz")
	if path == nil || path.Get == nil {
		t.Fatal("GET /readyz is missing")
	}
	operation := path.Get
	for status, wantReady := range map[string]bool{"200": true, "503": false} {
		response := operation.Responses.Value(status)
		if response == nil || response.Value == nil {
			t.Errorf("readyz response %s is missing", status)
			continue
		}
		media := response.Value.Content.Get("application/json")
		if media == nil || media.Schema == nil {
			t.Errorf("readyz response %s application/json schema is missing", status)
			continue
		}
		if got, want := media.Schema.Ref, "#/components/schemas/ReadyResponse"; got != want {
			t.Errorf("readyz response %s schema ref = %q, want %q", status, got, want)
		}
		example, ok := media.Example.(map[string]any)
		if !ok {
			t.Errorf("readyz response %s example = %T, want object", status, media.Example)
			continue
		}
		if got, ok := example["ready"].(bool); !ok || got != wantReady {
			t.Errorf("readyz response %s example ready = %#v, want %t", status, example["ready"], wantReady)
		}
		if _, ok := example["leader"].(bool); !ok {
			t.Errorf("readyz response %s example leader = %#v, want boolean", status, example["leader"])
		}
	}

	description := strings.Join(strings.Fields(strings.ToLower(operation.Description)), " ")
	for _, semantic := range []string{"required production dependencies", "dependency check failure returns 503", "`ready=false`"} {
		if !strings.Contains(description, semantic) {
			t.Errorf("readyz description does not state %q", semantic)
		}
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
