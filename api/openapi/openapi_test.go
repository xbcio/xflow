package openapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"reflect"
	"sort"
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
				ExecutionID:     execID,
				Status:          types.ExecutionStatusRunning,
				WorkflowID:      "wf-01H8XG",
				WorkflowVersion: "v7",
				CreatedAt:       &now,
				UpdatedAt:       &now,
				Nodes: []engine.NodeDetail{
					{Name: "Start", Status: types.NodeStatusSuccess, Attempt: 1, Port: "default",
						Output: map[string]any{"ok": true}, StartedAt: &now, FinishedAt: &now},
					{Name: "Fetch", Status: types.NodeStatusRunning, Attempt: 0, StartedAt: &now},
				},
			},
		},
		{
			// The derived shape: the same detail for an execution a rerun
			// created. Both link fields are omitempty, so without this case a
			// renamed or dropped tag would still validate against the schema.
			name:   "execution detail of a rerun",
			schema: "ExecutionDetail",
			value: engine.ExecutionDetail{
				ExecutionID: execID,
				Status:      types.ExecutionStatusRunning,
				ParentID:    "exec-parent",
				RerunFrom:   "Fetch",
				Nodes:       []engine.NodeDetail{{Name: "Fetch", Status: types.NodeStatusRunning}},
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
			// The parked shape: status queued plus the place in line. Both
			// fields are omitempty, so without this case a renamed or dropped
			// tag would still validate as a bare execution_id.
			name:   "execute workflow response (queued)",
			schema: "ExecuteWorkflowResponse",
			value:  apiserver.ExampleExecuteWorkflowResponseQueued(execID, 2),
		},
		{
			name:   "register workflow response with param issues",
			schema: "RegisterWorkflowResponse",
			value:  apiserver.ExampleRegisterWorkflowResponseWithParamIssues("wf-01H8XG"),
		},
		{
			name:   "execute workflow response with param issues",
			schema: "ExecuteWorkflowResponse",
			value:  apiserver.ExampleExecuteWorkflowResponseWithParamIssues(execID),
		},
		{
			name:   "param invalid error envelope",
			schema: "ParamInvalidErrorEnvelope",
			value:  apiserver.ExampleParamInvalidErrorEnvelope(),
		},
		{
			// The server-hosted webhook route answers with the bare entry-seed
			// shapes rather than an Envelope, so each of its three bodies is
			// validated against its own schema — the shared protocol DTOs are
			// marshaled through the same Example* seam as every other handler type.
			name:   "webhook entry admission",
			schema: "WebhookEntryAdmission",
			value:  apiserver.ExampleWebhookEntryAdmission(execID),
		},
		{
			name:   "webhook entry refusal",
			schema: "WebhookEntryRefusal",
			value:  apiserver.ExampleWebhookEntryRefusal(),
		},
		{
			name:   "webhook entry error",
			schema: "WebhookEntryError",
			value:  apiserver.ExampleWebhookEntryError(),
		},
		{
			// Every node type registered in this test process (the builtins
			// the server links), through the real projector.
			name:   "node types response",
			schema: "NodeTypesResponse",
			value:  apiserver.ExampleNodeTypesResponse(),
		},
		{
			name:   "node form schema (every member)",
			schema: "NodeFormSchema",
			value:  apiserver.ExampleNodeFormSchema(),
		},
		{
			name:   "signal request",
			schema: "SignalRequest",
			value:  apiserver.ExampleSignalRequest("approve", map[string]any{"ok": true}),
		},
		{
			name:   "rerun execution request",
			schema: "RerunExecutionRequest",
			value:  apiserver.ExampleRerunExecutionRequest(),
		},
		{
			name:   "rerun execution response (queued)",
			schema: "RerunExecutionResponse",
			value:  apiserver.ExampleRerunExecutionResponse(execID, 2),
		},
		{
			name:   "execution list item",
			schema: "ExecutionListItem",
			value:  apiserver.ExampleExecutionListItem(execID, "namespaceA", "wf-01H8XG", "health-check", types.ExecutionStatusFailed, "node \"work\" failed", now, now.Add(time.Minute)),
		},
		{
			// The queued shape, the only one that carries queue_position: the
			// property is optional, so a row without it cannot bind the
			// published integer (or its value) to the handler's projection.
			name:   "execution list item (queued)",
			schema: "ExecutionListItem",
			value:  apiserver.ExampleExecutionListItemQueued(execID, "namespaceA", "wf-01H8XG", "health-check", 2, now, now.Add(time.Minute)),
		},
		{
			// The definition-backed shape: every omitempty field populated and
			// both instants known. A renamed tag on any of them turns this red.
			name:   "schedule view",
			schema: "ScheduleView",
			value:  apiserver.ExampleScheduleView("wf-01H8XG", "nightly"),
		},
		{
			// The orphaned shape: empty kind, null instants. It is the case that
			// pins last_scheduled/next_fire as nullable — the populated view
			// above never exercises that branch.
			name:   "schedule view (orphaned overlay)",
			schema: "ScheduleView",
			value:  apiserver.ExampleScheduleViewOrphaned("wf-01H8XG", "retired"),
		},
		{
			name:   "schedule list response",
			schema: "ScheduleListResponse",
			value:  apiserver.ExampleScheduleListResponse("wf-01H8XG"),
		},
		{
			// The decoded PATCH body: one field with a value, one with an
			// explicit null. The null is the revert-to-definition case, so a
			// schema that lost its nullable branch fails here rather than
			// silently rejecting every revert a client sends.
			name:   "schedule patch request (three-state)",
			schema: "SchedulePatchRequest",
			value:  apiserver.ExampleSchedulePatchRequest(),
		},
		{
			name:   "schedule backfill request",
			schema: "ScheduleBackfillRequest",
			value:  apiserver.ExampleScheduleBackfillRequest(),
		},
		{
			// All three skip reasons plus an emission, so the enum and the two
			// list schemas are bound to the real response type.
			name:   "schedule backfill result",
			schema: "ScheduleBackfillResult",
			value:  apiserver.ExampleScheduleBackfillResult(execID),
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
			name:   "runner list item",
			schema: "RunnerListItem",
			value:  apiserver.ExampleRunnerListItem(),
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
				Control: &control.RunnerControlSnapshot{
					DesiredState: control.RunnerDesiredStateDraining,
					Generation:   7,
					RequestedAt:  &now,
					Reason:       "node maintenance",
					Drain: &control.RunnerDrainSnapshot{
						Phase:                    control.RunnerDrainPhaseQuiescing,
						ActiveClaims:             1,
						LeasedTasks:              2,
						UnsettledDebt:            3,
						HandoffDebt:              1,
						LeaseMayExistDebt:        1,
						ReplayableDebt:           2,
						PendingActivationCleanup: 1,
						ServerQuiescent:          false,
						RunnerQuiescent:          false,
					},
				},
			},
		},
		{
			name:   "runner control request",
			schema: "RunnerControlRequest",
			value:  apiserver.ExampleRunnerControlRequest("node maintenance"),
		},
		{
			name:   "runner control snapshot",
			schema: "RunnerControlSnapshot",
			value: control.RunnerControlSnapshot{
				DesiredState: control.RunnerDesiredStateDraining,
				Generation:   7,
				RequestedAt:  &now,
				Reason:       "node maintenance",
				Drain: &control.RunnerDrainSnapshot{
					Phase:                    control.RunnerDrainPhaseQuiescing,
					ActiveClaims:             1,
					LeasedTasks:              2,
					UnsettledDebt:            3,
					HandoffDebt:              1,
					LeaseMayExistDebt:        1,
					ReplayableDebt:           2,
					PendingActivationCleanup: 1,
					ServerQuiescent:          false,
					RunnerQuiescent:          false,
				},
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
		{
			name:   "registration code create response",
			schema: "RunnerPoolTokenCreateResponse",
			value: apiserver.ExampleRunnerPoolTokenCreateResponse(
				"rc-01H8XG", "plaintext-once", now.Add(24*time.Hour), 3,
			),
		},
		{
			name:   "registration code view",
			schema: "RunnerPoolTokenView",
			value: apiserver.ExampleRunnerPoolTokenView(
				"rc-01H8XG", now, now.Add(24*time.Hour),
				[]string{"team-a"}, []string{"http.request"}, false,
			),
		},
		{
			// A code that never expires omits expires_at entirely. Pinned
			// separately because the required list must NOT have grown to
			// include it — a schema that demands expires_at would reject every
			// code minted on a deployment without a lifetime ceiling.
			name:   "registration code view (never expires)",
			schema: "RunnerPoolTokenView",
			value: apiserver.ExampleRunnerPoolTokenView(
				"rc-01H8XH", now, time.Time{},
				[]string{"team-a"}, []string{"http.request"}, true,
			),
		},
		{
			// Nil scope lists. This is the shape that broke the contract: the
			// create handler persists allowed_node_types verbatim and the
			// field is optional, so a code minted without it holds a nil slice
			// — which marshals to `null`, not `[]`, and the array schemas
			// reject it. Both fields are in RunnerPoolTokenView's required
			// list, so dropping the projection's normalization fails this case
			// twice over: null against `type: array`, and (once the key is
			// gone entirely) a missing required property.
			name:   "registration code view (nil scope lists)",
			schema: "RunnerPoolTokenView",
			value: apiserver.ExampleRunnerPoolTokenView(
				"rc-01H8XJ", now, time.Time{}, nil, nil, false,
			),
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

// TestExecutionListQueuePositionStaysOptional pins the one ExecutionListItem
// property a fixture cannot bind on its own: queue_position is optional (only a
// queued row carries it), and OpenAPI allows undeclared properties, so deleting
// it from the spec would leave every row fixture green. The queued fixture in
// TestSchemasMatchHandlerTypes pins its type; this pins that the property is
// still published and still optional — a row without a position is valid.
func TestExecutionListQueuePositionStaysOptional(t *testing.T) {
	spec := loadSpec(t)
	schema := spec.Components.Schemas["ExecutionListItem"]
	if schema == nil || schema.Value == nil {
		t.Fatal("ExecutionListItem schema is missing")
	}
	prop := schema.Value.Properties["queue_position"]
	if prop == nil || prop.Value == nil || prop.Value.Type == nil || !prop.Value.Type.Is("integer") {
		t.Fatal("ExecutionListItem.queue_position is missing from the spec or is not an integer")
	}
	for _, name := range schema.Value.Required {
		if name == "queue_position" {
			t.Fatal("ExecutionListItem.queue_position must stay optional: a row that is not queued has no position")
		}
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
	if got, want := media.Schema.Ref, "#/components/schemas/WorkflowDefWithEditorMetadata"; got != want {
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

func TestRunnerControlContract(t *testing.T) {
	spec := loadSpec(t)
	for path, wantOperation := range map[string]string{
		"/v1/management/runners/{id}/drain":  "drainRunner",
		"/v1/management/runners/{id}/resume": "resumeRunner",
	} {
		item := spec.Paths.Value(path)
		if item == nil || item.Post == nil {
			t.Errorf("POST %s is missing", path)
			continue
		}
		op := item.Post
		if op.OperationID != wantOperation {
			t.Errorf("POST %s operationId = %q, want %q", path, op.OperationID, wantOperation)
		}
		requestIDRequired := false
		for _, parameter := range op.Parameters {
			if parameter == nil || parameter.Value == nil {
				continue
			}
			if parameter.Value.In == "header" && parameter.Value.Name == "X-Request-Id" {
				requestIDRequired = parameter.Value.Required
			}
		}
		if !requestIDRequired {
			t.Errorf("POST %s must require X-Request-Id as the idempotency key", path)
		}
		if op.RequestBody == nil || op.RequestBody.Value == nil {
			t.Errorf("POST %s request body is missing", path)
		} else if media := op.RequestBody.Value.Content.Get("application/json"); media == nil || media.Schema == nil || media.Schema.Ref != "#/components/schemas/RunnerControlRequest" {
			t.Errorf("POST %s request schema = %#v, want RunnerControlRequest", path, media)
		}
		response := op.Responses.Value("200")
		if response == nil || response.Value == nil {
			t.Errorf("POST %s success response is missing", path)
			continue
		}
		media := response.Value.Content.Get("application/json")
		if media == nil || media.Schema == nil || media.Schema.Value == nil || len(media.Schema.Value.AllOf) < 2 {
			t.Errorf("POST %s success envelope schema is missing", path)
			continue
		}
		data := media.Schema.Value.AllOf[1].Value.Properties["data"]
		if data == nil || data.Ref != "#/components/schemas/RunnerControlSnapshot" {
			t.Errorf("POST %s success data schema = %#v, want RunnerControlSnapshot", path, data)
		}
	}
}

func TestRunnerRosterContract(t *testing.T) {
	spec := loadSpec(t)
	path := spec.Paths.Value("/v1/management/runners")
	if path == nil || path.Get == nil {
		t.Fatal("GET /v1/management/runners is missing")
	}
	op := path.Get
	if op.OperationID != "listRunners" {
		t.Errorf("GET /v1/management/runners operationId = %q, want listRunners", op.OperationID)
	}

	for _, status := range []string{"200", "500", "501"} {
		response := op.Responses.Map()[status]
		if response == nil || response.Value == nil {
			t.Errorf("GET /v1/management/runners response %s is missing", status)
			continue
		}
		if status != "200" && response.Ref != "#/components/responses/DefaultError" {
			t.Errorf("GET /v1/management/runners response %s ref = %q, want DefaultError", status, response.Ref)
		}
	}

	success := op.Responses.Value("200")
	if success == nil || success.Value == nil {
		return
	}
	media := success.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil || len(media.Schema.Value.AllOf) < 2 {
		t.Fatal("GET /v1/management/runners success envelope schema is missing")
	}
	data := media.Schema.Value.AllOf[1].Value.Properties["data"]
	if data == nil || data.Value == nil || data.Value.Items == nil || data.Value.Items.Ref != "#/components/schemas/RunnerListItem" {
		t.Errorf("GET /v1/management/runners data schema = %#v, want RunnerListItem array", data)
	}

	runnerListItem := spec.Components.Schemas["RunnerListItem"]
	if runnerListItem == nil || runnerListItem.Value == nil {
		t.Fatal("RunnerListItem schema is missing")
	}
	assertStringEnum(t, runnerListItem.Value.Properties["state"], "online", "offline", "never_connected")
	assertStringEnum(t, runnerListItem.Value.Properties["desired_state"], "active", "draining")
}

func assertStringEnum(t *testing.T, schema *openapi3.SchemaRef, want ...string) {
	t.Helper()
	if schema == nil || schema.Value == nil {
		t.Fatalf("enum schema is missing")
	}
	got := make(map[string]struct{}, len(schema.Value.Enum))
	for _, value := range schema.Value.Enum {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("enum value %T(%v) is not a string", value, value)
		}
		got[text] = struct{}{}
	}
	if len(got) != len(want) {
		t.Fatalf("enum = %v, want exactly %v", schema.Value.Enum, want)
	}
	for _, value := range want {
		if _, ok := got[value]; !ok {
			t.Fatalf("enum = %v, missing %q", schema.Value.Enum, value)
		}
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

func TestRunnerDrainSnapshotContract(t *testing.T) {
	schemaRef := loadSpec(t).Components.Schemas["RunnerDrainSnapshot"]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatal("RunnerDrainSnapshot schema is missing")
	}
	schema := schemaRef.Value
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	for _, name := range []string{"server_quiescent", "runner_quiescent"} {
		if !required[name] {
			t.Errorf("RunnerDrainSnapshot must require %q", name)
		}
	}

	phase := schema.Properties["phase"]
	if phase == nil || phase.Value == nil {
		t.Fatal("RunnerDrainSnapshot phase schema is missing")
	}
	phaseValues := make(map[string]bool, len(phase.Value.Enum))
	for _, value := range phase.Value.Enum {
		if text, ok := value.(string); ok {
			phaseValues[text] = true
		}
	}
	for _, want := range []string{"quiescing", "complete", "timed_out"} {
		if !phaseValues[want] {
			t.Errorf("RunnerDrainSnapshot phase enum = %#v, missing %q", phase.Value.Enum, want)
		}
	}
	deadline := schema.Properties["deadline_at"]
	if deadline == nil || deadline.Value == nil || deadline.Value.Format != "date-time" {
		t.Fatal("RunnerDrainSnapshot deadline_at must be a date-time field")
	}

	server := schema.Properties["server_quiescent"]
	runner := schema.Properties["runner_quiescent"]
	if server == nil || server.Value == nil || runner == nil || runner.Value == nil {
		t.Fatal("RunnerDrainSnapshot quiescence field schemas are missing")
	}
	description := strings.Join([]string{schema.Description, phase.Value.Description, server.Value.Description, runner.Value.Description}, " ")
	for _, semantic := range []string{"current live session", "current control generation", "timed_out", "never proves that", "runner process exited"} {
		if !strings.Contains(description, semantic) {
			t.Errorf("RunnerDrainSnapshot descriptions must state %q", semantic)
		}
	}
}

// derefString reads an optional spec string as "" when absent (kin-openapi
// models a response description as a pointer, an operation's as a value).
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// scheduleOperation looks up one schedule operation by (path, method) so the
// contract test can assert on the same object the spec serves.
func scheduleOperation(t *testing.T, spec *openapi3.T, path, method string) *openapi3.Operation {
	t.Helper()
	item := spec.Paths.Value(path)
	if item == nil {
		t.Fatalf("%s is missing from the contract", path)
	}
	op := item.GetOperation(method)
	if op == nil {
		t.Fatalf("%s %s is missing from the contract", method, path)
	}
	return op
}

// scheduleResponseCodes collects the response codes an operation declares.
func scheduleResponseCodes(op *openapi3.Operation) []string {
	codes := make([]string, 0, len(op.Responses.Map()))
	for code := range op.Responses.Map() {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// TestScheduleContract pins the parts of the schedule API that JSON Schema
// cannot infer: which scope each route needs, which routes carry an If-Match
// precondition, and the semantics that make the endpoints safe to retry (the
// backfill cap, the duplicate skip, the orphan's delete-only reachability).
//
// The scopes matter most: schedule.read, schedule.write and schedule.backfill
// are three separate grants, and the whole point of the split is that a
// principal allowed to retime a schedule is not thereby allowed to run its past
// instants. A description that stopped stating them would leave the split
// undocumented, which is how the next route gets wired to the wrong one.
func TestScheduleContract(t *testing.T) {
	spec := loadSpec(t)
	const base = "/v1/workflows/{id}/schedules"

	type route struct {
		path, method, operationID, scope string
	}
	routes := []route{
		{base, "GET", "listSchedules", "schedule.read"},
		{base + "/{node}", "GET", "getSchedule", "schedule.read"},
		{base + "/{node}", "PATCH", "patchSchedule", "schedule.write"},
		{base + "/{node}/override", "DELETE", "deleteScheduleOverride", "schedule.write"},
		{base + "/{node}/pause", "POST", "pauseSchedule", "schedule.write"},
		{base + "/{node}/resume", "POST", "resumeSchedule", "schedule.write"},
		{base + "/{node}/backfill", "POST", "backfillSchedule", "schedule.backfill"},
	}

	for _, r := range routes {
		t.Run(r.operationID, func(t *testing.T) {
			op := scheduleOperation(t, spec, r.path, r.method)
			if op.OperationID != r.operationID {
				t.Errorf("operationId = %q, want %q", op.OperationID, r.operationID)
			}
			if len(op.Tags) == 0 || op.Tags[0] != "Schedules" {
				t.Errorf("tags = %v, want the Schedules tag", op.Tags)
			}
			// Every schedule route is declared against the workflow path
			// parameter, and the node names a trigger node of that workflow.
			refs := make(map[string]bool)
			for _, p := range op.Parameters {
				if p.Value != nil && p.Value.In == "path" {
					refs[p.Ref] = true
				}
			}
			if !refs["#/components/parameters/WorkflowId"] {
				t.Errorf("path parameters = %v, want the WorkflowId parameter", refs)
			}
			// The descriptions state the semantics in prose, so the checks
			// below read a whitespace-normalized, case-folded copy: the spec
			// capitalizes a word for emphasis ("ORPHANED") and wraps lines,
			// and neither is what the pin is about.
			description := strings.ToLower(strings.Join(strings.Fields(op.Description), " "))
			if !strings.Contains(description, "`"+r.scope+"` scope") {
				t.Errorf("description does not state the %s scope:\n%s", r.scope, description)
			}
			// A route that starts executions must say so, because that is what
			// separates its scope from schedule.write.
			if r.scope == "schedule.backfill" && !strings.Contains(description, "not `schedule.write`") {
				t.Errorf("backfill description does not separate it from schedule.write:\n%s", description)
			}
			// Every route that writes an overlay announces "stored but the host
			// was not told" as 202 rather than as a failure, and the routes that
			// do not carry a status they cannot return. Backfill is the POST
			// that writes no overlay: it starts executions, so it has no
			// pending-reload state to report.
			writesOverlay := r.operationID != "listSchedules" && r.operationID != "getSchedule" && r.operationID != "backfillSchedule"
			if got := op.Responses.Value("202") != nil; got != writesOverlay {
				t.Errorf("%s %s declares 202 = %v, want %v", r.method, r.path, got, writesOverlay)
			}
			// Availability is answered, not hidden: a deployment without an
			// overlay store serves every one of these routes with 501.
			if op.Responses.Value("501") == nil {
				t.Errorf("%s %s does not declare the 501 a deployment without an overlay store returns", r.method, r.path)
			}
			// The tenant boundary is a 404, never a 403: telling the two apart
			// would confirm that another namespace's workflow id exists.
			notFound := op.Responses.Value("404")
			if notFound == nil || notFound.Value == nil {
				t.Fatalf("%s %s does not declare its 404", r.method, r.path)
			}
			description404 := strings.ToLower(strings.Join(strings.Fields(derefString(notFound.Value.Description)), " "))
			if !strings.Contains(description404, "workflow_not_found") {
				t.Errorf("404 description does not name workflow_not_found:\n%s", description404)
			}
		})
	}

	// The conditional-write precondition rides exactly the routes that write an
	// overlay, and only those.
	for _, r := range routes {
		op := scheduleOperation(t, spec, r.path, r.method)
		hasIfMatch := false
		for _, p := range op.Parameters {
			if p.Ref == "#/components/parameters/IfMatchRevision" {
				hasIfMatch = true
				if p.Value != nil && (p.Value.Name != "If-Match" || p.Value.In != "header" || p.Value.Required) {
					t.Errorf("%s %s: If-Match must be an optional header, got %+v", r.method, r.path, p.Value)
				}
			}
		}
		writes := r.method == "PATCH" || r.method == "DELETE" || r.operationID == "pauseSchedule" || r.operationID == "resumeSchedule"
		if writes && !hasIfMatch {
			t.Errorf("%s %s does not carry the If-Match precondition", r.method, r.path)
		}
		if !writes && hasIfMatch {
			t.Errorf("%s %s carries an If-Match precondition it cannot honour", r.method, r.path)
		}
	}

	patch := scheduleOperation(t, spec, base+"/{node}", "PATCH")
	if patch.RequestBody == nil || patch.RequestBody.Value == nil || !patch.RequestBody.Value.Required {
		t.Fatal("PATCH request body must be present and required")
	}
	if got := patch.RequestBody.Value.Content.Get("application/json").Schema.Ref; got != "#/components/schemas/SchedulePatchRequest" {
		t.Errorf("PATCH request schema = %q, want SchedulePatchRequest", got)
	}
	if got, want := scheduleResponseCodes(patch), []string{"200", "202", "400", "404", "409", "501", "default"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PATCH responses = %v, want %v", got, want)
	}
	for _, status := range []string{"200", "202"} {
		media := patch.Responses.Value(status).Value.Content.Get("application/json")
		data := media.Schema.Value.AllOf[1].Value.Properties["data"]
		if data == nil || data.Ref != "#/components/schemas/ScheduleView" {
			t.Errorf("PATCH %s data schema = %#v, want ScheduleView", status, data)
		}
	}

	backfill := scheduleOperation(t, spec, base+"/{node}/backfill", "POST")
	description := strings.ToLower(strings.Join(strings.Fields(backfill.Description), " "))
	for _, semantic := range []string{
		"capped at 1000 instants",
		"refusal, not a truncation",
		"refused rather than clamped",
		"safe to repeat",
		"no plan is returned beside a 400",
	} {
		if !strings.Contains(description, semantic) {
			t.Errorf("backfill description does not state %q", semantic)
		}
	}
	if got := backfill.Responses.Value("200").Value.Content.Get("application/json").Schema.Value.AllOf[1].Value.Properties["data"].Ref; got != "#/components/schemas/ScheduleBackfillResult" {
		t.Errorf("backfill 200 data schema = %q, want ScheduleBackfillResult", got)
	}
	if backfill.Responses.Value("501") == nil {
		t.Error("backfill does not declare the 501 its trigger host may be missing")
	}

	deleteOverride := scheduleOperation(t, spec, base+"/{node}/override", "DELETE")
	deleteDescription := strings.ToLower(strings.Join(strings.Fields(deleteOverride.Description), " "))
	if !strings.Contains(deleteDescription, "orphaned overlay") {
		t.Errorf("DELETE override does not document its reach over an orphaned overlay:\n%s", deleteDescription)
	}

	// The view schema's required list is the wire contract: these fields are
	// always present, null only where the schema says so.
	view := spec.Components.Schemas["ScheduleView"]
	if view == nil || view.Value == nil {
		t.Fatal("ScheduleView schema is missing")
	}
	required := make(map[string]bool, len(view.Value.Required))
	for _, name := range view.Value.Required {
		required[name] = true
	}
	for _, name := range []string{"workflow_id", "node", "kind", "expression", "paused", "overridden", "orphaned", "last_scheduled", "next_fire", "revision"} {
		if !required[name] {
			t.Errorf("ScheduleView must require %q", name)
		}
	}
	for _, name := range []string{"last_scheduled", "next_fire"} {
		// A 3.1 type ARRAY, not a single type: the field is always present, and
		// null is the honest "unknown" (no firing recorded, or no trigger host
		// to ask) rather than an absent key.
		property := view.Value.Properties[name].Value.Type
		if !property.Includes("string") || !property.IncludesNull() {
			t.Errorf("ScheduleView.%s type = %v, want nullable string (unknown is not the same as absent)", name, property.Slice())
		}
	}
	if kind := view.Value.Properties["kind"].Value.Enum; len(kind) != 3 {
		t.Errorf("ScheduleView.kind enum = %v, want cron, timer and the empty kind of an orphan", kind)
	}

	skip := spec.Components.Schemas["ScheduleBackfillSkip"]
	if skip == nil || skip.Value == nil {
		t.Fatal("ScheduleBackfillSkip schema is missing")
	}
	assertStringEnum(t, skip.Value.Properties["reason"], "duplicate", "refused", "failed")
}
