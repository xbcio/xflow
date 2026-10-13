package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestValidateTriggerReplicaCardinality covers the standalone-node rule: a
// Pub/Sub trigger is the one kind whose replica count the platform cannot
// honour, and the rule must not touch the stream mode that genuinely does
// distribute entries across replicas.
func TestValidateTriggerReplicaCardinality(t *testing.T) {
	tests := []struct {
		name     string
		nodeType string
		params   map[string]any
		replicas uint32
		wantErr  bool
	}{
		{
			name:     "pubsub with three replicas",
			nodeType: redisTriggerType,
			params:   map[string]any{"mode": redisPubSubMode, "channel": "events"},
			replicas: 3,
			wantErr:  true,
		},
		{
			name:     "pubsub with one replica",
			nodeType: redisTriggerType,
			params:   map[string]any{"mode": redisPubSubMode, "channel": "events"},
			replicas: 1,
		},
		{
			name:     "pubsub with no replicas declared",
			nodeType: redisTriggerType,
			params:   map[string]any{"mode": redisPubSubMode, "channel": "events"},
		},
		{
			name:     "stream with three replicas",
			nodeType: redisTriggerType,
			params:   map[string]any{"mode": redisStreamMode, "stream": "orders", "group": "workers"},
			replicas: 3,
		},
		{
			name:     "unset mode is stream",
			nodeType: redisTriggerType,
			params:   map[string]any{"stream": "orders", "group": "workers"},
			replicas: 3,
		},
		{
			name:     "another trigger kind with three replicas",
			nodeType: "xflow.trigger.kafka",
			params:   map[string]any{"mode": redisPubSubMode},
			replicas: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTriggerReplicaCardinality("node \"in\"", tt.nodeType, tt.params, tt.replicas)
			if tt.wantErr && err == nil {
				t.Fatal("want an error for a Pub/Sub trigger hosted by several replicas")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && !strings.Contains(err.Error(), "activation_replicas") {
				t.Fatalf("error %q does not name the offending param", err)
			}
		})
	}
}

// TestCompileRejectsPubSubTriggerReplicas asserts the rule is reachable through
// Compile — the single gate every submission path shares — rather than only
// through its own unit test.
func TestCompileRejectsPubSubTriggerReplicas(t *testing.T) {
	def := pubSubWorkflowDef(3, redisPubSubMode)
	if _, err := Compile(def); err == nil {
		t.Fatal("Compile accepted a Pub/Sub trigger with activation_replicas=3")
	}

	def = pubSubWorkflowDef(3, redisStreamMode)
	if _, err := Compile(def); err != nil {
		t.Fatalf("Compile rejected a stream-mode trigger with replicas: %v", err)
	}

	def = pubSubWorkflowDef(1, redisPubSubMode)
	if _, err := Compile(def); err != nil {
		t.Fatalf("Compile rejected a single-replica Pub/Sub trigger: %v", err)
	}
}

// TestCompileRejectsPubSubTriggerReplicasInGroup covers the same limitation
// when the trigger is a group's entry member: the group's replicas each
// subscribe, so the group-level cardinality has to be checked too.
func TestCompileRejectsPubSubTriggerReplicasInGroup(t *testing.T) {
	def := pubSubWorkflowDef(0, redisPubSubMode)
	def.Groups = []types.GroupDef{{
		Name:               "ingest",
		Members:            []string{"in"},
		ActivationReplicas: 2,
	}}
	if _, err := Compile(def); err == nil {
		t.Fatal("Compile accepted a group hosting a Pub/Sub trigger with activation_replicas=2")
	}

	def.Groups[0].ActivationReplicas = 1
	if _, err := Compile(def); err != nil {
		t.Fatalf("Compile rejected a single-replica group hosting a Pub/Sub trigger: %v", err)
	}
}

// pubSubWorkflowDef builds the smallest workflow that reaches the replica rule:
// a redis trigger feeding one action node.
func pubSubWorkflowDef(replicas uint32, mode string) *types.WorkflowDef {
	params := map[string]any{"addr": "127.0.0.1:6379", "mode": mode}
	if mode == redisPubSubMode {
		params["channel"] = "events"
	} else {
		params["stream"] = "orders"
		params["group"] = "workers"
	}
	return &types.WorkflowDef{
		Name: "replicas",
		Nodes: []types.NodeDef{
			{Name: "in", Type: redisTriggerType, Kind: types.NodeKindTrigger, Parameters: params, ActivationReplicas: replicas},
			{Name: "work", Type: "test.work"},
		},
		Connections: types.Connections{
			"in": {"main": {Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
}
