package types

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestGroupDefJSONRoundTrip(t *testing.T) {
	g := GroupDef{
		Name:               "edge",
		Members:            []string{"ingest", "analyze"},
		RunnerSelector:     &RunnerSelector{Mode: RunnerSelectorModeRequired, MatchLabels: map[string]string{"cloud": "tencent"}},
		OnError:            string(OnErrorStop),
		Retry:              &RetrySettings{Enabled: true, MaxAttempts: 3},
		Timeout:            30 * time.Second,
		Mode:               "transient",
		ActivationReplicas: 3,
	}
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got GroupDef
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Name != "edge" || len(got.Members) != 2 || got.Mode != "transient" || got.ActivationReplicas != 3 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Timeout != 30*time.Second {
		t.Fatalf("timeout mismatch: %v", got.Timeout)
	}
}

func TestActivationReplicasJSONCompatibility(t *testing.T) {
	for name, value := range map[string]any{
		"group": GroupDef{Name: "edge", Members: []string{"ingest"}},
		"node":  NodeDef{Name: "ingest", Type: "kafka.source", Kind: NodeKindTrigger},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if bytes.Contains(data, []byte("activation_replicas")) {
				t.Fatalf("zero value changed legacy JSON payload: %s", data)
			}
		})
	}

	var group GroupDef
	if err := json.Unmarshal([]byte(`{"name":"edge","activation_replicas":4}`), &group); err != nil {
		t.Fatalf("unmarshal group: %v", err)
	}
	if group.ActivationReplicas != 4 {
		t.Fatalf("group ActivationReplicas = %d, want 4", group.ActivationReplicas)
	}
	var node NodeDef
	if err := json.Unmarshal([]byte(`{"name":"ingest","activation_replicas":5}`), &node); err != nil {
		t.Fatalf("unmarshal node: %v", err)
	}
	if node.ActivationReplicas != 5 {
		t.Fatalf("node ActivationReplicas = %d, want 5", node.ActivationReplicas)
	}
}

func TestWorkflowDefCarriesGroups(t *testing.T) {
	def := WorkflowDef{Groups: []GroupDef{{Name: "edge", Members: []string{"ingest"}}}}
	b, _ := json.Marshal(def)
	var got WorkflowDef
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Groups) != 1 || got.Groups[0].Name != "edge" {
		t.Fatalf("groups not carried: %+v", got.Groups)
	}
}
