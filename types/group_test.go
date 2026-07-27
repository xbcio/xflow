package types

import (
	"encoding/json"
	"testing"
	"time"
)

func TestGroupDefJSONRoundTrip(t *testing.T) {
	g := GroupDef{
		Name:           "edge",
		Members:        []string{"ingest", "analyze"},
		RunnerSelector: &RunnerSelector{Mode: RunnerSelectorModeRequired, MatchLabels: map[string]string{"cloud": "tencent"}},
		OnError:        string(OnErrorStop),
		Retry:          &RetrySettings{Enabled: true, MaxAttempts: 3},
		Timeout:        30 * time.Second,
		Mode:           "transient",
	}
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got GroupDef
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Name != "edge" || len(got.Members) != 2 || got.Mode != "transient" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Timeout != 30*time.Second {
		t.Fatalf("timeout mismatch: %v", got.Timeout)
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
