package protocol_test

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestSeedExecutionRequest_JSONRoundTrip(t *testing.T) {
	req := protocol.SeedExecutionRequest{
		WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "kafka-in",
		AdmissionKey: "ns/wf-1/v1/kafka-in/topic/0/5-5", Outcome: "success",
		Exits:           []protocol.BoundaryExit{{NodeName: "kafka-in", Port: "main", Data: map[string]any{"offset": float64(5)}}},
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.SeedExecutionRequest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.AdmissionKey != req.AdmissionKey || len(got.Exits) != 1 || got.Exits[0].Port != "main" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}
