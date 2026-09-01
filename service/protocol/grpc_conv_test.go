package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestRunnerFrameRoundTrip(t *testing.T) {
	lease := &engine.TaskLease{LeaseID: "lease-1", LeaseToken: "tok", NodeType: "xflow.function"}
	original := RunnerFrame{Result: &ResultFrame{
		LeaseID: "lease-1",
		Lease:   lease,
		Result:  engine.TaskResult{Output: &types.Output{Data: map[string]any{"k": "v"}}},
	}}
	pb, err := RunnerFrameToProto(original)
	if err != nil {
		t.Fatalf("to proto: %v", err)
	}
	got, err := RunnerFrameFromProto(pb)
	if err != nil {
		t.Fatalf("from proto: %v", err)
	}
	if got.Result == nil || got.Result.LeaseID != "lease-1" {
		t.Fatalf("lease id mismatch: %+v", got)
	}
	if got.Result.Lease == nil || got.Result.Lease.LeaseID != "lease-1" {
		t.Fatalf("lease payload mismatch: %+v", got.Result)
	}
	if got.Result.Result.Output == nil || got.Result.Result.Output.Data["k"] != "v" {
		t.Fatalf("result payload mismatch: %+v", got.Result.Result)
	}
}

func TestRunnerFrameRoundTripAllArms(t *testing.T) {
	cases := []struct {
		name  string
		frame RunnerFrame
		check func(t *testing.T, got RunnerFrame)
	}{
		{
			name: "Hello",
			frame: RunnerFrame{Hello: &HelloFrame{
				RunnerID:    "runner-42",
				Concurrency: 4,
				Capabilities: []Capability{
					{NodeType: "xflow.function", NodeVersion: 1, Features: []string{"entry.activation.replica.v1"}},
				},
				Labels: map[string]string{"region": "us-east"},
			}},
			check: func(t *testing.T, got RunnerFrame) {
				t.Helper()
				if got.Hello == nil {
					t.Fatal("Hello arm is nil after round-trip")
				}
				if got.Hello.RunnerID != "runner-42" {
					t.Errorf("RunnerID: want runner-42, got %q", got.Hello.RunnerID)
				}
				if got.Hello.Concurrency != 4 {
					t.Errorf("Concurrency: want 4, got %d", got.Hello.Concurrency)
				}
				if len(got.Hello.Capabilities) != 1 || got.Hello.Capabilities[0].NodeType != "xflow.function" {
					t.Errorf("Capabilities mismatch: %+v", got.Hello.Capabilities)
				}
				if len(got.Hello.Capabilities[0].Features) != 1 || got.Hello.Capabilities[0].Features[0] != "entry.activation.replica.v1" {
					t.Errorf("Capability features mismatch: %+v", got.Hello.Capabilities[0].Features)
				}
				if got.Hello.Labels["region"] != "us-east" {
					t.Errorf("Labels mismatch: %+v", got.Hello.Labels)
				}
			},
		},
		{
			name:  "Bye",
			frame: RunnerFrame{Bye: &ByeFrame{}},
			check: func(t *testing.T, got RunnerFrame) {
				t.Helper()
				if got.Bye == nil {
					t.Fatal("Bye arm is nil after round-trip")
				}
				if got.Hello != nil || got.Result != nil {
					t.Errorf("unexpected non-nil arms in Bye frame: %+v", got)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pb, err := RunnerFrameToProto(tc.frame)
			if err != nil {
				t.Fatalf("to proto: %v", err)
			}
			got, err := RunnerFrameFromProto(pb)
			if err != nil {
				t.Fatalf("from proto: %v", err)
			}
			tc.check(t, got)
		})
	}
}

func TestServerFrameRoundTrip(t *testing.T) {
	lease := &engine.TaskLease{LeaseID: "lease-2", LeaseToken: "tok-srv", NodeType: "xflow.function"}
	cases := []ServerFrame{
		{Welcome: &WelcomeFrame{RunnerID: "r1", ServerTime: time.Unix(42, 0).Unix()}},
		{Task: &TaskFrame{Lease: lease}},
		{Ack: &AckFrame{LeaseID: "lease-2", Accepted: true}},
		{Backoff: &BackoffFrame{Wait: 250 * time.Millisecond}},
		{Keepalive: &KeepaliveFrame{}},
	}
	for i, c := range cases {
		pb, err := ServerFrameToProto(c)
		if err != nil {
			t.Fatalf("case %d to proto: %v", i, err)
		}
		got, err := ServerFrameFromProto(pb)
		if err != nil {
			t.Fatalf("case %d from proto: %v", i, err)
		}
		if !serverFrameEqual(c, got) {
			t.Fatalf("case %d mismatch: want %+v got %+v", i, c, got)
		}
	}
}

func serverFrameEqual(a, b ServerFrame) bool {
	if (a.Welcome == nil) != (b.Welcome == nil) || (a.Task == nil) != (b.Task == nil) ||
		(a.Ack == nil) != (b.Ack == nil) || (a.Backoff == nil) != (b.Backoff == nil) ||
		(a.Keepalive == nil) != (b.Keepalive == nil) {
		return false
	}
	switch {
	case a.Welcome != nil:
		return a.Welcome.RunnerID == b.Welcome.RunnerID && a.Welcome.ServerTime == b.Welcome.ServerTime
	case a.Task != nil:
		return a.Task.Lease.LeaseID == b.Task.Lease.LeaseID &&
			a.Task.Lease.NodeType == b.Task.Lease.NodeType &&
			a.Task.Lease.LeaseToken == b.Task.Lease.LeaseToken
	case a.Ack != nil:
		return a.Ack.LeaseID == b.Ack.LeaseID && a.Ack.Accepted == b.Ack.Accepted && a.Ack.Error == b.Ack.Error
	case a.Backoff != nil:
		return a.Backoff.Wait == b.Backoff.Wait
	}
	return true
}

func TestRegisterResponseProtoRoundTripPreservesSessionID(t *testing.T) {
	want := RegisterRunnerResponse{
		RunnerID:  "runner-1",
		SessionID: "session-1",
	}

	got := RegisterResponseFromProto(RegisterResponseToProto(want))

	if got != want {
		t.Fatalf("register response round trip = %+v, want %+v", got, want)
	}
}

// TestReportResultRequestProtoRoundTripPreservesTraceCarrier proves the gRPC
// ReportResultRequest carries the W3C TraceCarrier in both directions so the
// server's commit span can be parented to the runner's execute span via a real
// W3C ExtractCarrier round-trip. This closes B1 blocker 1 (the gRPC proto /
// converter previously dropped TraceCarrier, forcing a fallback to the lease
// dispatch carrier and breaking execute→commit causality).
func TestReportResultRequestProtoRoundTripPreservesTraceCarrier(t *testing.T) {
	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "tok",
		Task:       engine.Task{ExecutionID: "exec-1", NodeName: "n"},
		NodeType:   "xflow.function",
	}
	original := ReportResultRequest{
		RunnerID:  "runner-1",
		SessionID: "session-1",
		Lease:     lease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"k": "v"}}},
		TraceCarrier: map[string]string{
			"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			"tracestate":  "vendor=xflow",
		},
	}
	pb, err := ReportResultRequestToProto(original)
	if err != nil {
		t.Fatalf("to proto: %v", err)
	}
	if len(pb.GetTraceCarrier()) == 0 {
		t.Fatal("trace_carrier_fields = 0 (gRPC proto did not carry TraceCarrier)")
	}
	got, err := ReportResultRequestFromProto(pb)
	if err != nil {
		t.Fatalf("from proto: %v", err)
	}
	if got.TraceCarrier["traceparent"] != original.TraceCarrier["traceparent"] {
		t.Fatalf("traceparent lost: got %q want %q", got.TraceCarrier["traceparent"], original.TraceCarrier["traceparent"])
	}
	if got.TraceCarrier["tracestate"] != original.TraceCarrier["tracestate"] {
		t.Fatalf("tracestate lost: got %q want %q", got.TraceCarrier["tracestate"], original.TraceCarrier["tracestate"])
	}
}

// TestReportResultRequestProtoRoundTripEmptyCarrier proves a request without a
// carrier round-trips cleanly (no carrier set), so old runners that do not
// populate TraceCarrier continue to work — the server falls back to the lease
// dispatch carrier.
func TestReportResultRequestProtoRoundTripEmptyCarrier(t *testing.T) {
	original := ReportResultRequest{
		RunnerID:  "runner-1",
		SessionID: "session-1",
		Lease:     &engine.TaskLease{LeaseID: "lease-1", LeaseToken: "tok"},
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"k": "v"}}},
	}
	pb, err := ReportResultRequestToProto(original)
	if err != nil {
		t.Fatalf("to proto: %v", err)
	}
	if len(pb.GetTraceCarrier()) != 0 {
		t.Fatalf("expected no carrier, got %v", pb.GetTraceCarrier())
	}
	got, err := ReportResultRequestFromProto(pb)
	if err != nil {
		t.Fatalf("from proto: %v", err)
	}
	if len(got.TraceCarrier) != 0 {
		t.Fatalf("expected no carrier on round-trip, got %v", got.TraceCarrier)
	}
}

// TestRegisterRequestProtoRoundTripActivations proves the gRPC RegisterRequest
// carries ActivationInventoryItems (including WorkflowVersion) through the proto
// layer in both directions, closing the gRPC inventory gap.
func TestRegisterRequestProtoRoundTripActivations(t *testing.T) {
	original := RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 4,
		Capabilities: []Capability{
			{NodeType: "kafka.source", NodeVersion: 1, Features: []string{"entry.activation.replica.v1"}},
		},
		Labels:     map[string]string{"zone": "a"},
		Namespaces: []string{"default"},
		Activations: []ActivationInventoryItem{
			{
				WorkflowID:      "wf-1",
				WorkflowVersion: "v2",
				EntryUnitID:     "tg-1",
				ReplicaIndex:    3,
				Generation:      3,
			},
			{
				WorkflowID:      "wf-2",
				WorkflowVersion: "",
				EntryUnitID:     "tg-2",
				Generation:      1,
			},
		},
	}

	pb := RegisterRequestToProto(original)
	got := RegisterRequestFromProto(pb)

	if got.RunnerID != original.RunnerID {
		t.Fatalf("RunnerID: got %q, want %q", got.RunnerID, original.RunnerID)
	}
	if got.Concurrency != original.Concurrency {
		t.Fatalf("Concurrency: got %d, want %d", got.Concurrency, original.Concurrency)
	}
	if len(got.Capabilities) != 1 || len(got.Capabilities[0].Features) != 1 || got.Capabilities[0].Features[0] != "entry.activation.replica.v1" {
		t.Fatalf("Capabilities: got %+v, want %+v", got.Capabilities, original.Capabilities)
	}
	if len(got.Activations) != len(original.Activations) {
		t.Fatalf("Activations len: got %d, want %d", len(got.Activations), len(original.Activations))
	}
	for i, want := range original.Activations {
		g := got.Activations[i]
		if g.WorkflowID != want.WorkflowID || g.WorkflowVersion != want.WorkflowVersion ||
			g.EntryUnitID != want.EntryUnitID || g.ReplicaIndex != want.ReplicaIndex || g.Generation != want.Generation {
			t.Fatalf("Activations[%d]: got %+v, want %+v", i, g, want)
		}
	}
}

func TestActivationReplicaIndexJSONZeroValueCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		decode func([]byte) uint32
	}{
		{
			name:  "activate",
			value: ActivateDirective{WorkflowID: "wf", EntryUnitID: "entry"},
			decode: func(data []byte) uint32 {
				var value ActivateDirective
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatalf("unmarshal activate: %v", err)
				}
				return value.ReplicaIndex
			},
		},
		{
			name:  "deactivate",
			value: DeactivateDirective{WorkflowID: "wf", EntryUnitID: "entry"},
			decode: func(data []byte) uint32 {
				var value DeactivateDirective
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatalf("unmarshal deactivate: %v", err)
				}
				return value.ReplicaIndex
			},
		},
		{
			name:  "ack",
			value: ActivationAck{WorkflowID: "wf", GroupID: "entry"},
			decode: func(data []byte) uint32 {
				var value ActivationAck
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatalf("unmarshal ack: %v", err)
				}
				return value.ReplicaIndex
			},
		},
		{
			name:  "inventory",
			value: ActivationInventoryItem{WorkflowID: "wf", EntryUnitID: "entry"},
			decode: func(data []byte) uint32 {
				var value ActivationInventoryItem
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatalf("unmarshal inventory: %v", err)
				}
				return value.ReplicaIndex
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if strings.Contains(string(data), "replica_index") {
				t.Fatalf("zero replica changed legacy JSON: %s", data)
			}
			if got := tc.decode(data); got != 0 {
				t.Fatalf("decoded replica = %d, want legacy zero", got)
			}
		})
	}
}

// TestRegisterRequestProtoRoundTripEmptyActivations proves that a request with
// no activations (fresh runner) round-trips cleanly.
func TestRegisterRequestProtoRoundTripEmptyActivations(t *testing.T) {
	original := RegisterRunnerRequest{
		RunnerID:    "runner-2",
		Concurrency: 2,
	}
	pb := RegisterRequestToProto(original)
	got := RegisterRequestFromProto(pb)
	if len(got.Activations) != 0 {
		t.Fatalf("expected empty activations, got %v", got.Activations)
	}
}
