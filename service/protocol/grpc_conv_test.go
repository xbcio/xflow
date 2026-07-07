package protocol

import (
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
					{NodeType: "xflow.function", NodeVersion: 1},
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
