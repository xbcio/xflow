package runner

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// descriptorCapturingClient records every Register request and ends the run
// on the first poll, so each Run call is exactly one registration.
type descriptorCapturingClient struct {
	mu        sync.Mutex
	registers []protocol.RegisterRunnerRequest
	cancel    context.CancelFunc
}

func (c *descriptorCapturingClient) Register(_ context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registers = append(c.registers, req)
	return protocol.RegisterRunnerResponse{RunnerID: req.RunnerID, SessionID: "session-descriptors"}, nil
}

func (*descriptorCapturingClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *descriptorCapturingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.cancel()
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (*descriptorCapturingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *descriptorCapturingClient) requests() []protocol.RegisterRunnerRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.RegisterRunnerRequest(nil), c.registers...)
}

func runOnceForDescriptors(t *testing.T, r *Runner, client *descriptorCapturingClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.cancel = cancel
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunnerForwardsDescriptorsJSONOnRegister(t *testing.T) {
	envelope, err := protocol.EncodeRunnerDescriptors([]protocol.RunnerDescriptor{{Type: "acme.analyse", Version: 2}})
	if err != nil {
		t.Fatalf("EncodeRunnerDescriptors() error = %v", err)
	}

	t.Run("ForwardedVerbatimOnEveryRegister", func(t *testing.T) {
		client := &descriptorCapturingClient{}
		r := New(client, execution.NewRegistry(), Config{
			RunnerID:          "runner-descriptors",
			Capabilities:      []protocol.Capability{{NodeType: "acme.analyse", NodeVersion: 2}},
			DescriptorsJSON:   envelope,
			HeartbeatInterval: time.Hour,
			PollWait:          time.Millisecond,
		})
		// A reconnect re-runs Run on the same Runner; the new session must
		// report the same set, since control replaces it per registration.
		runOnceForDescriptors(t, r, client)
		runOnceForDescriptors(t, r, client)

		got := client.requests()
		if len(got) != 2 {
			t.Fatalf("Register calls = %d, want 2", len(got))
		}
		for i, req := range got {
			if string(req.DescriptorsJSON) != string(envelope) {
				t.Fatalf("Register[%d].DescriptorsJSON = %s, want %s", i, req.DescriptorsJSON, envelope)
			}
		}
	})

	t.Run("NilWhenUnset", func(t *testing.T) {
		client := &descriptorCapturingClient{}
		r := New(client, execution.NewRegistry(), Config{
			RunnerID:          "runner-no-descriptors",
			HeartbeatInterval: time.Hour,
			PollWait:          time.Millisecond,
		})
		runOnceForDescriptors(t, r, client)

		got := client.requests()
		if len(got) != 1 {
			t.Fatalf("Register calls = %d, want 1", len(got))
		}
		if got[0].DescriptorsJSON != nil {
			t.Fatalf("DescriptorsJSON = %s, want nil", got[0].DescriptorsJSON)
		}
		raw, err := json.Marshal(got[0])
		if err != nil {
			t.Fatalf("marshal register request: %v", err)
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatalf("unmarshal register request: %v", err)
		}
		if _, ok := wire["descriptors_json"]; ok {
			t.Fatalf("HTTP register body carries descriptors_json when none are reported: %s", raw)
		}
	})
}
