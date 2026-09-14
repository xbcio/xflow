package control

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// capturePollClaimDirectory exposes the exact request Core passes through the
// common HTTP/gRPC poll boundary while delegating all unrelated operations.
type capturePollClaimDirectory struct {
	RunnerDirectory
	got ClaimRequest
}

func (d *capturePollClaimDirectory) ClaimForRunner(_ context.Context, req ClaimRequest) (Claim, bool, error) {
	d.got = req
	return Claim{}, false, nil
}

func TestPollTaskDoesNotForwardClientRoutingMetadata(t *testing.T) {
	directory := &capturePollClaimDirectory{RunnerDirectory: NewMemoryRunnerDirectory()}
	core := &Core{runners: directory, pollWait: time.Second}
	activeLeaseIDs := []string{"lease-a", "lease-b"}

	resp, err := core.pollTask(context.Background(), protocol.PollTaskRequest{
		RunnerID:  "local-runner",
		SessionID: "session-1",
		//nolint:staticcheck // Deliberately supply deprecated compatibility inputs to prove Core drops them.
		Capacity: 99,
		//nolint:staticcheck // Deliberately supply deprecated compatibility inputs to prove Core drops them.
		Labels: map[string]string{"workload": "sas-runner"},
		//nolint:staticcheck // Deliberately supply deprecated compatibility inputs to prove Core drops them.
		Capabilities:   []protocol.Capability{{NodeType: "xflow.map"}},
		ActiveLeaseIDs: activeLeaseIDs,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask: %v", err)
	}
	if resp.Wait != time.Second {
		t.Errorf("poll wait = %s, want %s", resp.Wait, time.Second)
	}
	if directory.got.RunnerID != "local-runner" || directory.got.SessionID != "session-1" {
		t.Errorf("claim identity = (%q, %q), want local-runner/session-1", directory.got.RunnerID, directory.got.SessionID)
	}
	if directory.got.Capacity != 0 {
		t.Errorf("claim capacity = %d, want zero because Poll capacity is not authoritative", directory.got.Capacity)
	}
	if directory.got.Labels != nil {
		t.Errorf("claim labels = %#v, want nil because Poll labels are not authoritative", directory.got.Labels)
	}
	if directory.got.Capabilities != nil {
		t.Errorf("claim capabilities = %#v, want nil because Poll capabilities are not authoritative", directory.got.Capabilities)
	}
	if !reflect.DeepEqual(directory.got.ActiveLeaseIDs, activeLeaseIDs) {
		t.Errorf("active lease IDs = %#v, want %#v", directory.got.ActiveLeaseIDs, activeLeaseIDs)
	}
}
