package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/service/protocol"
)

// Rotation delivery is convergent: the server hands back a key only when the ID
// the runner reports differs from its own. That makes this report the trigger —
// a runner that never reports its key ID is indistinguishable from a converged
// one and is never offered a rotation, so it stays on a key that decrypts
// nothing while heartbeating perfectly healthily.
func TestSupplyKeyIDReportsTheHeldKey(t *testing.T) {
	key, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	f := &HTTPSupplyFetcher{BaseURL: "http://unused"}
	r := &Runner{supplyGate: NewSupplyGate(f, nil, quietLogger())}

	r.installSupplyKey(key)

	if got := r.supplyKeyID(); got != key.ID {
		t.Fatalf("supplyKeyID() = %q, want the installed key's ID %q; the server "+
			"cannot tell this runner needs a rotation", got, key.ID)
	}
}

// After a rotation lands, the report must follow it — otherwise the runner keeps
// asking for the key it already installed and the server keeps redelivering,
// which evicts the previous key from the two-slot keyring on every heartbeat.
func TestSupplyKeyIDFollowsARotation(t *testing.T) {
	first, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	second, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	f := &HTTPSupplyFetcher{BaseURL: "http://unused"}
	r := &Runner{supplyGate: NewSupplyGate(f, nil, quietLogger())}
	r.installSupplyKey(first)

	r.processSupplyKeyRotation(protocol.HeartbeatResponse{
		SupplyKeyRotation: second.ToBase64(),
	})

	if got := r.supplyKeyID(); got != second.ID {
		t.Fatalf("supplyKeyID() = %q after a rotation, want the rotated key's ID %q", got, second.ID)
	}
}

// A runner with no gate, or one whose fetcher holds no keyring, reports nothing.
// That empty value is what keeps the heartbeat body byte-identical for runners
// that use no supply encryption at all.
func TestSupplyKeyIDEmptyWithoutAKeyring(t *testing.T) {
	if got := (&Runner{}).supplyKeyID(); got != "" {
		t.Errorf("supplyKeyID() = %q with no gate, want empty", got)
	}
	f := &HTTPSupplyFetcher{BaseURL: "http://unused"}
	r := &Runner{supplyGate: NewSupplyGate(f, nil, quietLogger())}
	if got := r.supplyKeyID(); got != "" {
		t.Errorf("supplyKeyID() = %q with no key installed, want empty", got)
	}
}

// The report only matters if the heartbeat actually carries it. This runs the
// real Run() loop rather than calling heartbeat() directly, so it catches the
// field being computed but never put on the request — in which case the server
// sees "" forever, reads it as "old runner", and never rotates this runner's key.
func TestHeartbeatCarriesTheSupplyKeyID(t *testing.T) {
	key, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	f := &HTTPSupplyFetcher{BaseURL: "http://unused", Keyring: supplyenc.NewKeyring(key)}

	client := &keyIDCapturingClient{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		HeartbeatInterval: 10 * time.Millisecond,
		PollWait:          time.Millisecond,
		SupplyGate:        NewSupplyGate(f, nil, quietLogger()),
	})

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()
	got := client.waitForKeyID(t)
	cancel()
	<-errCh

	if got != key.ID {
		t.Fatalf("heartbeat carried SupplyKeyID = %q, want %q; the server cannot "+
			"tell this runner apart from one that predates the field, so it will "+
			"never be offered a rotation", got, key.ID)
	}
}

// keyIDCapturingClient records the SupplyKeyID of every heartbeat it receives.
type keyIDCapturingClient struct {
	mu     sync.Mutex
	keyIDs []string
}

func (c *keyIDCapturingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{SessionID: "session-1"}, nil
}

func (c *keyIDCapturingClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	c.keyIDs = append(c.keyIDs, req.SupplyKeyID)
	c.mu.Unlock()
	return protocol.HeartbeatResponse{}, nil
}

func (c *keyIDCapturingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *keyIDCapturingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

// waitForKeyID returns the first heartbeat's reported key ID, failing the test
// if no heartbeat arrives in time.
func (c *keyIDCapturingClient) waitForKeyID(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.keyIDs)
		var first string
		if n > 0 {
			first = c.keyIDs[0]
		}
		c.mu.Unlock()
		if n > 0 {
			return first
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for a heartbeat")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
