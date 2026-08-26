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

// registerKeyClient hands back a SupplyKey from Register and records the key ID
// every heartbeat reports. The fetcher this runner is built with holds NO
// keyring, so the only way a key ID can appear on a heartbeat is if Register's
// key travelled the real assembly path.
type registerKeyClient struct {
	supplyKey string

	mu     sync.Mutex
	keyIDs []string
}

func (c *registerKeyClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{SessionID: "session-1", SupplyKey: c.supplyKey}, nil
}

func (c *registerKeyClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	c.keyIDs = append(c.keyIDs, req.SupplyKeyID)
	c.mu.Unlock()
	return protocol.HeartbeatResponse{}, nil
}

func (c *registerKeyClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *registerKeyClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

// beats returns the key IDs reported so far.
func (c *registerKeyClient) beats() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.keyIDs...)
}

// waitForBeats blocks until at least n heartbeats have arrived, or fails.
func (c *registerKeyClient) waitForBeats(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.beats(); len(got) >= n {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d heartbeats arrived in 2s, want at least %d", len(c.beats()), n)
	return nil
}

// runRunnerUntil starts r.Run in a goroutine, waits for want heartbeats, then
// cancels and drains. Returns what the client saw.
func runRunnerUntil(t *testing.T, client *registerKeyClient, fetcher *HTTPSupplyFetcher, want int) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		HeartbeatInterval: 5 * time.Millisecond,
		PollWait:          time.Millisecond,
		SupplyGate:        NewSupplyGate(fetcher, nil, quietLogger()),
	})

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()
	got := client.waitForBeats(t, want)
	cancel()
	<-errCh
	return got
}

// TestRegisterInstallsTheSupplyKeyItWasHanded closes the gap
// service/runner/doc.go:85-90 names by hand:
//
//	"tests that assign a fake key directly to a fetcher's Keyring field bypass
//	the real assembly path (installSupplyKey → HTTPSupplyFetcher.Keyring). All
//	tests that pass such an assembly must also exercise the real path
//	(Register → SupplyKey → installSupplyKey) in at least one integration-level
//	test; a unit test where you set the field directly proves nothing about the
//	wiring."
//
// Until this test, no such test existed. Grepping the whole repository for
// RegisterRunnerResponse's SupplyKey field in any *_test.go returns nothing:
// every keyring in this package's tests is a struct literal
// (HTTPSupplyFetcher{Keyring: ...}) or a direct r.installSupplyKey call. That
// includes supply_client_encrypted_test.go, whose own comment claimed to be the
// test doc.go asks for — it is not, and its comment has been corrected.
//
// So runner.go:198-205 — the block that decodes the server's key and installs
// it — is deletable today with the whole package green:
//
//	if registerResp.SupplyKey != "" { ... r.installSupplyKey(key) }
//
// What that costs: the runner registers, heartbeats, polls and executes
// perfectly healthily, but its fetcher holds no keyring. HTTPSupplyFetcher.Fetch
// gates the Accept: application/x-xflow-encrypted header on
// f.Keyring.HasKeys(), so the runner stops asking for ciphertext and the server
// serves supplies — credentials among them — as cleartext over the wire. Both
// ends report success. And because supplyKeyID() then reports "", the server
// reads this runner as one that predates the field and never offers it a
// rotation, so the condition is permanent rather than self-correcting.
//
// The fetcher below is built with no Keyring on purpose: an assertion that
// passes because the test pre-installed the key would be exactly the toothless
// shape doc.go is warning about. The only path from the server's base64 string
// to the reported ID runs through the code under test.
func TestRegisterInstallsTheSupplyKeyItWasHanded(t *testing.T) {
	key, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	client := &registerKeyClient{supplyKey: key.ToBase64()}
	fetcher := &HTTPSupplyFetcher{BaseURL: "http://unused"}

	beats := runRunnerUntil(t, client, fetcher, 1)

	if beats[0] != key.ID {
		t.Fatalf("the first heartbeat reported supply key ID %q, want %q: the key "+
			"the server handed back from Register never reached the fetcher, so this "+
			"runner asks for supplies in cleartext and is never offered a rotation",
			beats[0], key.ID)
	}
	if !fetcher.Keyring.HasKeys() {
		t.Fatal("the fetcher holds no keys after Register handed one over; without " +
			"them Fetch omits the Accept: application/x-xflow-encrypted header and " +
			"silently downgrades every supply fetch to cleartext")
	}
	if got := fetcher.Keyring.CurrentKeyID(); got != key.ID {
		t.Fatalf("the fetcher's current key ID is %q, want %q", got, key.ID)
	}
}

// TestRegisterWithAnUndecodableSupplyKeyKeepsRunning pins the other half of the
// same block — runner.go:199-202, the branch that logs and carries on rather
// than aborting startup.
//
// Refusing to start on a malformed key would be defensible, but it is not what
// the code does, and the difference matters operationally: a server that ships
// a corrupt key would otherwise take down every runner that reconnects to it.
// What must NOT happen is a partially-installed keyring — a fetcher holding
// something that is not the server's key would send Accept:
// application/x-xflow-encrypted and then fail to decrypt every supply, turning
// a logged warning into a total supply outage.
//
// This is a two-sided assertion on purpose: the runner keeps heartbeating AND
// reports an empty key ID. Checking only that it survives would also pass if a
// garbage key had been installed.
func TestRegisterWithAnUndecodableSupplyKeyKeepsRunning(t *testing.T) {
	client := &registerKeyClient{supplyKey: "!!! not base64 !!!"}
	fetcher := &HTTPSupplyFetcher{BaseURL: "http://unused"}

	beats := runRunnerUntil(t, client, fetcher, 2)

	for i, got := range beats {
		if got != "" {
			t.Fatalf("heartbeat %d reported supply key ID %q, want empty: an "+
				"undecodable key must leave the keyring untouched, or the runner "+
				"advertises encryption it cannot actually perform", i, got)
		}
	}
	if fetcher.Keyring.HasKeys() {
		t.Fatal("a key was installed from an undecodable base64 string; the fetcher " +
			"will now request ciphertext it cannot open, failing every supply fetch")
	}
}
