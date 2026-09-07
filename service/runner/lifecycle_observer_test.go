package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
)

type recordingLifecycleObserver struct {
	mu             sync.Mutex
	registered     []string
	supplyKeySeen  []bool
	heartbeats     []bool
	gateWired      []bool
	supplyFetchLog []string
}

func (r *recordingLifecycleObserver) OnRegistered(_ context.Context, runnerID string, supplyKeyIssued bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registered = append(r.registered, runnerID)
	r.supplyKeySeen = append(r.supplyKeySeen, supplyKeyIssued)
}

func (r *recordingLifecycleObserver) OnHeartbeat(_ context.Context, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeats = append(r.heartbeats, ok)
}

func (r *recordingLifecycleObserver) OnSupplyGateWired(present bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gateWired = append(r.gateWired, present)
}

func (r *recordingLifecycleObserver) OnSupplyFetch(_ context.Context, name, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.supplyFetchLog = append(r.supplyFetchLog, name+"="+result)
}

func (r *recordingLifecycleObserver) snapshot() ([]string, []bool, []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.registered...),
		append([]bool(nil), r.supplyKeySeen...),
		append([]bool(nil), r.heartbeats...)
}

// TestLifecycleObserverSeesRegistrationAndHeartbeat pins the two transitions a
// host process needs to answer a readiness probe: neither is visible today,
// because Run is the only exported method and it returns nothing until it ends.
func TestLifecycleObserverSeesRegistrationAndHeartbeat(t *testing.T) {
	obs := &recordingLifecycleObserver{}
	client := &registerKeyClient{supplyKey: ""}
	fetcher := &HTTPSupplyFetcher{BaseURL: "http://unused"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		HeartbeatInterval: 10 * time.Millisecond,
		PollWait:          time.Millisecond,
		SupplyGate:        NewSupplyGate(fetcher, nil, quietLogger()),
		LifecycleObserver: obs,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()
	client.waitForBeats(t, 1)
	cancel()
	<-errCh

	registered, supplyKey, heartbeats := obs.snapshot()
	if len(registered) != 1 {
		t.Fatalf("OnRegistered calls = %d, want 1", len(registered))
	}
	if len(supplyKey) != 1 || supplyKey[0] {
		t.Fatalf("supplyKeyIssued = %v, want [false]: the fake server issues no key", supplyKey)
	}
	if len(heartbeats) == 0 || !heartbeats[0] {
		t.Fatalf("heartbeats = %v, want a leading true", heartbeats)
	}
}
