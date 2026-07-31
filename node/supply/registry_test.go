package supply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingConsumer struct {
	mu   sync.Mutex
	seen []Snapshot
	fail error
}

func (c *recordingConsumer) OnSupplyChanged(_ context.Context, snap Snapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, snap)
	return c.fail
}

func (c *recordingConsumer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

func snap(name, content string, rev uint64) Snapshot {
	return Snapshot{Name: name, Content: []byte(content), Hash: "h-" + content,
		Revision: rev, FetchedAt: time.Now()}
}

func TestApplyStoresAndNotifies(t *testing.T) {
	r := NewRegistry()
	c := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", c)

	if err := r.Apply(context.Background(), snap("rules", "v1", 1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, ok := r.Get("rules")
	if !ok || string(got.Content) != "v1" || got.Revision != 1 {
		t.Fatalf("Get = %+v ok=%v", got, ok)
	}
	if c.count() != 1 {
		t.Fatalf("consumer notified %d times, want 1", c.count())
	}
}

// Identical content must not re-notify: a revision bump with unchanged bytes is
// a no-op for consumers (rebuilding a wasm pool for identical rules is waste).
func TestApplySkipsNotifyOnUnchangedHash(t *testing.T) {
	r := NewRegistry()
	c := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", c)

	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1))
	_ = r.Apply(ctx, snap("rules", "v1", 2))

	if c.count() != 1 {
		t.Fatalf("consumer notified %d times, want 1", c.count())
	}
	got, _ := r.Get("rules")
	if got.Revision != 2 {
		t.Fatalf("revision = %d, want 2 (cache must still record the newer revision)", got.Revision)
	}
}

// A rejecting consumer must not prevent the cache update (ordinary nodes reading
// via $supplies should see the new value) but the error must surface.
func TestApplySurfacesConsumerError(t *testing.T) {
	r := NewRegistry()
	bad := &recordingConsumer{fail: errors.New("configure returned -1")}
	good := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/a", bad)
	r.RegisterConsumer("rules", "node/b", good)

	err := r.Apply(context.Background(), snap("rules", "v1", 1))
	if err == nil || !strings.Contains(err.Error(), "configure returned -1") {
		t.Fatalf("Apply err = %v, want the consumer error", err)
	}
	if good.count() != 1 {
		t.Fatal("a failing consumer must not stop the others")
	}
	if got, ok := r.Get("rules"); !ok || string(got.Content) != "v1" {
		t.Fatalf("cache must still hold the new content: %+v ok=%v", got, ok)
	}
}

func TestRegisterConsumerReplacesSameKey(t *testing.T) {
	r := NewRegistry()
	first, second := &recordingConsumer{}, &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", first)
	r.RegisterConsumer("rules", "node/clean", second)

	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if first.count() != 0 || second.count() != 1 {
		t.Fatalf("re-registering the same key must replace: first=%d second=%d",
			first.count(), second.count())
	}
}

func TestUnregisterConsumerStopsNotifications(t *testing.T) {
	r := NewRegistry()
	c := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", c)
	r.UnregisterConsumer("rules", "node/clean")
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if c.count() != 0 {
		t.Fatalf("unregistered consumer notified %d times", c.count())
	}
}

// A newly-registered consumer must immediately receive the current snapshot:
// activation order between a supply and its consumer is not guaranteed (spec C3).
func TestRegisterConsumerReceivesCurrentSnapshot(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	c := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", c)
	if c.count() != 1 {
		t.Fatalf("late consumer got %d notifications, want the current snapshot", c.count())
	}
}

func TestReadyReportsMissingNames(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	if missing := r.Ready([]string{"rules"}); missing != nil {
		t.Fatalf("Ready = %v, want nil", missing)
	}
	missing := r.Ready([]string{"rules", "tags", "aaa"})
	if len(missing) != 2 || missing[0] != "aaa" || missing[1] != "tags" {
		t.Fatalf("Ready = %v, want sorted [aaa tags]", missing)
	}
	if missing := r.Ready(nil); missing != nil {
		t.Fatalf("Ready(nil) = %v, want nil", missing)
	}
}

func TestGetReturnsCopiedContent(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	got, _ := r.Get("rules")
	got.Content[0] = 'X'
	again, _ := r.Get("rules")
	if string(again.Content) != "v1" {
		t.Fatalf("caller mutated the cached content: %s", again.Content)
	}
}

// panickingConsumer panics on every call to OnSupplyChanged.
type panickingConsumer struct{}

func (panickingConsumer) OnSupplyChanged(context.Context, Snapshot) error {
	panic("boom")
}

// A panicking consumer must not prevent other consumers from being notified, and
// Apply itself must not panic-escape.
func TestApplyRecoversPanickingConsumer(t *testing.T) {
	r := NewRegistry()
	good := &recordingConsumer{}
	// Register three consumers: two panicking and one good. Because map iteration
	// order is non-deterministic, having two panicking consumers ensures that the
	// good one is reached regardless of ordering.
	r.RegisterConsumer("rules", "panic/1", panickingConsumer{})
	r.RegisterConsumer("rules", "panic/2", panickingConsumer{})
	r.RegisterConsumer("rules", "good", good)

	err := r.Apply(context.Background(), snap("rules", "v1", 1))
	// The error must surface (same path as a returned error).
	if err == nil || !strings.Contains(err.Error(), "consumer panicked") {
		t.Fatalf("Apply err = %v, want panic-derived error", err)
	}
	// The good consumer must still be notified.
	if good.count() != 1 {
		t.Fatalf("good consumer notified %d times, want 1", good.count())
	}
	// Cache must still hold the value.
	if got, ok := r.Get("rules"); !ok || string(got.Content) != "v1" {
		t.Fatalf("cache must hold content after panic: %+v ok=%v", got, ok)
	}
}

// A panicking consumer during RegisterConsumer's immediate notification must not
// crash the caller.
func TestRegisterConsumerRecoversPanic(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	// Must not panic-escape.
	r.RegisterConsumer("rules", "panic/late", panickingConsumer{})

	// Registry still functional after the recovered panic.
	good := &recordingConsumer{}
	r.RegisterConsumer("rules", "good", good)
	if good.count() != 1 {
		t.Fatalf("good consumer not notified after panic recovery: %d", good.count())
	}
}

// Concurrent Apply and Get must not race. This gives the race detector real
// concurrency to analyze.
func TestConcurrentApplyAndGet(t *testing.T) {
	r := NewRegistry()
	c := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", c)

	const iterations = 500
	var wg sync.WaitGroup

	// Writer goroutine: repeatedly Apply with changing content.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			content := fmt.Sprintf("v%d", i)
			s := Snapshot{
				Name:      "rules",
				Content:   []byte(content),
				Hash:      fmt.Sprintf("hash-%d", i),
				Revision:  uint64(i + 1),
				FetchedAt: time.Now(),
			}
			_ = r.Apply(context.Background(), s)
		}
	}()

	// Reader goroutines: repeatedly Get.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, _ = r.Get("rules")
			}
		}()
	}

	wg.Wait()

	// Final state: last Apply wrote revision=iterations.
	got, ok := r.Get("rules")
	if !ok {
		t.Fatal("Get returned !ok after concurrent writes")
	}
	if got.Revision != iterations {
		t.Fatalf("revision = %d, want %d", got.Revision, iterations)
	}
}
