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

// --- IsReady tests ---

func TestIsReadyTrueWhenNoConsumers(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if !r.IsReady("rules") {
		t.Fatal("IsReady must be true when content is cached and no consumers exist")
	}
}

func TestIsReadyFalseWhenConsumerRejects(t *testing.T) {
	r := NewRegistry()
	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "node/a", bad)

	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if r.IsReady("rules") {
		t.Fatal("IsReady must be false when a consumer rejected")
	}
}

func TestIsReadyFalseWhenNoSnapshot(t *testing.T) {
	r := NewRegistry()
	if r.IsReady("rules") {
		t.Fatal("IsReady must be false when no snapshot exists")
	}
}

func TestIsReadyBecomesTrue_AfterReapplyWithAcceptingConsumer(t *testing.T) {
	r := NewRegistry()
	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "node/a", bad)
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if r.IsReady("rules") {
		t.Fatal("precondition: must be not-ready")
	}

	// Consumer starts accepting.
	bad.fail = nil
	// Re-apply same content: since accepted=false, Apply re-notifies.
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if !r.IsReady("rules") {
		t.Fatal("IsReady must become true after consumer accepts on re-apply")
	}
}

func TestIsReadyBecomesTrueAfterUnregister(t *testing.T) {
	r := NewRegistry()
	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "node/a", bad)
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if r.IsReady("rules") {
		t.Fatal("precondition: must be not-ready")
	}

	r.UnregisterConsumer("rules", "node/a")
	if !r.IsReady("rules") {
		t.Fatal("IsReady must become true after the rejecting consumer is unregistered")
	}
}

// Unchanged hash with accepted=true must NOT re-notify (preserving the
// optimization in TestApplySkipsNotifyOnUnchangedHash).
func TestApplySkipsNotifyOnUnchangedHashWhenAccepted(t *testing.T) {
	r := NewRegistry()
	c := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/clean", c)

	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1))
	_ = r.Apply(ctx, snap("rules", "v1", 2))

	if c.count() != 1 {
		t.Fatalf("consumer notified %d times, want 1 (unchanged hash, already accepted)", c.count())
	}
}

// Unchanged hash with accepted=false MUST re-notify (the fix for the rejected
// content regression).
func TestApplyRenotifiesOnUnchangedHashWhenRejected(t *testing.T) {
	r := NewRegistry()
	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "node/a", bad)

	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1))
	// Second apply with same hash: consumer should be re-notified because
	// accepted is false.
	_ = r.Apply(ctx, snap("rules", "v1", 2))

	if bad.count() != 2 {
		t.Fatalf("consumer notified %d times, want 2 (re-notify on rejected)", bad.count())
	}
}

// --- Fix Round 2: RegisterConsumer's immediate-notify must feed accepted too ---

// The exact 4-step sequence from the round-2 finding: content is cached BEFORE
// any consumer registers (the gate's normal order — fetch, then Apply, then a
// consumer registers later). A rejecting consumer's immediate notify must mark
// the supply unaccepted, and that state must not silently vanish on the next
// same-hash Apply.
func TestRegisterConsumerRejectionMarksUnaccepted(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	// Step 1: Apply caches content while no consumer is registered.
	_ = r.Apply(ctx, snap("rules", "v1", 1))
	if !r.IsReady("rules") {
		t.Fatal("step 1: no consumer yet, must be ready")
	}

	// Step 2: A consumer registers and its immediate-notify rejects.
	bad := &recordingConsumer{fail: errors.New("configure returned -1")}
	r.RegisterConsumer("rules", "node/a", bad)
	if r.IsReady("rules") {
		t.Fatal("step 2: rejecting consumer's immediate notify must mark the supply NOT ready")
	}

	// Step 3: a later same-hash Apply must re-notify (not silently skip) because
	// accepted is false — the error must not become invisible.
	err := r.Apply(ctx, snap("rules", "v1", 2))
	if bad.count() != 2 {
		t.Fatalf("step 3: consumer notified %d times, want 2 (re-apply must re-notify when unaccepted)", bad.count())
	}
	if err == nil || !strings.Contains(err.Error(), "configure returned -1") {
		t.Fatalf("step 3: Apply err = %v, want the consumer's rejection surfaced, not nil", err)
	}
	if r.IsReady("rules") {
		t.Fatal("step 3: must remain NOT ready — the state must not self-heal on its own without an actual acceptance")
	}
}

// Recovery: once the rejecting consumer is replaced by (or itself becomes) one
// that accepts, and a same-hash Apply re-notifies, IsReady becomes true.
func TestRegisterConsumerRejectionRecoversOnReapply(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	_ = r.Apply(ctx, snap("rules", "v1", 1))
	bad := &recordingConsumer{fail: errors.New("configure returned -1")}
	r.RegisterConsumer("rules", "node/a", bad)
	if r.IsReady("rules") {
		t.Fatal("precondition: must be not-ready after rejecting register")
	}

	// The consumer starts accepting.
	bad.fail = nil
	if err := r.Apply(ctx, snap("rules", "v1", 2)); err != nil {
		t.Fatalf("Apply after consumer fixed: %v", err)
	}
	if !r.IsReady("rules") {
		t.Fatal("IsReady must become true once the consumer accepts on re-apply")
	}
}

// Recovery via replacement: RegisterConsumer under the same key replaces the
// rejecting consumer with one that accepts; the replacement's own immediate
// notify marks the supply ready without needing a new Apply.
func TestRegisterConsumerReplacementAcceptsMarksReady(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	_ = r.Apply(ctx, snap("rules", "v1", 1))
	bad := &recordingConsumer{fail: errors.New("configure returned -1")}
	r.RegisterConsumer("rules", "node/a", bad)
	if r.IsReady("rules") {
		t.Fatal("precondition: must be not-ready")
	}

	good := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/a", good) // same key, replaces bad
	if !r.IsReady("rules") {
		t.Fatal("IsReady must become true once the replacement consumer accepts on immediate notify")
	}
}

// The no-consumer case must remain ready — RegisterConsumer's change must not
// regress the common case of a supply read only through $supplies expressions.
func TestRegisterConsumerNoConsumerStaysReady(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))
	if !r.IsReady("rules") {
		t.Fatal("no consumer at all: must stay ready")
	}
}

// An accepting consumer that registers after content is cached must leave the
// supply ready, with no spurious transition to not-ready in between.
func TestRegisterConsumerAcceptingStaysReady(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	good := &recordingConsumer{}
	r.RegisterConsumer("rules", "node/a", good)
	if !r.IsReady("rules") {
		t.Fatal("an accepting consumer's immediate notify must not mark the supply unready")
	}
}

// Ready(names) must agree with IsReady, not merely check snapshot presence:
// a name with a rejected consumer must be reported missing even though a
// snapshot is cached.
func TestReadyAgreesWithIsReady(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1))
	_ = r.Apply(ctx, snap("tags", "v1", 1))

	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "node/a", bad) // rejects -> rules becomes unready

	missing := r.Ready([]string{"rules", "tags", "absent"})
	if len(missing) != 2 || missing[0] != "absent" || missing[1] != "rules" {
		t.Fatalf("Ready = %v, want sorted [absent rules] (tags is ready, rules rejected, absent has no snapshot)", missing)
	}
}


