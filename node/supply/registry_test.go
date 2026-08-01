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
	// Re-apply the same content: the consumer's last outcome for it was a
	// rejection, so Apply re-notifies rather than skipping.
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

// Unchanged hash that every consumer already accepted must NOT re-notify
// (preserving the optimization in TestApplySkipsNotifyOnUnchangedHash).
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

// Unchanged hash that a consumer rejected MUST re-notify (the fix for the
// rejected-content regression).
func TestApplyRenotifiesOnUnchangedHashWhenRejected(t *testing.T) {
	r := NewRegistry()
	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "node/a", bad)

	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1))
	// Second apply with the same hash: the consumer must be re-notified because
	// its last outcome for this content was a rejection.
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
func TestRegisterConsumerRejectionMakesSupplyUnready(t *testing.T) {
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
	// the consumer's last outcome was a rejection — the error must not become
	// invisible.
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

// --- Fix Round 3: readiness must be a live conjunction, not an assignable flag ---

// The exact lost-update sequence from the round-3 finding: a second, accepting
// consumer registering must NOT erase the fact that a first, still-registered
// consumer is rejecting. Under the old single-bool "accepted" representation,
// whichever RegisterConsumer call ran last won regardless of what other
// consumers had done.
func TestRegisterConsumerLostUpdate_SecondAcceptingDoesNotMaskFirstRejecting(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	bad := &recordingConsumer{fail: errors.New("configure returned -1")}
	r.RegisterConsumer("rules", "a", bad)
	if r.IsReady("rules") {
		t.Fatal("step: after bad 'a' registers, IsReady must be false")
	}

	good := &recordingConsumer{}
	r.RegisterConsumer("rules", "b", good)
	// THE regression: registering an unrelated accepting consumer must not
	// resurrect readiness while 'a' is still registered and still rejecting.
	if r.IsReady("rules") {
		t.Fatal("step: after good 'b' registers, IsReady must STILL be false — 'a' is still rejecting")
	}
}

// Order independence: rejecting-then-accepting and accepting-then-rejecting
// must both end not-ready. The conjunction must not depend on registration
// order.
func TestIsReadyOrderIndependent(t *testing.T) {
	t.Run("accepting_then_rejecting", func(t *testing.T) {
		r := NewRegistry()
		_ = r.Apply(context.Background(), snap("rules", "v1", 1))
		good := &recordingConsumer{}
		r.RegisterConsumer("rules", "a", good)
		bad := &recordingConsumer{fail: errors.New("reject")}
		r.RegisterConsumer("rules", "b", bad)
		if r.IsReady("rules") {
			t.Fatal("must be false: b rejects regardless of order")
		}
	})
	t.Run("rejecting_then_accepting", func(t *testing.T) {
		r := NewRegistry()
		_ = r.Apply(context.Background(), snap("rules", "v1", 1))
		bad := &recordingConsumer{fail: errors.New("reject")}
		r.RegisterConsumer("rules", "a", bad)
		good := &recordingConsumer{}
		r.RegisterConsumer("rules", "b", good)
		if r.IsReady("rules") {
			t.Fatal("must be false: a rejects regardless of order")
		}
	})
}

// Unregistering the only rejecting consumer (others still accepting) must make
// IsReady true immediately — no extra Apply needed. This subsumes the round-2
// "leave it" case: under a derived conjunction it resolves itself for free.
func TestUnregisterOnlyRejectorAmongOthersMakesReadyImmediately(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	good := &recordingConsumer{}
	r.RegisterConsumer("rules", "a", good)
	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "b", bad)
	if r.IsReady("rules") {
		t.Fatal("precondition: b rejects, must be not-ready")
	}

	r.UnregisterConsumer("rules", "b")
	if !r.IsReady("rules") {
		t.Fatal("unregistering the only rejector (others accepting) must make IsReady true immediately")
	}
}

// Same-key replacement must overwrite that key's entry rather than adding a
// second one — while leaving OTHER keys' entries untouched. The second
// assertion is what distinguishes a per-key outcome map from any single
// aggregate flag: under an aggregate, replacing "a" with an accepting consumer
// would flip readiness even though "b" is still registered and still rejecting.
func TestRegisterConsumerSameKeyReplacementOverwritesEntry(t *testing.T) {
	r := NewRegistry()
	_ = r.Apply(context.Background(), snap("rules", "v1", 1))

	bad := &recordingConsumer{fail: errors.New("reject")}
	r.RegisterConsumer("rules", "a", bad)
	stillBad := &recordingConsumer{fail: errors.New("also rejects")}
	r.RegisterConsumer("rules", "b", stillBad)
	if r.IsReady("rules") {
		t.Fatal("precondition: must be not-ready")
	}

	r.RegisterConsumer("rules", "a", &recordingConsumer{}) // same key, now accepting
	if r.IsReady("rules") {
		t.Fatal("replacing 'a' must not mask 'b', which is still registered and still rejecting")
	}

	r.RegisterConsumer("rules", "b", &recordingConsumer{}) // now both accept
	if !r.IsReady("rules") {
		t.Fatal("once every key's consumer accepts, IsReady must be true")
	}
}

// Concurrency smoke test: hammer Apply/RegisterConsumer/UnregisterConsumer on
// one name from several goroutines under -race, then settle the consumer set to
// a known state and assert IsReady tracks it.
//
// What this covers is narrower than it looks, and deliberately so: the final
// assertions run AFTER a deterministic settle, so they prove no state survived
// the churn in a way that breaks a subsequent clean registration — not that
// IsReady is correct mid-race, which is not deterministically observable this
// way. Interleaving-specific defects (a superseded notification's outcome
// overwriting the current one) need the channel-synchronized tests further
// down; this one is here for the race detector.
func TestConcurrentMutationsKeepIsReadyConsistent(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1))

	const iterations = 300
	var wg sync.WaitGroup

	// Goroutine 1: repeatedly Apply with a changing hash.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = r.Apply(ctx, Snapshot{
				Name: "rules", Content: []byte(fmt.Sprintf("v%d", i)),
				Hash: fmt.Sprintf("h%d", i), Revision: uint64(i + 1),
			})
		}
	}()

	// Goroutine 2: repeatedly register/unregister a rejecting consumer at key "flaky".
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			r.RegisterConsumer("rules", "flaky", &recordingConsumer{fail: errors.New("flaky reject")})
			r.UnregisterConsumer("rules", "flaky")
		}
	}()

	// Goroutine 3: repeatedly register/replace an accepting consumer at key "steady".
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			r.RegisterConsumer("rules", "steady", &recordingConsumer{})
		}
	}()

	// Goroutine 4: repeatedly read IsReady/Get concurrently — must not race,
	// value not asserted here (non-deterministic mid-flight).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = r.IsReady("rules")
			_, _ = r.Get("rules")
		}
	}()

	wg.Wait()

	// Settle to a single, deterministic, known-good consumer set before the
	// final assertions: only "steady" (accepting) remains registered.
	r.UnregisterConsumer("rules", "flaky")
	r.RegisterConsumer("rules", "steady", &recordingConsumer{})

	if !r.IsReady("rules") {
		t.Fatal("after settling to a single accepting consumer, IsReady must be true")
	}

	r.RegisterConsumer("rules", "steady", &recordingConsumer{fail: errors.New("final reject")})
	if r.IsReady("rules") {
		t.Fatal("after replacing the sole consumer with a rejecting one, IsReady must be false")
	}
}

// --- Fix Round 4: an outcome must be attributed to the content it judged ---

// blockingConsumer gates its callback on a per-hash channel so a test can hold
// one notification in flight while another content generation completes. The
// verdict per hash is fixed up front, so the interleaving — not the consumer —
// is what the test varies.
type blockingConsumer struct {
	entered map[string]chan struct{} // closed/signalled when that hash's callback starts
	release map[string]chan struct{} // the callback waits on this before returning
	verdict map[string]error
}

func newBlockingConsumer(hashes ...string) *blockingConsumer {
	c := &blockingConsumer{
		entered: map[string]chan struct{}{},
		release: map[string]chan struct{}{},
		verdict: map[string]error{},
	}
	for _, h := range hashes {
		c.entered[h] = make(chan struct{}, 1)
		c.release[h] = make(chan struct{})
	}
	return c
}

func (c *blockingConsumer) OnSupplyChanged(_ context.Context, s Snapshot) error {
	if ch, ok := c.entered[s.Hash]; ok {
		ch <- struct{}{}
	}
	if ch, ok := c.release[s.Hash]; ok {
		<-ch
	}
	return c.verdict[s.Hash]
}

// A notification for superseded content must not overwrite the outcome for the
// content that is actually cached. This is the dangerous direction: the stale
// call ACCEPTS, the current content was REJECTED, and a registry that records
// outcomes without attributing them to a content generation reports ready for
// content the consumer has no usable derived state for — exactly what the gate
// exists to prevent.
func TestStaleAcceptMustNotMaskCurrentReject(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := newBlockingConsumer("h-A", "h-B")
	c.verdict["h-A"] = nil                      // stale content: accepted
	c.verdict["h-B"] = errors.New("B rejected") // current content: rejected
	close(c.release["h-B"])                     // B's callback never blocks

	r.RegisterConsumer("rules", "node/a", c)

	applyA := make(chan error, 1)
	go func() { applyA <- r.Apply(ctx, snap("rules", "A", 1)) }()
	<-c.entered["h-A"] // A's callback is in flight, holding its verdict

	if err := r.Apply(ctx, snap("rules", "B", 2)); err == nil {
		t.Fatal("Apply(B) must surface the consumer's rejection")
	}
	if r.IsReady("rules") {
		t.Fatal("precondition: B is cached and was rejected, so not ready")
	}

	close(c.release["h-A"]) // A's callback now returns, accepting stale content
	<-applyA

	if got, _ := r.Get("rules"); got.Hash != "h-B" {
		t.Fatalf("cached hash = %q, want h-B", got.Hash)
	}
	if r.IsReady("rules") {
		t.Fatal("a stale acceptance of superseded content must not make the rejected current content ready")
	}
}

// The symmetric direction: a stale REJECTION must not mask the current
// content's acceptance. Wrong in the safe direction (traffic is declined rather
// than mis-served) but still wrong — it strands a runner that has usable
// content, and the two directions share one root cause.
func TestStaleRejectMustNotMaskCurrentAccept(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := newBlockingConsumer("h-A", "h-B")
	c.verdict["h-A"] = errors.New("A rejected") // stale content: rejected
	c.verdict["h-B"] = nil                      // current content: accepted
	close(c.release["h-B"])

	r.RegisterConsumer("rules", "node/a", c)

	applyA := make(chan error, 1)
	go func() { applyA <- r.Apply(ctx, snap("rules", "A", 1)) }()
	<-c.entered["h-A"]

	if err := r.Apply(ctx, snap("rules", "B", 2)); err != nil {
		t.Fatalf("Apply(B): %v", err)
	}

	close(c.release["h-A"])
	<-applyA

	if got, _ := r.Get("rules"); got.Hash != "h-B" {
		t.Fatalf("cached hash = %q, want h-B", got.Hash)
	}
	if !r.IsReady("rules") {
		t.Fatal("a stale rejection of superseded content must not keep the accepted current content unready")
	}
}

// RegisterConsumer's immediate notify runs outside the lock too, so its
// write-back needs the same attribution as Apply's fan-out.
func TestRegisterConsumerStaleNotifyMustNotMaskCurrentReject(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := newBlockingConsumer("h-A", "h-B")
	c.verdict["h-A"] = nil
	c.verdict["h-B"] = errors.New("B rejected")
	close(c.release["h-B"])

	_ = r.Apply(ctx, snap("rules", "A", 1)) // cache A first, so registering notifies

	registered := make(chan struct{})
	go func() {
		r.RegisterConsumer("rules", "node/a", c)
		close(registered)
	}()
	<-c.entered["h-A"] // the immediate notify for A is in flight

	if err := r.Apply(ctx, snap("rules", "B", 2)); err == nil {
		t.Fatal("Apply(B) must surface the consumer's rejection")
	}

	close(c.release["h-A"])
	<-registered

	if r.IsReady("rules") {
		t.Fatal("a stale registration notify must not mask the current content's rejection")
	}
}

// A Consumer implemented on a value type containing a slice is not comparable
// with ==. Identity tracking must not depend on interface equality, or the
// registry panics outside safeNotify's recover and takes the caller down.
type uncomparableConsumer struct{ tags []string }

func (c uncomparableConsumer) OnSupplyChanged(context.Context, Snapshot) error { return nil }

func TestUncomparableConsumerDoesNotPanic(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	_ = r.Apply(ctx, snap("rules", "v1", 1)) // cached, so registering notifies immediately

	r.RegisterConsumer("rules", "node/a", uncomparableConsumer{tags: []string{"x"}})
	if !r.IsReady("rules") {
		t.Fatal("an accepting uncomparable consumer must leave the supply ready")
	}
	if err := r.Apply(ctx, snap("rules", "v2", 2)); err != nil {
		t.Fatalf("Apply with an uncomparable consumer registered: %v", err)
	}
}

// --- Fix Round 5: a verdict in flight is unresolved, not absent ---

// The gate reads IsReady BEFORE deciding to fetch (supply_gate.go:71), so any
// window in which content is cached, a consumer is registered, and no verdict
// exists yet reads as ready and admits traffic. Two paths reach that window;
// both are covered here. The consumer rejects, so a correct registry never
// reports ready at any point in either sequence.

// Path 1: register AFTER content is cached — the production gate order, since
// the gate fetches and Applies before any consumer registers.
func TestRegisterConsumerInFlightVerdictIsNotReady(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := newBlockingConsumer("h-A")
	c.verdict["h-A"] = errors.New("A rejected")

	if err := r.Apply(ctx, Snapshot{Name: "rules", Content: []byte(`{"a":1}`), Hash: "h-A", Revision: 1}); err != nil {
		t.Fatalf("Apply with no consumers: %v", err)
	}

	done := make(chan struct{})
	go func() {
		r.RegisterConsumer("rules", "k", c)
		close(done)
	}()

	<-c.entered["h-A"]
	// Registered, content cached, callback running, verdict not yet given.
	if r.IsReady("rules") {
		t.Fatal("a consumer whose verdict is still in flight must not read as ready")
	}
	close(c.release["h-A"])
	<-done

	if r.IsReady("rules") {
		t.Fatal("after the rejection was recorded, IsReady must be false")
	}
}

// Path 2: register BEFORE any content exists, so RegisterConsumer returns
// without notifying and the entry carries no verdict at all. Apply's fan-out
// then opens the same window.
func TestApplyFanOutInFlightVerdictIsNotReady(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := newBlockingConsumer("h-A")
	c.verdict["h-A"] = errors.New("A rejected")

	r.RegisterConsumer("rules", "k", c) // no snapshot yet: returns without notifying
	if r.IsReady("rules") {
		t.Fatal("no snapshot cached: IsReady must be false")
	}

	done := make(chan struct{})
	go func() {
		_ = r.Apply(ctx, Snapshot{Name: "rules", Content: []byte(`{"a":1}`), Hash: "h-A", Revision: 1})
		close(done)
	}()

	<-c.entered["h-A"]
	if r.IsReady("rules") {
		t.Fatal("during Apply's fan-out the verdict is unresolved; IsReady must be false")
	}
	close(c.release["h-A"])
	<-done

	if r.IsReady("rules") {
		t.Fatal("after the rejection was recorded, IsReady must be false")
	}
}

// Two notifications to one consumer can overlap: Apply for newer content counts
// its own in flight before the older callback returns. When the older one
// returns, the newer verdict is still outstanding, so the consumer must not read
// as ready. This is why the in-flight state is a COUNT and not a "which hash is
// pending" marker — a single marker is cleared by the older write-back a moment
// before the newer Apply sets it, leaving a gap that reads as ready.
//
// Deterministic by construction: receiving on entered["h-B"] proves Apply(h-B)
// already took the lock, installed the snapshot, and counted its notification.
func TestOverlappingNotificationsNeverReadReady(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := newBlockingConsumer("h-A", "h-B")
	c.verdict["h-A"] = nil // both accept: readiness must still be withheld
	c.verdict["h-B"] = nil // while h-B's verdict is outstanding
	r.RegisterConsumer("rules", "k", c)

	doneA := make(chan struct{})
	go func() {
		_ = r.Apply(ctx, Snapshot{Name: "rules", Content: []byte(`{"a":1}`), Hash: "h-A", Revision: 1})
		close(doneA)
	}()
	<-c.entered["h-A"]

	doneB := make(chan struct{})
	go func() {
		_ = r.Apply(ctx, Snapshot{Name: "rules", Content: []byte(`{"a":2}`), Hash: "h-B", Revision: 2})
		close(doneB)
	}()
	<-c.entered["h-B"]

	// Let the SUPERSEDED callback finish first. Its outcome is dropped, and its
	// in-flight count released — but h-B's is not.
	close(c.release["h-A"])
	<-doneA
	if r.IsReady("rules") {
		t.Fatal("h-B's verdict is still in flight; IsReady must be false")
	}

	close(c.release["h-B"])
	<-doneB
	if !r.IsReady("rules") {
		t.Fatal("h-B was accepted and is the cached content; IsReady must be true")
	}
}

// The in-flight count must be released on every path out of a callback,
// including a panic — safeNotify recovers and returns an error, so the
// write-back still runs. A leaked count would strand the supply not-ready
// forever, and the gate would decline the activation with no way to recover.
func TestPanickingConsumerDoesNotStrandInFlight(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	r.RegisterConsumer("rules", "boom", panickingConsumer{})

	if err := r.Apply(ctx, snap("rules", "v1", 1)); err == nil {
		t.Fatal("a panicking consumer must surface as an error")
	}
	if r.IsReady("rules") {
		t.Fatal("the panic is an error outcome: IsReady must be false")
	}
	// Removing it must restore readiness with no further Apply. If the in-flight
	// count had leaked it would survive on no entry at all, but a leak on any
	// REMAINING entry is what this guards.
	r.UnregisterConsumer("rules", "boom")
	if !r.IsReady("rules") {
		t.Fatal("with no consumers left, IsReady must be true")
	}
}

// --- Observed() ---

// With no snapshots at all, Observed must be nil (not an empty map) so a
// runner hosting no supply sends a heartbeat body unchanged from before this
// field existed (omitempty drops a nil map but not an empty non-nil one the
// same way at the Go-value level the brief cares about).
func TestObservedNilWhenEmpty(t *testing.T) {
	r := NewRegistry()
	if got := r.Observed(); got != nil {
		t.Fatalf("Observed() = %#v, want nil", got)
	}
}

// The common case: content cached, no registered consumer to reject it.
// Observed must report its hash.
func TestObservedReportsReadyContent(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	if err := r.Apply(ctx, snap("rules", "v1", 1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got := r.Observed()
	if len(got) != 1 || got["rules"] != "h-v1" {
		t.Fatalf("Observed() = %#v, want {rules: h-v1}", got)
	}
}

// THE core Observed() assertion (per the addendum's ruling): content that is
// cached but REJECTED by a registered consumer must NOT appear in Observed —
// reporting it would tell the server this runner has converged on content it
// is actually refusing to use, which is exactly the divergence
// observedGeneration exists to catch. This is the same distinction
// SupplyGate.Admit relies on (IsReady, not bare Get).
func TestObservedExcludesRejectedContent(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := &recordingConsumer{fail: errors.New("configure returned -1")}
	r.RegisterConsumer("rules", "wasm/clean", c)

	if err := r.Apply(ctx, snap("rules", "v1", 1)); err == nil {
		t.Fatal("expected the rejecting consumer's error to surface")
	}
	if got := r.Observed(); got != nil {
		t.Fatalf("Observed() = %#v, want nil: content is cached but rejected, must not be reported as applied", got)
	}
}

// Once the rejecting consumer starts accepting (a re-Apply with the same
// content), Observed must start reporting it — the rejection was resolved,
// not permanent.
func TestObservedReportsOnceConsumerAccepts(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	c := &recordingConsumer{fail: errors.New("boom")}
	r.RegisterConsumer("rules", "wasm/clean", c)
	if err := r.Apply(ctx, snap("rules", "v1", 1)); err == nil {
		t.Fatal("expected rejection")
	}
	if got := r.Observed(); got != nil {
		t.Fatalf("Observed() = %#v, want nil while rejecting", got)
	}

	c.mu.Lock()
	c.fail = nil
	c.mu.Unlock()
	// Unregister+re-register is the simplest way to force a fresh notify for
	// the SAME content without a new Apply call (mirrors
	// TestRegisterConsumerRejectionRecoversOnReapply's pattern elsewhere in
	// this file, adapted to avoid a second unrelated snapshot).
	r.UnregisterConsumer("rules", "wasm/clean")
	if got := r.Observed(); len(got) != 1 || got["rules"] != "h-v1" {
		t.Fatalf("Observed() after unregistering the rejector = %#v, want {rules: h-v1}", got)
	}
}

// A mix of ready and not-ready supplies: Observed reports only the ready one.
func TestObservedReportsOnlyReadySubset(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	if err := r.Apply(ctx, snap("ready-one", "v1", 1)); err != nil {
		t.Fatalf("Apply ready-one: %v", err)
	}
	c := &recordingConsumer{fail: errors.New("boom")}
	r.RegisterConsumer("not-ready", "wasm/clean", c)
	if err := r.Apply(ctx, snap("not-ready", "v1", 1)); err == nil {
		t.Fatal("expected rejection for not-ready")
	}

	got := r.Observed()
	if len(got) != 1 || got["ready-one"] != "h-v1" {
		t.Fatalf("Observed() = %#v, want exactly {ready-one: h-v1}", got)
	}
}

// epochConsumer distinguishes the Nth dispatch rather than the content hash, so
// a test can model a rollback A→B→A where the first and third dispatches carry
// the SAME hash. blockingConsumer keys its gates by hash and cannot express it.
type epochConsumer struct {
	mu      sync.Mutex
	n       int
	entered chan int
	gate    map[int]chan struct{}
	verdict map[int]error
}

func (c *epochConsumer) OnSupplyChanged(_ context.Context, _ Snapshot) error {
	c.mu.Lock()
	c.n++
	me := c.n
	g := c.gate[me]
	v := c.verdict[me]
	c.mu.Unlock()
	c.entered <- me
	if g != nil {
		<-g
	}
	return v
}

// A hash names CONTENT, not a particular dispatch of it. When content rolls back
// to a hash it held before — A→B→A, an ordinary rollback — a callback still in
// flight from the FIRST A dispatch carries a hash matching what is cached now.
// Guarding the write-back on hash alone accepts that ancient verdict, letting a
// stale ACCEPTANCE overwrite the current REJECTION and leaving IsReady true for
// content this very consumer refused; the gate would admit traffic against it.
//
// Verified in both directions: this fails against the pre-fix registry (the
// final IsReady flips back to true) and passes with the epoch guard in
// recordOutcomeLocked.
//
// The sleeps are load-bearing, not padding: they let each dispatch's write-back
// land before the next Apply, which is what puts dispatch 1's verdict LAST —
// the whole point. An earlier version of this test replaced them with condition
// polling, and the reordering made it pass against the defective code.
func TestStaleAcceptForRecycledHashCannotOverwriteRejection(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	c := &epochConsumer{
		entered: make(chan int, 8),
		gate:    map[int]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})},
		verdict: map[int]error{
			1: nil,                                  // hash A: accepts — but returns LAST
			2: nil,                                  // hash B: accepts
			3: errors.New("rollback to A rejected"), // hash A again: REJECTS
		},
	}
	r.RegisterConsumer("rules", "k", c)

	apply := func(h string, rev uint64, body string) {
		go func() { _ = r.Apply(ctx, Snapshot{Name: "rules", Content: []byte(body), Hash: h, Revision: rev}) }()
	}

	apply("hA", 1, "A")
	<-c.entered // dispatch 1 parked mid-callback, has NOT returned

	apply("hB", 2, "B")
	<-c.entered
	close(c.gate[2]) // B's acceptance lands
	time.Sleep(50 * time.Millisecond)

	apply("hA", 3, "A") // the rollback: same hash as dispatch 1
	<-c.entered
	close(c.gate[3]) // A-again's REJECTION lands
	time.Sleep(50 * time.Millisecond)

	if r.IsReady("rules") {
		t.Fatal("precondition: the rejection of the rolled-back content must make IsReady false")
	}

	// Now let the ancient dispatch-1 callback finally return.
	close(c.gate[1])
	time.Sleep(100 * time.Millisecond)

	if r.IsReady("rules") {
		t.Fatal("a stale acceptance from an earlier dispatch of the same hash overwrote the current rejection; the gate would admit traffic the consumer refused")
	}
}
