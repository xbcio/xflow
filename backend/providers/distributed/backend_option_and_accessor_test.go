package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file closes a set of zero-coverage gaps found in backend.go's config
// Option constructors and Backend accessor methods. None of the assertions
// below existed anywhere in the package before this file: each was checked by
// grepping every *_test.go for the symbol under test and finding no call site
// that exercises the branch in question.
//
// Two different failure shapes are covered:
//
//   - Accessors (Registry, TriggerPrimitives, NewEntryActivationStore) can be
//     satisfied by *any* value of the right type, including a freshly
//     constructed, empty one that shares no state with the Backend. A test
//     that only checks "the call does not panic / returns non-nil" cannot
//     distinguish "returns b's own registry" from "returns execution.NewRegistry()".
//     Each test below proves identity/state-sharing with the Backend's own
//     Redis or registry, not just non-nilness.
//   - Option nil-guards (`if obs != nil { ... }`) silently do nothing when
//     called with nil, by design (so a caller can pass a possibly-nil optional
//     dependency without an explicit branch). Nothing pinned that the guard
//     leaves a previously-configured value alone rather than always writing.

// probeActionHandler is a minimal types.ActionHandler stub used only to prove
// that Backend.Registry() resolves through the same *execution.Registry the
// rest of the backend uses, not a lookalike.
type probeActionHandler struct{}

func (probeActionHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.probe-action"}
}

func (probeActionHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

// TestRegistryAccessorResolvesHandlersRegisteredOnTheBackendsOwnRegistry pins
// Backend.Registry() (backend.go: `func (b *Backend) Registry() engine.HandlerRegistry
// { return b.registry }`) to actually returning b's own *execution.Registry.
// A mutation that swaps the body for `return execution.NewRegistry()` (a fresh,
// empty registry) compiles and satisfies engine.HandlerRegistry.
//
// This is NOT zero-to-one coverage, and the distinction is worth stating
// precisely. Within this package -- and across sdk/xflow, service/apiserver,
// service/control, cmd/server and cmd/xflow -- that mutation is invisible: all
// six packages stay green. But it IS caught, by
// test/integration's TestRemoteTriggerHosting_Redis, which fails with
// `entry-seed: unexpected status 404` because the handler never lands in the
// registry the runner resolves through.
//
// So what this test buys is not new coverage, it is a cheaper and far more
// legible failure: a sub-second in-package assertion naming the exact accessor,
// instead of a 404 surfacing from a Redis-backed end-to-end trigger-hosting
// test whose message says nothing about Registry().
func TestRegistryAccessorResolvesHandlersRegisteredOnTheBackendsOwnRegistry(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	defer mr.Close()

	b, err := New(mr.Addr(), nil, WithConsumer(false))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer b.nonConsumerStop()()

	h := probeActionHandler{}
	execID := types.ExecutionID("registry-accessor-probe")
	b.registry.RegisterExecutionHandler(execID, "probe-node", h)

	got, err := b.Registry().Get(execID, "probe-node", "irrelevant-node-type", 0)
	if err != nil {
		t.Fatalf("Registry().Get() error = %v: the accessor did not resolve a "+
			"handler registered on the backend's own registry", err)
	}
	if got != h {
		t.Fatalf("Registry().Get() = %#v, want the exact handler instance "+
			"registered on b.registry: the accessor is not exposing the "+
			"backend's shared registry", got)
	}
}

// TestTriggerPrimitivesAccessorIsBackedByTheBackendsOwnRedis pins
// Backend.TriggerPrimitives() to sharing Redis state with the rest of the
// Backend, rather than merely returning a non-nil backend.TriggerPrimitives.
// Nothing in the suite called .TriggerPrimitives() before this test. A
// mutation that built a fresh trigger.Primitives against an unrelated (or
// unreachable) Redis client would still satisfy the interface and would only
// be caught by observing that state written through the accessor is visible
// on a second call — exactly what Dedup's SETNX-once semantics let us probe.
func TestTriggerPrimitivesAccessorIsBackedByTheBackendsOwnRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	defer mr.Close()

	b, err := New(mr.Addr(), nil, WithConsumer(false))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer b.nonConsumerStop()()

	ctx := context.Background()
	key := "trigger-primitives-accessor-probe"

	first, err := b.TriggerPrimitives().Dedup(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("first Dedup() error = %v", err)
	}
	if !first {
		t.Fatal("first Dedup() = false, want true for a fresh key")
	}

	second, err := b.TriggerPrimitives().Dedup(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("second Dedup() error = %v", err)
	}
	if second {
		t.Fatal("second Dedup() = true, want false: TriggerPrimitives() is not " +
			"exposing state shared with the backend's own Redis (a fresh or " +
			"disconnected instance would also see the key as new)")
	}
}

// TestNewEntryActivationStoreHonorsTheConfiguredTTLOnTheBackendsRedis pins
// Backend.NewEntryActivationStore(ttl) (`return rstate.NewEntryActivationStore(b.rdb, ttl)`)
// to (a) actually passing ttl through rather than a hardcoded default, and
// (b) building the store on the backend's own Redis client.
//
// The ttl argument is genuinely unpinned anywhere. Hardcoding the body to
// `rstate.NewEntryActivationStore(b.rdb, time.Hour)` -- ignoring the caller's
// value entirely -- was verified green across this package, sdk/xflow,
// service/apiserver, service/control, cmd/server, cmd/xflow, AND across
// test/integration's two live callers (TestEntryActivationLifecycleE2E_Redis
// and TestRemoteTriggerHosting_Redis, both of which pass time.Minute and both
// of which still passed). Three real call sites hand this accessor a TTL and
// not one of them notices when it is thrown away.
//
// rstate's Upsert EXPIREs only the activation hash key (KEYS[2]); the workflow
// watermark key (KEYS[1]) is written with SET and never expires. So after one
// Upsert exactly one key in the whole database should carry a TTL, and it
// should equal the ttl argument exactly (EXPIRE takes whole seconds and both
// probed durations here are whole seconds, so no truncation ambiguity).
func TestNewEntryActivationStoreHonorsTheConfiguredTTLOnTheBackendsRedis(t *testing.T) {
	for _, ttl := range []time.Duration{2 * time.Second, 9 * time.Second} {
		t.Run(ttl.String(), func(t *testing.T) {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatalf("miniredis.Run() error = %v", err)
			}
			defer mr.Close()

			b, err := New(mr.Addr(), nil, WithConsumer(false))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer b.nonConsumerStop()()

			store := b.NewEntryActivationStore(ttl)
			// Deliberately no RegistryRevision here: that field exists only in a
			// parallel in-flight change to engine.EntryActivation and is absent at
			// HEAD. This test is about the ttl argument reaching Redis, so it must
			// not be coupled to a field that may or may not exist.
			act := engine.EntryActivation{
				Namespace:       namespace.Default,
				WorkflowID:      types.WorkflowID("wf-entryact-probe"),
				WorkflowVersion: "v1",
				EntryUnitID:     "unit-0",
			}
			if err := store.Upsert(context.Background(), act); err != nil {
				t.Fatalf("Upsert() error = %v: NewEntryActivationStore is not "+
					"reachable through the backend's own Redis client", err)
			}

			var expiring []string
			for _, k := range mr.Keys() {
				if mr.TTL(k) > 0 {
					expiring = append(expiring, k)
				}
			}
			if len(expiring) != 1 {
				t.Fatalf("keys with a positive TTL after Upsert = %v, want exactly "+
					"1 (the activation hash); the workflow watermark key must stay "+
					"persistent", expiring)
			}
			if got := mr.TTL(expiring[0]); got != ttl {
				t.Fatalf("activation key TTL = %v, want exactly the configured %v: "+
					"NewEntryActivationStore is not passing the ttl argument through "+
					"to the store it builds", got, ttl)
			}
		})
	}
}

// stubAuditObserver, stubLeaseObserver and stubQueueObserver are minimal
// implementations of backend.go's AuditObserver/LeaseObserver/QueueObserver
// type aliases (rstate.AuditObserver, rstate.LeaseObserver, queue.Observer),
// implemented structurally here so this file does not need to import either
// internal/rstate or internal/queue.
type stubAuditObserver struct{}

func (stubAuditObserver) OnAuditOK(context.Context, string)            {}
func (stubAuditObserver) OnAuditFailed(context.Context, string, error) {}

type stubLeaseObserver struct{}

func (stubLeaseObserver) OnLeaseAcquire(context.Context, string, time.Duration)        {}
func (stubLeaseObserver) OnLeaseExpiryScan(context.Context, int, time.Duration, error) {}
func (stubLeaseObserver) OnLeaseRepair(context.Context, int, time.Duration, error)     {}

type stubQueueObserver struct{}

func (stubQueueObserver) OnEnqueue(string, time.Duration, error) {}

// TestOptionNilGuardsLeaveAnAlreadyConfiguredValueAlone pins the `if x != nil`
// guard present in every one of these Option constructors: WithAuditObserver,
// WithLeaseObserver, WithStateLogger, WithQueueObserver, WithShutdownObserver
// and WithTransport all read "if the caller passes nil, don't overwrite
// whatever is already configured." Nothing in the suite ever called any of
// these Option constructors with nil, so a mutation that dropped any single
// guard (unconditionally assigning the nil argument) would compile clean and
// pass every existing test.
//
// Each sub-test configures a real value first, then applies the same Option
// with nil, and asserts the real value survived. Sub-tests are independent
// config values, so a guard removed from one Option function only fails its
// own sub-test — this is intentional: it lets a single mutation be pinpointed
// by running `-run .../<OptionName>` rather than re-deriving which of six
// guards broke from one failure.
func TestOptionNilGuardsLeaveAnAlreadyConfiguredValueAlone(t *testing.T) {
	t.Run("WithAuditObserver", func(t *testing.T) {
		sentinel := stubAuditObserver{}
		c := &config{}
		WithAuditObserver(sentinel)(c)
		WithAuditObserver(nil)(c)
		if c.auditObserver != sentinel {
			t.Fatalf("auditObserver = %#v, want the sentinel to survive a nil call", c.auditObserver)
		}
	})

	t.Run("WithLeaseObserver", func(t *testing.T) {
		sentinel := stubLeaseObserver{}
		c := &config{}
		WithLeaseObserver(sentinel)(c)
		WithLeaseObserver(nil)(c)
		if c.leaseObserver != sentinel {
			t.Fatalf("leaseObserver = %#v, want the sentinel to survive a nil call", c.leaseObserver)
		}
	})

	t.Run("WithStateLogger", func(t *testing.T) {
		sentinel := &recordingLogger{}
		c := &config{}
		WithStateLogger(sentinel)(c)
		WithStateLogger(nil)(c)
		if c.logger != sentinel {
			t.Fatalf("logger = %#v, want the sentinel to survive a nil call", c.logger)
		}
	})

	t.Run("WithQueueObserver", func(t *testing.T) {
		sentinel := stubQueueObserver{}
		c := &config{}
		WithQueueObserver(sentinel)(c)
		WithQueueObserver(nil)(c)
		if c.queueObserver != sentinel {
			t.Fatalf("queueObserver = %#v, want the sentinel to survive a nil call", c.queueObserver)
		}
	})

	t.Run("WithShutdownObserver", func(t *testing.T) {
		sentinel := &recordingShutdownObserver{}
		c := &config{}
		WithShutdownObserver(sentinel)(c)
		WithShutdownObserver(nil)(c)
		if c.shutdownObserver != sentinel {
			t.Fatalf("shutdownObserver = %#v, want the sentinel to survive a nil call", c.shutdownObserver)
		}
	})

	t.Run("WithTransport", func(t *testing.T) {
		sentinel := &stubTransport{}
		c := &config{}
		WithTransport(sentinel)(c)
		WithTransport(nil)(c)
		if c.transport != sentinel {
			t.Fatalf("transport = %#v, want the sentinel to survive a nil call", c.transport)
		}
	})
}

// TestWithArtifactCodeResolverHasNoNilGuard documents, rather than pins as
// desirable, that WithArtifactCodeResolver unconditionally assigns
// (`c.artifactCode = fn`, no nil check, unlike its six siblings above). A
// nil-guard test here would be testing the absence of a feature. Instead this
// pins the one property that *is* load-bearing: the exact function value
// passed in is the one later invoked, not a copy, wrapper, or the default.
func TestWithArtifactCodeResolverStoresTheExactResolverPassedIn(t *testing.T) {
	want := []byte("first-resolver")
	c := &config{}
	WithArtifactCodeResolver(func(context.Context, string) ([]byte, error) {
		return want, nil
	})(c)

	if c.artifactCode == nil {
		t.Fatal("artifactCode = nil after WithArtifactCodeResolver with a non-nil function")
	}
	got, err := c.artifactCode(context.Background(), "digest")
	if err != nil {
		t.Fatalf("artifactCode() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("artifactCode() = %q, want %q: the configured resolver was not "+
			"the one invoked", got, want)
	}
}

// TestWithConcurrencyRejectsNonPositiveValues pins WithConcurrency's `if n > 0`
// guard (backend.go's only defense against a zero/negative consumer
// goroutine count, which for an asynq-backed queue means the process accepts
// tasks but never runs any of them). Every existing call site
// (backend_test.go, transport_pluggable_test.go, ...) only ever passes a
// positive concurrency, so the guard branch itself was never exercised.
func TestWithConcurrencyRejectsNonPositiveValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int
	}{
		{"positive value is applied", 7, 7},
		{"zero is rejected", 0, 3},
		{"negative is rejected", -5, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &config{concurrency: 3}
			WithConcurrency(tc.in)(c)
			if c.concurrency != tc.want {
				t.Fatalf("WithConcurrency(%d) left concurrency = %d, want %d",
					tc.in, c.concurrency, tc.want)
			}
		})
	}
}

// TestWithExecTTLRejectsNonPositiveValues pins WithExecTTL, which had zero
// coverage in the suite (grepping every _test.go for "WithExecTTL" matched
// nothing before this test) including its `if d > 0` guard, the only thing
// standing between a caller-supplied non-positive duration and Redis keys
// that expire immediately (or never, if a negative duration were passed
// straight to an EXPIRE-style call elsewhere).
func TestWithExecTTLRejectsNonPositiveValues(t *testing.T) {
	const baseline = 6 * time.Hour
	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"positive value is applied", 30 * time.Minute, 30 * time.Minute},
		{"zero is rejected", 0, baseline},
		{"negative is rejected", -time.Hour, baseline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &config{execTTL: baseline}
			WithExecTTL(tc.in)(c)
			if c.execTTL != tc.want {
				t.Fatalf("WithExecTTL(%v) left execTTL = %v, want %v",
					tc.in, c.execTTL, tc.want)
			}
		})
	}
}
