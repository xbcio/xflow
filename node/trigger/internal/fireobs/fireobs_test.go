package fireobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xbcio/xflow/types"
)

// recordingObserver collects the reasons reported to it. The callers are the
// triggers' own firing goroutines, so the append is guarded.
type recordingObserver struct {
	mu      sync.Mutex
	reasons []string
	errs    []error
}

func (o *recordingObserver) OnTriggerFireDropped(_ context.Context, reason string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reasons = append(o.reasons, reason)
	o.errs = append(o.errs, err)
}

func (o *recordingObserver) seen() ([]string, []error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.reasons...), append([]error(nil), o.errs...)
}

// TestCounterWithoutAnObserverIsSilent pins the deployment that collects no
// metrics: a process that never installs an observer still drops firings, and
// Observe is called on every one of them. Nothing there may panic or block — a
// counter that turns a dropped firing into a crashed runner is worse than the
// missing series it was added for.
func TestCounterWithoutAnObserverIsSilent(t *testing.T) {
	var c Counter
	c.Observe(context.Background(), types.TriggerFireDroppedOverlap, nil)
}

// TestCounterForwardsTheFailureUnclassified pins what this layer does NOT do. Its
// callers are the trigger packages, which cannot import package engine, so the
// one failure worth splitting out — the workflow's own concurrency limit — is
// classified by the observer instead. If this layer dropped the error, the reason
// would stay emit_error forever and a schedule being throttled by its own
// workflow would be indistinguishable from an engine that refuses submissions.
func TestCounterForwardsTheFailureUnclassified(t *testing.T) {
	var c Counter
	rec := &recordingObserver{}
	c.Set(rec)
	defer c.Set(nil)

	refusal := fmt.Errorf("admit: %w", errors.New("engine says no"))
	c.Observe(context.Background(), types.TriggerFireDroppedEmitError, refusal)
	c.Observe(context.Background(), types.TriggerFireDroppedOverlap, nil)

	reasons, errs := rec.seen()
	if len(reasons) != 2 || reasons[0] != types.TriggerFireDroppedEmitError || reasons[1] != types.TriggerFireDroppedOverlap {
		t.Fatalf("reasons = %v, want [%s %s]", reasons, types.TriggerFireDroppedEmitError, types.TriggerFireDroppedOverlap)
	}
	// Identity, not just non-nil: the observer's errors.Is check has to see the
	// same chain the runtime produced.
	if !errors.Is(errs[0], refusal) {
		t.Fatalf("the reported error = %v, want the failure the caller passed (%v)", errs[0], refusal)
	}
	if errs[1] != nil {
		t.Fatalf("a drop the trigger decided itself carried error %v, want nil", errs[1])
	}
}

// TestCounterReleaseStopsReporting covers the two edges of the slot's life: what
// is installed is reported, and the nil install that follows it silences the
// counter again. The release half is what a test uses to put the slot back, so a
// release that left the old observer installed would leak one test's recorder
// into every later test in the process.
func TestCounterReleaseStopsReporting(t *testing.T) {
	var c Counter
	ctx := context.Background()
	first := &recordingObserver{}
	c.Set(first)
	c.Observe(ctx, types.TriggerFireDroppedRefused, nil)
	c.Set(nil)

	// A SECOND observer after the release must be accepted: the release is what
	// makes the slot reusable, and a flag left set would panic here.
	second := &recordingObserver{}
	c.Set(second)
	c.Observe(ctx, types.TriggerFireDroppedTransport, nil)

	if reasons, _ := first.seen(); len(reasons) != 1 || reasons[0] != types.TriggerFireDroppedRefused {
		t.Fatalf("the first observer saw %v, want only the report made while it was installed", reasons)
	}
	if reasons, _ := second.seen(); len(reasons) != 1 || reasons[0] != types.TriggerFireDroppedTransport {
		t.Fatalf("the second observer saw %v, want only the report made after it was installed", reasons)
	}
}

// TestCounterRefusesASecondInstall pins the panic the kafka trigger's observer
// sets the precedent for: two installers of a process-wide slot means one of them
// silently loses every observation it expected to make. Failing loudly beats a
// metric that is missing in production and correct in every test.
func TestCounterRefusesASecondInstall(t *testing.T) {
	var c Counter
	first := &recordingObserver{}
	c.Set(first)

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("a second non-nil Set did not panic; the two installers would " +
					"silently share one slot and one of them would report nothing")
			}
		}()
		c.Set(&recordingObserver{})
	}()

	// The panic must not corrupt the slot: the installer that is already there
	// keeps reporting, because a process that catches this panic goes on running
	// with that observer.
	c.Observe(context.Background(), types.TriggerFireDroppedRefused, nil)
	if reasons, _ := first.seen(); len(reasons) != 1 {
		t.Fatalf("after the refused second install the slot reported %v, want the first "+
			"observer to still be installed", reasons)
	}
}
