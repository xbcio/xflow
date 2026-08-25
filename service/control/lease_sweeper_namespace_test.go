package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// reclaimOutcome is the (ok, err) pair ReclaimLease returns for one node, which
// is what selects the sweep loop's four-way branch.
type reclaimOutcome struct {
	ok  bool
	err error
}

// nsReclaimer answers per node name so one sweep can drive all four reclaim
// outcomes in a single pass.
type nsReclaimer struct {
	results map[string]reclaimOutcome
}

func (r *nsReclaimer) ReclaimLease(_ context.Context, lease engine.ExpiredLease) (bool, error) {
	res := r.results[lease.NodeName]
	return res.ok, res.err
}

func (r *nsReclaimer) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return nil, nil
}

func (r *nsReclaimer) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	return nil
}

func (r *nsReclaimer) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}

// nsObserver records the namespace carried by the context of every observer
// call, keyed by the callback that made it plus the node it was about. It
// implements SweepObserver, SweepTimingObserver and reclaimAppliedObserver so a
// single instance covers every callback the sweep loop can reach.
type nsObserver struct {
	mu sync.Mutex
	// seen maps "callback/node" to the namespace on the call's context.
	seen map[string]namespace.Namespace
	// listNS is the namespace on the cross-namespace scan callback.
	listNS namespace.Namespace
}

func newNSObserver() *nsObserver {
	return &nsObserver{seen: map[string]namespace.Namespace{}}
}

func (o *nsObserver) record(ctx context.Context, key string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen[key] = namespace.FromContext(ctx)
}

func (o *nsObserver) OnSweepReclaim(ctx context.Context, _, node string, _ int64) {
	o.record(ctx, "reclaim/"+node)
}

func (o *nsObserver) OnSweepRace(ctx context.Context, _, node string) {
	o.record(ctx, "race/"+node)
}

func (o *nsObserver) OnSweepError(ctx context.Context, _, node string, _ error) {
	o.record(ctx, "error/"+node)
}

func (o *nsObserver) OnSweepReclaimApplied(ctx context.Context, _, node string, _ int64) {
	o.record(ctx, "applied/"+node)
}

func (o *nsObserver) OnSweepReclaimResult(ctx context.Context, result string, _ time.Duration) {
	// Keyed by result rather than node because the callback is not told which
	// lease it is describing; the four leases below map one-to-one onto the four
	// results, so this is still unambiguous.
	o.record(ctx, "result/"+result)
}

func (o *nsObserver) OnSweepListExpired(ctx context.Context, _ int, _ time.Duration, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.listNS = namespace.FromContext(ctx)
}

func (o *nsObserver) OnSweepRepair(context.Context, int, time.Duration, error) {}

func (o *nsObserver) get(t *testing.T, key string) namespace.Namespace {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	ns, ok := o.seen[key]
	if !ok {
		t.Fatalf("observer callback %q never fired; the branch it guards did not run, "+
			"so any namespace assertion about it would be vacuous", key)
	}
	return ns
}

// TestSweepObservationsCarryTheLeaseOwnersNamespace pins that each per-lease
// metric is attributed to the tenant that owns the lease.
//
// ControlPlane.Start builds the sweeper's context from context.Background() and
// never injects a namespace. ListExpiredLeases deliberately spans every
// namespace and stamps each ExpiredLease with its owner, but the loop passed the
// sweeper's own context to every observer call. withNamespace falls back to
// "default" on a namespace-less context rather than omitting the label, so the
// reclaim counter, the lease-age histogram, the reclaim-duration histogram and
// the sweep error counter all reported namespace="default" no matter which
// tenant's runner had crashed.
//
// That is worse than a missing label. A missing label is visibly absent; this
// one is present, plausible, and files tenant B's crashed leases under tenant A.
// An operator filtering xflow_lease_sweep_errors_total by their own namespace
// sees other tenants' failures as their own, and a tenant with no leases at all
// still shows a nonzero reclaim rate.
//
// The four leases map one-to-one onto the four reclaim branches, each in a
// DIFFERENT namespace and none of them "default". Same-namespace fixtures would
// not separate the behaviours: with everything in one namespace the buggy code
// and the fixed code agree whenever that namespace happens to be default, and
// with everything in one non-default namespace a single wrongly-shared context
// would still look right.
func TestSweepObservationsCarryTheLeaseOwnersNamespace(t *testing.T) {
	boom := errors.New("reclaim failed")

	leases := []struct {
		node   string
		ns     namespace.Namespace
		result string // the OnSweepReclaimResult label this branch emits
		key    string // the SweepObserver callback this branch emits
		out    reclaimOutcome
	}{
		{"n-reclaimed", "tenant-a", "reclaimed", "reclaim/n-reclaimed", reclaimOutcome{true, nil}},
		{"n-applied", "tenant-b", "applied_pending", "applied/n-applied", reclaimOutcome{true, boom}},
		{"n-race", "tenant-c", "race", "race/n-race", reclaimOutcome{false, nil}},
		{"n-error", "tenant-d", "error", "error/n-error", reclaimOutcome{false, boom}},
	}

	state := &fakeLeaseLister{}
	reclaimer := &nsReclaimer{results: map[string]reclaimOutcome{}}
	for _, l := range leases {
		state.expired = append(state.expired, engine.ExpiredLease{
			ExecutionID: types.ExecutionID("e-" + l.node),
			NodeName:    l.node,
			Namespace:   l.ns,
			LeaseToken:  engine.LeaseToken("tok-" + l.node),
			IssuedAt:    time.Now().Add(-time.Minute),
			TTL:         time.Second,
		})
		reclaimer.results[l.node] = l.out
	}

	obs := newNSObserver()
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Observer: obs})

	// context.Background(), matching what ControlPlane.Start hands the sweeper.
	// Passing an already-namespaced context here would hide the defect.
	sw.SweepOnce(context.Background())

	for _, l := range leases {
		if got := obs.get(t, l.key); got != l.ns {
			t.Errorf("%s reported namespace %q, want %q: the lease belongs to %q and the "+
				"metric is labelled with whatever the context carries.", l.key, got, l.ns, l.ns)
		}
		if got := obs.get(t, "result/"+l.result); got != l.ns {
			t.Errorf("OnSweepReclaimResult(%q) reported namespace %q, want %q.",
				l.result, got, l.ns)
		}
	}

	// The scan callback is the deliberate exception: one ListExpiredLeases call
	// covers every namespace, so there is no owner to attribute it to and no
	// honest way to divide one duration among tenants. It stays on the sweeper's
	// context, which reads as "default". Asserted rather than left implicit so
	// that changing it is a decision rather than an accident — the help text for
	// xflow_lease_sweep_candidates is what tells operators the label means the
	// sweeper and not a tenant.
	if obs.listNS != namespace.Default {
		t.Errorf("scan callback namespace = %q, want %q: the scan is cross-namespace by "+
			"construction. If this is now split per namespace, update the help text for "+
			"xflow_lease_sweep_candidates, which currently promises the opposite.",
			obs.listNS, namespace.Default)
	}
}
