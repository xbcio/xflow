package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// nsGroupObserver records the namespace each group hook's ctx carries. The
// metrics adapter labels its series from that ctx, so what the engine hands the
// hook is what the operator sees.
type nsGroupObserver struct {
	mu        sync.Mutex
	acquired  []namespace.Namespace
	admission []namespace.Namespace
}

func (o *nsGroupObserver) OnGroupLeaseAcquired(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.acquired = append(o.acquired, namespace.FromContext(ctx))
}
func (o *nsGroupObserver) OnGroupLeaseExpired(context.Context)                      {}
func (o *nsGroupObserver) OnGroupLeaseRenew(context.Context, string, time.Duration) {}
func (o *nsGroupObserver) OnGroupCommit(context.Context, string, time.Duration)     {}
func (o *nsGroupObserver) OnGroupAdmission(ctx context.Context, _ string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.admission = append(o.admission, namespace.FromContext(ctx))
}

var _ GroupObserver = (*nsGroupObserver)(nil)

// The admission backend resolves the namespace from the request first and only
// falls back to ctx, so an embedded caller can name a namespace the context
// does not carry. The hook must see the namespace the admission used.
// Mutation: passing ctx instead of notifyCtx to notifyGroupAdmission in
// SeedExecutionFromEntry reports the admission under "default".
func TestSeedExecutionFromEntry_GroupAdmissionObservedUnderRequestNamespace(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	tests := []struct {
		name string
		ctx  context.Context
		req  namespace.Namespace
		want namespace.Namespace
	}{
		{"request names a namespace the ctx lacks", context.Background(), "ns-b", "ns-b"},
		{"request overrides a different ctx namespace", namespace.WithNamespace(context.Background(), "ns-a"), "ns-b", "ns-b"},
		{"request without namespace keeps the ctx one", namespace.WithNamespace(context.Background(), "ns-a"), "", "ns-a"},
		{"neither names one: default", context.Background(), "", namespace.Default},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obs := &nsGroupObserver{}
			eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{
				State:       AdmissionStateAccepted,
				ExecutionID: "exec-admission-ns",
			}, nil, WithGroupObserver(obs))

			if _, err := eng.SeedExecutionFromEntry(tc.ctx, SeedExecutionFromEntryRequest{
				Namespace:    tc.req,
				Graph:        g,
				EntryUnitIdx: groupUnitIdx,
			}); err != nil {
				t.Fatalf("SeedExecutionFromEntry: %v", err)
			}
			if len(obs.admission) != 1 || obs.admission[0] != tc.want {
				t.Fatalf("admission hook namespaces = %v, want exactly [%s]", obs.admission, tc.want)
			}
		})
	}
}

// BuildGroupLease runs on a ctx the control plane already stamped with the
// assignment's namespace; the hook must receive that ctx untouched.
// Mutation: handing notifyGroupLeaseAcquired a fresh context.Background()
// reports every acquisition under "default".
func TestBuildGroupLease_AcquiredObservedUnderCtxNamespace(t *testing.T) {
	obs := &nsGroupObserver{}
	eng, g, execID := newObserverGroupLeaseEngine(t, WithGroupObserver(obs))
	ctx := namespace.WithNamespace(context.Background(), "ns-b")

	gm := g.Groups()[0]
	if _, _, err := eng.BuildGroupLease(ctx, &Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        TaskTypeGroupExec,
	}); err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}
	if len(obs.acquired) != 1 || obs.acquired[0] != "ns-b" {
		t.Fatalf("acquired hook namespaces = %v, want exactly [ns-b]", obs.acquired)
	}
}
