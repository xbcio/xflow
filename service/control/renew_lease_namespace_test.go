package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// renewNSEngine records the namespace the renewal ctx carries when it reaches
// the engine. The Redis group/node lease stores derive their keys from
// namespace.FromContext(ctx), so a renewal that arrives without the lease's
// own namespace renews (or fails to renew) the default namespace's key.
type renewNSEngine struct {
	*groupFakeEngine
	groupRenewNS namespace.Namespace
	nodeRenewNS  namespace.Namespace
}

func (r *renewNSEngine) RenewGroupLease(ctx context.Context, lease *engine.TaskLease, extend time.Duration) (bool, error) {
	r.groupRenewNS = namespace.FromContext(ctx)
	return r.groupFakeEngine.RenewGroupLease(ctx, lease, extend)
}

func (r *renewNSEngine) RenewTaskLease(ctx context.Context, lease *engine.TaskLease, extend time.Duration) (bool, error) {
	r.nodeRenewNS = namespace.FromContext(ctx)
	return r.groupFakeEngine.RenewTaskLease(ctx, lease, extend)
}

// The runner protocol's request context carries no namespace (there is no
// principal resolver on that path), so renewLease must inject the
// authoritative lease namespace before calling the engine, exactly as the poll
// and report paths do.
// Mutation: removing the namespace injection in renewLease leaves ctx on the
// default namespace for a lease owned by another namespace.
func TestRenewLeaseCarriesTheLeaseNamespaceToTheEngine(t *testing.T) {
	const ns namespace.Namespace = "ns-b"
	tests := []struct {
		name      string
		group     bool
		observed  func(*renewNSEngine) namespace.Namespace
		configure func(*renewNSEngine)
	}{
		{"group lease", true, func(r *renewNSEngine) namespace.Namespace { return r.groupRenewNS },
			func(r *renewNSEngine) { r.groupRenewResult = true }},
		{"node lease", false, func(r *renewNSEngine) namespace.Namespace { return r.nodeRenewNS },
			func(r *renewNSEngine) { r.nodeRenewResult = true }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := NewMemoryRunnerDirectory()
			session, err := dir.Register(ctx, RegisterRunnerRequest{
				RunnerID:     "runner-grp",
				Capacity:     2,
				Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
				Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
				Namespaces:   []namespace.Namespace{ns},
			})
			if err != nil {
				t.Fatal(err)
			}

			assignment := groupTestAssignment()
			if !tc.group {
				assignment.Task.Type = engine.TaskTypeNodeExec
			}
			assignment.Namespace = ns
			mustEnqueueAssignment(t, ctx, dir, assignment)
			claim := mustClaimGroupAssignment(t, ctx, dir, session)

			lease := &engine.TaskLease{
				LeaseID:    "lease-ns-renew",
				LeaseToken: "token-ns-renew",
				Task:       assignment.Task,
				Attempt:    1,
				NodeType:   "xflow.group",
				Namespace:  ns,
			}
			if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
				t.Fatalf("FinalizeClaim() error = %v", err)
			}

			fake := &renewNSEngine{groupFakeEngine: &groupFakeEngine{}}
			tc.configure(fake)
			core := &Core{engine: fake, runners: dir, pollWait: time.Second}

			resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
				RunnerID:   session.RunnerID,
				SessionID:  session.SessionID,
				LeaseID:    string(lease.LeaseID),
				LeaseToken: string(lease.LeaseToken),
				Extend:     30000,
			}, TransportInfo{})
			if err != nil {
				t.Fatalf("renewLease() error = %v", err)
			}
			if !resp.Renewed {
				t.Fatal("renewLease() renewed=false, want true")
			}
			if got := tc.observed(fake); got != ns {
				t.Fatalf("engine saw namespace %q on the renew ctx, want %q", got, ns)
			}
		})
	}
}
