package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// fakeInProcessSeeder records the last admission it received.
type fakeInProcessSeeder struct {
	runnerID string
	ns       string
	req      protocol.SeedExecutionRequest
	calls    int
	resp     types.EntrySeedResponse
	err      error
}

func (f *fakeInProcessSeeder) SeedEntry(_ context.Context, runnerID, ns string, req protocol.SeedExecutionRequest) (types.EntrySeedResponse, error) {
	f.runnerID, f.ns, f.req = runnerID, ns, req
	f.calls++
	return f.resp, f.err
}

// TestTriggerActivationHandler_InProcessSeederInstallsInProcessRuntime asserts
// the single-node path switches transport when a seeder is installed, and that
// every field the HTTP runtime used to carry is carried here too.
func TestTriggerActivationHandler_InProcessSeederInstallsInProcessRuntime(t *testing.T) {
	fh := &fakeTriggerHandler{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}}
	seeder := &fakeInProcessSeeder{}
	obs := &recordingSeedObserver{}
	h := NewTriggerActivationHandler("", "", lookup,
		WithSeedRunnerID("runner-inproc"),
		WithSeedRequestTimeout(42*time.Second),
		WithInProcessEntrySeeder(seeder),
		WithSeedObserver(obs))

	d := protocol.ActivateDirective{
		Namespace:       "ns1",
		WorkflowID:      "wf1",
		WorkflowVersion: "v1",
		EntryUnitID:     "t1",
		NodeType:        "fake",
		Generation:      9,
		ReplicaIndex:    3,
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	rt, ok := fh.gotInput.Runtime.(*inProcessEntrySeedRuntime)
	if !ok {
		t.Fatalf("Runtime type = %T, want *inProcessEntrySeedRuntime", fh.gotInput.Runtime)
	}
	if rt.Seeder != InProcessEntrySeeder(seeder) {
		t.Fatal("runtime did not receive the installed seeder")
	}
	if rt.RunnerID != "runner-inproc" || rt.Namespace != "ns1" {
		t.Fatalf("runtime identity = (%q, %q), want (runner-inproc, ns1)", rt.RunnerID, rt.Namespace)
	}
	if rt.Generation != 9 || rt.ReplicaIndex != 3 {
		t.Fatalf("runtime generation = %d/%d, want 9/3", rt.Generation, rt.ReplicaIndex)
	}
	if rt.RequestTimeout != 42*time.Second {
		t.Fatalf("runtime timeout = %v, want 42s", rt.RequestTimeout)
	}
	if rt.Observer != types.EntrySeedObserver(obs) {
		t.Fatal("runtime did not receive the installed observer")
	}

	// The wire request is identical to the HTTP runtime's, and the attempt is
	// observed exactly once.
	seeder.resp = types.EntrySeedResponse{Accepted: true, ExecutionID: "exec-9"}
	got, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey:    "key-1",
		WorkflowID:      "wf1",
		WorkflowVersion: "v1",
		EntryUnitID:     "t1",
		Outcome:         "success",
		Exits:           []types.BoundaryExit{{NodeName: "t1", Port: "main", Data: map[string]any{"ok": true}}},
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if !got.Accepted || got.ExecutionID != "exec-9" {
		t.Fatalf("response = %+v, want the seeder's accepted response", got)
	}
	if seeder.calls != 1 {
		t.Fatalf("seeder calls = %d, want 1", seeder.calls)
	}
	if seeder.runnerID != "runner-inproc" || seeder.ns != "ns1" {
		t.Fatalf("seeder identity = (%q, %q), want (runner-inproc, ns1)", seeder.runnerID, seeder.ns)
	}
	if seeder.req.ProtocolVersion != protocol.EntrySeedProtocolVersion ||
		seeder.req.WorkflowID != "wf1" || seeder.req.AdmissionKey != "key-1" ||
		seeder.req.Generation != 9 || seeder.req.ReplicaIndex != 3 {
		t.Fatalf("wire request = %+v, want the activation identity stamped", seeder.req)
	}
	if len(seeder.req.Exits) != 1 || seeder.req.Exits[0].NodeName != "t1" ||
		seeder.req.Exits[0].Port != "main" || seeder.req.Exits[0].Data["ok"] != true {
		t.Fatalf("wire exits = %+v, want the caller's exits carried through", seeder.req.Exits)
	}
	obs.mu.Lock()
	outcomes := append([]string(nil), obs.outcomes...)
	obs.mu.Unlock()
	if len(outcomes) != 1 || outcomes[0] != "accepted" {
		t.Fatalf("observed outcomes = %v, want [accepted]", outcomes)
	}
}

// TestTriggerActivationHandler_InProcessSeederReachesTheGroupPath is the group
// half: activateGroup must use the same construction point, so the group seed
// runtime is the in-process one too.
func TestTriggerActivationHandler_InProcessSeederReachesTheGroupPath(t *testing.T) {
	fh := &groupExecFakeTriggerHandler{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{"test.grouplocal.trigger": fh}}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})
	groupRT := NewGroupRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 10}), WithSuspendDisabled())
	seeder := &fakeInProcessSeeder{}

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "trig",
		Def: &types.WorkflowDef{
			Name: "g",
			Nodes: []types.NodeDef{
				{Name: "trig", Type: "test.grouplocal.trigger", Version: 1},
				{Name: "__collector_trig_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_trig_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{{CollectorNode: "__collector_trig_main", SrcNode: "trig", Port: "main"}},
	}

	h := NewTriggerActivationHandler("", "", lookup,
		WithGroupRuntime(groupRT),
		WithInProcessEntrySeeder(seeder),
		WithSeedRunnerID("runner-inproc"))
	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		Namespace:   "ns1",
		WorkflowID:  "wf-1",
		EntryUnitID: "g",
		NodeType:    "xflow.group",
		Generation:  4,
		PackageHash: "pkg-sha256:v1:x",
		Package:     pkg,
	}); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	gr, ok := fh.gotInput.Runtime.(*groupExecTriggerRuntime)
	if !ok {
		t.Fatalf("Runtime type = %T, want *groupExecTriggerRuntime", fh.gotInput.Runtime)
	}
	rt, ok := gr.seedRuntime.(*inProcessEntrySeedRuntime)
	if !ok {
		t.Fatalf("group seed runtime = %T, want *inProcessEntrySeedRuntime", gr.seedRuntime)
	}
	if rt.Seeder != InProcessEntrySeeder(seeder) || rt.RunnerID != "runner-inproc" ||
		rt.Namespace != "ns1" || rt.Generation != 4 {
		t.Fatalf("group seed runtime = %+v, want the in-process runtime stamped from the directive", rt)
	}
}

// TestInProcessEntrySeedRuntimePropagatesSeederErrors: the caller's errors.Is
// checks must still name control-plane sentinels, and the attempt must be
// observed as an error.
func TestInProcessEntrySeedRuntimePropagatesSeederErrors(t *testing.T) {
	sentinel := errors.New("control plane said no")
	seeder := &fakeInProcessSeeder{err: sentinel}
	obs := &recordingSeedObserver{}
	rt := &inProcessEntrySeedRuntime{Seeder: seeder, Observer: obs}

	_, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{AdmissionKey: "k"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want the seeder error wrapped but matchable", err)
	}
	obs.mu.Lock()
	outcomes := append([]string(nil), obs.outcomes...)
	obs.mu.Unlock()
	if len(outcomes) != 1 || outcomes[0] != "error" {
		t.Fatalf("observed outcomes = %v, want [error]", outcomes)
	}
}

// TestInProcessEntrySeedRuntimeFailsClosedOnTheLegacyPath pins the same posture
// as protocol.HTTPEntrySeedRuntime: a trigger mis-wired onto Emit must not
// silently drop its message.
func TestInProcessEntrySeedRuntimeFailsClosedOnTheLegacyPath(t *testing.T) {
	rt := &inProcessEntrySeedRuntime{Seeder: &fakeInProcessSeeder{}}
	if _, err := rt.Emit(context.Background(), "wf", "node", nil); !errors.Is(err, protocol.ErrNonSeedPathUnsupported) {
		t.Fatalf("Emit error = %v, want ErrNonSeedPathUnsupported", err)
	}
	if _, err := rt.Dedup(context.Background(), "key", time.Minute); !errors.Is(err, protocol.ErrNonSeedPathUnsupported) {
		t.Fatalf("Dedup error = %v, want ErrNonSeedPathUnsupported", err)
	}
	if _, _, err := rt.TryLock(context.Background(), "key", time.Minute); !errors.Is(err, protocol.ErrNonSeedPathUnsupported) {
		t.Fatalf("TryLock error = %v, want ErrNonSeedPathUnsupported", err)
	}
	if st := rt.State(context.Background(), "scope"); st != nil {
		t.Fatalf("State = %v, want nil (no watermark on either seed transport)", st)
	}
}

// TestInProcessEntrySeedRuntimeAppliesTheProtocolDefaultDeadline guards the
// zero-timeout trap: an unset option must become the shared default, never an
// already-expired context.
func TestInProcessEntrySeedRuntimeAppliesTheProtocolDefaultDeadline(t *testing.T) {
	rt := &inProcessEntrySeedRuntime{}
	if got := rt.requestTimeout(); got != protocol.DefaultEntrySeedRequestTimeout {
		t.Fatalf("requestTimeout = %v, want %v", got, protocol.DefaultEntrySeedRequestTimeout)
	}
}
