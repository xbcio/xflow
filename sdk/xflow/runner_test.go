package xflow

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// buildRunnerServiceConfig is the seam these tests assert on: NewRunner's
// assembly is only observable through the runnersvc.Config it produces, and
// running the real service would require a live server. Every test below
// inspects that config rather than a stub's call record, so the assertions
// hold against the same code path Run uses.

// A runner assembled without a SubgraphRuntime cannot execute a batch lease:
// the batch names a synthetic node ("m/_batch/0") that carries no Input and
// has no registered handler, so the ordinary node path has nothing to run.
// cmd/runner grew this wiring only after every map node turned out to be
// undeployable; an SDK runner must not reintroduce the gap.
func TestNewRunnerWiresTheSubgraphRuntime(t *testing.T) {
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.map", "xflow.function"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.SubgraphRuntime == nil {
		t.Error("no SubgraphRuntime; every batch lease this runner claims fails " +
			"with 'no SubgraphRuntime configured'")
	}
}

// The group counterpart, and the reason it hid longer: a runner with no
// GroupRuntime does not FAIL a group lease, it never receives one. A group
// unit's routing demands the group.exec.v1 feature, so a runner that does not
// advertise it is filtered out during assignment and the task sits queued —
// no error, no log, no lease. Both halves are asserted because neither is
// sufficient alone.
func TestNewRunnerWiresAndAdvertisesGroupExecution(t *testing.T) {
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.function"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.GroupRuntime == nil {
		t.Error("no GroupRuntime; a group lease reaching this runner falls through " +
			"to the handler path, which has no handler for the group's synthetic node name")
	}

	var advertised bool
	for _, c := range cfg.Capabilities {
		if c.NodeType != engine.GroupNodeType {
			continue
		}
		for _, f := range c.Features {
			if f == engine.FeatureGroupExecV1 {
				advertised = true
			}
		}
	}
	if !advertised {
		t.Errorf("capability %s+%s not advertised; the selector filters this runner "+
			"out of every group assignment and the task queues silently",
			engine.GroupNodeType, engine.FeatureGroupExecV1)
	}
}

// The ordering constraint cmd/runner/run.go:253-266 records in prose: the
// GroupRuntime must exist BEFORE the TriggerActivationHandler is built, because
// a runner that both hosts triggers and executes groups needs the SAME instance
// in both places. Building it afterwards meant the handler could never see it
// and every trigger-group activation on a production runner failed closed.
//
// Asserted by driving a real activation through the tracker: the failure must
// come from the nil-package guard, not from the missing-runtime guard.
func TestNewRunnerGivesTheTriggerHandlerTheGroupRuntime(t *testing.T) {
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.trigger.kafka"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.GroupRuntime == nil {
		t.Fatal("cfg.GroupRuntime is nil")
	}
	if cfg.ActivationTracker == nil {
		t.Fatal("cfg.ActivationTracker is nil — a trigger capability should have wired it")
	}

	var activateErr error
	cfg.ActivationTracker.SetOnActivateFailed(func(_ protocol.ActivateDirective, err error) {
		activateErr = err
	})
	if err := cfg.ActivationTracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{{
			Namespace: "default", WorkflowID: "wf-1", WorkflowVersion: "v1",
			EntryUnitID: "g", NodeType: engine.GroupNodeType, Generation: 1,
			PackageHash: "pkg-sha256:v1:x",
			// Package deliberately omitted so the activation fails; which guard
			// it trips is the assertion.
		}},
	}); err != nil {
		t.Fatalf("ProcessDirectives: %v", err)
	}
	if activateErr == nil {
		t.Fatal("group activation with a nil Package unexpectedly succeeded")
	}
	if strings.Contains(activateErr.Error(), "no GroupRuntime configured") {
		t.Errorf("activation failed at the missing-runtime guard: %v\n"+
			"the TriggerActivationHandler was built before the GroupRuntime existed; "+
			"every trigger-group activation on this runner fails closed", activateErr)
	}
	if !strings.Contains(activateErr.Error(), "carries no package") {
		t.Errorf("activation error = %v, want the nil-package guard", activateErr)
	}
}

// One artifact resolver, three consumers. A resolver reaching only the
// top-level dispatcher leaves scripts nested inside a group or a map body
// unable to fetch their wasm module by digest — the SAS filtering path runs
// exactly there.
//
// service/runner already proves each runtime honours its own resolver option
// (artifact_in_subgraph_test.go). What only the assembly can get wrong is that
// all three consumers receive the SAME resolver the caller configured.
//
// Coverage here is uneven, and the comment used to say otherwise — it claimed
// "each runtime is driven through its public constructor seam and the shared
// counter is what ties them together", which is true of the dispatcher alone.
// What is actually checked:
//
//	dispatcher — the configured resolver is invoked and the call is counted, so
//	             a substituted or wrapped function fails.
//	group      — non-nil only.
//	subgraph   — non-nil only.
//
// So passing a different function (or nil) at the group/subgraph wiring points
// in runner.go still passes this test. Closing that needs the two runtimes
// executed for real, since their resolver field is unexported and in another
// package, and both service/runner/group_runtime.go and subgraph_runtime.go are
// mid-edit in a parallel change — building the behavioural test on their
// current shape would pin something about to move. Left as the honest smaller
// assertion rather than a comment claiming the larger one.
func TestNewRunnerSharesTheArtifactResolverWithNestedRuntimes(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	resolver := func(_ context.Context, digest string) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		seen[digest]++
		return []byte("wasm"), nil
	}

	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.script"},
	}, WithRunnerArtifactResolver(resolver))
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}

	if cfg.ArtifactCodeResolver == nil {
		t.Fatal("dispatcher got no ArtifactCodeResolver; a top-level script node " +
			"cannot fetch its module")
	}
	if _, err := cfg.ArtifactCodeResolver(context.Background(), "dispatcher"); err != nil {
		t.Fatalf("dispatcher resolver: %v", err)
	}
	if cfg.GroupRuntime == nil {
		t.Fatal("no GroupRuntime; a script inside a group cannot fetch its module")
	}
	if cfg.SubgraphRuntime == nil {
		t.Fatal("no SubgraphRuntime; a script inside a map body cannot fetch its module")
	}

	mu.Lock()
	defer mu.Unlock()
	if seen["dispatcher"] != 1 {
		t.Errorf("configured resolver called %d times for the dispatcher, want 1 — "+
			"NewRunner substituted its own", seen["dispatcher"])
	}
}

// Labels are what a node-level RunnerSelector matches against, so a runner that
// drops them is invisible to every selector-pinned workflow — the mechanism a
// SAS-specific runner relies on to receive only its own nodes.
func TestNewRunnerCarriesLabelsToTheServiceConfig(t *testing.T) {
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.function"},
		Labels:       map[string]string{"env": "test", "app": "sas"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if got := cfg.Labels["env"]; got != "test" {
		t.Errorf("Labels[env] = %q, want %q", got, "test")
	}
	if got := cfg.Labels["app"]; got != "sas" {
		t.Errorf("Labels[app] = %q, want %q", got, "sas")
	}
}

// A ServerURL is the one field with no usable default: without it the runner
// builds a client pointed at nothing and fails at the first poll, far from the
// cause.
func TestNewRunnerRejectsAMissingServerURL(t *testing.T) {
	_, err := NewRunner(RunnerConfig{Capabilities: []string{"xflow.function"}})
	if err == nil {
		t.Fatal("NewRunner accepted an empty ServerURL")
	}
	if !strings.Contains(err.Error(), "server") {
		t.Errorf("error = %v, want it to name the missing server URL", err)
	}
}

// runnerCapabilities builds three distinct Features literals: the plain
// non-group branch, the group-override branch taken when the caller declares
// engine.GroupNodeType, and the synthesized group entry appended when it does
// not. Each one must carry engine.FeatureWasmSupplyDeclarationV1 — that is
// what tells the control plane this runner can host declaration-shaped supply
// consumer activations. Without it on any one of the three, a runner built
// through that path reads as correctly configured while the control plane
// refuses to place those activations on it, and the workflow sits pending
// with nothing obviously wrong. Deliberately not a whole-list equality check:
// other features are added to these lists independently of this one.
func TestRunnerCapabilitiesAllCarryWasmSupplyDeclaration(t *testing.T) {
	assertHasFeature := func(t *testing.T, c protocol.Capability) {
		t.Helper()
		for _, f := range c.Features {
			if f == engine.FeatureWasmSupplyDeclarationV1 {
				return
			}
		}
		t.Errorf("capability NodeType=%q Features=%v is missing %s", c.NodeType, c.Features,
			engine.FeatureWasmSupplyDeclarationV1)
	}

	// Non-group declared type plus the synthesized group entry appended
	// because no group node type was declared.
	nonGroupCaps := runnerCapabilities([]string{"xflow.script"})
	if len(nonGroupCaps) != 2 {
		t.Fatalf("runnerCapabilities([]string{\"xflow.script\"}) returned %d entries, want 2 "+
			"(declared + synthesized group): %+v", len(nonGroupCaps), nonGroupCaps)
	}
	for _, c := range nonGroupCaps {
		assertHasFeature(t, c)
	}

	// Declaring the group node type directly takes the group-override branch
	// instead of the synthesized one.
	groupCaps := runnerCapabilities([]string{engine.GroupNodeType})
	if len(groupCaps) != 1 {
		t.Fatalf("runnerCapabilities([]string{engine.GroupNodeType}) returned %d entries, want 1: %+v",
			len(groupCaps), groupCaps)
	}
	assertHasFeature(t, groupCaps[0])
}
