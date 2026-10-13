package xflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// This file covers the SDK half of "invoke one version" (versioning design
// §2.11): WithWorkflowVersion pins a call to one published version of the
// workflow addressed by id, retired versions included.
//
// The properties pinned here are the ones a mistake would not surface as a
// compile error:
//
//   - the execution's graph snapshot carries the version the call pinned, and
//     the node set that ran is that version's, not the current record's;
//   - omitting the option, or pinning the current version, runs the live
//     record — the behavior Invoke had before versions existed;
//   - an unknown version is backend.ErrWorkflowVersionNotFound;
//   - an archived payload the current compiler reproduces to a DIFFERENT hash
//     fails closed (backend.ErrWorkflowVersionUnavailable) instead of running
//     under another version's label;
//   - a registry with no version history refuses a pin rather than silently
//     running the current version;
//   - a pin and an idempotency key compose: the key's fingerprint comes from
//     the version the call resolved, so a pinned retry after an intervening
//     publish is answered as a duplicate while an unpinned one is refused as
//     engine.ErrIdempotencyConflict;
//   - a retry whose pinned version retention has evicted fails closed with
//     backend.ErrWorkflowVersionNotFound, never a duplicate.

// pinVersionWorkflow builds the two-version shape the tests below publish:
// same name, different version and a differently named work node, so the node
// set tells the versions apart.
func pinVersionWorkflow(name, version, workNode string) *WorkflowBuilder {
	wf := Workflow(name).Version(version)
	start := wf.Node("start", node.Start())
	work := wf.Node(workNode, node.Function("return input"))
	wf.Connect(start, work)
	return wf
}

// publishWorkflowVersion retires the current record of id and installs wf's
// definition as the next one, through the same atomic compare-and-swap the
// HTTP PUT /v1/workflows/{id} uses. It is how a publish creates history: the
// SDK has no by-id replace of its own, so the test drives the registry call
// that route makes.
func publishWorkflowVersion(t *testing.T, ctx context.Context, registry backend.WorkflowRegistry, id types.WorkflowID, wf *WorkflowBuilder) {
	t.Helper()
	def, err := wf.build()
	if err != nil {
		t.Fatalf("build %s: %v", wf.name, err)
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile %s: %v", def.Version, err)
	}
	hash, err := runtimeHash(def)
	if err != nil {
		t.Fatalf("hash %s: %v", def.Version, err)
	}
	audit, err := legacyDefinitionHash(def)
	if err != nil {
		t.Fatalf("audit hash %s: %v", def.Version, err)
	}
	existing, err := registry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("GetWorkflow(%q): %v", id, err)
	}
	replacer, ok := registry.(backend.DurableWorkflowReplaceCapability)
	if !ok {
		t.Fatal("the local workflow registry does not support atomic replacement")
	}
	result, err := replacer.CompareAndReplaceWorkflow(
		namespace.WithNamespace(ctx, namespace.Namespace(def.Namespace)),
		backend.WorkflowReplaceRequest{
			MutationID: "sdk-pin-test:" + def.Name + ":" + def.Version,
			Expected:   backend.RevisionOfWorkflow(existing),
			Replacement: backend.WorkflowRecord{
				ID:               id,
				Key:              workflowKey(def),
				Namespace:        def.Namespace,
				Name:             def.Name,
				Version:          def.Version,
				DefinitionHash:   hash,
				AuditFingerprint: audit,
				Definition:       def,
				Graph:            g,
			},
		})
	if err != nil {
		t.Fatalf("publish %s: %v", def.Version, err)
	}
	if result.Current.Version != def.Version {
		t.Fatalf("published version = %q, want %q", result.Current.Version, def.Version)
	}
}

// executedGraph returns the graph snapshot the engine recorded for id.
func executedGraph(t *testing.T, ctx context.Context, eng *Engine, id types.ExecutionID) *graph.Graph {
	t.Helper()
	snap, err := eng.eng.State().GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution(%q): %v", id, err)
	}
	if snap == nil || snap.Graph == nil {
		t.Fatalf("execution %q has no stored graph snapshot", id)
	}
	return snap.Graph
}

// TestInvokeWithWorkflowVersionRunsThePinnedGraph is the core semantics: after
// v2 is published, a call pinned to v1 runs from v1's archived snapshot — its
// node set and its graph version — while the unpinned call and the
// pin-the-current-version call both run the live v2.
func TestInvokeWithWorkflowVersionRunsThePinnedGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	id, err := eng.AddWorkflow(ctx, pinVersionWorkflow("sdk-pin", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	publishWorkflowVersion(t, ctx, eng.workflowRegistry, id, pinVersionWorkflow("sdk-pin", "v2", "only-v2"))

	cases := []struct {
		name    string
		opts    []InvokeOption
		version string
		node    string
	}{
		{name: "pinned retired version", opts: []InvokeOption{WithWorkflowVersion("v1")}, version: "v1", node: "only-v1"},
		{name: "segment omitted runs current", opts: nil, version: "v2", node: "only-v2"},
		{name: "pinned current version", opts: []InvokeOption{WithWorkflowVersion("v2")}, version: "v2", node: "only-v2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execID, err := eng.Invoke(ctx, id, Start(), map[string]any{"k": "v"}, tc.opts...)
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			g := executedGraph(t, ctx, eng, execID)
			if got := g.WorkflowVersion(); got != tc.version {
				t.Fatalf("execution graph version = %q, want %q", got, tc.version)
			}
			if _, ok := g.NodeIndex(tc.node); !ok {
				t.Fatalf("execution graph has no %q node; the pinned version did not run", tc.node)
			}
			for i := 0; i < g.NodeCount(); i++ {
				if other := g.NodeName(i); other != tc.node && (other == "only-v1" || other == "only-v2") {
					t.Fatalf("execution graph carries %q from the other version", other)
				}
			}
			// The pinned archived run reaches terminal status, i.e. it is a
			// real execution and not just a snapshot that was never dispatched.
			if _, err := eng.Wait(ctx, execID); err != nil {
				t.Fatalf("Wait: %v", err)
			}
		})
	}
}

// TestInvokeWithWorkflowVersionUnknownVersionIsNotFound pins the 404 shape: a
// version the workflow never published is reported as such, not as a missing
// workflow and not as a fallback to the current record.
func TestInvokeWithWorkflowVersionUnknownVersionIsNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	id, err := eng.AddWorkflow(ctx, pinVersionWorkflow("sdk-pin-unknown", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	_, err = eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v9"))
	if !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("Invoke(pinned v9) error = %v, want backend.ErrWorkflowVersionNotFound", err)
	}
}

// TestInvokeWithWorkflowVersionFailsClosedOnDrift pins the fail-closed rule
// (design Q10) on the SDK path: a retired payload whose stored hash the current
// compiler does not reproduce must not run under the version's label, while the
// live record — which is not pinned — keeps working.
func TestInvokeWithWorkflowVersionFailsClosedOnDrift(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	// Seed v1 with a hash this compiler will not reproduce, then publish v2:
	// the retirement carries the drifted payload into history, which is the
	// state a hashing-rules change would leave behind.
	def, err := pinVersionWorkflow("sdk-pin-drift", "v1", "only-v1").build()
	if err != nil {
		t.Fatalf("build v1: %v", err)
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile v1: %v", err)
	}
	const driftID = "wf-sdk-pin-drift"
	if _, err := eng.workflowRegistry.AddWorkflow(ctx, backend.WorkflowRecord{
		ID:             driftID,
		Key:            workflowKey(def),
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: "sha256:stale-drift",
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("seed drifted v1: %v", err)
	}
	publishWorkflowVersion(t, ctx, eng.workflowRegistry, driftID, pinVersionWorkflow("sdk-pin-drift", "v2", "only-v2"))

	_, err = eng.Invoke(ctx, driftID, Start(), nil, WithWorkflowVersion("v1"))
	if !errors.Is(err, backend.ErrWorkflowVersionUnavailable) {
		t.Fatalf("Invoke(pinned drifted v1) error = %v, want backend.ErrWorkflowVersionUnavailable", err)
	}

	// The unpinned call is unaffected: it addresses the live record, which is
	// not resolved through history at all.
	execID, err := eng.Invoke(ctx, driftID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke(unpinned): %v", err)
	}
	if got := executedGraph(t, ctx, eng, execID).WorkflowVersion(); got != "v2" {
		t.Fatalf("unpinned execution graph version = %q, want v2", got)
	}
}

// versionlessRegistry is a workflow registry without history: the embedded
// interface exposes only the methods of backend.WorkflowRegistry, so the
// wrapper does not satisfy backend.WorkflowVersionCapability even though the
// registry behind it does.
type versionlessRegistry struct{ backend.WorkflowRegistry }

// TestInvokeWorkflowVersionIdempotencyReplay pins the user-visible contract of
// a pinned version composed with an idempotency key (versioning design §5): the
// key is scoped to the workflow — never to a version, so a retry after an
// upgrade still finds its original execution — while the request's fingerprint
// includes the version the call RESOLVED, so the same key against a moved live
// record is a conflict rather than a silent duplicate of other work.
//
// Phase A (pinned): a keyed call pinned to v1 creates an execution; v2 is then
// published; the retry, still pinned to v1, resolves the same archived version
// and is answered with the original execution. Phase B (unpinned): a keyed call
// runs the live v2; v3 is published; the retry now resolves v3, whose
// fingerprint differs, and is refused with engine.ErrIdempotencyConflict —
// while a pinned retry to v2 recovers the original execution, which is the
// whole point of having the version in the fingerprint and not in the scope.
func TestInvokeWorkflowVersionIdempotencyReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	id, err := eng.AddWorkflow(ctx, pinVersionWorkflow("sdk-pin-replay", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	// Phase A: the pinned retry survives an intervening publish.
	first, err := eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v1"), WithIdempotencyKey("replay-pinned"))
	if err != nil {
		t.Fatalf("first pinned keyed Invoke: %v", err)
	}
	if _, err := eng.Wait(ctx, first); err != nil {
		t.Fatalf("Wait(%q): %v", first, err)
	}
	if got := executedGraph(t, ctx, eng, first).WorkflowVersion(); got != "v1" {
		t.Fatalf("first execution graph version = %q, want v1", got)
	}

	publishWorkflowVersion(t, ctx, eng.workflowRegistry, id, pinVersionWorkflow("sdk-pin-replay", "v2", "only-v2"))

	replay, err := eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v1"), WithIdempotencyKey("replay-pinned"))
	if err != nil {
		t.Fatalf("pinned replay after the publish: %v, want the duplicate answer", err)
	}
	if replay != first {
		t.Fatalf("pinned replay returned %q, want the original %q", replay, first)
	}
	if got := executedGraph(t, ctx, eng, replay).WorkflowVersion(); got != "v1" {
		t.Fatalf("after the replay the execution graph version = %q, want v1", got)
	}

	// Phase B: unpinned, the retry resolves the new live version and conflicts.
	second, err := eng.Invoke(ctx, id, Start(), nil, WithIdempotencyKey("replay-unpinned"))
	if err != nil {
		t.Fatalf("first unpinned keyed Invoke: %v", err)
	}
	if _, err := eng.Wait(ctx, second); err != nil {
		t.Fatalf("Wait(%q): %v", second, err)
	}
	if got := executedGraph(t, ctx, eng, second).WorkflowVersion(); got != "v2" {
		t.Fatalf("second execution graph version = %q, want v2", got)
	}
	if second == first {
		t.Fatal("a different key returned the other key's execution")
	}

	publishWorkflowVersion(t, ctx, eng.workflowRegistry, id, pinVersionWorkflow("sdk-pin-replay", "v3", "only-v3"))

	if _, err := eng.Invoke(ctx, id, Start(), nil, WithIdempotencyKey("replay-unpinned")); !errors.Is(err, enginecore.ErrIdempotencyConflict) {
		t.Fatalf("unpinned replay after the publish error = %v, want engine.ErrIdempotencyConflict", err)
	}

	// The version lives in the fingerprint, not in the key scope: pinning the
	// version the keyed execution ran recovers that execution.
	recovered, err := eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v2"), WithIdempotencyKey("replay-unpinned"))
	if err != nil {
		t.Fatalf("pinned replay to the keyed execution's version: %v", err)
	}
	if recovered != second {
		t.Fatalf("pinned replay returned %q, want the keyed execution %q", recovered, second)
	}

	// The workflow is not wedged by the conflict: a fresh key runs the new live
	// version.
	third, err := eng.Invoke(ctx, id, Start(), nil, WithIdempotencyKey("replay-v3"))
	if err != nil {
		t.Fatalf("fresh keyed Invoke on the new version: %v", err)
	}
	if got := executedGraph(t, ctx, eng, third).WorkflowVersion(); got != "v3" {
		t.Fatalf("third execution graph version = %q, want v3", got)
	}
}

// TestInvokeWithWorkflowVersionRequiresHistoryCapability pins the fail-closed
// answer for a registry that serves no history: a pin must be refused, because
// answering it by running the current version is exactly the silent version
// split a pin exists to prevent.
func TestInvokeWithWorkflowVersionRequiresHistoryCapability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	id, err := eng.AddWorkflow(ctx, pinVersionWorkflow("sdk-pin-history", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	publishWorkflowVersion(t, ctx, eng.workflowRegistry, id, pinVersionWorkflow("sdk-pin-history", "v2", "only-v2"))

	// Baseline: with the real registry the retired pin resolves.
	if _, err := eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v1")); err != nil {
		t.Fatalf("Invoke(pinned v1) with history: %v", err)
	}

	eng.workflowRegistry = versionlessRegistry{eng.workflowRegistry}

	_, err = eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v1"))
	if err == nil || !strings.Contains(err.Error(), "does not support version history") {
		t.Fatalf("Invoke(pinned, no history) error = %v, want a version-history capability refusal", err)
	}
}

// evictedVersionRegistry hides one version from the history the way retention
// eviction does (registry contract: RetentionEvictsOldestRetired drops the
// oldest retirements once the limit is exceeded): GetWorkflowVersion answers
// backend.ErrWorkflowVersionNotFound for it. Every other method stays the real
// registry's, so the engine sees an otherwise healthy workflow whose pinned
// version has aged out. Both capability methods are declared explicitly
// because an embedded interface only promotes its own method set.
type evictedVersionRegistry struct {
	backend.WorkflowRegistry
	evicted string
}

func (r evictedVersionRegistry) GetWorkflowVersion(ctx context.Context, id types.WorkflowID, version string) (backend.WorkflowRecord, error) {
	if version == r.evicted {
		return backend.WorkflowRecord{}, backend.ErrWorkflowVersionNotFound
	}
	versions, ok := r.WorkflowRegistry.(backend.WorkflowVersionCapability)
	if !ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowVersionNotFound
	}
	return versions.GetWorkflowVersion(ctx, id, version)
}

func (r evictedVersionRegistry) ListWorkflowVersions(ctx context.Context, id types.WorkflowID) ([]backend.WorkflowVersionInfo, error) {
	versions, ok := r.WorkflowRegistry.(backend.WorkflowVersionCapability)
	if !ok {
		return nil, backend.ErrWorkflowNotFound
	}
	return versions.ListWorkflowVersions(ctx, id)
}

// TestInvokePinnedVersionEvictedFailsClosed pins the answer when a keyed
// retry's version pin has aged out of history: retention evicted the oldest
// retired versions, so the archived payload that would verify the retry is
// gone. The pin is resolved before the idempotency lookup (invoke.go), so the
// retry fails closed with backend.ErrWorkflowVersionNotFound — the HTTP face's
// 404 workflow_version_not_found — instead of answering duplicate for an
// execution the call can no longer verify.
func TestInvokePinnedVersionEvictedFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	id, err := eng.AddWorkflow(ctx, pinVersionWorkflow("sdk-pin-evicted", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	first, err := eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v1"), WithIdempotencyKey("evicted-pin"))
	if err != nil {
		t.Fatalf("first pinned keyed Invoke: %v", err)
	}
	if _, err := eng.Wait(ctx, first); err != nil {
		t.Fatalf("Wait(%q): %v", first, err)
	}

	publishWorkflowVersion(t, ctx, eng.workflowRegistry, id, pinVersionWorkflow("sdk-pin-evicted", "v2", "only-v2"))
	eng.workflowRegistry = evictedVersionRegistry{eng.workflowRegistry, "v1"}

	replay, err := eng.Invoke(ctx, id, Start(), nil, WithWorkflowVersion("v1"), WithIdempotencyKey("evicted-pin"))
	if !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("retry pinned to an evicted version = (%q, %v), want backend.ErrWorkflowVersionNotFound", replay, err)
	}
	if replay != "" {
		t.Fatalf("evicted pin returned execution %q, want none", replay)
	}

	// The failure is the pin resolution, not a broken engine after the swap: a
	// fresh unpinned key still runs the live version.
	live, err := eng.Invoke(ctx, id, Start(), nil, WithIdempotencyKey("evicted-pin-live"))
	if err != nil {
		t.Fatalf("unpinned keyed Invoke after the eviction: %v", err)
	}
	if got := executedGraph(t, ctx, eng, live).WorkflowVersion(); got != "v2" {
		t.Fatalf("live execution graph version = %q, want v2", got)
	}
}
