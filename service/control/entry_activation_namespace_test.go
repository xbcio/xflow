package control

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// The calls below pass a trailing `excluded` argument to chooseRunner and
// fallbackChooseRunner. That parameter is added by an in-flight change that
// landed after the commit introducing this file, so this file does not compile
// against that commit in isolation. It was written against the working tree so
// the tests would genuinely run; the namespace guard they exercise sits at the
// same position in both signatures, and `excluded` is orthogonal to it.
//
// If the excluded-parameter change is ever reverted, drop the trailing argument
// here rather than assuming these tests are stale.

// TestChooseRunnerSkipsRunnerOutsideActivationNamespace pins that entry
// activation host selection honours the runner's namespace membership. The
// activation's namespace is server-authoritative (it comes from the durable
// EntryActivationStore, never from a runner request), so it is a sound basis
// for the comparison.
func TestChooseRunnerSkipsRunnerOutsideActivationNamespace(t *testing.T) {
	now := time.Now()
	r := &EntryActivationReconciler{}
	act := &engine.EntryActivation{Namespace: "team-a"}

	live := []RunnerSnapshot{
		{RunnerID: "runner-b", LastHeartbeat: now, Namespaces: []namespace.Namespace{"team-b"}},
	}

	if _, ok := r.chooseRunner(act, live, now, nil); ok {
		t.Fatal("chose a runner that does not serve the activation's namespace")
	}
}

// TestChooseRunnerPicksRunnerInActivationNamespace is the positive control:
// without it the test above would pass against a chooseRunner that returns
// false unconditionally.
func TestChooseRunnerPicksRunnerInActivationNamespace(t *testing.T) {
	now := time.Now()
	r := &EntryActivationReconciler{}
	act := &engine.EntryActivation{Namespace: "team-a"}

	live := []RunnerSnapshot{
		{RunnerID: "runner-b", LastHeartbeat: now, Namespaces: []namespace.Namespace{"team-b"}},
		{RunnerID: "runner-a", LastHeartbeat: now, Namespaces: []namespace.Namespace{"team-a"}},
	}

	got, ok := r.chooseRunner(act, live, now, nil)
	if !ok {
		t.Fatal("no runner chosen despite one serving the namespace")
	}
	if got.RunnerID != "runner-a" {
		t.Fatalf("chose %q, want runner-a", got.RunnerID)
	}
}

// TestChooseRunnerEmptyNamespacesServesDefaultOnly pins the same back-compat
// rule the rest of the package uses: a runner that declared no namespaces
// serves the default namespace and nothing else.
func TestChooseRunnerEmptyNamespacesServesDefaultOnly(t *testing.T) {
	now := time.Now()
	r := &EntryActivationReconciler{}
	live := []RunnerSnapshot{{RunnerID: "legacy", LastHeartbeat: now}}

	if _, ok := r.chooseRunner(&engine.EntryActivation{Namespace: namespace.Default}, live, now, nil); !ok {
		t.Fatal("a runner with no declared namespaces must still serve default")
	}
	if _, ok := r.chooseRunner(&engine.EntryActivation{Namespace: "team-a"}, live, now, nil); ok {
		t.Fatal("a runner with no declared namespaces must not serve a non-default namespace")
	}
}

// TestFallbackChooseRunnerStillHonoursNamespace pins that the default-selector
// fallback relaxes labels only. The fallback exists so a default-selector
// activation is not stranded when no labelled runner appears; it must not turn
// into a cross-namespace escape hatch.
func TestFallbackChooseRunnerStillHonoursNamespace(t *testing.T) {
	now := time.Now()
	r := &EntryActivationReconciler{noMatchSince: map[engine.EntryActivationKey]time.Time{}}
	act := &engine.EntryActivation{Namespace: "team-a"}
	key := engine.EntryActivationKey{}

	// First call only starts the grace window and returns false by design.
	if _, ok := r.fallbackChooseRunner(act, nil, now, key, nil); ok {
		t.Fatal("fallback chose a runner on the first (tracking) call")
	}

	past := now.Add(-2 * DefaultSelectorFallback)
	r.noMatchSince[key] = past

	live := []RunnerSnapshot{
		{RunnerID: "runner-b", LastHeartbeat: now, Namespaces: []namespace.Namespace{"team-b"}},
	}
	if got, ok := r.fallbackChooseRunner(act, live, now, key, nil); ok {
		t.Fatalf("fallback chose %q from another namespace after grace elapsed", got.RunnerID)
	}

	// Positive control: a same-namespace runner IS chosen by the fallback,
	// so the assertion above cannot pass merely because fallback never fires.
	r.noMatchSince[key] = past
	live = append(live, RunnerSnapshot{RunnerID: "runner-a", LastHeartbeat: now, Namespaces: []namespace.Namespace{"team-a"}})
	got, ok := r.fallbackChooseRunner(act, live, now, key, nil)
	if !ok {
		t.Fatal("fallback chose nobody despite a same-namespace runner being live")
	}
	if got.RunnerID != "runner-a" {
		t.Fatalf("fallback chose %q, want runner-a", got.RunnerID)
	}
}
