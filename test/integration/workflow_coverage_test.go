//go:build integration

package integration

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
)

// testFixtureNodeTypePrefix marks node types that exist only as fixtures of
// this test package.
const testFixtureNodeTypePrefix = "test."

// TestWorkflowNodeCoverage asserts the tier definitions together exercise every
// registered action type the graph compiler accepts.
//
// Coverage is measured from the compiled definitions, not from a written-down
// list: a list is a second copy of the registry that goes stale the moment a
// definition drops a node, and a suite asserting against the stale copy reports
// coverage the workflows no longer have.
//
// The browser node is not subtracted — TestWorkflowBrowserCDPFailsClosed runs
// its real handler and asserts the fail-closed classification. Nothing here is
// coverage by declaration.
func TestWorkflowNodeCoverage(t *testing.T) {
	seen := map[string][]string{} // node type -> definitions carrying it
	for _, def := range workflows.Definitions() {
		built, err := def.Build().Definition()
		if err != nil {
			t.Fatalf("%s: Definition: %v", def.Name, err)
		}
		if _, err := graph.Compile(built); err != nil {
			t.Fatalf("%s: Compile: %v", def.Name, err)
		}
		for nodeType := range workflows.NodeTypesIn(built) {
			seen[nodeType] = append(seen[nodeType], def.Name)
		}
	}

	var missing, covered []string
	for _, nodeType := range registry.Types() {
		// Sibling tests in this package register "test."-prefixed fixture
		// handlers into the process-global registry (supply gating, WASM
		// version switch, ...). Whether they are present here depends only on
		// test order, and they are not product types a tier definition could
		// exercise, so they are not coverage gaps.
		if strings.HasPrefix(nodeType, testFixtureNodeTypePrefix) {
			continue
		}
		if len(seen[nodeType]) > 0 {
			covered = append(covered, nodeType)
			continue
		}
		missing = append(missing, nodeType)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("registered node types no tier definition exercises (%d):\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}

	// The reverse direction is a different defect: a type named by a definition
	// but absent from the registry would compile only while a handler happened to
	// be registered ad hoc, and would fail on a real runner. Declaration-only
	// types are the recorded exception.
	registered := map[string]bool{}
	for _, nodeType := range registry.Types() {
		registered[nodeType] = true
	}
	for _, nodeType := range workflows.DeclarationOnlyNodeTypes {
		registered[nodeType] = true
	}
	var unknown []string
	for nodeType := range seen {
		if !registered[nodeType] {
			unknown = append(unknown, nodeType)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		t.Errorf("tier definitions use node types absent from the registry: %v", unknown)
	}

	t.Logf("node coverage: %d/%d registered action types across %d definitions",
		len(covered), len(covered)+len(missing), len(workflows.Definitions()))
}

// TestWorkflowTriggerCoverage asserts every registered trigger kind appears as
// the entry node of some trigger tier. Triggers are counted separately from
// action nodes because registry.Types() does not list them: a trigger registers
// through RegisterTrigger and carries a different handler contract, so a
// coverage assertion over action types alone would silently skip all five.
func TestWorkflowTriggerCoverage(t *testing.T) {
	seen := map[string]string{} // trigger type -> definition carrying it
	for _, def := range workflows.Definitions() {
		built, err := def.Build().Definition()
		if err != nil {
			t.Fatalf("%s: Definition: %v", def.Name, err)
		}
		for nodeType := range workflows.NodeTypesIn(built) {
			if strings.HasPrefix(nodeType, "xflow.trigger.") {
				seen[nodeType] = def.Name
			}
		}
	}

	var missing []string
	kinds := 0
	for _, triggerType := range registry.TriggerTypes() {
		// Same exclusion as TestWorkflowNodeCoverage: sibling tests register
		// "test."-prefixed fixture triggers into the global registry, and
		// whether they are present depends only on test order.
		if strings.HasPrefix(triggerType, testFixtureNodeTypePrefix) {
			continue
		}
		kinds++
		if _, ok := seen[triggerType]; !ok {
			missing = append(missing, triggerType)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("registered trigger kinds no tier definition covers (%d): %v", len(missing), missing)
	}
	t.Logf("trigger coverage: %d/%d kinds", len(seen), kinds)
}

// TestWorkflowCoverageSummary logs per-definition size and kinds, so a CI log
// carries the measured shape even when every assertion above passes.
func TestWorkflowCoverageSummary(t *testing.T) {
	for _, def := range workflows.Definitions() {
		built, err := def.Build().Definition()
		if err != nil {
			t.Fatalf("%s: Definition: %v", def.Name, err)
		}
		counts := workflows.NodeTypesIn(built)
		kinds := make([]string, 0, len(counts))
		nodes := 0
		for nodeType, n := range counts {
			kinds = append(kinds, nodeType)
			nodes += n
		}
		sort.Strings(kinds)
		t.Logf("%-16s nodes=%d kinds=%d %s", def.Name, nodes, len(kinds), strings.Join(kinds, ","))
	}
}

// TestWorkflowBrowserCDPFailsClosed runs the xflow.browser.cdp handler for real
// and asserts its fail-closed default.
//
// The node dials a remote Chrome, and endpoint admission defaults to an empty
// allowlist, so a request naming any endpoint is denied by policy before a
// connection is attempted. That denial is the whole of the behaviour this
// environment can observe: the node's navigation policy also blocks the private
// addresses a locally launched Chrome would answer on, so there is no harness in
// which the harvest path runs without weakening the very control under test.
// Asserting the denial is asserting the control holds.
//
// The error port is what makes the assertion possible: the node reports the
// denial as a permanent classified error, and the workflow's recovery node is
// reached only if both the classification and the port routing hold.
func TestWorkflowBrowserCDPFailsClosed(t *testing.T) {
	wf := workflows.BrowserCDPWorkflow()
	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if _, err := graph.Compile(def); err != nil {
		t.Fatalf("Compile: %v", err)
	}

	eng, execID, result := runLocalWorkflow(t, wf, map[string]any{})
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want %s", result.Status, types.ExecutionStatusSuccess)
	}

	ctx := context.Background()
	sub := inspectNode(t, ctx, eng, execID, "subject")
	if !strings.Contains(sub.Error, "browser.host_denied") {
		t.Fatalf("subject error = %q, want it to contain browser.host_denied", sub.Error)
	}
	if sub.Port != "error" {
		t.Fatalf("subject port = %q, want error (the denial is a permanent failure)", sub.Port)
	}

	recover := inspectNode(t, ctx, eng, execID, "recover")
	if recover.Status != types.NodeStatusSuccess {
		t.Fatalf("recover node status = %s, want success (the error port must route the denial)", recover.Status)
	}
}
