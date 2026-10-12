package xflow

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// A trigger node's activation parameters must carry the operator's overlay.
// This is the in-process half of the schedule contract: the schedule API writes
// an overlay, and the host is the only thing that turns it into what the trigger
// actually runs. A host that merged into the wrong parameter, or forgot to merge
// at all, would leave the API reporting an override that has no effect.

const (
	testCronType  = "xflow.trigger.cron"
	testTimerType = "xflow.trigger.timer"
)

// overlayWorkflowDef declares one cron schedule, one timer, one schedule inside
// a co-location group, and one non-schedule trigger.
func overlayWorkflowDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "overlay",
		Context: &types.WorkflowContext{
			Config: map[string]any{"expr": "0 2 * * *"},
		},
		Nodes: []types.NodeDef{
			{
				Name: "nightly", Type: testCronType, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "${{ $config.expr }}", "timezone": "UTC", "misfire": "skip"},
			},
			{
				Name: "poll", Type: testTimerType, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"interval": "1m"},
			},
			{
				Name: "grouped", Type: testCronType, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "*/5 * * * *"},
			},
			{
				Name: "hook", Type: "xflow.trigger.webhook", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"path": "hook"},
			},
			{Name: "job", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"grouped", "job"}}},
		Connections: types.Connections{
			"nightly": {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
			"poll":    {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
			"grouped": {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
			"hook":    {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
		},
	}
}

// overlayHost builds a trigger runtime over a fresh overlay store, plus the
// record it activates from.
func overlayHost(t *testing.T) (*triggerRuntime, backend.WorkflowRecord, *memstore.Store) {
	t.Helper()
	def := overlayWorkflowDef()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	overrides := memstore.New()
	r := &triggerRuntime{overrides: overrides}
	rec := backend.WorkflowRecord{
		ID:         types.WorkflowID("wf-overlay"),
		Namespace:  "tenant-a",
		Definition: def,
		Graph:      g,
	}
	return r, rec, overrides
}

func putOverlay(t *testing.T, overrides *memstore.Store, rec backend.WorkflowRecord, node string, ov *store.ScheduleOverride) {
	t.Helper()
	ov.Namespace = "tenant-a"
	ov.WorkflowID = rec.ID
	ov.Node = node
	if _, err := overrides.PutScheduleOverride(context.Background(), ov, nil); err != nil {
		t.Fatalf("put overlay: %v", err)
	}
}

func nodeDef(t *testing.T, rec backend.WorkflowRecord, name string) types.NodeDef {
	t.Helper()
	for _, nd := range rec.Definition.Nodes {
		if nd.Name == name {
			return nd
		}
	}
	t.Fatalf("no node %q in the definition", name)
	return types.NodeDef{}
}

func strPtr(s string) *string { return &s }

// TestActivationParamsMergeTheOverlay is the positive path: the override's
// expression, timezone and policies replace the definition's rendered values.
func TestActivationParamsMergeTheOverlay(t *testing.T) {
	r, rec, overrides := overlayHost(t)
	putOverlay(t, overrides, rec, "nightly", &store.ScheduleOverride{
		Expression: strPtr("0 3 * * *"),
		Timezone:   strPtr("Europe/Berlin"),
		Misfire:    strPtr("fire_all"),
		Overlap:    strPtr("skip"),
	})

	params, err := r.activationParams(context.Background(), rec, nodeDef(t, rec, "nightly"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["expression"] != "0 3 * * *" {
		t.Errorf("expression = %v, want the overlay's -- the operator's change has no other effect", params["expression"])
	}
	if params["timezone"] != "Europe/Berlin" {
		t.Errorf("timezone = %v", params["timezone"])
	}
	if params["misfire"] != "fire_all" {
		t.Errorf("misfire = %v", params["misfire"])
	}
	if params["overlap"] != "skip" {
		t.Errorf("overlap = %v", params["overlap"])
	}
}

// TestActivationParamsRenderBeforeMerging pins the order: a template is rendered
// from the definition, and the overlay is applied to the result. An overlay
// merged before rendering would be run through $config, which no operator's
// literal should be.
func TestActivationParamsRenderBeforeMerging(t *testing.T) {
	r, rec, overrides := overlayHost(t)
	// Without an overlay the template renders.
	params, err := r.activationParams(context.Background(), rec, nodeDef(t, rec, "nightly"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["expression"] != "0 2 * * *" {
		t.Errorf("expression = %v, want the rendered $config value", params["expression"])
	}

	// With one, the overlay wins over what the template rendered.
	putOverlay(t, overrides, rec, "nightly", &store.ScheduleOverride{Expression: strPtr("0 4 * * *")})
	params, err = r.activationParams(context.Background(), rec, nodeDef(t, rec, "nightly"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["expression"] != "0 4 * * *" {
		t.Errorf("expression = %v, want the overlay", params["expression"])
	}
	// Fields the overlay does not set keep following the definition.
	if params["timezone"] != "UTC" || params["misfire"] != "skip" {
		t.Errorf("params = %#v, want the definition's other fields untouched", params)
	}
}

// TestActivationParamsTimerOverlayLandsOnItsInterval pins the per-kind mapping:
// a timer's timing is its `interval`, and an overlay that landed on
// "expression" would leave the timer exactly as it was.
func TestActivationParamsTimerOverlayLandsOnItsInterval(t *testing.T) {
	r, rec, overrides := overlayHost(t)
	putOverlay(t, overrides, rec, "poll", &store.ScheduleOverride{
		Expression: strPtr("5m"),
		// Fields the timer has no parameter for must not appear: the handler
		// would ignore them, and an activation carrying them would claim the
		// timer has a policy it does not.
		Timezone: strPtr("Europe/Berlin"),
		Misfire:  strPtr("fire_all"),
	})

	params, err := r.activationParams(context.Background(), rec, nodeDef(t, rec, "poll"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["interval"] != "5m" {
		t.Errorf("interval = %v, want the overlay's", params["interval"])
	}
	if _, ok := params["expression"]; ok {
		t.Errorf("the timer grew an expression key: %#v", params)
	}
	for _, key := range []string{"timezone", "misfire", "overlap"} {
		if _, ok := params[key]; ok {
			t.Errorf("a cron-only field reached the timer: %#v", params)
		}
	}
}

// TestActivationParamsIgnoreOverlaysTheApiWouldNotServe pins the two node
// shapes the schedule API never writes for. Applying one of these would run a
// schedule the API reports as not in effect.
func TestActivationParamsIgnoreOverlaysTheApiWouldNotServe(t *testing.T) {
	r, rec, overrides := overlayHost(t)
	// A grouped trigger node: the group is the entry unit, so the API reports
	// this node as an orphan and applies nothing.
	putOverlay(t, overrides, rec, "grouped", &store.ScheduleOverride{Expression: strPtr("0 9 * * *")})
	// A trigger kind with no schedule at all.
	putOverlay(t, overrides, rec, "hook", &store.ScheduleOverride{Expression: strPtr("0 9 * * *")})

	params, err := r.activationParams(context.Background(), rec, nodeDef(t, rec, "grouped"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["expression"] != "*/5 * * * *" {
		t.Errorf("grouped expression = %v, want the definition's", params["expression"])
	}
	params, err = r.activationParams(context.Background(), rec, nodeDef(t, rec, "hook"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["path"] != "hook" {
		t.Errorf("hook params = %#v, want them untouched", params)
	}
}

// TestActivationParamsWithoutAStore pins the no-overlay deployment: the
// definition's own (rendered) parameters, unchanged.
func TestActivationParamsWithoutAStore(t *testing.T) {
	r, rec, _ := overlayHost(t)
	r.overrides = nil
	params, err := r.activationParams(context.Background(), rec, nodeDef(t, rec, "nightly"))
	if err != nil {
		t.Fatalf("activationParams: %v", err)
	}
	if params["expression"] != "0 2 * * *" {
		t.Errorf("expression = %v, want the rendered definition value", params["expression"])
	}
}

// TestActivationParamsFailClosedOnAStoreFault pins the direction of a failed
// overlay read: the activation is refused rather than run on the definition.
// The overlays a read would have returned include the paused ones, so a
// fallback here is how a pause stops meaning anything.
func TestActivationParamsFailClosedOnAStoreFault(t *testing.T) {
	r, rec, _ := overlayHost(t)
	r.overrides = failingOverrideStore{err: errors.New("overlay store is unreachable")}

	if _, err := r.activationParams(context.Background(), rec, nodeDef(t, rec, "nightly")); err == nil {
		t.Fatal("a failed overlay read was swallowed; the trigger would run the definition's schedule")
	}
}

// failingOverrideStore is every method a real store has, with reads failing.
type failingOverrideStore struct{ err error }

func (s failingOverrideStore) GetScheduleOverride(context.Context, namespace.Namespace, types.WorkflowID, string) (*store.ScheduleOverride, error) {
	return nil, s.err
}

func (s failingOverrideStore) ListScheduleOverrides(context.Context, namespace.Namespace, types.WorkflowID) ([]*store.ScheduleOverride, error) {
	return nil, s.err
}

func (s failingOverrideStore) PutScheduleOverride(context.Context, *store.ScheduleOverride, *uint64) (*store.ScheduleOverride, error) {
	return nil, s.err
}

func (s failingOverrideStore) DeleteScheduleOverride(context.Context, namespace.Namespace, types.WorkflowID, string, *uint64) error {
	return s.err
}

func (s failingOverrideStore) DeleteScheduleOverridesForWorkflow(context.Context, namespace.Namespace, types.WorkflowID) (int, error) {
	return 0, s.err
}
