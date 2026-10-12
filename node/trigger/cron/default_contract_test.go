package cron

// Trigger-activate Default contract (descriptor-contract design §2.4): the
// location a cron schedule is evaluated in must be the same whether timezone is
// absent or set to the descriptor's Default. The location is observed on the
// first event the activation emits (newCronTriggerEvent stamps the firing time
// in the schedule's location); a non-default zone is the control that proves
// the observation sees the parameter at all.
//
// The catch-up params declare no Default at all — their fallbacks live in
// node/internal.BuiltinFallbacks — so their contract is the table's other half:
// TestFallbackContractCron below proves the runtime substitutes exactly the
// value the table advertises.

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"
	// The control zone must resolve even on a host without system zoneinfo.
	_ "time/tzdata"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func cronDescriptorDefault(t *testing.T, name string) any {
	t.Helper()
	for _, p := range New().Descriptor().Params {
		if p.Name == name {
			if p.Default == nil {
				t.Fatalf("xflow.trigger.cron/%s has no Default", name)
			}
			return p.Default
		}
	}
	t.Fatalf("xflow.trigger.cron has no param %q", name)
	return nil
}

// firedLocation activates a once-a-second schedule and returns the location of
// the first emitted event's time.
func firedLocation(t *testing.T, params map[string]any) string {
	t.Helper()
	return firedLocationIn(t, params, "")
}

// firedLocationIn is firedLocation with a workflow settings.timezone supplied
// on the activation input.
func firedLocationIn(t *testing.T, params map[string]any, workflowTZ string) string {
	t.Helper()
	rt := triggertest.NewFakeRuntime()
	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "cron",
		Params:     params,
		Runtime:    rt,

		WorkflowTimezone: workflowTZ,
	})
	if err != nil {
		t.Fatalf("Activate(%v): %v", params, err)
	}
	defer func() { _ = sub.Close(context.Background()) }()
	if !rt.WaitEmit(3 * time.Second) {
		t.Fatal("cron did not fire")
	}
	return rt.Events()[0].Time.Location().String()
}

func TestDefaultContractCronTimezone(t *testing.T) {
	def := cronDescriptorDefault(t, "timezone")
	variants := map[string]map[string]any{
		"absent":  {"expression": "@every 1s"},
		"default": {"expression": "@every 1s", "timezone": def},
		// Explicit "" is read as absent here (cast.ToString then == ""), unlike
		// approval.mode.
		"empty": {"expression": "@every 1s", "timezone": ""},
		// Control: a real non-default zone must be observed as different.
		"control": {"expression": "@every 1s", "timezone": "Asia/Tokyo"},
	}
	got := make(map[string]string, len(variants))
	var mu sync.Mutex
	// The group returns only once its parallel subtests have finished, so the
	// four ~1s activations overlap instead of running back to back.
	t.Run("fire", func(t *testing.T) {
		for name, params := range variants {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				loc := firedLocation(t, params)
				mu.Lock()
				got[name] = loc
				mu.Unlock()
			})
		}
	})
	if got["absent"] == "" {
		t.Fatalf("no location observed: %v", got)
	}
	if got["default"] != got["absent"] || got["empty"] != got["absent"] {
		t.Errorf("timezone locations = %v; absent, empty and Default %v must match", got, def)
	}
	if got["control"] == got["absent"] {
		t.Errorf("control zone read as %q like absent; the probe does not observe timezone", got["control"])
	}
}

// TestCronTimezonePrecedence pins the resolution order: the node's own
// timezone, then the workflow settings.timezone, then UTC.
func TestCronTimezonePrecedence(t *testing.T) {
	cases := map[string]struct {
		params     map[string]any
		workflowTZ string
		want       string
	}{
		"node wins":           {map[string]any{"expression": "@every 1s", "timezone": "Asia/Tokyo"}, "Asia/Shanghai", "Asia/Tokyo"},
		"workflow when unset": {map[string]any{"expression": "@every 1s"}, "Asia/Shanghai", "Asia/Shanghai"},
		"UTC when neither":    {map[string]any{"expression": "@every 1s"}, "", "UTC"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := firedLocationIn(t, tc.params, tc.workflowTZ); got != tc.want {
				t.Fatalf("location = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCronUnsetTimezoneIsOmitted: an unset node timezone must stay
// distinguishable from an explicit "UTC", or the workflow default could never
// apply to a Go-built cron node.
func TestCronUnsetTimezoneIsOmitted(t *testing.T) {
	params := New().Cron("* * * * *").RawParams().(map[string]any)
	if _, ok := params["timezone"]; ok {
		t.Fatalf("RawParams() = %v, want no timezone key when unset", params)
	}
}

// cronContract is one activation of a trigger that was down across its
// schedule, with exactly one catch-up param either absent or set. The firings
// it admits are the observable: a policy, a window and a cap are all decisions
// about how many of the missed instants run.
type cronContract struct {
	expression string
	last       time.Time
	others     map[string]any
}

// firings runs the contract's activation with param either unset or set to
// value, and returns how many firings it admitted.
func (c cronContract) firings(t *testing.T, param string, value any, set bool) int {
	t.Helper()
	ctx := context.Background()
	rt := triggertest.NewFakeEntrySeedRuntime()
	if err := writeWatermark(ctx, rt.State(ctx, watermarkScope("wf-1", "entry")), c.last); err != nil {
		t.Fatalf("writeWatermark: %v", err)
	}

	params := map[string]any{
		"expression":       c.expression,
		"entry_seed":       true,
		"entry_unit_id":    "entry",
		"workflow_version": "v1",
	}
	maps.Copy(params, c.others)
	if set {
		params[param] = value
	}

	sub, err := New().Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     params,
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()
	return rt.SeedCount()
}

// TestFallbackContractCron exercises every BuiltinFallbacks entry for the cron
// trigger through Activate: the param absent, then the param set to the value
// the table declares, both against a trigger that missed firings. Each check
// carries the count the declared value must produce, so a fallback that drifted
// from the table — or a table that drifted from the handler — fails here rather
// than in a deployment whose schedule silently stopped catching up.
func TestFallbackContractCron(t *testing.T) {
	now := time.Now().UTC()
	checks := map[string]func(*testing.T, nodeinternal.Fallback){
		// Behind by half an hour with the cap raised: skip admits nothing, and
		// any policy that fires would admit thirty-odd firings here.
		"misfire": func(t *testing.T, f nodeinternal.Fallback) {
			c := cronContract{
				expression: "*/1 * * * *",
				last:       now.Add(-30 * time.Minute),
				others:     map[string]any{"catchup_window": "1h", "max_catchup": float64(1000)},
			}
			if absent, set := c.firings(t, "misfire", f.Value, false), c.firings(t, "misfire", f.Value, true); absent != 0 || set != 0 {
				t.Fatalf("firings = %d absent, %d with %s=%v; want none", absent, set, f.Param, f.Value)
			}
		},
		// Behind by three hours on an hourly schedule: a one-hour window leaves
		// exactly the firing of the hour the activation is in, and a longer
		// fallback window would leave two or more.
		"catchup_window": func(t *testing.T, f nodeinternal.Fallback) {
			c := cronContract{
				expression: "0 * * * *",
				last:       now.Add(-3 * time.Hour),
				others:     map[string]any{"misfire": "fire_all", "max_catchup": float64(1000)},
			}
			if absent, set := c.firings(t, "catchup_window", f.Value, false), c.firings(t, "catchup_window", f.Value, true); absent != 1 || set != 1 {
				t.Fatalf("firings = %d absent, %d with %s=%v; want 1 each", absent, set, f.Param, f.Value)
			}
		},
		// Behind by half an hour with everything else firing: the cap is the
		// only thing deciding how many of the thirty-odd instants run.
		"max_catchup": func(t *testing.T, f nodeinternal.Fallback) {
			c := cronContract{
				expression: "*/1 * * * *",
				last:       now.Add(-30 * time.Minute),
				others:     map[string]any{"misfire": "fire_all", "catchup_window": "1h"},
			}
			if absent, set := c.firings(t, "max_catchup", f.Value, false), c.firings(t, "max_catchup", f.Value, true); absent != 10 || set != 10 {
				t.Fatalf("firings = %d absent, %d with %s=%v; want 10 each", absent, set, f.Param, f.Value)
			}
		},
	}
	runFallbackChecks(t, "xflow.trigger.cron", 1, checks)
}

// runFallbackChecks runs one check per BuiltinFallbacks entry of typ@version and
// fails on a missing or an unclaimed one, so the table and the handler cannot
// drift apart in either direction.
func runFallbackChecks(t *testing.T, typ string, version int, checks map[string]func(*testing.T, nodeinternal.Fallback)) {
	t.Helper()
	entries := nodeinternal.BuiltinFallbacksFor(typ, version)
	if len(entries) == 0 {
		t.Fatalf("BuiltinFallbacks has no entry for %s@%d", typ, version)
	}
	seen := map[string]bool{}
	for _, f := range entries {
		seen[f.Param] = true
		check, ok := checks[f.Param]
		if !ok {
			t.Errorf("BuiltinFallbacks entry %s@%d/%s has no handler check", typ, version, f.Param)
			continue
		}
		t.Run(f.Param, func(t *testing.T) { check(t, f) })
	}
	for param := range checks {
		if !seen[param] {
			t.Errorf("handler check %s checks a param with no BuiltinFallbacks entry", param)
		}
	}
}
