package cron

// Trigger-activate Default contract (descriptor-contract design §2.4): the
// location a cron schedule is evaluated in must be the same whether timezone is
// absent or set to the descriptor's Default. The location is observed on the
// first event the activation emits (newCronTriggerEvent stamps the firing time
// in the schedule's location); a non-default zone is the control that proves
// the observation sees the parameter at all.

import (
	"context"
	"sync"
	"testing"
	"time"
	// The control zone must resolve even on a host without system zoneinfo.
	_ "time/tzdata"

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
