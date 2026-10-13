package cron

import (
	"strings"
	"testing"
	"time"
)

// TestCatchupConfigDefaults pins what an absent param resolves to. These three
// values are the ones node/internal.BuiltinFallbacks advertises to the editor,
// and TestFallbackContractCron proves the runtime agrees with them.
func TestCatchupConfigDefaults(t *testing.T) {
	cfg, err := catchupConfigFrom(map[string]any{"expression": "*/1 * * * *"})
	if err != nil {
		t.Fatalf("catchupConfigFrom: %v", err)
	}
	want := catchupConfig{policy: misfireSkip, window: defaultCatchupWindow, max: defaultMaxCatchup}
	if cfg != want {
		t.Fatalf("defaults = %+v, want %+v", cfg, want)
	}
}

// TestCatchupConfigReadsEveryParam covers the set side, including the two params
// that arrive as strings from a definition and as numbers from a DTO.
func TestCatchupConfigReadsEveryParam(t *testing.T) {
	cfg, err := catchupConfigFrom(map[string]any{
		"misfire":        "fire_all",
		"catchup_window": "30m",
		"max_catchup":    float64(3),
	})
	if err != nil {
		t.Fatalf("catchupConfigFrom: %v", err)
	}
	want := catchupConfig{policy: misfireFireAll, window: 30 * time.Minute, max: 3}
	if cfg != want {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

// TestCatchupConfigRejectsUnreadableValues is the fail-closed half: a value
// written on purpose that the trigger cannot honour must not be replaced by the
// default in silence. A "fire-all" with a hyphen, a window that is not a
// duration, and a cap of zero each turn a schedule into something other than
// what its author asked for, so each is an activation error.
func TestCatchupConfigRejectsUnreadableValues(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   string
	}{
		{"a policy that is not one of the three", map[string]any{"misfire": "fire-all"}, "misfire must be one of"},
		{"a policy in the wrong case", map[string]any{"misfire": "Skip"}, "misfire must be one of"},
		{"a window that is not a duration", map[string]any{"catchup_window": "soon"}, "catchup_window"},
		{"a window of zero", map[string]any{"catchup_window": "0s"}, "catchup_window"},
		{"a negative window", map[string]any{"catchup_window": "-1h"}, "catchup_window"},
		{"a negative cap", map[string]any{"max_catchup": float64(-1)}, "max_catchup"},
		{"a cap of zero", map[string]any{"max_catchup": float64(0)}, "max_catchup"},
		{"a cap that is not a number", map[string]any{"max_catchup": "ten"}, "max_catchup"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := catchupConfigFrom(tc.params)
			if err == nil {
				t.Fatal("the value was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name %q", err, tc.want)
			}
		})
	}
}
