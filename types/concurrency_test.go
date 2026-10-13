package types

import (
	"encoding/json"
	"testing"
	"time"
)

// TestConcurrencySettingsAcceptsBothForms pins the DSL's two wire forms: a bare
// int means QUEUE (the mode the shorthand denotes), and the object form carries
// an explicit mode plus the queue controls.
func TestConcurrencySettingsAcceptsBothForms(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want ConcurrencySettings
	}{
		{"bare int", `{"concurrency":10}`, ConcurrencySettings{Limit: 10}},
		{"explicit queue", `{"concurrency":{"limit":4,"mode":"queue"}}`, ConcurrencySettings{Limit: 4, Mode: ConcurrencyModeQueue}},
		{"reject", `{"concurrency":{"limit":2,"mode":"reject"}}`, ConcurrencySettings{Limit: 2, Mode: ConcurrencyModeReject}},
		{"queue controls", `{"concurrency":{"limit":3,"max_queued":9,"queue_timeout":30000000000}}`,
			ConcurrencySettings{Limit: 3, MaxQueued: 9, QueueTimeout: 30 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s WorkflowSettings
			if err := json.Unmarshal([]byte(tc.in), &s); err != nil {
				t.Fatalf("Unmarshal(%s) error = %v", tc.in, err)
			}
			if s.Concurrency == nil {
				t.Fatalf("Unmarshal(%s) left Concurrency nil", tc.in)
			}
			got := *s.Concurrency
			if got.Limit != tc.want.Limit || got.Mode != tc.want.Mode ||
				got.MaxQueued != tc.want.MaxQueued || got.QueueTimeout != tc.want.QueueTimeout {
				t.Errorf("Unmarshal(%s) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func TestConcurrencySettingsEffectiveModeDefaultsToQueue(t *testing.T) {
	if got := (*ConcurrencySettings)(nil).EffectiveMode(); got != ConcurrencyModeQueue {
		t.Errorf("nil EffectiveMode() = %q, want %q", got, ConcurrencyModeQueue)
	}
	if got := (&ConcurrencySettings{Limit: 1}).EffectiveMode(); got != ConcurrencyModeQueue {
		t.Errorf("bare-int EffectiveMode() = %q, want %q", got, ConcurrencyModeQueue)
	}
	if got := (&ConcurrencySettings{Mode: ConcurrencyModeReject}).EffectiveMode(); got != ConcurrencyModeReject {
		t.Errorf("reject EffectiveMode() = %q, want %q", got, ConcurrencyModeReject)
	}
}

func TestConcurrencySettingsEffectiveLimitNil(t *testing.T) {
	if got := (*ConcurrencySettings)(nil).EffectiveLimit(); got != 0 {
		t.Errorf("nil EffectiveLimit() = %d, want 0", got)
	}
}

// TestConcurrencySettingsRoundTripIsStable is the property the workflow
// definition hash depends on: decoding a stored definition and re-encoding it
// must produce the same bytes, or re-registering an unchanged workflow would
// report a conflict. In particular the bare-int shorthand must not silently
// expand into the object form.
func TestConcurrencySettingsRoundTripIsStable(t *testing.T) {
	inputs := []string{
		`{"concurrency":10}`,
		`{"concurrency":{"limit":5,"mode":"reject"}}`,
		`{"concurrency":{"limit":5,"mode":"queue","max_queued":3}}`,
		`{"concurrency":{"limit":5,"mode":"queue"}}`,
	}
	for _, in := range inputs {
		var s WorkflowSettings
		if err := json.Unmarshal([]byte(in), &s); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", in, err)
		}
		first, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("Marshal(%s) error = %v", in, err)
		}
		var again WorkflowSettings
		if err := json.Unmarshal(first, &again); err != nil {
			t.Fatalf("re-Unmarshal(%s) error = %v", first, err)
		}
		second, err := json.Marshal(again)
		if err != nil {
			t.Fatalf("re-Marshal(%s) error = %v", first, err)
		}
		if string(first) != string(second) {
			t.Errorf("%s: round trip drifted: %s then %s", in, first, second)
		}
		if string(first) != in {
			t.Errorf("%s: first encode = %s, want the input verbatim", in, first)
		}
	}
}

func TestConcurrencySettingsRejectsMalformedForms(t *testing.T) {
	for _, in := range []string{
		`{"concurrency":"wide"}`,
		`{"concurrency":{"limit":"3"}}`,
		`{"concurrency":null}`,
	} {
		var s WorkflowSettings
		err := json.Unmarshal([]byte(in), &s)
		if in == `{"concurrency":null}` {
			// JSON null is the absent value, not a malformed setting.
			if err != nil || s.Concurrency != nil {
				t.Errorf("Unmarshal(%s) = (%+v, %v), want nil settings and no error", in, s.Concurrency, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("Unmarshal(%s) succeeded, want an error", in)
		}
	}
}

// TestConcurrencySettingsOmitsWhenAbsent guards the hash stability of every
// definition that predates the setting.
func TestConcurrencySettingsOmitsWhenAbsent(t *testing.T) {
	out, err := json.Marshal(WorkflowSettings{Timezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"timezone":"UTC"}` {
		t.Errorf("Marshal = %s, want no concurrency key", out)
	}
}
