package types

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSettingsTimeoutJSONRoundTrip pins the wire form of settings.timeout: a
// time.Duration travels as an integer count of nanoseconds, the same form
// ConcurrencySettings.QueueTimeout uses, and survives a marshal/unmarshal trip
// unchanged. Zero is the spelling of "no budget" and is omitted from the output.
func TestSettingsTimeoutJSONRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
	}{
		{"thirty seconds", 30 * time.Second},
		{"ninety minutes", 90 * time.Minute},
		{"one year", 24 * time.Hour * 365},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(WorkflowSettings{Timeout: tc.timeout})
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			want := `{"timeout":` + strconv.FormatInt(int64(tc.timeout), 10) + `}`
			if string(raw) != want {
				t.Fatalf("Marshal() = %s, want %s: timeout must be integer nanoseconds", raw, want)
			}
			var back WorkflowSettings
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatalf("Unmarshal(%s) error = %v", raw, err)
			}
			if back.Timeout != tc.timeout {
				t.Fatalf("round trip = %v, want %v", back.Timeout, tc.timeout)
			}
		})
	}
}

// TestSettingsTimeoutZeroIsOmitted: the zero value is the "unset" spelling, so
// it must not appear on the wire at all, and an object without the key reads
// back as zero.
func TestSettingsTimeoutZeroIsOmitted(t *testing.T) {
	raw, err := json.Marshal(WorkflowSettings{Timezone: "UTC"})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if strings.Contains(string(raw), "timeout") {
		t.Fatalf("Marshal() = %s, want no timeout key for a zero budget", raw)
	}
	var back WorkflowSettings
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", raw, err)
	}
	if back.Timeout != 0 {
		t.Fatalf("Timeout = %v, want 0", back.Timeout)
	}
}

// TestSettingsTimeoutRejectsDurationStrings records the behavior as it stands:
// WorkflowSettings has no custom decoder for timeout, so the standard library's
// rule for time.Duration applies and only a JSON integer (nanoseconds) is
// accepted. A Go duration string such as "30s", and even a numeric string, is an
// unmarshal error, not a silent zero. If a string form is ever accepted this
// test is the place to change deliberately.
func TestSettingsTimeoutRejectsDurationStrings(t *testing.T) {
	for _, in := range []string{
		`{"timeout":"30s"}`,
		`{"timeout":"30000000000"}`,
	} {
		t.Run(in, func(t *testing.T) {
			var s WorkflowSettings
			if err := json.Unmarshal([]byte(in), &s); err == nil {
				t.Fatalf("Unmarshal(%s) accepted a string, got Timeout = %v", in, s.Timeout)
			}
			if s.Timeout != 0 {
				t.Fatalf("Unmarshal(%s) left Timeout = %v on error, want 0", in, s.Timeout)
			}
		})
	}
}
