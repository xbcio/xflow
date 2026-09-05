package main

import "testing"

// The production gate at main.go's validateProduction call must accept a
// server whose runner auth comes from enrollment rather than a runners.yaml.
// Leaving it keyed on --auth-policy alone would make "enroll-only production"
// impossible to start, and the workaround would be an empty policy file —
// auth theater.
func TestRunnerAuthConfiguredCountsEnrollment(t *testing.T) {
	cases := []struct {
		name       string
		authPolicy string
		enroll     bool
		want       bool
	}{
		{"neither", "", false, false},
		{"policy file only", "/etc/xflow/runners.yaml", false, true},
		{"enroll only", "", true, true},
		{"both", "/etc/xflow/runners.yaml", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runnerAuthConfigured(serverConfig{authPolicy: tc.authPolicy, enroll: tc.enroll})
			if got != tc.want {
				t.Fatalf("runnerAuthConfigured = %v, want %v", got, tc.want)
			}
		})
	}
}
