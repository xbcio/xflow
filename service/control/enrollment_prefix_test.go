package control

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestNormalizeEnrollmentRunnerIDPrefix(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "default", want: "runner-"},
		{name: "trimmed", input: "  workload-  ", want: "workload-"},
		{name: "safe punctuation", input: "runner.v2_", want: "runner.v2_"},
		{name: "reject whitespace", input: "runner id", wantErr: true},
		{name: "reject delimiter", input: "runner:{", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeEnrollmentRunnerIDPrefix(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("normalizeEnrollmentRunnerIDPrefix(%q) succeeded, want error", tt.input)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("normalizeEnrollmentRunnerIDPrefix(%q) = (%q, %v), want (%q, nil)", tt.input, got, err, tt.want)
			}
		})
	}
}

func TestEnrollUsesConfiguredRunnerIDPrefixAndPolicy(t *testing.T) {
	core, _, ids, code := enrollFixture(t, []string{"team-a"}, []string{"xflow.function"})
	core.enrollmentRunnerIDPrefix = "workload-"

	resp, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: code,
		Namespaces:       []string{"team-a"},
		NodeTypes:        []string{"xflow.function"},
	}, TransportInfo{SourceIP: "127.0.0.1"})
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if !strings.HasPrefix(resp.RunnerID, "workload-") {
		t.Fatalf("RunnerID = %q, want workload- prefix", resp.RunnerID)
	}
	issued, ok, err := ids.Lookup(context.Background(), resp.RunnerID)
	if err != nil || !ok {
		t.Fatalf("issued identity lookup = (%v, %v), want (true, nil)", ok, err)
	}
	if issued.Scope.IDPrefix != "workload-" {
		t.Fatalf("issued policy prefix = %q, want workload-", issued.Scope.IDPrefix)
	}
}

func TestEnrollDefaultRunnerIDPrefixPreservesLegacyRunnerPrefix(t *testing.T) {
	core, _, ids, code := enrollFixture(t, []string{"team-a"}, []string{"xflow.function"})
	resp, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: code,
		Namespaces:       []string{"team-a"},
		NodeTypes:        []string{"xflow.function"},
	}, TransportInfo{SourceIP: "127.0.0.1"})
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if !strings.HasPrefix(resp.RunnerID, "runner-") {
		t.Fatalf("RunnerID = %q, want legacy runner- prefix", resp.RunnerID)
	}
	issued, ok, err := ids.Lookup(context.Background(), resp.RunnerID)
	if err != nil || !ok {
		t.Fatalf("issued identity lookup = (%v, %v), want (true, nil)", ok, err)
	}
	if issued.Scope.IDPrefix != "runner-" {
		t.Fatalf("issued policy prefix = %q, want runner-", issued.Scope.IDPrefix)
	}
}
