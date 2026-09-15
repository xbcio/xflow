package xflow

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

func TestWithServerEnrollmentRunnerIDPrefixReachesIssuedPolicy(t *testing.T) {
	ctx := context.Background()
	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
	id, code, err := control.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(ctx, control.RegistrationCode{
		ID: id, CodeHash: control.HashSecret(code), CreatedAt: time.Now().UTC(),
		AllowedNamespaces: []string{"team-a"}, AllowedNodeTypes: []string{"xflow.function"},
	}); err != nil {
		t.Fatalf("codes.Create: %v", err)
	}

	srv, err := NewServer(ServerConfig{},
		WithServerEnroll(codes, ids),
		WithServerEnrollmentRunnerIDPrefix("sas-runner-"),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	issued, err := protocol.NewClient(ts.URL, ts.Client()).Enroll(ctx, protocol.EnrollRequest{
		RegistrationCode: code,
		Namespaces:       []string{"team-a"},
		NodeTypes:        []string{"xflow.function"},
	})
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if !strings.HasPrefix(issued.RunnerID, "sas-runner-") {
		t.Fatalf("issued runner ID = %q, want sas-runner- prefix", issued.RunnerID)
	}
	stored, ok, err := ids.Lookup(ctx, issued.RunnerID)
	if err != nil || !ok {
		t.Fatalf("issued identity lookup = (%v, %v), want (true, nil)", ok, err)
	}
	if stored.Scope.IDPrefix != "sas-runner-" {
		t.Fatalf("issued policy IDPrefix = %q, want sas-runner-", stored.Scope.IDPrefix)
	}
}

func TestWithServerEnrollmentRunnerIDPrefixRejectsUnsafeValue(t *testing.T) {
	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
	_, err := NewServer(ServerConfig{},
		WithServerEnroll(codes, ids),
		WithServerEnrollmentRunnerIDPrefix("runner bad"),
	)
	if err == nil {
		t.Fatal("NewServer accepted an unsafe enrollment runner ID prefix")
	}
}
