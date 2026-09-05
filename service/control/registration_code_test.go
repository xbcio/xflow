package control

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
)

func TestGenerateRegistrationCodeHasAtLeast32BytesOfEntropy(t *testing.T) {
	_, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(plaintext)
	if err != nil {
		t.Fatalf("plaintext is not base64url: %v", err)
	}
	if len(raw) < 32 {
		t.Fatalf("code entropy = %d bytes, want >= 32 (spec Global Constraints)", len(raw))
	}
}

func TestGenerateRegistrationCodeNeverRepeats(t *testing.T) {
	seen := make(map[string]bool, 256)
	for i := 0; i < 128; i++ {
		id, plaintext, err := GenerateRegistrationCode()
		if err != nil {
			t.Fatalf("GenerateRegistrationCode: %v", err)
		}
		if seen[plaintext] {
			t.Fatalf("duplicate plaintext on iteration %d", i)
		}
		if seen[id] {
			t.Fatalf("duplicate id on iteration %d", i)
		}
		seen[plaintext], seen[id] = true, true
	}
}

// newTestCode returns a stored code plus the plaintext the caller must present.
func newTestCode(t *testing.T, namespaces, nodeTypes []string) (RegistrationCode, string) {
	t.Helper()
	id, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	return RegistrationCode{
		ID:                id,
		CodeHash:          HashSecret(plaintext),
		AllowedNamespaces: namespaces,
		AllowedNodeTypes:  nodeTypes,
		CreatedAt:         time.Unix(1700000000, 0).UTC(),
	}, plaintext
}

func TestStoredRecordNeverContainsPlaintext(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryRegistrationCodeStore()
	code, plaintext := newTestCode(t, []string{"sas"}, []string{"*"})
	if err := st.Create(ctx, code); err != nil {
		t.Fatalf("Create: %v", err)
	}
	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List len = %d, want 1", len(list))
	}
	if strings.Contains(fmt.Sprintf("%#v", list[0]), plaintext) {
		t.Fatal("stored record contains the plaintext code; only the hash may be persisted")
	}
}

func TestResolveByPlaintextMatchesOnlyTheIssuedCode(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryRegistrationCodeStore()
	want, plaintext := newTestCode(t, []string{"sas"}, []string{"*"})
	other, _ := newTestCode(t, []string{"other"}, []string{"*"})
	if err := st.Create(ctx, other); err != nil {
		t.Fatalf("Create other: %v", err)
	}
	if err := st.Create(ctx, want); err != nil {
		t.Fatalf("Create want: %v", err)
	}

	got, err := st.ResolveByPlaintext(ctx, plaintext)
	if err != nil {
		t.Fatalf("ResolveByPlaintext: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("resolved id = %q, want %q", got.ID, want.ID)
	}

	if _, err := st.ResolveByPlaintext(ctx, "not-a-real-code"); err != ErrRegistrationCodeUnknown {
		t.Fatalf("unknown code err = %v, want ErrRegistrationCodeUnknown", err)
	}
}

func TestResolveByPlaintextRejectsRevoked(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryRegistrationCodeStore()
	code, plaintext := newTestCode(t, []string{"sas"}, []string{"*"})
	if err := st.Create(ctx, code); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := st.Revoke(ctx, code.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := st.ResolveByPlaintext(ctx, plaintext); err != ErrRegistrationCodeRevoked {
		t.Fatalf("revoked code err = %v, want ErrRegistrationCodeRevoked", err)
	}
	if err := st.Revoke(ctx, "no-such-id"); err != ErrRegistrationCodeNotFound {
		t.Fatalf("Revoke(missing) = %v, want ErrRegistrationCodeNotFound", err)
	}
}

func TestPolicyEnforcesNamespaceAndNodeTypeScope(t *testing.T) {
	code := RegistrationCode{
		ID:                "code-1",
		AllowedNamespaces: []string{"a"},
		AllowedNodeTypes:  []string{"kafka.trigger"},
	}
	p := code.Policy()
	if !p.AllowsNamespace(namespace.Namespace("a")) {
		t.Fatal("namespace a must be allowed")
	}
	if p.AllowsNamespace(namespace.Namespace("b")) {
		t.Fatal("namespace b must be denied")
	}
	if !p.Allows("kafka.trigger") {
		t.Fatal("node type kafka.trigger must be allowed")
	}
	if p.Allows("http.request") {
		t.Fatal("node type http.request must be denied")
	}
}

func TestEnrollAuditRecordsBothOutcomes(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryRegistrationCodeStore()
	at := time.Unix(1700000000, 0).UTC()
	for _, rec := range []EnrollAuditRecord{
		{CodeID: "code-1", Success: false, Reason: "unknown code", SourceIP: "10.0.0.1", At: at},
		{CodeID: "code-1", Success: true, RunnerID: "runner-x", SourceIP: "10.0.0.2", At: at},
	} {
		if err := st.AppendEnrollAudit(ctx, rec); err != nil {
			t.Fatalf("AppendEnrollAudit: %v", err)
		}
	}
	got, err := st.EnrollAudit(ctx, "code-1")
	if err != nil {
		t.Fatalf("EnrollAudit: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("audit len = %d, want 2 (failures must be recorded too)", len(got))
	}
	if got[0].Success || !got[1].Success {
		t.Fatalf("audit order/outcome wrong: %+v", got)
	}
	other, err := st.EnrollAudit(ctx, "code-2")
	if err != nil {
		t.Fatalf("EnrollAudit(other): %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("audit for unrelated code returned %d rows, want 0", len(other))
	}
}
