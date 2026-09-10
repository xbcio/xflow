package apiserver

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
)

// TestListRegistrationCodesEmitsEmptyArraysNotNull guards the wire encoding of
// a code whose scope lists are nil.
//
// The create handler persists req.AllowedNodeTypes verbatim and that field is
// optional, so a nil slice reaches the store on an ordinary request — and
// RegistrationCode.Clone() preserves nil rather than widening it (correctly:
// the store must not invent a distinction the caller did not make). Marshaled
// straight, a nil []string becomes JSON `null`, which the published schema's
// `type: array` rejects. The server was emitting a body its own contract
// forbids.
//
// This asserts the RAW body rather than decoding into []string, because
// decoding is exactly what hides the bug: `null` and `[]` both unmarshal into
// a nil-vs-empty distinction Go treats as equal under most comparisons. The
// generated clients that consume this API do not have that luxury.
//
// It is also the handler-side half of the guard. api/openapi's contract test
// covers newRegistrationCodeView's output, but nothing there proves the
// handler CALLS it — a future edit rebuilding the view by struct literal in
// the loop would leave the contract test green and reintroduce the null.
func TestListRegistrationCodesEmitsEmptyArraysNotNull(t *testing.T) {
	h := newRegistrationCodeTestServer(t)
	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	// Both lists nil, which is what the store holds for a code minted without
	// them. OwnerNamespace matches the test principal so list can see it.
	if err := h.codes.Create(t.Context(), control.RegistrationCode{
		ID: id, CodeHash: control.HashSecret(plaintext),
		OwnerNamespace: "namespaceA",
		CreatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	raw := h.doJSON(t, http.MethodGet, "/v1/management/registration-codes", "", http.StatusOK)
	for _, field := range []string{"allowed_namespaces", "allowed_node_types"} {
		if strings.Contains(raw, `"`+field+`":null`) {
			t.Fatalf("%s serialized as null; the schema declares an array: %s", field, raw)
		}
		if !strings.Contains(raw, `"`+field+`":[]`) {
			t.Fatalf("%s is not the empty array the projection must emit: %s", field, raw)
		}
	}
}

// TestNewRegistrationCodeViewNormalizesScopeLists pins the projection itself
// at the two boundaries the handler test above cannot isolate: that a nil list
// becomes non-nil, and that a NON-nil list is passed through untouched rather
// than rebuilt (which would drop or reorder entries a caller asked for).
func TestNewRegistrationCodeViewNormalizesScopeLists(t *testing.T) {
	created := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	got := newRegistrationCodeView(control.RegistrationCode{
		ID: "rc-1", CreatedAt: created,
	})
	if got.AllowedNamespaces == nil || got.AllowedNodeTypes == nil {
		t.Fatalf("nil scope list survived the projection: %+v", got)
	}
	if len(got.AllowedNamespaces) != 0 || len(got.AllowedNodeTypes) != 0 {
		t.Fatalf("projection invented entries: %+v", got)
	}
	// An empty list is NOT "no restriction" — RunnerPolicy.Allows reports
	// false for every node type against it. The projection must not paper
	// over that by substituting a wildcard, which would advertise a grant the
	// code does not hold.
	for _, e := range got.AllowedNodeTypes {
		if e == "*" {
			t.Fatal("projection substituted a wildcard for an empty node-type list")
		}
	}
	if got.ExpiresAt != "" {
		t.Fatalf("ExpiresAt = %q, want absent for a code that never expires", got.ExpiresAt)
	}

	full := newRegistrationCodeView(control.RegistrationCode{
		ID:                "rc-2",
		AllowedNamespaces: []string{"team-a", "team-b"},
		AllowedNodeTypes:  []string{"*"},
		CreatedAt:         created,
		ExpiresAt:         created.Add(time.Hour),
	})
	if strings.Join(full.AllowedNamespaces, ",") != "team-a,team-b" {
		t.Fatalf("AllowedNamespaces = %v, want the stored list verbatim", full.AllowedNamespaces)
	}
	if full.ExpiresAt != created.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("ExpiresAt = %q, want RFC3339 of the stored deadline", full.ExpiresAt)
	}

	// The projection must still carry neither the plaintext nor the hash. The
	// marshaled body is the check, not the struct: a field added without a
	// json tag would be invisible to a field-by-field assertion but very
	// visible on the wire.
	body, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, forbidden := range []string{"hash", "code_hash", `"code"`} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("projection leaked %s: %s", forbidden, body)
		}
	}
}
