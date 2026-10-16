package xflow

import (
	"strings"
	"testing"
)

// The namespace ends up in Redis keys, queue names and runner-directory
// prefixes. A value carrying the key schema's own delimiters ('{', '}':', glob
// meta-characters) must be refused at construction — minting keys from it would
// silently collide with the key layout long after the misconfiguration is out
// of sight, and the first symptom would be cross-deployment data access rather
// than an error.
//
// Mutation: drop validateNamespaceConfig from NewLocal/NewCluster — NewLocal
// accepts the malformed namespace and the test fails.
func TestNewLocalRefusesInvalidNamespace(t *testing.T) {
	eng, err := NewLocal(WithNamespace("bad:ns"))
	if err == nil {
		eng.Stop()
		t.Fatal("NewLocal accepted a namespace containing ':' — it would collapse Redis key segments")
	}
	if !strings.Contains(err.Error(), "invalid namespace") {
		t.Fatalf("error = %v, want it to name the invalid namespace", err)
	}
}

// TestNewLocalAcceptsValidNamespace guards the other direction: the validation
// must not refuse values the deployment actually uses (letters, digits, '-',
// '_').
func TestNewLocalAcceptsValidNamespace(t *testing.T) {
	eng, err := NewLocal(WithNamespace("tenant-b_2"))
	if err != nil {
		t.Fatalf("NewLocal(tenant-b_2): %v", err)
	}
	defer eng.Stop()
}
