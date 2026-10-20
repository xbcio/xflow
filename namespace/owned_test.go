package namespace

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// Mutation note: deleting the `seen` dedup branch in NormalizeOwned must turn
// TestNormalizeOwnedDedupesAndSorts red (duplicate surviving in output).
func TestNormalizeOwnedDedupesAndSorts(t *testing.T) {
	got, err := NormalizeOwned([]Namespace{"zeta", "", "alpha", "zeta", "mike"})
	if err != nil {
		t.Fatalf("NormalizeOwned = %v, want nil", err)
	}
	want := []Namespace{"alpha", "mike", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeOwned = %v, want %v", got, want)
	}
}

// Mutation note: dropping the empty-input nil collapse (returning an empty
// slice instead) must turn TestNormalizeOwnedEmptyIsLegacyNil red.
func TestNormalizeOwnedEmptyIsLegacyNil(t *testing.T) {
	for _, in := range [][]Namespace{
		nil,
		{},
		{""},
		{"", ""},
	} {
		got, err := NormalizeOwned(in)
		if err != nil {
			t.Fatalf("NormalizeOwned(%q) = %v, want nil", in, err)
		}
		if got != nil {
			t.Fatalf("NormalizeOwned(%q) = %v, want nil (legacy mode)", in, got)
		}
	}
}

// Mutation note: removing the Validate call in NormalizeOwned must turn both
// error tests red (invalid names passing through into key-embedding positions).
func TestNormalizeOwnedRejectsForbiddenCharacters(t *testing.T) {
	_, err := NormalizeOwned([]Namespace{"ns:with-colon"})
	if !errors.Is(err, ErrNameChars) {
		t.Fatalf("NormalizeOwned = %v, want ErrNameChars", err)
	}
}

func TestNormalizeOwnedRejectsTooLong(t *testing.T) {
	long := Namespace(strings.Repeat("x", MaxNameLen+1))
	_, err := NormalizeOwned([]Namespace{long})
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("NormalizeOwned = %v, want ErrNameTooLong", err)
	}
}

// Mutation note: flipping the `contains Default -> ""` branch to return the
// non-default element must turn TestDeploymentSuffixDefaultDeploymentKeepsLegacy
// red — and with it the rolling-upgrade invariant it pins.
func TestDeploymentSuffixDefaultDeploymentKeepsLegacy(t *testing.T) {
	for _, owned := range [][]Namespace{
		nil,
		{},
		{Default},
		{Default, "x"},
		{"x", Default},
	} {
		got, err := DeploymentSuffix(owned)
		if err != nil {
			t.Fatalf("DeploymentSuffix(%v) = %v, want nil", owned, err)
		}
		if got != "" {
			t.Fatalf("DeploymentSuffix(%v) = %q, want \"\" (legacy names)", owned, got)
		}
	}
}

func TestDeploymentSuffixSingleDedicatedNamespace(t *testing.T) {
	got, err := DeploymentSuffix([]Namespace{"local-jack"})
	if err != nil {
		t.Fatalf("DeploymentSuffix = %v, want nil", err)
	}
	if got != "local-jack" {
		t.Fatalf("DeploymentSuffix = %q, want local-jack", got)
	}
}

// Mutation note: relaxing the >=2 non-default rejection (e.g. joining with a
// separator) must turn this red — the ambiguity is exactly why the set is
// rejected at construction time.
func TestDeploymentSuffixRejectsAmbiguousMultiNamespaceSet(t *testing.T) {
	_, err := DeploymentSuffix([]Namespace{"alpha", "bravo"})
	if err == nil {
		t.Fatal("DeploymentSuffix({alpha,bravo}) = nil error, want ambiguity error")
	}
}

// Defensive re-validation: DeploymentSuffix may be handed a set that skipped
// NormalizeOwned. The suffix is embedded into Redis keys, so an unvalidated
// element must fail closed here too.
func TestDeploymentSuffixRevalidatesElements(t *testing.T) {
	_, err := DeploymentSuffix([]Namespace{"bad:ns"})
	if !errors.Is(err, ErrNameChars) {
		t.Fatalf("DeploymentSuffix = %v, want ErrNameChars", err)
	}
}
