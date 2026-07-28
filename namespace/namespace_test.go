package namespace

import (
	"context"
	"strings"
	"testing"
)

func TestWithNamespaceRoundTrip(t *testing.T) {
	ctx := WithNamespace(context.Background(), "namespace-a")
	if got := FromContext(ctx); got != "namespace-a" {
		t.Fatalf("expected namespace-a, got %q", got)
	}
}

func TestFromContextDefault(t *testing.T) {
	got := FromContext(context.Background())
	if got != Default {
		t.Fatalf("expected Default %q, got %q", Default, got)
	}
}

func TestFromContextEmptyFallsBackToDefault(t *testing.T) {
	ctx := WithNamespace(context.Background(), "")
	if got := FromContext(ctx); got != Default {
		t.Fatalf("expected Default for empty namespace, got %q", got)
	}
}

func TestContextIsolation(t *testing.T) {
	parent := WithNamespace(context.Background(), "parent")
	child := WithNamespace(parent, "child")
	sibling := WithNamespace(parent, "sibling")

	if got := FromContext(parent); got != "parent" {
		t.Fatalf("parent namespace changed: %q", got)
	}
	if got := FromContext(child); got != "child" {
		t.Fatalf("child namespace: %q", got)
	}
	if got := FromContext(sibling); got != "sibling" {
		t.Fatalf("sibling namespace: %q", got)
	}
}

func TestValidateAcceptsValidNames(t *testing.T) {
	for _, name := range []Namespace{
		"a",
		"acme",
		"namespace-1",
		"under_score",
		"dots.ok",
		Namespace(strings.Repeat("x", MaxNameLen)),
	} {
		if err := Validate(name); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", name, err)
		}
	}
}

func TestValidateRejectsEmpty(t *testing.T) {
	if err := Validate(""); err != ErrNameEmpty {
		t.Fatalf("Validate(\"\") = %v, want ErrNameEmpty", err)
	}
}

func TestValidateRejectsForbiddenCharacters(t *testing.T) {
	for _, name := range []Namespace{
		"namespace:with-colon",
		"namespace{with-brace",
		"namespace}with-brace",
		"namespace:with{both}",
		":start",
		"end:",
		"{}",
	} {
		if err := Validate(name); err != ErrNameChars {
			t.Errorf("Validate(%q) = %v, want ErrNameChars", name, err)
		}
	}
}

func TestValidateRejectsGlobMetaCharacters(t *testing.T) {
	for _, name := range []Namespace{
		"ns*wildcard",
		"*",
		"ns?single",
		"ns[range]",
		"ns[a",
		"ns]b",
		`ns\escaped`,
		`\`,
	} {
		if err := Validate(name); err != ErrNameChars {
			t.Errorf("Validate(%q) = %v, want ErrNameChars", name, err)
		}
	}
}

func TestValidateRejectsTooLong(t *testing.T) {
	long := Namespace(strings.Repeat("x", MaxNameLen+1))
	if err := Validate(long); err != ErrNameTooLong {
		t.Fatalf("Validate(%d chars) = %v, want ErrNameTooLong", len(long), err)
	}
}
