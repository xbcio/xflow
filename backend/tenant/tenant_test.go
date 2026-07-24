package tenant

import (
	"context"
	"strings"
	"testing"
)

func TestWithTenantRoundTrip(t *testing.T) {
	ctx := WithTenant(context.Background(), "tenant-a")
	if got := FromContext(ctx); got != "tenant-a" {
		t.Fatalf("expected tenant-a, got %q", got)
	}
}

func TestFromContextDefault(t *testing.T) {
	got := FromContext(context.Background())
	if got != DefaultTenant {
		t.Fatalf("expected DefaultTenant %q, got %q", DefaultTenant, got)
	}
}

func TestFromContextEmptyFallsBackToDefault(t *testing.T) {
	// An explicitly injected empty tenant is treated as unset and falls back
	// to DefaultTenant, matching the single-tenant default semantics.
	ctx := WithTenant(context.Background(), "")
	if got := FromContext(ctx); got != DefaultTenant {
		t.Fatalf("expected DefaultTenant for empty tenant, got %q", got)
	}
}

func TestContextIsolation(t *testing.T) {
	parent := WithTenant(context.Background(), "parent")
	child := WithTenant(parent, "child")
	sibling := WithTenant(parent, "sibling")

	if got := FromContext(parent); got != "parent" {
		t.Fatalf("parent tenant changed: %q", got)
	}
	if got := FromContext(child); got != "child" {
		t.Fatalf("child tenant: %q", got)
	}
	if got := FromContext(sibling); got != "sibling" {
		t.Fatalf("sibling tenant: %q", got)
	}
}

func TestValidateAcceptsValidNames(t *testing.T) {
	for _, name := range []TenantID{
		"a",
		"acme",
		"tenant-1",
		"under_score",
		"dots.ok",
		TenantID(strings.Repeat("x", MaxNameLen)),
	} {
		if err := Validate(name); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", name, err)
		}
	}
}

func TestValidateRejectsEmpty(t *testing.T) {
	if err := Validate(""); err != ErrTenantNameEmpty {
		t.Fatalf("Validate(\"\") = %v, want ErrTenantNameEmpty", err)
	}
}

func TestValidateRejectsForbiddenCharacters(t *testing.T) {
	for _, name := range []TenantID{
		"tenant:with-colon",
		"tenant{with-brace",
		"tenant}with-brace",
		"tenant:with{both}",
		":start",
		"end:",
		"{}",
	} {
		if err := Validate(name); err != ErrTenantNameChars {
			t.Errorf("Validate(%q) = %v, want ErrTenantNameChars", name, err)
		}
	}
}

func TestValidateRejectsTooLong(t *testing.T) {
	long := TenantID(strings.Repeat("x", MaxNameLen+1))
	if err := Validate(long); err != ErrTenantNameTooLong {
		t.Fatalf("Validate(%d chars) = %v, want ErrTenantNameTooLong", len(long), err)
	}
}
