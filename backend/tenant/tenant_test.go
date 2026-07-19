package tenant

import (
	"context"
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
