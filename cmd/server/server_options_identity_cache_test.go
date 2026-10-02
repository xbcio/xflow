package main

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

// TestParseServerConfigEnablesTheIdentityCacheByDefault pins the default in the
// direction it was deliberately chosen: ON. The read this cache removes sits on
// the hottest path a runner has and costs 80-2500ms against a remote store, and
// the staleness it introduces is bounded by the TTL and documented. A regression
// to "off unless asked" would be invisible — every request still succeeds, just
// hundreds of milliseconds slower — so it is pinned here rather than left to the
// flag's zero value.
func TestParseServerConfigEnablesTheIdentityCacheByDefault(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-enroll"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if cfg.runnerIdentityCacheTTL != store.DefaultIssuedIdentityCacheTTL {
		t.Fatalf("runnerIdentityCacheTTL = %v when the flag is absent, want %v: the cache is "+
			"on by default", cfg.runnerIdentityCacheTTL, store.DefaultIssuedIdentityCacheTTL)
	}
}

func TestParseServerConfigIdentityCacheFlagOverridesTheDefault(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-enroll", "-runner-identity-cache-ttl", "90s"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if cfg.runnerIdentityCacheTTL != 90*time.Second {
		t.Fatalf("runnerIdentityCacheTTL = %v, want 90s", cfg.runnerIdentityCacheTTL)
	}
}

// TestParseServerConfigIdentityCacheZeroDisables is the incident-response path:
// turning the cache off has to be reachable from the command line alone, without
// a code change or a rollout, and 0 is the documented way to say so.
func TestParseServerConfigIdentityCacheZeroDisables(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-enroll", "-runner-identity-cache-ttl", "0"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if cfg.runnerIdentityCacheTTL != 0 {
		t.Fatalf("runnerIdentityCacheTTL = %v, want 0", cfg.runnerIdentityCacheTTL)
	}
	inner := control.NewMemoryIssuedIdentityStore()
	if wrapped := store.NewCachedIssuedIdentityStore(inner, cfg.runnerIdentityCacheTTL, nil); wrapped != control.IssuedIdentityStore(inner) {
		t.Fatalf("ttl=0 produced %T, want the inner store unchanged: the disable path must "+
			"not merely configure a cache that is still there", wrapped)
	}
}

func TestParseServerConfigRejectsNegativeIdentityCacheTTL(t *testing.T) {
	// 0 already means "off"; a negative value is an operator who meant something
	// else, so it fails where they are still looking at their own command line
	// instead of silently behaving like the disable they did not ask for.
	if _, err := parseServerConfig([]string{"-memory", "-enroll", "-runner-identity-cache-ttl", "-1s"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for negative --runner-identity-cache-ttl")
	}
}
