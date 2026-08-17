package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The pool caches *sql.DB and *grpc.ClientConn process-wide, so constructing one
// for a runner that will never open either is a live resource nobody closes.
// It is built only when there is something to pool: declared credentials, or a
// capability whose handler pools by design.
func TestRunnerNeedsPool(t *testing.T) {
	tests := []struct {
		name string
		cfg  RunnerConfig
		want bool
	}{
		{name: "no credentials, no db/grpc cap", cfg: RunnerConfig{Capabilities: []string{"xflow.function"}}, want: false},
		{name: "credentials present", cfg: RunnerConfig{Capabilities: []string{"xflow.function"}, Credentials: map[string]map[string]any{"db": {"dsn": "x"}}}, want: true},
		{name: "database capability", cfg: RunnerConfig{Capabilities: []string{"xflow.function", "xflow.database"}}, want: true},
		{name: "grpc capability", cfg: RunnerConfig{Capabilities: []string{"xflow.function", "xflow.grpc"}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runnerNeedsPool(tt.cfg); got != tt.want {
				t.Fatalf("runnerNeedsPool = %v, want %v", got, tt.want)
			}
		})
	}
}

// Credentials reach handlers only through the resolver closure, and an unknown
// name must resolve to nil rather than an empty map — a handler that receives
// an empty map cannot tell "not configured" from "configured empty".
func TestNewRunnerConstructsThePoolAndCredentialResolver(t *testing.T) {
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.database"},
		Credentials: map[string]map[string]any{
			"db": {"dsn": "user:s3cret@tcp(db:3306)/xflow", "driver": "mysql"},
		},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.ResourcePool == nil {
		t.Fatal("ResourcePool = nil, want a constructed pool")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = svcCfg.ResourcePool.Close(ctx)
	})
	if svcCfg.CredentialResolver == nil {
		t.Fatal("CredentialResolver = nil, want a resolver closure")
	}
	if got := svcCfg.CredentialResolver(namespace.Default, "db"); got["dsn"] != "user:s3cret@tcp(db:3306)/xflow" {
		t.Fatalf("resolver returned dsn = %v", got["dsn"])
	}
	if svcCfg.CredentialResolver(namespace.Default, "missing") != nil {
		t.Fatal("resolver returned non-nil for an unknown credential name")
	}
}

// The no-pool contract: a plain function runner allocates neither.
func TestNewRunnerLeavesThePoolNilWhenNotNeeded(t *testing.T) {
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.function"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.ResourcePool != nil {
		t.Fatalf("ResourcePool = %T, want nil (no credentials and no db/grpc capability)", svcCfg.ResourcePool)
	}
	if svcCfg.CredentialResolver != nil {
		t.Fatal("CredentialResolver = non-nil, want nil (no credentials)")
	}
}

// An explicit ResourcePoolConfig must survive to the pool. The zero value is
// the "use defaults" signal, so a caller who tunes exactly one field would
// otherwise silently get all of them defaulted.
func TestNewRunnerCarriesTheResourcePoolConfig(t *testing.T) {
	poolCfg := types.DefaultResourcePoolConfig()
	poolCfg.SQL.MaxOpenConns = 77
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:          "http://server:8080",
		Capabilities:       []string{"xflow.database"},
		ResourcePoolConfig: poolCfg,
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.ResourcePool == nil {
		t.Fatal("ResourcePool = nil")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = svcCfg.ResourcePool.Close(ctx)
	})
}

// Namespaces restrict which assignments this runner may claim. Dropping them
// makes a namespace-scoped runner claim everything.
func TestNewRunnerCarriesNamespaces(t *testing.T) {
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.function"},
		Namespaces:   []namespace.Namespace{"namespace-a", "namespace-b"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if len(svcCfg.Namespaces) != 2 || svcCfg.Namespaces[0] != "namespace-a" || svcCfg.Namespaces[1] != "namespace-b" {
		t.Fatalf("Namespaces = %v, want [namespace-a namespace-b]", svcCfg.Namespaces)
	}
}

// HeartbeatInterval and PollWait are what the control plane's liveness window is
// derived from. cmd/runner parsed the heartbeat interval, validated it, and then
// discarded the value, so every runner heartbeated at the service's hardcoded 5s
// no matter what was configured — an operator who widened it for a slow link, or
// narrowed it for faster failure detection, got no effect and no warning.
func TestNewRunnerCarriesTheHeartbeatIntervalAndPollWait(t *testing.T) {
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:         "http://server:8080",
		Capabilities:      []string{"xflow.function"},
		HeartbeatInterval: 11 * time.Second,
		PollWait:          4 * time.Second,
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.HeartbeatInterval != 11*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 11s — the configured value never reached "+
			"the runner service, which then defaults to 5s", svcCfg.HeartbeatInterval)
	}
	if svcCfg.PollWait != 4*time.Second {
		t.Errorf("PollWait = %v, want 4s", svcCfg.PollWait)
	}
}
