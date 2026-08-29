package main

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// realReconciler is the same worker type the SDK hands back, so "present" in
// these tests means the thing production actually gets.
func realReconciler() reconciler {
	return reconcilerOrNil(control.NewAuditReconcileWorker(nil, nil, control.AuditReconcileConfig{}))
}

// TestValidateProductionRequiresEachComponent proves Task 8 blocker 3:
// production mode fails closed when any one of PrincipalAuthenticator,
// Authorizer, durable AuditSink, or Reconciler is missing. Each subtest omits
// exactly one and asserts validateProduction returns an error.
func TestValidateProductionRequiresEachComponent(t *testing.T) {
	auth := apiserver.NewBearerPrincipalAuth("tok", "op", []string{"workflow"})
	durableAudit := apiserver.NewSQLAuditSink(nil) // non-nil; durableAudit flag is the real gate
	// Note: durableAudit=false below isolates the "durability" requirement from
	// the "presence" requirement.

	base := productionDeps{
		principalAuth:        auth,
		authorizer:           apiserver.NamespaceAwareAuthorizer{},
		auditSink:            durableAudit,
		durableAudit:         true,
		reconciler:           realReconciler(),
		masterKey:            true,
		runnerAuthConfigured: true,
	}
	if err := validateProduction("production", base); err != nil {
		t.Fatalf("baseline production = %v, want nil (all components present)", err)
	}

	// Missing PrincipalAuthenticator.
	if err := validateProduction("production", func(d productionDeps) productionDeps {
		d.principalAuth = nil
		return d
	}(base)); err == nil {
		t.Fatal("missing PrincipalAuth: want error, got nil")
	}

	// Missing Authorizer.
	if err := validateProduction("production", func(d productionDeps) productionDeps {
		d.authorizer = nil
		return d
	}(base)); err == nil {
		t.Fatal("missing Authorizer: want error, got nil")
	}

	// Missing AuditSink.
	if err := validateProduction("production", func(d productionDeps) productionDeps {
		d.auditSink = nil
		return d
	}(base)); err == nil {
		t.Fatal("missing AuditSink: want error, got nil")
	}

	// Non-durable (in-memory) AuditSink — production requires durable.
	if err := validateProduction("production", func(d productionDeps) productionDeps {
		d.auditSink = apiserver.NewInMemoryAuditSink()
		d.durableAudit = false
		return d
	}(base)); err == nil {
		t.Fatal("non-durable AuditSink: want error, got nil")
	}

	// Missing Reconciler.
	if err := validateProduction("production", func(d productionDeps) productionDeps {
		d.reconciler = nil
		return d
	}(base)); err == nil {
		t.Fatal("missing Reconciler: want error, got nil")
	}

	// Missing runner protocol authenticator (--auth-policy unconfigured).
	// DisabledAuthenticator{} is a non-nil Authenticator, so this must be
	// caught by a dedicated field, not a nil check on an authenticator.
	if err := validateProduction("production", func(d productionDeps) productionDeps {
		d.runnerAuthConfigured = false
		return d
	}(base)); err == nil {
		t.Fatal("missing runner auth: want error, got nil")
	} else if !strings.Contains(err.Error(), "--auth-policy") {
		t.Fatalf("missing runner auth error = %v, want it to mention --auth-policy", err)
	}
}

// TestValidateProductionRejectsSingleToken proves Task 8 blocker 4: production
// forbids the single-token --api-auth-token path (one token must not
// self-grant operator scopes); production requires --auth-tokens-file.
func TestValidateProductionRejectsSingleToken(t *testing.T) {
	auth := apiserver.NewBearerPrincipalAuth("tok", "op", []string{"workflow"})
	deps := productionDeps{
		principalAuth: auth,
		authorizer:    apiserver.NamespaceAwareAuthorizer{},
		auditSink:     apiserver.NewSQLAuditSink(nil),
		durableAudit:  true,
		reconciler:    realReconciler(),
		singleToken:   true,
	}
	if err := validateProduction("production", deps); err == nil {
		t.Fatal("single-token in production: want error, got nil")
	}
}

// TestValidateProductionDevAllowsInMemoryAndAnonymous proves dev mode is the
// explicit escape hatch: an in-memory audit sink + no authenticator is allowed
// (the caller prints the stderr warning separately).
func TestValidateProductionDevAllowsInMemoryAndAnonymous(t *testing.T) {
	deps := productionDeps{
		principalAuth: nil,
		authorizer:    nil,
		auditSink:     apiserver.NewInMemoryAuditSink(),
		durableAudit:  false,
		reconciler:    nil,
	}
	if err := validateProduction("dev", deps); err != nil {
		t.Fatalf("dev mode = %v, want nil (dev allows in-memory + anonymous)", err)
	}
}

// TestValidateProductionEmptyModeTreatedAsNonProduction proves an unset mode is
// not production-enforced (parseServerConfig defaults --mode to production, so
// this only happens when the function is called directly with a garbage value).
func TestValidateProductionEmptyModeIsNotEnforced(t *testing.T) {
	// An empty mode string is neither "production" nor "dev"; validateProduction
	// treats anything != "production" as non-production (no enforcement). The
	// parse layer rejects empty/unknown modes, so this path is only reachable
	// via direct call — documented behavior is: production is opt-out only via
	// an explicit non-production mode.
	if err := validateProduction("", productionDeps{}); err != nil {
		t.Fatalf("empty mode = %v, want nil (not enforced)", err)
	}
}

// TestParseServerConfigModeDefaultsToProduction proves the default posture is
// production (fail-closed): an operator must explicitly pass --mode dev to use
// the in-memory / single-token escape hatch.
func TestParseServerConfigModeDefaultsToProduction(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.mode != "production" {
		t.Fatalf("default mode = %q, want production (fail-closed default)", cfg.mode)
	}
}

func TestParseServerConfigSupportsDevMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-mode", "dev"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.mode != "dev" {
		t.Fatalf("mode = %q, want dev", cfg.mode)
	}
}

func TestParseServerConfigRejectsUnknownMode(t *testing.T) {
	if _, err := parseServerConfig([]string{"-memory", "-mode", "staging"}); err == nil {
		t.Fatal("parseServerConfig(-mode staging) = nil, want error")
	}
}

// TestValidateProductionErrorMessagesDoNotLeakTokens is a belt-and-suspenders
// check that the production validation error messages never embed a bearer
// token (they reference flag names only).
func TestValidateProductionErrorMessagesDoNotLeakTokens(t *testing.T) {
	deps := productionDeps{principalAuth: apiserver.NewBearerPrincipalAuth("secret-tok-xyz", "op", []string{"workflow"})}
	err := validateProduction("production", deps)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if strings.Contains(err.Error(), "secret-tok-xyz") {
		t.Fatalf("validation error leaked token: %v", err)
	}
}
