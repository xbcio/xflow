package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
)

// The production posture itself is enforced and tested in service/apiserver
// (production.go / production_test.go), the one layer this binary and every
// SDK embedder both pass through. What remains this binary's job, and what is
// tested here, is the two-way translation across that boundary: deriving the
// declaration from how the flags built things, and turning the requirements
// the gate reports back into flag names.

// TestProductionDeclarationDerivesEachFact pins the derivation. A field
// hard-coded to true would disable the corresponding check with nothing else
// in the system to notice.
func TestProductionDeclarationDerivesEachFact(t *testing.T) {
	multiToken := apiserver.NewBearerPrincipalAuthMulti(nil)
	single := apiserver.NewBearerPrincipalAuth("tok", "op", []string{"workflow"})

	t.Run("all present", func(t *testing.T) {
		got := productionDeclaration(multiToken, false, true, true)
		if !got.DurableAudit || !got.MultiTokenPrincipalAuth || !got.SupplyEncryptionAtRest {
			t.Fatalf("fully-configured server declared %+v, want all true", got)
		}
	})

	// --mysql-dsn unset: the audit sink is in-memory and forgets every
	// mutation on restart. Declaring it durable would defeat the check.
	t.Run("no durable audit", func(t *testing.T) {
		if got := productionDeclaration(multiToken, false, false, true); got.DurableAudit {
			t.Fatal("declared DurableAudit with no durable store")
		}
	})

	// --api-auth-token: one shared token that self-grants every scope.
	t.Run("single shared token", func(t *testing.T) {
		if got := productionDeclaration(single, true, true, true); got.MultiTokenPrincipalAuth {
			t.Fatal("declared MultiTokenPrincipalAuth for the single-token path")
		}
	})

	// No principal authenticator at all: there is nothing to call multi-token.
	t.Run("no principal auth", func(t *testing.T) {
		if got := productionDeclaration(nil, false, true, true); got.MultiTokenPrincipalAuth {
			t.Fatal("declared MultiTokenPrincipalAuth with no authenticator")
		}
	})

	// No KEK: supply content lands in the store as plaintext, and supply
	// content carries credentials. A silent downgrade would look identical to
	// a working server from the outside — the same principle as on_invalid:
	// a config that asked for encryption is never quietly given none.
	t.Run("no master key", func(t *testing.T) {
		if got := productionDeclaration(multiToken, false, true, false); got.SupplyEncryptionAtRest {
			t.Fatal("declared SupplyEncryptionAtRest with no master key loaded")
		}
	})
}

// TestProductionFlagHintsAreExhaustive fails when a requirement is added to
// apiserver without a flag hint here. Without it the omission surfaces as a
// blank "set:" line at start-up, in front of the operator least able to guess.
func TestProductionFlagHintsAreExhaustive(t *testing.T) {
	all := apiserver.AllProductionRequirements()
	if len(all) == 0 {
		t.Fatal("apiserver.AllProductionRequirements is empty")
	}
	for _, r := range all {
		if strings.TrimSpace(productionFlagHint[r]) == "" {
			t.Errorf("requirement %q has no flag hint in productionFlagHint", r)
		}
	}
	if len(productionFlagHint) != len(all) {
		t.Errorf("productionFlagHint has %d entries, apiserver reports %d requirements",
			len(productionFlagHint), len(all))
	}
}

// TestExplainProductionGateNamesFlags proves the operator gets a flag, not
// just a requirement slug, for every unmet requirement — and gets all of them
// in one message rather than one per restart.
func TestExplainProductionGateNamesFlags(t *testing.T) {
	gate := &apiserver.ProductionGateError{Unmet: apiserver.AllProductionRequirements()}

	msg := explainProductionGate(gate).Error()
	for _, r := range apiserver.AllProductionRequirements() {
		if !strings.Contains(msg, string(r)) {
			t.Errorf("message does not name requirement %q:\n%s", r, msg)
		}
		if !strings.Contains(msg, productionFlagHint[r]) {
			t.Errorf("message does not carry the flag hint for %q:\n%s", r, msg)
		}
	}
	// The escape hatch must be discoverable from the failure itself.
	if !strings.Contains(msg, "--mode=dev") {
		t.Errorf("message does not mention --mode=dev:\n%s", msg)
	}
}

// Anything that is not a posture failure must reach the operator unchanged —
// a translator that swallowed, say, a Redis dial error would turn every
// start-up failure into a misleading configuration complaint.
func TestExplainProductionGatePassesOtherErrorsThrough(t *testing.T) {
	orig := errors.New("dial redis: connection refused")
	if got := explainProductionGate(orig); got != orig {
		t.Fatalf("explainProductionGate rewrote an unrelated error: %v", got)
	}
	if explainProductionGate(nil) != nil {
		t.Fatal("explainProductionGate(nil) is not nil")
	}
}

// The translated message references flag names only. A bearer token reaching
// it would be written to stderr and to whatever collects this process's logs.
func TestExplainProductionGateDoesNotLeakTokens(t *testing.T) {
	gate := &apiserver.ProductionGateError{Unmet: apiserver.AllProductionRequirements()}
	msg := explainProductionGate(gate).Error()

	for _, secret := range []string{"secret-tok-xyz", "Bearer "} {
		if strings.Contains(msg, secret) {
			t.Fatalf("message contains %q:\n%s", secret, msg)
		}
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
