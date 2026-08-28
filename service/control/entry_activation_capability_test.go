package control

import (
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// TestDeclarationUnitRequiresFeature: an entry unit whose bindings are
// declarations must demand the feature, so the reconciler never places it on a
// runner that would receive a shape it does not understand. An old runner that
// silently registers nothing is §9.3 failure (3) -- traffic hosted with no rules
// and no diagnostic, which for SAS means credentials reaching the sink in the
// clear.
func TestDeclarationUnitRequiresFeature(t *testing.T) {
	g := mustCompile(t, wasmConsumerDef())
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(units) == 0 {
		t.Fatal("no entry units")
	}
	var found bool
	for _, r := range units[0].Requirements {
		if r.Feature == engine.FeatureWasmSupplyDeclarationV1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("requirements %+v do not demand %q",
			units[0].Requirements, engine.FeatureWasmSupplyDeclarationV1)
	}
}

// TestOldRunnerCannotHostDeclarationUnit is the other half: the matcher must
// actually reject. Asserting only that the requirement is emitted would pass
// even if capabilitiesSatisfy ignored it.
func TestOldRunnerCannotHostDeclarationUnit(t *testing.T) {
	act := &engine.EntryActivation{
		NodeType: "xflow.trigger.kafka",
		Requirements: []engine.CapabilityRequirement{{
			NodeType: "xflow.trigger.kafka",
			Feature:  engine.FeatureWasmSupplyDeclarationV1,
		}},
	}
	old := RunnerSnapshot{
		RunnerID: "old",
		Capabilities: []protocol.Capability{{
			NodeType: "xflow.trigger.kafka",
			Features: []string{engine.FeatureEntryActivationReplicaV1},
		}},
	}
	if capabilitiesSatisfy(act, old) {
		t.Fatal("a runner without the declaration feature was accepted; it would " +
			"register no consumer and host traffic with no rules")
	}
	upgraded := RunnerSnapshot{
		RunnerID: "new",
		Capabilities: []protocol.Capability{{
			NodeType: "xflow.trigger.kafka",
			Features: []string{
				engine.FeatureEntryActivationReplicaV1,
				engine.FeatureWasmSupplyDeclarationV1,
			},
		}},
	}
	if !capabilitiesSatisfy(act, upgraded) {
		t.Fatal("an upgraded runner was rejected; the gate is too tight and nothing " +
			"would ever be scheduled")
	}
}
