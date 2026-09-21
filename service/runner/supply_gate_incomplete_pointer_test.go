package runner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// This file pins the rule "an incomplete artifact set must not start any node".
//
// It exists because that rule did NOT hold before AdmitWithDeclarations. Activate
// evaluates the gate BEFORE registerSupplyConsumers, so on the first activation a
// pointer supply has no consumers yet — and isReadyLocked is a conjunction over
// the consumer set, which makes zero consumers mean ready. A pointer publishing
// an artifact for one declared wasm node and nothing usable for another was
// therefore ADMITTED, the Kafka subscription started, and offsets advanced for a
// workflow that could not run. The declared half of the work was only fixed per
// message, which is too late: once a subscription is live, a message whose
// artifact is missing has nowhere safe to go.
//
// measuredMatrix below is the readiness side of the same question.

// TestIncompletePointerDeclinesActivation is the regression test for the defect
// above. It installs a REAL SupplyGate, as production does, and requires the
// activation to be DECLINED — not merely to fail later. "Admitted" is the
// failure: reaching the trigger handler is what starts the subscription.
//
// Two shapes are covered because they are the two ways a pointer is incomplete
// in practice: half-published (one node done, the other not) and declared-but-
// unpublished (the pointer exists so the workflow may name it, and no artifact
// has been uploaded yet). The second is the state a control plane that creates
// the pointer at registration time leaves behind, and it must hold the workflow
// at the gate until the artifacts arrive rather than start it with nothing.
func TestIncompletePointerDeclinesActivation(t *testing.T) {
	cases := []struct {
		name    string
		content func(valid string) string
	}{
		{"half-published", func(v string) string {
			return fmt.Sprintf(`{"decode":{"digest":%q,"version":"v1"},"clean":{"digest":"","version":""}}`, v)
		}},
		{"declared-but-unpublished", func(string) string {
			return `{"decode":{"digest":""},"clean":{"digest":""}}`
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newWasmActivationFixture(t)
			const res = "incomplete-pointer-resource"
			ptr := f.supplyName + "-ptr"
			content := tc.content(f.digest)

			fetcher := &stubFetcher{content: map[string][]byte{res: []byte(content)}}
			gate := NewSupplyGate(fetcher, supply.Default, quietLogger())

			fh := &fakeTriggerHandler{}
			h := NewTriggerActivationHandler("https://control.internal", "",
				fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
				WithSupplyGate(gate),
				WithArtifactCodeResolver(f.resolver))

			decls := make([]engine.SupplyConsumerBinding, 0, 2)
			for _, nodeName := range []string{"decode", "clean"} {
				decls = append(decls, engine.SupplyConsumerBinding{
					SupplyNode:   ptr,
					WorkflowName: f.supplyName + "-collect",
					NodeName:     nodeName,
					DigestExpr:   fmt.Sprintf(`${{ $supplies[%q].%s.digest }}`, ptr, nodeName),
				})
			}
			t.Cleanup(func() {
				_ = h.Deactivate(protocol.DeactivateDirective{WorkflowID: "wf-incomplete", EntryUnitID: "trig"})
			})

			directive := func(gen uint64) protocol.ActivateDirective {
				return protocol.ActivateDirective{
					WorkflowID:      "wf-incomplete",
					EntryUnitID:     "trig",
					NodeType:        "fake",
					Generation:      gen,
					Supplies:        []engine.SupplyRequirement{{Node: ptr, Resource: res, RequireReady: true}},
					SupplyConsumers: decls,
				}
			}

			// The first call is the one that regressed: it is where the consumers
			// do not exist yet. The second is level-triggered control — the
			// activation must stay declined while the pointer stays incomplete,
			// not be admitted on a retry.
			for _, gen := range []uint64{1, 2} {
				err := h.Activate(context.Background(), directive(gen))
				if fh.gotInput != nil {
					t.Fatalf("generation %d: activation was ADMITTED although the pointer "+
						"publishes no usable artifact for every declared wasm node; the Kafka "+
						"subscription is now live and committing offsets for a workflow that "+
						"cannot run (gate err=%v)", gen, err)
				}
				var notReady *NotReadyError
				if !errors.As(err, &notReady) {
					t.Fatalf("generation %d: Activate err = %v, want *NotReadyError naming the "+
						"incomplete pointer supply", gen, err)
				}
			}
		})
	}
}

// TestIncompletePointerReadinessMatrix measures the per-shape verdict for a
// pointer that declares two wasm nodes and resolves only one.
//
// The cases fall into two groups and the second group is a KNOWN GAP, recorded
// here rather than hidden:
//
//   - A declared node whose digest renders to an unusable value (empty, or a
//     sub-object with no digest key) is NOT ready. That is the shape a
//     half-written pointer has, and the gate now declines on it.
//
//   - A declared node whose key is absent from the content ENTIRELY reads ready.
//     The digest expression cannot be rendered at all, and both this gate and
//     wasmWarmupConsumer deliberately skip unrenderable expressions, because an
//     artifact_digest may legitimately reference $input, $env or $credentials —
//     roots that do not exist in either environment — and rejecting those would
//     leave a correctly-configured node permanently not-ready. Separating
//     "cannot render because the key is missing" from "cannot render because it
//     uses another root" needs expression-root introspection that exprx does not
//     expose. This is unreachable through the control plane's own write path,
//     where validateArtifactPointers requires every declared node's key to be
//     present; it is reachable via a hand-written supply row.
func TestIncompletePointerReadinessMatrix(t *testing.T) {
	cases := []struct {
		name    string
		content func(valid string) string
		want    bool
	}{
		{"declared-value-empty", func(v string) string {
			return fmt.Sprintf(`{"decode":{"digest":%q,"version":"v1"},"clean":{"digest":"","version":""}}`, v)
		}, false},
		{"declared-digest-key-absent", func(v string) string {
			return fmt.Sprintf(`{"decode":{"digest":%q,"version":"v1"},"clean":{}}`, v)
		}, false},
		{"both-empty", func(string) string {
			return `{"decode":{"digest":""},"clean":{"digest":""}}`
		}, false},
		{"declared-key-absent-entirely", func(v string) string {
			return fmt.Sprintf(`{"decode":{"digest":%q,"version":"v1"}}`, v)
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newWasmActivationFixture(t)
			ptr := f.supplyName + "-ptr"

			if err := supply.Default.Apply(context.Background(), supply.Snapshot{
				Name:      ptr,
				Content:   []byte(tc.content(f.digest)),
				Hash:      "matrix-" + tc.name,
				Revision:  1,
				FetchedAt: time.Now(),
			}); err != nil {
				t.Fatalf("apply pointer: %v", err)
			}

			fh := &fakeTriggerHandler{}
			h := NewTriggerActivationHandler("https://control.internal", "",
				fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
				WithArtifactCodeResolver(f.resolver))

			decls := make([]engine.SupplyConsumerBinding, 0, 2)
			for _, nodeName := range []string{"decode", "clean"} {
				decls = append(decls, engine.SupplyConsumerBinding{
					SupplyNode:   ptr,
					WorkflowName: f.supplyName + "-collect",
					NodeName:     nodeName,
					DigestExpr:   fmt.Sprintf(`${{ $supplies[%q].%s.digest }}`, ptr, nodeName),
				})
			}
			t.Cleanup(func() {
				_ = h.Deactivate(protocol.DeactivateDirective{
					WorkflowID: "wf-matrix", EntryUnitID: "trig",
				})
			})

			_ = h.Activate(context.Background(), protocol.ActivateDirective{
				WorkflowID:      "wf-matrix",
				EntryUnitID:     "trig",
				NodeType:        "fake",
				Generation:      1,
				Supplies:        []engine.SupplyRequirement{{Node: ptr}},
				SupplyConsumers: decls,
			})

			if got := supply.Default.IsReady(ptr); got != tc.want {
				t.Errorf("IsReady = %v, want %v for pointer content %s", got, tc.want, tc.content(f.digest))
			}
		})
	}
}
