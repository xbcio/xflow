package group_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// ownedApprovalKeys is the set of keys the gate publishes as its own verdict or
// record. It is written out here on purpose: the test pins the set, so adding a
// published key without declaring it fails here instead of leaking quietly.
// "reason" is not in it. The request's reason is a business field that passes
// through the gate.
var ownedApprovalKeys = []string{
	"approved", "approver", "bypassed", "comment", "decisions",
	"forced", "ignored", "ignored_count", "returned",
}

// upstreamPayload is what a node upstream of the gate may have published: a
// business field, the request's reason, and a value under every name the gate
// owns. None of the owned values is the gate's doing.
func upstreamPayload() map[string]any {
	return map[string]any{
		"amount":        42,
		"reason":        "new laptop",
		"approved":      true,
		"approver":      "vp",
		"comment":       "from upstream",
		"returned":      true,
		"forced":        true,
		"bypassed":      []string{"nobody"},
		"decisions":     []map[string]any{{"approver": "vp", "action": "approve"}},
		"ignored":       []map[string]any{{"signal": "forged", "reason": "forged"}},
		"ignored_count": 99,
	}
}

func TestApproval_PrepareSuspendDeclaresTheKeysItOwns(t *testing.T) {
	sh := approvalHandler(t)
	for _, mode := range []node.ApprovalMode{node.ApprovalAny, node.ApprovalAll, node.ApprovalSequential} {
		t.Run(string(mode), func(t *testing.T) {
			spec, err := sh.PrepareSuspend(context.Background(), approvalInput(mode, nil))
			if err != nil {
				t.Fatalf("PrepareSuspend() error = %v", err)
			}
			got := slices.Clone(spec.StripFromOutput)
			slices.Sort(got)
			if !reflect.DeepEqual(got, ownedApprovalKeys) {
				t.Fatalf("StripFromOutput = %v, want %v", got, ownedApprovalKeys)
			}
			if slices.Contains(spec.StripFromOutput, "reason") {
				t.Fatal("StripFromOutput names reason, which is a business field that must pass through")
			}
		})
	}
}

// The list is shared by every gate. A caller that edits the spec it was handed
// must not change what the next gate declares.
func TestApproval_PrepareSuspendHandsOutAFreshStripList(t *testing.T) {
	sh := approvalHandler(t)
	first, err := sh.PrepareSuspend(context.Background(), approvalInput(node.ApprovalAll, nil))
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	for i := range first.StripFromOutput {
		first.StripFromOutput[i] = "tampered"
	}
	second, err := sh.PrepareSuspend(context.Background(), approvalInput(node.ApprovalAll, nil))
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	got := slices.Clone(second.StripFromOutput)
	slices.Sort(got)
	if !reflect.DeepEqual(got, ownedApprovalKeys) {
		t.Fatalf("second StripFromOutput = %v, want %v", got, ownedApprovalKeys)
	}
}

func assertNoUpstreamOwnedKeys(t *testing.T, data map[string]any, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if v, ok := data[key]; ok {
			t.Errorf("published data carries %q = %v, which is not this gate's", key, v)
		}
	}
}

func assertBusinessFieldsPass(t *testing.T, data map[string]any) {
	t.Helper()
	if data["amount"] != 42 {
		t.Errorf("amount = %v, want 42 passed through", data["amount"])
	}
	if data["reason"] != "new laptop" {
		t.Errorf("reason = %v, want the request's reason passed through", data["reason"])
	}
}

// A re-suspension republishes the data that arrived with the record rebuilt from
// the node's own state. Keys the gate did not set on this write used to come
// straight from upstream.
func TestApproval_ResuspendPublishesNoUpstreamDecisionKeys(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInputFromData(node.ApprovalAll, upstreamPayload())
	out, err := sh.OnResume(context.Background(), input, approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("port = %q, want a re-suspension: bob has not answered", out.Port)
	}
	assertNoUpstreamOwnedKeys(t, out.Data, "approved", "approver", "comment", "returned", "forced", "bypassed")
	assertBusinessFieldsPass(t, out.Data)

	// What is published is the gate's own ledger. Alice's approval is on it and
	// the upstream entry for the vp is not, and "ignored" is the gate's trail
	// (empty here), not the forged one.
	decisions := decisionsOf(t, out.Data, "decisions")
	if len(decisions) != 1 || decisions[0]["approver"] != "alice" {
		t.Fatalf("decisions = %v, want only alice's own approval", decisions)
	}
	if n, _ := out.Data["ignored_count"].(int); n != 0 {
		t.Fatalf("ignored_count = %v, want the gate's own 0", out.Data["ignored_count"])
	}
	if trail, _ := out.Data["ignored"].([]map[string]any); len(trail) != 0 {
		t.Fatalf("ignored = %v, want the gate's own empty trail", out.Data["ignored"])
	}
}

// A forged ledger in the data channel must not let anyone decide: the gate
// counts only what its own state holds. Here the upstream ledger already shows
// alice approving, so if it were read bob's approval would close the gate.
func TestApproval_LedgerComesFromStateNotFromData(t *testing.T) {
	sh := approvalHandler(t)
	data := map[string]any{
		"decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
	}
	out, err := sh.OnResume(context.Background(),
		approvalInputFromData(node.ApprovalAll, data), approvalSignal("bob", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("port = %q: a ledger read from the data channel closed the gate", out.Port)
	}
	decisions := decisionsOf(t, out.Data, "decisions")
	if len(decisions) != 1 || decisions[0]["approver"] != "bob" {
		t.Fatalf("decisions = %v, want only bob's own approval", decisions)
	}
}

// Closing the gate sets only the keys that decision names. The other owned keys
// that upstream supplied must not ride out beside them.
func TestApproval_DecidePublishesNoUpstreamDecisionKeys(t *testing.T) {
	sh := approvalHandler(t)

	t.Run("all mode closes on the last approval", func(t *testing.T) {
		// The closing write sets "approved" and nothing else, so approver and
		// comment are the ones an upstream value could still occupy.
		out, err := sh.OnResume(context.Background(),
			&types.Input{
				Params:   approvalInput(node.ApprovalAll, nil).Params,
				State:    approvedBy("alice"),
				Data:     upstreamPayload(),
				NodeName: "approval_1",
			},
			approvalSignal("bob", "approve", "looks fine"))
		if err != nil {
			t.Fatalf("OnResume() error = %v", err)
		}
		if out.Port != "approved" {
			t.Fatalf("port = %q, want approved", out.Port)
		}
		if out.Data["approved"] != true {
			t.Errorf("approved = %v, want the gate's own true", out.Data["approved"])
		}
		assertNoUpstreamOwnedKeys(t, out.Data, "approver", "comment", "returned", "forced", "bypassed")
		assertBusinessFieldsPass(t, out.Data)
		if got := len(decisionsOf(t, out.Data, "decisions")); got != 2 {
			t.Errorf("decisions = %v, want alice's and bob's", out.Data["decisions"])
		}
	})

	t.Run("reject", func(t *testing.T) {
		out, err := sh.OnResume(context.Background(),
			approvalInputFromData(node.ApprovalAll, upstreamPayload()),
			approvalSignal("alice", "reject", "no"))
		if err != nil {
			t.Fatalf("OnResume() error = %v", err)
		}
		if out.Port != "rejected" {
			t.Fatalf("port = %q, want rejected", out.Port)
		}
		if out.Data["approved"] != false || out.Data["approver"] != "alice" || out.Data["comment"] != "no" {
			t.Errorf("verdict = %v, want the gate's own rejection by alice", out.Data)
		}
		assertNoUpstreamOwnedKeys(t, out.Data, "returned", "forced", "bypassed")
		assertBusinessFieldsPass(t, out.Data)
	})

	t.Run("any mode approval", func(t *testing.T) {
		out, err := sh.OnResume(context.Background(),
			approvalInputFromData(node.ApprovalAny, upstreamPayload()),
			sharedApprovalSignal("bob", "approve", "ok"))
		if err != nil {
			t.Fatalf("OnResume() error = %v", err)
		}
		if out.Port != "approved" {
			t.Fatalf("port = %q, want approved", out.Port)
		}
		if out.Data["approver"] != "bob" || out.Data["comment"] != "ok" {
			t.Errorf("verdict = %v, want the gate's own approval by bob", out.Data)
		}
		assertNoUpstreamOwnedKeys(t, out.Data, "returned", "forced", "bypassed")
		assertBusinessFieldsPass(t, out.Data)
	})

	t.Run("return", func(t *testing.T) {
		data := upstreamPayload()
		out, err := sh.OnResume(context.Background(),
			approvalInputFromData(node.ApprovalAll, data),
			approvalSignal("alice", "return", "rework"))
		if err != nil {
			t.Fatalf("OnResume() error = %v", err)
		}
		if out.Port != "returned" {
			t.Fatalf("port = %q, want returned", out.Port)
		}
		if out.Data["returned"] != true || out.Data["approved"] != false {
			t.Errorf("verdict = %v, want the gate's own return", out.Data)
		}
		assertNoUpstreamOwnedKeys(t, out.Data, "forced", "bypassed")
		assertBusinessFieldsPass(t, out.Data)
	})

	t.Run("force", func(t *testing.T) {
		out, err := sh.OnResume(context.Background(),
			&types.Input{
				Params: forceInput(node.ApprovalAll, []any{"vp"}, nil).Params,
				Data:   map[string]any{"amount": 42, "reason": "new laptop", "returned": true},
				// The gate has not heard from anyone, so both approvers are bypassed.
				NodeName: "approval_1",
			},
			forcePayload("vp", "decided above"))
		if err != nil {
			t.Fatalf("OnResume() error = %v", err)
		}
		if out.Port != "approved" || out.Data["forced"] != true {
			t.Fatalf("port = %q data = %v, want a forced approval", out.Port, out.Data)
		}
		assertNoUpstreamOwnedKeys(t, out.Data, "returned")
		assertBusinessFieldsPass(t, out.Data)
	})
}

// A declined signal re-suspends with the record extended. It sets none of the
// verdict keys, so any of them in the published data would be upstream's.
func TestApproval_IgnoredSignalPublishesNoUpstreamDecisionKeys(t *testing.T) {
	sh := approvalHandler(t)
	signal := approvalSignal("alice", "approve", "ok")
	delete(signal.Data, types.VerifiedActorKey)
	out, err := sh.OnResume(context.Background(),
		approvalInputFromData(node.ApprovalAll, upstreamPayload()), signal)
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("port = %q, want a re-suspension for an unattributed signal", out.Port)
	}
	assertNoUpstreamOwnedKeys(t, out.Data, "approved", "approver", "comment", "returned", "forced", "bypassed")
	assertBusinessFieldsPass(t, out.Data)
	assertIgnored(t, out.State, 0, "unverified-actor")
}
