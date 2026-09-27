package group_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// A signal the node declines leaves a trail entry. Two properties matter for a
// trail a caller can influence: it must not grow without bound, and it must not
// be the only thing distinguishing a handful of refusals from a flood.

func TestApproval_IgnoredTrailIsBoundedAndCounted(t *testing.T) {
	sh := approvalHandler(t)
	ctx := context.Background()
	input := approvalInput(node.ApprovalAll, nil)

	const deliveries = 25
	for i := 0; i < deliveries; i++ {
		var signal *types.SignalPayload
		switch i % 2 {
		case 0:
			signal = approvalSignal("alice", "cancel", "")
		default:
			signal = &types.SignalPayload{
				Triggered: types.SignalReceived,
				Name:      "approval_1/approval/alice",
				Data:      map[string]any{"approver": "alice"},
			}
		}
		out, err := sh.OnResume(ctx, input, signal)
		if err != nil {
			t.Fatalf("delivery %d: OnResume() error = %v", i, err)
		}
		input = approvalInput(node.ApprovalAll, out.Data)
	}

	if got := input.Data["_ignored_count"]; got != deliveries {
		t.Fatalf("_ignored_count = %v (%T), want %d: the count is the only place a "+
			"refusal beyond the trail limit is visible", got, got, deliveries)
	}
	trail, ok := input.Data["_ignored"].([]map[string]any)
	if !ok {
		t.Fatalf("no ignored trail in %v", input.Data)
	}
	if len(trail) != 20 {
		t.Fatalf("len(trail) = %d, want 20 (bounded): an unbounded trail lets a "+
			"caller grow a suspended node's stored output", len(trail))
	}
	// The trail keeps the most recent entries, so its first entry is the 6th
	// delivery (index 5), an odd one and therefore malformed-action.
	if trail[0]["reason"] != reasonMalformedAction {
		t.Fatalf("trail[0] = %v, want the most recent 20 entries (oldest kept "+
			"should be delivery 5, %s)", trail[0], reasonMalformedAction)
	}
	if trail[len(trail)-1]["reason"] != reasonUnknownAction {
		t.Fatalf("trail[last] = %v, want the newest delivery", trail[len(trail)-1])
	}
}

func TestApproval_TruncatesAnOverlongSignalNameInTheTrail(t *testing.T) {
	sh := approvalHandler(t)
	longName := strings.Repeat("x", 4096)

	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, nil),
		&types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      longName,
			Data:      map[string]any{"approver": "alice", "action": "approve"},
		})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	trail, ok := out.Data["_ignored"].([]map[string]any)
	if !ok || len(trail) != 1 {
		t.Fatalf("ignored trail = %v, want one entry", out.Data["_ignored"])
	}
	// The signal name is whatever the caller sent, so recording it verbatim
	// puts caller-controlled bytes into the node's persisted output.
	recorded, ok := trail[0]["signal"].(string)
	if !ok {
		t.Fatalf("recorded signal = %T, want a string", trail[0]["signal"])
	}
	if len(recorded) != 128 {
		t.Fatalf("len(recorded signal) = %d, want 128 (truncated from %d)", len(recorded), len(longName))
	}
}
