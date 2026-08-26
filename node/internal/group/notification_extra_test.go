package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// notification_test.go sets `data` via SetData() in
// TestNotification_ExecuteReturnsDeterministicPayload but never reads it back
// from the output. That leaves the merge of input.Params["data"] into the
// output entirely unpinned: deleting the whole `if extra, ok :=
// input.Params["data"].(map[string]any); ok { ... }` block in Execute leaves
// every existing test green, silently dropping any structured payload the
// caller attached via .SetData(...).
func TestNotification_ExecuteMergesParamsDataIntoOutput(t *testing.T) {
	h, found := registry.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	out, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"channel": "email",
			"to":      "ops@example.com",
			"data":    map[string]any{"order_id": "ord-1"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out.Data["order_id"]; got != "ord-1" {
		t.Fatalf("order_id = %v, want ord-1: params[\"data\"] was not merged into "+
			"the output, so downstream nodes lose the structured payload attached "+
			"via .SetData(...)", got)
	}
}

// isEmptyNotificationRecipient has five branches (nil, string, []string,
// []any, default). Only `case nil` is exercised by
// TestNotification_RequiresChannelAndRecipient, which omits the "to" key
// entirely. Each of the other four branches can be rewritten to unconditionally
// `return false` and the existing suite stays green, because it only ever
// supplies a non-empty string ("ops@example.com") for "to" on the success
// path. The four tests below each pin one branch by supplying a value of that
// exact type which IS empty, so the node must still reject it.

func TestNotification_ExecuteRejectsEmptyStringRecipient(t *testing.T) {
	h, found := registry.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	_, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{"channel": "email", "to": ""},
	})
	if err == nil {
		t.Fatal("expected error for empty string recipient: the `case string` " +
			"branch of isEmptyNotificationRecipient must still check t == \"\"")
	}
}

func TestNotification_ExecuteRejectsEmptyStringSliceRecipient(t *testing.T) {
	h, found := registry.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	_, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{"channel": "email", "to": []string{}},
	})
	if err == nil {
		t.Fatal("expected error for empty []string recipient: the `case []string` " +
			"branch of isEmptyNotificationRecipient must still check len(t) == 0")
	}
}

func TestNotification_ExecuteRejectsEmptyAnySliceRecipient(t *testing.T) {
	h, found := registry.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	_, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{"channel": "email", "to": []any{}},
	})
	if err == nil {
		t.Fatal("expected error for empty []any recipient: the `case []any` " +
			"branch of isEmptyNotificationRecipient must still check len(t) == 0")
	}
}

func TestNotification_ExecuteRejectsUnrecognizedEmptyRecipient(t *testing.T) {
	h, found := registry.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	// struct{}{} hits neither nil, string, []string, nor []any, so it falls
	// into the default branch. cast.ToString(struct{}{}) == "" (cast has no
	// conversion for it), so the default branch's own rule says this is an
	// empty recipient.
	_, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{"channel": "email", "to": struct{}{}},
	})
	if err == nil {
		t.Fatal("expected error for an unrecognized recipient type that casts to " +
			"an empty string: the `default` branch of isEmptyNotificationRecipient " +
			"must still check cast.ToString(t) == \"\"")
	}
}
