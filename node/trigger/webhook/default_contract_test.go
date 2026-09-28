package webhook

// Trigger-activate Default contract (descriptor-contract design §2.4): the body
// cap Activate installs must be the same whether max_body_bytes is absent, set
// to the descriptor's Default, or set to that Default after a JSON round trip.
// The cap is observed through the request handler Activate registers: a body of
// exactly the cap is accepted and one byte more is rejected.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"
)

func webhookDescriptorDefault(t *testing.T, name string) any {
	t.Helper()
	for _, p := range New().Descriptor().Params {
		if p.Name == name {
			if p.Default == nil {
				t.Fatalf("xflow.trigger.webhook/%s has no Default", name)
			}
			return p.Default
		}
	}
	t.Fatalf("xflow.trigger.webhook has no param %q", name)
	return nil
}

func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// acceptsBody activates a webhook with params and reports whether a body of
// size bytes is accepted.
func acceptsBody(t *testing.T, params map[string]any, size int64) bool {
	t.Helper()
	rt := newFakeWebhookTriggerRuntime()
	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "webhook",
		Params:     params,
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()
	req := httptest.NewRequest(http.MethodPost, "/hooks/x", bytes.NewReader(bytes.Repeat([]byte("a"), int(size))))
	req.Header.Set("X-Event-ID", "evt")
	_, err = rt.webhooks.invoke(req)
	return err == nil
}

func TestDefaultContractWebhookMaxBodyBytes(t *testing.T) {
	def := webhookDescriptorDefault(t, "max_body_bytes")
	limit := cast.ToInt64(def)
	if limit <= 0 {
		t.Fatalf("max_body_bytes Default %#v is not a positive size", def)
	}
	base := func() map[string]any {
		return map[string]any{"method": http.MethodPost, "path": "/hooks/x", "event_id_header": "X-Event-ID"}
	}
	for variant, value := range map[string]any{"absent": nil, "default": def, "default_json": jsonRoundTrip(t, def)} {
		t.Run(variant, func(t *testing.T) {
			params := base()
			if value != nil {
				params["max_body_bytes"] = value
			}
			if !acceptsBody(t, params, limit) {
				t.Errorf("a %d-byte body was rejected; the effective cap is below the Default", limit)
			}
			if acceptsBody(t, params, limit+1) {
				t.Errorf("a %d-byte body was accepted; the effective cap is above the Default", limit+1)
			}
		})
	}
}
