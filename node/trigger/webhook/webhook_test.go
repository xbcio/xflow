package webhook

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func TestWebhookTriggerDescriptor(t *testing.T) {
	n := New()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.webhook" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestWebhookTriggerRequiresMethodAndPath(t *testing.T) {
	_, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "webhook",
		Params:     map[string]any{},
		Runtime:    newFakeWebhookTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing method/path error")
	}
}

func TestWebhookTriggerRequestEmitsEventAndDedupsByHeader(t *testing.T) {
	rt := newFakeWebhookTriggerRuntime()
	tr := New().Method(http.MethodPost).Path("/hooks/orders").EventIDHeader("X-Event-ID")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "webhook",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewBufferString(`{"id":1}`))
		req.Header.Set("X-Event-ID", "evt-1")
		if _, err := rt.webhooks.invoke(req); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(rt.emits); got != 1 {
		t.Fatalf("emits = %d, want 1", got)
	}
	if rt.emits[0].ID != "evt-1" {
		t.Fatalf("event ID = %q, want evt-1", rt.emits[0].ID)
	}
}

type fakeWebhookTriggerRuntime struct {
	webhooks *fakeWebhookRuntime
	emits    []*types.TriggerEvent
	seen     map[string]struct{}
	// dedups records what Dedup was actually called with. The parameter used to
	// be named `_`, which made the TTL — the entire width of the replay window —
	// unobservable to every test in this package.
	dedups []triggertest.DedupCall
}

func newFakeWebhookTriggerRuntime() *fakeWebhookTriggerRuntime {
	return &fakeWebhookTriggerRuntime{webhooks: newFakeWebhookRuntime(), seen: make(map[string]struct{})}
}

func (r *fakeWebhookTriggerRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.emits = append(r.emits, event)
	return "exec-1", nil
}

func (r *fakeWebhookTriggerRuntime) Dedup(_ context.Context, key string, ttl time.Duration) (bool, error) {
	r.dedups = append(r.dedups, triggertest.DedupCall{Key: key, TTL: ttl})
	if _, ok := r.seen[key]; ok {
		return false, nil
	}
	r.seen[key] = struct{}{}
	return true, nil
}

func (r *fakeWebhookTriggerRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return triggertest.FakeLock{}, true, nil
}

func (r *fakeWebhookTriggerRuntime) State(context.Context, string) types.TriggerState { return nil }
func (r *fakeWebhookTriggerRuntime) Webhooks() types.WebhookRuntime                   { return r.webhooks }

type fakeWebhookRuntime struct {
	method  string
	path    string
	handler types.WebhookHandler
}

func newFakeWebhookRuntime() *fakeWebhookRuntime { return &fakeWebhookRuntime{} }

func (r *fakeWebhookRuntime) Handle(method string, path string, handler types.WebhookHandler) (types.TriggerSubscription, error) {
	r.method = method
	r.path = path
	r.handler = handler
	return types.CloseFunc(func(context.Context) error {
		r.handler = nil
		return nil
	}), nil
}

func (r *fakeWebhookRuntime) invoke(req *http.Request) (*types.TriggerEvent, error) {
	return r.handler(req.Context(), req)
}

func TestWebhookNodeTypeAndParamsAreFrozen(t *testing.T) {
	n := New().Method("POST").Path("/hooks/x").EventIDHeader("X-Id")
	if n.NodeType() != "xflow.trigger.webhook" {
		t.Fatalf("NodeType = %q, want xflow.trigger.webhook", n.NodeType())
	}
	params := n.RawParams().(map[string]any)
	want := map[string]any{
		"method":          "POST",
		"path":            "/hooks/x",
		"event_id_header": "X-Id",
		"max_body_bytes":  int64(1 << 20),
	}
	if len(params) != len(want) {
		t.Fatalf("RawParams keys = %v, want exactly %v", params, want)
	}
	for k, v := range want {
		if params[k] != v {
			t.Fatalf("RawParams[%q] = %#v, want %#v", k, params[k], v)
		}
	}
}

func TestWebhookMaxBodyBytesFallsBackToDefault(t *testing.T) {
	// A non-positive cap must not disable the limit — RawParams normalizes it
	// back to the 1 MiB default, and webhookMaxBodyBytes does the same for the
	// YAML path where the key is absent or garbage.
	params := New().MaxBodyBytes(0).RawParams().(map[string]any)
	if params["max_body_bytes"] != int64(1<<20) {
		t.Fatalf("max_body_bytes = %#v, want %d", params["max_body_bytes"], int64(1<<20))
	}
	if got := webhookMaxBodyBytes(nil); got != int64(1<<20) {
		t.Fatalf("webhookMaxBodyBytes(nil) = %d, want %d", got, int64(1<<20))
	}
	if got := webhookMaxBodyBytes("not-a-number"); got != int64(1<<20) {
		t.Fatalf("webhookMaxBodyBytes(garbage) = %d, want %d", got, int64(1<<20))
	}
}

func TestReadWebhookBodyRejectsOversizedBody(t *testing.T) {
	if _, err := readWebhookBody(strings.NewReader("abcd"), 3); err == nil {
		t.Fatal("expected an oversized-body error")
	}
	body, err := readWebhookBody(strings.NewReader("abc"), 3)
	if err != nil {
		t.Fatalf("exactly-at-cap body rejected: %v", err)
	}
	if string(body) != "abc" {
		t.Fatalf("body = %q, want abc", body)
	}
}
