package node

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestWebhookTriggerDescriptor(t *testing.T) {
	n := WebhookTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.webhook" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestWebhookTriggerRequiresMethodAndPath(t *testing.T) {
	_, err := WebhookTrigger().Activate(context.Background(), &types.TriggerActivateInput{
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
	tr := WebhookTrigger().Method(http.MethodPost).Path("/hooks/orders").EventIDHeader("X-Event-ID")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "webhook",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(context.Background())

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
}

func newFakeWebhookTriggerRuntime() *fakeWebhookTriggerRuntime {
	return &fakeWebhookTriggerRuntime{webhooks: newFakeWebhookRuntime(), seen: make(map[string]struct{})}
}

func (r *fakeWebhookTriggerRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.emits = append(r.emits, event)
	return "exec-1", nil
}

func (r *fakeWebhookTriggerRuntime) Dedup(_ context.Context, key string, _ time.Duration) (bool, error) {
	if _, ok := r.seen[key]; ok {
		return false, nil
	}
	r.seen[key] = struct{}{}
	return true, nil
}

func (r *fakeWebhookTriggerRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return fakeTriggerLock{}, true, nil
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
