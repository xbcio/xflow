package webhook

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
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

// TestWebhookTriggerSuccessfulEmitKeepsDedupKey is the other half of the
// release contract: a claim whose emit SUCCEEDED must survive, or a sender's
// retry starts a second execution.
func TestWebhookTriggerSuccessfulEmitKeepsDedupKey(t *testing.T) {
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
		req.Header.Set("X-Event-ID", "evt-keep")
		if _, err := rt.webhooks.invoke(req); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(rt.emits); got != 1 {
		t.Fatalf("emits = %d, want 1: the successful emit's claim was released, so the retry re-ran the workflow", got)
	}
	if got := len(rt.releases); got != 0 {
		t.Fatalf("released %d dedup keys after successful emits, want 0", got)
	}
}

// TestWebhookTriggerEmitFailureReleasesDedupKey: the claim is taken before the
// emit, so an emit failure must give it back. Otherwise a sender's retry is
// answered as a duplicate and dropped for the whole 24h window — the trigger
// would be at-most-once.
func TestWebhookTriggerEmitFailureReleasesDedupKey(t *testing.T) {
	rt := newFakeWebhookTriggerRuntime()
	rt.emitErr = errors.New("boom")
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

	req := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewBufferString(`{"id":1}`))
	req.Header.Set("X-Event-ID", "evt-retry")
	if _, err := rt.webhooks.invoke(req); err == nil {
		t.Fatal("emit failure was not reported to the caller")
	}
	const wantKey = "trigger:wf-1:webhook:evt-retry"
	if got := rt.releases; len(got) != 1 || got[0] != wantKey {
		t.Fatalf("released keys = %v, want [%s]: the failed emit left its claim set", got, wantKey)
	}

	// The same delivery must be processed, not folded into the failed attempt's
	// claim.
	rt.emitErr = nil
	retry := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewBufferString(`{"id":1}`))
	retry.Header.Set("X-Event-ID", "evt-retry")
	if _, err := rt.webhooks.invoke(retry); err != nil {
		t.Fatalf("redelivery after a failed emit was not processed: %v", err)
	}
	if got := len(rt.emits); got != 2 {
		t.Fatalf("emit attempts = %d, want 2 (one failed, one redelivered)", got)
	}
}

// TestWebhookTriggerReleaseFailureReturnsEmitError: a release that fails is
// logged, not surfaced in place of the emit error, and it must not make the
// handler emit again.
func TestWebhookTriggerReleaseFailureReturnsEmitError(t *testing.T) {
	rt := newFakeWebhookTriggerRuntime()
	emitErr := errors.New("emit boom")
	rt.emitErr = emitErr
	rt.releaseErr = errors.New("release boom")
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

	req := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewBufferString(`{"id":1}`))
	req.Header.Set("X-Event-ID", "evt-release-fail")
	if _, err := rt.webhooks.invoke(req); !errors.Is(err, emitErr) {
		t.Fatalf("handler error = %v, want the original emit error %v", err, emitErr)
	}
	if got := len(rt.releases); got != 1 {
		t.Fatalf("release attempts = %d, want 1", got)
	}

	// The release failed, so the claim is still held: the retry is a duplicate
	// and must not reach Emit a second time.
	retry := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewBufferString(`{"id":1}`))
	retry.Header.Set("X-Event-ID", "evt-release-fail")
	if _, err := rt.webhooks.invoke(retry); err != nil {
		t.Fatalf("duplicate retry after a failed release errored: %v", err)
	}
	if got := len(rt.emits); got != 1 {
		t.Fatalf("emit attempts = %d after a failed release, want 1: the event was emitted twice", got)
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
	// emitErr makes the next emits fail, so a test can put the handler on the
	// path where the dedup claim has to be released.
	emitErr error
	// releaseErr makes ReleaseDedup fail without releasing the claim; releases
	// records every release attempt.
	releases   []string
	releaseErr error
}

func newFakeWebhookTriggerRuntime() *fakeWebhookTriggerRuntime {
	return &fakeWebhookTriggerRuntime{webhooks: newFakeWebhookRuntime(), seen: make(map[string]struct{})}
}

func (r *fakeWebhookTriggerRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.emits = append(r.emits, event)
	return "exec-1", r.emitErr
}

func (r *fakeWebhookTriggerRuntime) Dedup(_ context.Context, key string, ttl time.Duration) (bool, error) {
	r.dedups = append(r.dedups, triggertest.DedupCall{Key: key, TTL: ttl})
	if _, ok := r.seen[key]; ok {
		return false, nil
	}
	r.seen[key] = struct{}{}
	return true, nil
}

// ReleaseDedup mirrors the primitives: the claim is removed only when the
// release succeeds, so a test can model the failure that leaves it held.
func (r *fakeWebhookTriggerRuntime) ReleaseDedup(_ context.Context, key string) error {
	r.releases = append(r.releases, key)
	if r.releaseErr != nil {
		return r.releaseErr
	}
	delete(r.seen, key)
	return nil
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

// TestWebhookSecretSupplyIsEmittedOnlyWhenSet pins both halves of the
// parameter's contract: a definition that names a secret carries the key, and one
// that does not stays byte-for-byte what it was before the parameter existed —
// which is what keeps the workflow hash of every already-registered server-hosted
// webhook unchanged. The key is what the SERVER reads to find the secret; the
// local runtime ignores it, so a definition without one is still a valid
// definition, just not a registrable server-hosted entry (registration refuses
// it — see service/apiserver's webhook tests).
func TestWebhookSecretSupplyIsEmittedOnlyWhenSet(t *testing.T) {
	if params := New().Method("POST").Path("/hooks/x").RawParams().(map[string]any); params["secret_supply"] != nil {
		t.Fatalf("secret_supply = %#v on a node that names no secret, want the key omitted", params["secret_supply"])
	}
	params := New().SecretSupply("hook-secret").RawParams().(map[string]any)
	if params["secret_supply"] != "hook-secret" {
		t.Fatalf("secret_supply = %#v, want %q", params["secret_supply"], "hook-secret")
	}

	// The editor offers the parameter, and it is NOT Required: the local runtime
	// never reads it, so requiring it would refuse definitions that need no
	// signing key at all.
	var found bool
	for _, spec := range New().Descriptor().Params {
		if spec.Name != "secret_supply" {
			continue
		}
		found = true
		if spec.Required {
			t.Fatal("secret_supply is Required; it is read by the server only, so a local definition must not need it")
		}
		if spec.Type != types.ParamString {
			t.Fatalf("secret_supply type = %q, want %q", spec.Type, types.ParamString)
		}
		if spec.Widget != nodeinternal.WidgetCredentialSelect {
			t.Fatalf("secret_supply widget = %q, want %q: the editor must render the supply reference with the reference picker, not a plain text box", spec.Widget, nodeinternal.WidgetCredentialSelect)
		}
		if spec.Secret {
			t.Fatal("secret_supply is Secret; the param holds a supply resource reference name, not the secret itself")
		}
	}
	if !found {
		t.Fatal("the descriptor declares no secret_supply param, so no UI could ever set it")
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
