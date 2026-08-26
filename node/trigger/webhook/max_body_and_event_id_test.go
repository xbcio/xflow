package webhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestWebhookHonoursTheConfiguredMaxBodyBytesThroughTheHandler pins the one hop
// nothing tests: webhook.go:100 and :106, where Activate reads the configured
// cap out of in.Params and hands it to the request handler it assembles.
//
//	maxBodyBytes := webhookMaxBodyBytes(in.Params["max_body_bytes"])
//	...
//	body, err := readWebhookBody(req.Body, maxBodyBytes)
//
// Both ends of that hop already have tests and neither covers the hop.
// TestWebhookMaxBodyBytesFallsBackToDefault (webhook_test.go:144) calls
// webhookMaxBodyBytes directly with nil and a garbage string;
// TestReadWebhookBodyRejectsOversizedBody (webhook_test.go:160) calls
// readWebhookBody directly with a literal 3. Neither ever sends a request
// through the closure Activate returns. So replacing line 100 with
//
//	maxBodyBytes := defaultWebhookMaxBodyBytes
//
// — the configured value simply discarded — leaves this package green.
//
// The body cap is the only input defence this trigger has. There is no
// signature check, no token check and no origin check anywhere in this package;
// if the host's types.WebhookRuntime implementation adds one it does so
// outside. So a webhook node deliberately configured down to a few KiB for a
// public endpoint would silently accept 1 MiB per request instead, on a path
// that buffers the whole body into memory (readWebhookBody does io.ReadAll)
// before any dedup or emit. The gap between configured and actual is the
// amplification factor, and nothing reports it — the requests succeed.
func TestWebhookHonoursTheConfiguredMaxBodyBytesThroughTheHandler(t *testing.T) {
	// 16 rather than the 1 MiB default so that "ignored the config and used the
	// default" and "honoured the config" produce different outcomes for a body
	// that is trivial to build.
	const bodyCap = 16

	rt := newFakeWebhookTriggerRuntime()
	tr := New().Method(http.MethodPost).Path("/hooks/orders").MaxBodyBytes(bodyCap)
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

	oversized := bytes.Repeat([]byte("a"), bodyCap+1)
	req := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewReader(oversized))
	if _, err := rt.webhooks.invoke(req); err == nil {
		t.Fatalf("a %d-byte body was accepted against a configured cap of %d: the "+
			"handler is using the 1 MiB default instead of the value the node was "+
			"configured with, so the only input-size defence this trigger has is "+
			"off by whatever ratio the operator thought they were setting",
			len(oversized), bodyCap)
	}
	if got := len(rt.emits); got != 0 {
		t.Fatalf("emits = %d, want 0: an over-cap request must not reach the workflow", got)
	}

	// The other side of the same assertion: exactly at the cap must still pass,
	// so a mutation that rejects everything cannot pass this test either.
	atCap := bytes.Repeat([]byte("b"), bodyCap)
	req = httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewReader(atCap))
	if _, err := rt.webhooks.invoke(req); err != nil {
		t.Fatalf("a body of exactly %d bytes was rejected against a cap of %d: %v", bodyCap, bodyCap, err)
	}
	if got := len(rt.emits); got != 1 {
		t.Fatalf("emits = %d, want 1 after an at-cap request", got)
	}
}

// TestWebhookWithoutAnEventIDHeaderDedupsByBodyAndMinute pins webhook.go:
// 120-123, the fallback that synthesises an event ID when the request carries
// none:
//
//	if eventID == "" {
//		sum := sha256.Sum256(body)
//		eventID = hex.EncodeToString(sum[:]) + fmt.Sprintf("/%d", time.Now().Unix()/60)
//	}
//
// Every existing test in this package supplies X-Event-ID —
// webhook_test.go:52 and dedup_window_test.go:41 are the only two requests the
// suite ever makes, and both set it. So the fallback is unreachable from the
// tests and free to change.
//
// It is not a cosmetic default. The event ID is the sole input to the dedup key
// at webhook.go:137, and this branch is what a sender that does not stamp an
// idempotency header gets — which is most of them. Two properties hold it up,
// and each fails differently:
//
//   - Derived from the body. Replace sha256(body) with anything per-request
//     unique (a UUID, UnixNano) and every retry becomes a fresh execution.
//     Webhook senders retry on timeout by design, so this is duplicate side
//     effects on the ordinary failure path, not an exotic one.
//   - Bucketed to the minute. The /60 is the replay window's width for
//     header-less senders. Drop it and the window collapses to one second,
//     which is shorter than the retry backoff of every sender that matters.
//
// The suffix assertion below recomputes time.Now().Unix()/60, which would be
// worthless if the production expression could not differ from it — but it can:
// seconds, nanoseconds and an hour bucket all produce a different value, and
// those are exactly the mutations that break the window. The bucket is sampled
// on both sides of the request so a run that straddles a minute boundary
// accepts either value rather than flaking.
func TestWebhookWithoutAnEventIDHeaderDedupsByBodyAndMinute(t *testing.T) {
	rt := newFakeWebhookTriggerRuntime()
	// EventIDHeader deliberately unset: this is the shape of a sender that does
	// not stamp an idempotency header.
	tr := New().Method(http.MethodPost).Path("/hooks/orders")
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

	post := func(body string) *types.TriggerEvent {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/hooks/orders", strings.NewReader(body))
		event, err := rt.webhooks.invoke(req)
		if err != nil {
			t.Fatalf("invoke(%q): %v", body, err)
		}
		return event
	}

	const firstBody = `{"order":"o-1"}`
	bucketBefore := time.Now().Unix() / 60
	first := post(firstBody)
	bucketAfter := time.Now().Unix() / 60

	sum := sha256.Sum256([]byte(firstBody))
	wantPrefix := hex.EncodeToString(sum[:])
	if !strings.HasPrefix(first.ID, wantPrefix+"/") {
		t.Fatalf("synthesised event ID = %q, want it to start with %q/: the ID must "+
			"be derived from the request body, or a retry of the same payload gets "+
			"a new ID and runs the workflow a second time", first.ID, wantPrefix)
	}
	gotBucket := strings.TrimPrefix(first.ID, wantPrefix+"/")
	if gotBucket != fmt.Sprint(bucketBefore) && gotBucket != fmt.Sprint(bucketAfter) {
		t.Fatalf("synthesised event ID bucket = %q, want %d or %d (unix seconds / 60): "+
			"this divisor is the whole width of the replay window for senders that do "+
			"not stamp an idempotency header",
			gotBucket, bucketBefore, bucketAfter)
	}

	// Same body again: must be recognised as the same event and not re-emitted.
	second := post(firstBody)
	if second.ID != first.ID {
		t.Fatalf("the same body produced two IDs, %q then %q", first.ID, second.ID)
	}
	if got := len(rt.emits); got != 1 {
		t.Fatalf("emits = %d after posting the same body twice, want 1: a retried "+
			"delivery started a second execution of the workflow", got)
	}

	// A different body must NOT be deduplicated away — otherwise "1 emit" above
	// would also be satisfied by an implementation that emits nothing after the
	// first request ever.
	third := post(`{"order":"o-2"}`)
	if third.ID == first.ID {
		t.Fatalf("two different bodies produced the same event ID %q: distinct "+
			"deliveries would be silently collapsed into one", third.ID)
	}
	if got := len(rt.emits); got != 2 {
		t.Fatalf("emits = %d after a genuinely new body, want 2", got)
	}
}
