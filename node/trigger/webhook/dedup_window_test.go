package webhook

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestWebhookTriggerRequestEmitsEventAndDedupsByHeader proves the fake's own map
// suppressed the second delivery. It cannot prove anything about the window that
// suppression lasts for in production, because the fake ignored the TTL it was
// handed.
//
// The 24 hours is the whole defence against replay. A webhook sender that retries
// on a timeout, a CDN that replays, an operator re-firing a delivery from a
// provider console — all of those arrive minutes or hours later, and a window
// that does not cover them turns one logical event into several executions.
// Nothing about that failure is visible: each delivery looks like a fresh,
// well-formed request.

func TestWebhookDedupWindowCoversDelayedRedelivery(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/hooks/orders", bytes.NewBufferString(`{"id":1}`))
	req.Header.Set("X-Event-ID", "evt-1")
	if _, err := rt.webhooks.invoke(req); err != nil {
		t.Fatal(err)
	}

	if len(rt.dedups) != 1 {
		t.Fatalf("dedup calls = %d, want 1", len(rt.dedups))
	}
	if got := rt.dedups[0].TTL; got != 24*time.Hour {
		t.Fatalf("dedup TTL = %v, want 24h. Senders retry over minutes and hours, "+
			"and a shorter window admits the retry as a new event: one order becomes "+
			"two executions with nothing in the logs to say so", got)
	}
	// The key is what stops two workflows both listening for the same provider's
	// event IDs from cancelling each other's deliveries.
	if got, want := rt.dedups[0].Key, "trigger:wf-1:webhook:evt-1"; got != want {
		t.Fatalf("dedup key = %q, want %q", got, want)
	}
}
