package xflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestWebhookRuntimeHandlesRegisteredRoute(t *testing.T) {
	rt := newWebhookRuntime()
	_, err := rt.Handle(http.MethodPost, "/hooks/orders", func(ctx context.Context, req *http.Request) (*types.TriggerEvent, error) {
		return &types.TriggerEvent{ID: req.Header.Get("X-Event-ID")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/hooks/orders", nil)
	req.Header.Set("X-Event-ID", "evt-1")
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
}
