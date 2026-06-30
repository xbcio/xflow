package types

import (
	"context"
	"net/http"
)

type WebhookRuntime interface {
	Handle(method string, path string, handler WebhookHandler) (TriggerSubscription, error)
}

type WebhookHandler func(ctx context.Context, req *http.Request) (*TriggerEvent, error)
