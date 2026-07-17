package xflow

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type webhookRuntime struct {
	mu     sync.RWMutex
	routes map[string]types.WebhookHandler
	logger engine.Logger
}

func newWebhookRuntime(logger engine.Logger) *webhookRuntime {
	return &webhookRuntime{routes: make(map[string]types.WebhookHandler), logger: logger}
}

func (r *webhookRuntime) Handle(method string, path string, handler types.WebhookHandler) (types.TriggerSubscription, error) {
	if method == "" {
		return nil, fmt.Errorf("webhook method is required")
	}
	if path == "" {
		return nil, fmt.Errorf("webhook path is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("webhook handler is required")
	}
	key := webhookRouteKey(method, path)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.routes[key]; exists {
		return nil, fmt.Errorf("webhook route %s %s already registered", method, path)
	}
	r.routes[key] = handler
	return types.CloseFunc(func(context.Context) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.routes, key)
		return nil
	}), nil
}

func (r *webhookRuntime) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	handler := r.routes[webhookRouteKey(req.Method, req.URL.Path)]
	r.mu.RUnlock()
	if handler == nil {
		http.NotFound(w, req)
		return
	}
	if _, err := handler(req.Context(), req); err != nil {
		// Never echo the internal error to the caller — it may leak handler
		// internals, paths, or credentials. Return a fixed message and log the
		// real error server-side.
		if r.logger != nil {
			r.logger.Errorf("webhook handler error: %v", err)
		} else {
			log.Printf("webhook handler error: %v", err)
		}
		http.Error(w, "webhook handler error", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func webhookRouteKey(method string, path string) string {
	return strings.ToUpper(method) + " " + path
}
