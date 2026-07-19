package xflow

import (
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend"
	backendlocal "github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/engine"
)

// startBinderWrapper wraps a local backend and adds backend.StartBinder. It lets
// us inject a start error into newFromConfig without running Redis.
type startBinderWrapper struct {
	*backendlocal.Backend
	startErr   error
	stopCalled bool
}

func (w *startBinderWrapper) StartBinding(eng *engine.Engine) (func(), error) {
	if w.startErr != nil {
		return nil, w.startErr
	}
	stop := w.Backend.Bind(eng)
	return func() {
		w.stopCalled = true
		stop()
	}, nil
}

// legacyProvider wraps a local backend and does NOT implement StartBinder, so
// newFromConfig falls back to the deprecated Bind path.
type legacyProvider struct {
	*backendlocal.Backend
	stopCalled bool
}

func (l *legacyProvider) Bind(eng *engine.Engine) func() {
	stop := l.Backend.Bind(eng)
	return func() {
		l.stopCalled = true
		stop()
	}
}

// TestNewFromConfigPropagatesStartBindingError verifies the SDK fail-closed
// path: when the backend's StartBinding returns an error, newFromConfig returns
// nil, error instead of a ready Engine.
func TestNewFromConfigPropagatesStartBindingError(t *testing.T) {
	provider := &startBinderWrapper{
		Backend:  backendlocal.New(backendlocal.WithConcurrency(1)),
		startErr: errors.New("consumer start failed"),
	}
	cfg := &engineConfig{concurrency: 1}

	_, err := newFromConfig(cfg, provider)
	if err == nil {
		t.Fatal("newFromConfig error = nil, want start backend error")
	}
	if !strings.Contains(err.Error(), "start backend") {
		t.Fatalf("error = %q, want 'start backend' wrapper", err.Error())
	}
}

// TestNewFromConfigPrefersStartBinderOverBind verifies that a backend
// implementing both StartBinder and Provider.Bind uses StartBinding.
func TestNewFromConfigPrefersStartBinderOverBind(t *testing.T) {
	provider := &startBinderWrapper{
		Backend: backendlocal.New(backendlocal.WithConcurrency(1)),
	}
	cfg := &engineConfig{concurrency: 1}

	eng, err := newFromConfig(cfg, provider)
	if err != nil {
		t.Fatalf("newFromConfig error = %v", err)
	}
	eng.Stop()
	if !provider.stopCalled {
		t.Fatal("StartBinding stop was not called")
	}
}

// TestNewFromConfigFallsBackToLegacyBind verifies that a backend without
// StartBinder still works via the deprecated Bind path.
func TestNewFromConfigFallsBackToLegacyBind(t *testing.T) {
	provider := &legacyProvider{
		Backend: backendlocal.New(backendlocal.WithConcurrency(1)),
	}
	cfg := &engineConfig{concurrency: 1}

	eng, err := newFromConfig(cfg, provider)
	if err != nil {
		t.Fatalf("newFromConfig error = %v", err)
	}
	eng.Stop()
	if !provider.stopCalled {
		t.Fatal("legacy Bind stop was not called")
	}
}

// Compile-time assertions.
var _ backend.Provider = (*startBinderWrapper)(nil)
var _ backend.StartBinder = (*startBinderWrapper)(nil)
var _ backend.Provider = (*legacyProvider)(nil)
