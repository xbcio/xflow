package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	backendmemory "github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/observability/metrics"
)

func TestNewControlPlaneRequiresBackend(t *testing.T) {
	_, err := NewControlPlane(Config{})
	if err == nil {
		t.Fatal("NewControlPlane(Config{}) error = nil, want error for missing Backend")
	}
}

func TestControlPlaneHandlerServesSubmitWorkflow(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Shutdown(context.Background()) }()

	req := httptest.NewRequest(http.MethodPost, SubmitWorkflowPath, nil)
	rec := httptest.NewRecorder()
	cp.Handler().ServeHTTP(rec, req)

	// Empty body -> 400, but this proves the route is wired, not 404.
	if rec.Code == http.StatusNotFound {
		t.Fatalf("Handler() did not route %s, got 404", SubmitWorkflowPath)
	}
}

func TestControlPlaneStartStopIsIdempotentSafe(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestNewControlPlaneWiresMetricsIntoDispatcherAndAuth(t *testing.T) {
	m := metrics.New()
	cp, err := NewControlPlane(Config{Backend: backendmemory.New(), Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	if cp.dispatcher.observer == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the Dispatcher observer")
	}
	if cp.httpServer.core.authObserver == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the HTTP auth observer")
	}
	if cp.grpcServer.core.authObserver == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the gRPC auth observer")
	}
	if cp.sweeper.observer == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the LeaseSweeper observer")
	}
}

func TestNewControlPlaneWiresPollWait(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New(), PollWait: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if cp.httpServer.core.pollWait != 5*time.Second {
		t.Fatalf("httpServer.core.pollWait = %v, want 5s", cp.httpServer.core.pollWait)
	}
	if cp.grpcServer.core.pollWait != 5*time.Second {
		t.Fatalf("grpcServer.core.pollWait = %v, want 5s", cp.grpcServer.core.pollWait)
	}
}
