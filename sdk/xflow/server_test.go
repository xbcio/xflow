package xflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
)

func TestNewServerMemoryBackendServesHandler(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	req := httptest.NewRequest(http.MethodPost, control.SubmitWorkflowPath, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatalf("Handler() did not route %s, got 404", control.SubmitWorkflowPath)
	}
}

func TestNewServerShutdownIsGraceful(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestNewServerMountableOnHostMux(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/xflow/", http.StripPrefix("/xflow", srv.Handler()))

	req := httptest.NewRequest(http.MethodPost, "/xflow"+control.SubmitWorkflowPath, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatal("host mux did not route through mounted xflow handler")
	}
}
