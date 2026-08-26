package runner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPSupplyFetcher_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/v1/supplies/shared-rules") {
			t.Errorf("path = %s, want suffix /v1/supplies/shared-rules", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok123" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok123")
		}
		w.Header().Set("ETag", "sha256:abcdef1234567890")
		w.Header().Set("X-Supply-Revision", "42")
		_, _ = w.Write([]byte(`{"rules":[]}`))
	}))
	defer srv.Close()

	f := &HTTPSupplyFetcher{BaseURL: srv.URL, Token: "tok123", Client: srv.Client()}
	content, hash, revision, err := f.Fetch(context.Background(), "shared-rules")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(content) != `{"rules":[]}` {
		t.Errorf("content = %q", content)
	}
	if hash != "sha256:abcdef1234567890" {
		t.Errorf("hash = %q", hash)
	}
	if revision != 42 {
		t.Errorf("revision = %d, want 42", revision)
	}
}

func TestHTTPSupplyFetcher_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	f := &HTTPSupplyFetcher{BaseURL: srv.URL, Client: srv.Client()}
	_, _, _, err := f.Fetch(context.Background(), "no-such")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if err != ErrSupplyNotFound {
		t.Fatalf("err = %v, want ErrSupplyNotFound", err)
	}
}

func TestHTTPSupplyFetcher_PathEscapes(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RawPath
		if gotPath == "" {
			gotPath = r.URL.Path
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	f := &HTTPSupplyFetcher{BaseURL: srv.URL, Client: srv.Client()}
	_, _, _, _ = f.Fetch(context.Background(), "a/b?c=d")
	// url.PathEscape("a/b?c=d") = "a%2Fb%3Fc=d" — must be one segment,
	// slashes and question marks escaped so they don't alter routing.
	if !strings.HasSuffix(gotPath, "/v1/supplies/a%2Fb%3Fc=d") {
		t.Errorf("path = %q, want path-escaped name as single segment", gotPath)
	}
}

func TestHTTPSupplyFetcher_BodyTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", "h")
		// Write more than maxSupplyResponseBytes (1 MiB).
		big := make([]byte, maxSupplyResponseBytes+1)
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	f := &HTTPSupplyFetcher{BaseURL: srv.URL, Client: srv.Client()}
	_, _, _, err := f.Fetch(context.Background(), "huge")
	if err == nil {
		t.Fatal("expected error for oversized body")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want 'exceeds' mention", err)
	}
}

// TestHTTPSupplyFetcher_EmptyName pins the guard that rejects an empty name
// before any request is built.
//
// It asserts the guard's own message rather than "err != nil". BaseURL here
// never resolves, so deleting the guard entirely still produces an error — a
// DNS failure — and a bare non-nil check passes for a fetcher that happily
// builds "<base>/v1/supplies/" and asks the server for the empty supply. The
// message is the only thing that distinguishes the guard from the network.
func TestHTTPSupplyFetcher_EmptyName(t *testing.T) {
	f := &HTTPSupplyFetcher{BaseURL: "http://unused"}
	_, _, _, err := f.Fetch(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if !strings.Contains(err.Error(), "empty name") {
		t.Fatalf("err = %v, want the empty-name guard to reject it before the request "+
			"is built; this error came from somewhere else", err)
	}
}
