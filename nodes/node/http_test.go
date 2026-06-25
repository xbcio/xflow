package node_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func withHTTPClient(t *testing.T, fn roundTripFunc) {
	t.Helper()
	orig := node.DefaultHTTPClient
	node.DefaultHTTPClient = &http.Client{Transport: fn}
	t.Cleanup(func() { node.DefaultHTTPClient = orig })
}

func jsonResponse(status int, body string, headers http.Header) *http.Response {
	if headers == nil {
		headers = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     headers,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func TestHTTP_Factory(t *testing.T) {
	b := node.HTTP("POST", "https://api.example.com/users").
		SetHeaders(map[string]any{"X-Token": "abc"}).
		SetBody(map[string]any{"name": "test"}).
		SetQuery(map[string]any{"page": "1"}).
		Auth("my_cred").
		Timeout("10s")

	if b.NodeType() != "xflow.http" {
		t.Fatalf("expected xflow.http, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["method"] != "POST" {
		t.Fatalf("expected POST, got %v", params["method"])
	}
	if params["authentication"] != "my_cred" {
		t.Fatalf("expected auth=my_cred, got %v", params["authentication"])
	}
	opts := params["options"].(map[string]any)
	if opts["timeout"] != "10s" {
		t.Fatalf("expected timeout=10s, got %v", opts["timeout"])
	}
}

func TestHTTP_GET(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}
		if r.URL.Query().Get("page") != "1" {
			t.Fatalf("expected query page=1, got %s", r.URL.Query().Get("page"))
		}
		return jsonResponse(200, `{"ok":true}`, http.Header{"Content-Type": []string{"application/json"}}), nil
	})

	h, _ := node.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetQuery(map[string]any{"page": "1"})
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("expected port \"main\", got %q", out.Port)
	}
	if out.Data["status"] != 200 {
		t.Fatalf("expected status 200, got %v", out.Data["status"])
	}
	body, _ := out.Data["body"].(map[string]any)
	if body["ok"] != true {
		t.Fatalf("expected body.ok=true, got %v", body)
	}
}

func TestHTTP_POST_WithBody(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("expected json content-type, got %s", r.Header.Get("Content-Type"))
		}
		return jsonResponse(201, `{"id":42}`, nil), nil
	})

	h, _ := node.Lookup("xflow.http")
	b := node.HTTP("POST", "https://example.test").SetBody(map[string]any{"name": "test"})
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["status"] != 201 {
		t.Fatalf("expected status 201, got %v", out.Data["status"])
	}
}

func TestHTTP_ErrorPort_On4xx(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(404, `{"error":"not found"}`, nil), nil
	})

	h, _ := node.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test")
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("expected port \"error\", got %q", out.Port)
	}
}

func TestHTTP_CustomHeaders(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-Custom") != "hello" {
			t.Fatalf("expected X-Custom=hello, got %s", r.Header.Get("X-Custom"))
		}
		return jsonResponse(200, `{}`, nil), nil
	})

	h, _ := node.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetHeaders(map[string]any{"X-Custom": "hello"})
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	_, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTP_MissingURL(t *testing.T) {
	h, _ := node.Lookup("xflow.http")
	input := &node.Input{
		Params: map[string]any{"method": "GET"},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing url")
	}
}
