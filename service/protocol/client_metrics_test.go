package protocol

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientReportMetricsSendsRawProtobufNotJSON(t *testing.T) {
	var (
		gotMethod  string
		gotPath    string
		gotHeaders http.Header
		gotBody    []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotHeaders = r.Method, r.URL.Path, r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, srv.Client()).WithToken("secret-token")
	payload := []byte{0x1f, 0x8b, 0x00, 0x99}
	if err := client.ReportMetrics(context.Background(), "runner-a", "session-1", payload); err != nil {
		t.Fatalf("ReportMetrics: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	if gotPath != ReportMetricsPath {
		t.Errorf("path = %s, want %s", gotPath, ReportMetricsPath)
	}
	// Raw bytes, not base64-in-JSON: JSON-encoding protobuf costs a flat 33%.
	if !bytes.Equal(gotBody, payload) {
		t.Errorf("body = %v, want the payload verbatim %v", gotBody, payload)
	}
	if got := gotHeaders.Get("Content-Type"); got != ReportMetricsContentType {
		t.Errorf("Content-Type = %q, want %q", got, ReportMetricsContentType)
	}
	if got := gotHeaders.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", got)
	}
	if got := gotHeaders.Get(RunnerIDHeader); got != "runner-a" {
		t.Errorf("%s = %q, want runner-a", RunnerIDHeader, got)
	}
	if got := gotHeaders.Get(SessionIDHeader); got != "session-1" {
		t.Errorf("%s = %q, want session-1", SessionIDHeader, got)
	}
	if got := gotHeaders.Get("Authorization"); got != "Bearer secret-token" {
		t.Errorf("Authorization = %q, want the registration token", got)
	}
}

func TestClientReportMetricsSurfacesServerErrorWithoutEchoingTheToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, srv.Client()).WithToken("secret-token")
	err := client.ReportMetrics(context.Background(), "runner-a", "session-1", []byte("x"))
	if err == nil {
		t.Fatal("ReportMetrics must surface a non-2xx status as an error")
	}
	if !strings.Contains(err.Error(), "413") {
		t.Errorf("error %q should name the status code", err)
	}
	// Org policy §7: the bearer token is on the absolute log blacklist, and this
	// error string is what the reporter logs on failure.
	if strings.Contains(err.Error(), "secret-token") {
		t.Errorf("error string leaked the auth token: %q", err)
	}
}

func TestClientReportMetricsRejectsEmptyIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request should reach the server without a full identity")
	}))
	defer srv.Close()

	client := NewClient(srv.URL, srv.Client()).WithToken("t")
	for _, tc := range []struct{ runnerID, sessionID string }{
		{"", "session-1"},
		{"runner-a", ""},
	} {
		if err := client.ReportMetrics(context.Background(), tc.runnerID, tc.sessionID, []byte("x")); err == nil {
			t.Errorf("ReportMetrics(%q, %q) = nil, want an error", tc.runnerID, tc.sessionID)
		}
	}
}
