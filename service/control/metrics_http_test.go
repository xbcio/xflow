package control

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xbcio/xflow/service/protocol"
)

// newMetricsEndpointFixture wires a real Server over a real Core with a real
// inbox, then registers one runner so its session id is genuine. Nothing here
// assigns to a dependency field directly: a probe that stuffs the field it is
// meant to prove would pass even if the production wiring were absent.
func newMetricsEndpointFixture(t *testing.T) (http.Handler, *MetricsInbox, RunnerSession) {
	t.Helper()
	directory := NewMemoryRunnerDirectory()
	at := time.Unix(1754000000, 0).UTC()
	session, err := directory.Register(context.Background(), RegisterRunnerRequest{
		RunnerID: "runner-a", Capacity: 1, Now: at,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	// Store and inbox must share one clock: the store evicts by the stamp the
	// inbox writes.
	now := func() time.Time { return at }
	inbox := NewMetricsInbox(MetricsInboxConfig{
		Store: NewMemoryMetricsStoreWith(DefaultMetricsRetention, now),
		Self:  prometheus.NewRegistry(),
		Live:  NewDirectoryLiveness(directory, DefaultRunnerSelector()),
		Now:   now,
	})
	return newTestMetricsServer(t, directory, inbox).Handler(), inbox, session
}

func postMetrics(t *testing.T, h http.Handler, runnerID, sessionID, token string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, protocol.ReportMetricsPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", protocol.ReportMetricsContentType)
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set(protocol.RunnerIDHeader, runnerID)
	req.Header.Set(protocol.SessionIDHeader, sessionID)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReportMetricsAcceptsAuthenticatedRunner(t *testing.T) {
	h, inbox, session := newMetricsEndpointFixture(t)
	body := encodeFamilies(t, counterFamily("probe_total", "h", 1))

	rec := postMetrics(t, h, "runner-a", session.SessionID, "runner-a-token", body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body: %s", rec.Code, rec.Body.String())
	}

	fams, err := inbox.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(fams) != 1 || fams[0].GetName() != "probe_total" {
		t.Fatalf("inbox holds %d families, want the reported one", len(fams))
	}
}

func TestReportedRunnerIDIsIgnoredInFavorOfAuthenticated(t *testing.T) {
	// The single guard for the design's security constraint: any runner holding
	// a valid token must not be able to write another runner's series and point
	// alerts at an innocent instance.
	h, inbox, session := newMetricsEndpointFixture(t)
	body := encodeFamilies(t, counterFamily("probe_total", "h", 1, "runner_id", "victim"))

	rec := postMetrics(t, h, "runner-a", session.SessionID, "runner-a-token", body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body: %s", rec.Code, rec.Body.String())
	}

	fams, err := inbox.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(fams) != 1 {
		t.Fatalf("got %d families, want 1", len(fams))
	}
	for _, m := range fams[0].Metric {
		for _, lp := range m.Label {
			if lp.GetName() == "runner_id" && lp.GetValue() != "runner-a" {
				t.Errorf("runner_id = %q, want the authenticated runner-a", lp.GetValue())
			}
		}
	}
}

func TestReportMetricsRejectsStaleSession(t *testing.T) {
	h, inbox, session := newMetricsEndpointFixture(t)
	body := encodeFamilies(t, counterFamily("probe_total", "h", 1))

	rec := postMetrics(t, h, "runner-a", session.SessionID+"-old", "runner-a-token", body)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 for a superseded session; body: %s", rec.Code, rec.Body.String())
	}
	fams, err := inbox.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(fams) != 0 {
		t.Errorf("a zombie session's report must not reach the inbox, got %d families", len(fams))
	}
}

func TestReportMetricsRejectsBadToken(t *testing.T) {
	h, _, session := newMetricsEndpointFixture(t)
	rec := postMetrics(t, h, "runner-a", session.SessionID, "wrong-token",
		encodeFamilies(t, counterFamily("probe_total", "h", 1)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	// Org policy §7: the token is on the absolute log blacklist, and the response
	// body is the most easily-leaked surface there is.
	if strings.Contains(rec.Body.String(), "wrong-token") {
		t.Errorf("response echoed the token: %s", rec.Body.String())
	}
}

func TestReportMetricsRejectsMissingIdentityHeaders(t *testing.T) {
	h, _, session := newMetricsEndpointFixture(t)
	body := encodeFamilies(t, counterFamily("probe_total", "h", 1))
	for _, tc := range []struct{ name, runnerID, sessionID string }{
		{"no runner id", "", session.SessionID},
		{"no session id", "runner-a", ""},
	} {
		rec := postMetrics(t, h, tc.runnerID, tc.sessionID, "runner-a-token", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", tc.name, rec.Code)
		}
	}
}

func TestReportMetricsRejectsOversizedPayload(t *testing.T) {
	h, inbox, session := newMetricsEndpointFixture(t)
	oversized := bytes.Repeat([]byte{0x1f}, protocol.MaxRunnerMetricsBytes+1)

	rec := postMetrics(t, h, "runner-a", session.SessionID, "runner-a-token", oversized)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	fams, err := inbox.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(fams) != 0 {
		t.Errorf("an oversized report must not be stored, got %d families", len(fams))
	}
}

func TestReportMetricsRejectsUnsupportedEncoding(t *testing.T) {
	h, _, session := newMetricsEndpointFixture(t)
	req := httptest.NewRequest(http.MethodPost, protocol.ReportMetricsPath,
		bytes.NewReader([]byte("plain")))
	req.Header.Set("Content-Type", protocol.ReportMetricsContentType)
	// No Content-Encoding: gzip.
	req.Header.Set(protocol.RunnerIDHeader, "runner-a")
	req.Header.Set(protocol.SessionIDHeader, session.SessionID)
	req.Header.Set("Authorization", "Bearer runner-a-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", rec.Code)
	}
}

func TestReportMetricsRejectsGET(t *testing.T) {
	h, _, _ := newMetricsEndpointFixture(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, protocol.ReportMetricsPath, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestReportMetricsWithoutInboxIsUnavailable(t *testing.T) {
	// A server built without the proxy must say so rather than 404 (which reads
	// as "wrong URL") or 500 (which reads as "broken").
	directory := NewMemoryRunnerDirectory()
	at := time.Unix(1754000000, 0).UTC()
	session, err := directory.Register(context.Background(), RegisterRunnerRequest{
		RunnerID: "runner-a", Capacity: 1, Now: at,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	h := newTestMetricsServer(t, directory, nil).Handler()
	rec := postMetrics(t, h, "runner-a", session.SessionID, "runner-a-token", []byte("x"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// newTestMetricsServer builds a control server the same way newAckTestServer
// does, with a real single-entry file policy so token checks are genuine rather
// than disabled.
func newTestMetricsServer(t *testing.T, directory RunnerDirectory, inbox *MetricsInbox) *Server {
	t.Helper()
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:     "metrics-runner",
			IDPrefix: "runner-",
			Token:    "runner-a-token",
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(&fakeControlEngine{}, directory, WithAuthenticator(store))
	srv.core.metricsInbox = inbox
	return srv
}
