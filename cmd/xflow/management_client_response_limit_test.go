package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// managementRawBodyServer answers every request with body verbatim.
func managementRawBodyServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestManagementClientResponseByteCeiling pins managementClient.do's 4 MiB
// response cap, the counterpart of TestAPIDeadLetterClientResponseByteCeiling
// for the shared client. The two clients buffer independently, so the runner
// and execution commands need their own guard: deleting the length check here
// leaves every other test in the package green.
//
// Both over-cap arms matter for the same reason as the dead-letter test: at
// cap+1 the guard is the only refusal (io.LimitReader reads one byte past the
// cap and the body still decodes), while far over the cap the reader truncates
// and a missing guard surfaces a misleading decode error instead of the size.
// The at-cap arm pins the comparison as `>`, not `>=`.
func TestManagementClientResponseByteCeiling(t *testing.T) {
	dataBytes := mustRawJSON(t, map[string]any{"runner_id": "r1"})

	t.Run("exactly at the ceiling decodes", func(t *testing.T) {
		body := envelopeOfExactly(t, managementCeilingBytes, dataBytes, "at-cap")
		srv := managementRawBodyServer(t, body)
		client := &managementClient{server: srv.URL, token: "tok-secret", httpClient: srv.Client()}

		var out map[string]any
		if err := client.do(context.Background(), http.MethodGet, "/v1/management/runners/r1", nil, nil, &out); err != nil {
			t.Fatalf("a response of exactly %d bytes was rejected: %v; the cap is `>`, "+
				"so the largest legal body must still decode", managementCeilingBytes, err)
		}
		if out["runner_id"] != "r1" {
			t.Fatalf("decoded body = %v, want the seeded runner_id: the at-cap body "+
				"must decode in full, not merely avoid an error", out)
		}
	})

	for _, tc := range []struct {
		name string
		size int
	}{
		{"one byte over the ceiling is refused by size", managementCeilingBytes + 1},
		{"far over the ceiling is refused by size, not by truncation", 2 * managementCeilingBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const marker = "over-cap-body-marker"
			body := envelopeOfExactly(t, tc.size, dataBytes, marker)
			srv := managementRawBodyServer(t, body)
			client := &managementClient{server: srv.URL, token: "tok-secret", httpClient: srv.Client()}

			var out map[string]any
			err := client.do(context.Background(), http.MethodGet, "/v1/management/runners/r1", nil, nil, &out)
			if err == nil {
				t.Fatalf("a %d-byte response was accepted; the CLI buffers the whole body "+
					"with io.ReadAll before decoding, so an unbounded server response is an "+
					"unbounded allocation in the operator's process", len(body))
			}
			if !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("error = %v, want one naming the size limit; an error that merely "+
					"reports missing JSON means the body was read past the cap and the "+
					"failure came from truncation rather than from the guard", err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error echoed the response body: %v", err)
			}
			if strings.Contains(err.Error(), "tok-secret") {
				t.Fatalf("error leaked the bearer token: %v", err)
			}
		})
	}
}
