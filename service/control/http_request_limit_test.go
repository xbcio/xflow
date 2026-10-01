package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestRunnerHTTPBodyLimitMatchesClientResponseLimit(t *testing.T) {
	if MaxRegisterRunnerBodyBytes != protocol.MaxRunnerResponseBodyBytes {
		t.Fatalf("MaxRegisterRunnerBodyBytes = %d, protocol.MaxRunnerResponseBodyBytes = %d, want equal",
			MaxRegisterRunnerBodyBytes, protocol.MaxRunnerResponseBodyBytes)
	}
}

// TestRunnerHTTPRequestBodiesAreCapped covers every runner-protocol route that
// decodes through decodeJSON. Register, metrics, and enroll carry their own
// caps and tests.
func TestRunnerHTTPRequestBodiesAreCapped(t *testing.T) {
	ts := httptest.NewServer(NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory()).Handler())
	t.Cleanup(ts.Close)

	// One byte over the cap, otherwise well-formed JSON, so only the size can
	// cause the rejection.
	const prefix, suffix = `{"runner_id":"runner-1","pad":"`, `"}`
	oversize := prefix + strings.Repeat("x", MaxRegisterRunnerBodyBytes+1-len(prefix)-len(suffix)) + suffix

	paths := []string{
		protocol.HeartbeatPath,
		protocol.PollTaskPath,
		protocol.ReportResultPath,
		protocol.RenewLeasePath,
		protocol.ActivationAckPath,
		protocol.RenewIdentityPath,
		protocol.DeregisterPath,
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			resp, err := ts.Client().Post(ts.URL+path, "application/json", strings.NewReader(oversize))
			if err != nil {
				t.Fatalf("POST %s error = %v", path, err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("POST %s with %d-byte body status = %d, want 413", path, len(oversize), resp.StatusCode)
			}
			var body errorResponse
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode %s error body: %v", path, err)
			}
			if body.Error != ErrRequestBodyTooLarge.Error() {
				t.Fatalf("POST %s error = %q, want %q", path, body.Error, ErrRequestBodyTooLarge.Error())
			}
		})
	}

	t.Run("malformed JSON stays 400", func(t *testing.T) {
		resp, err := ts.Client().Post(ts.URL+protocol.HeartbeatPath, "application/json", strings.NewReader("{"))
		if err != nil {
			t.Fatalf("POST error = %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("POST malformed heartbeat status = %d, want 400", resp.StatusCode)
		}
	})
}
