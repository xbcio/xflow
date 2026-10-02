package protocol

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// seedBodyOfSize returns a well-formed body of exactly size bytes whose first
// field is the given prefix, so only its length decides whether the runtime
// accepts it.
func seedBodyOfSize(t *testing.T, prefix string, size int) string {
	t.Helper()
	const suffix = `"}`
	pad := size - len(prefix) - len(suffix)
	if pad < 0 {
		t.Fatalf("size %d too small for a seed body", size)
	}
	return prefix + strings.Repeat("x", pad) + suffix
}

func seedOnce(t *testing.T, status int, body string) (types.EntrySeedResponse, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client()}
	return rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{AdmissionKey: "ak-1"})
}

func TestHTTPEntrySeedCapsResponseBody(t *testing.T) {
	const accepted = `{"state":"accepted","execution_id":"exec-1","pad":"`
	const conflict = `{"state":"conflict","execution_id":"exec-1","pad":"`
	tests := []struct {
		name    string
		status  int
		prefix  string
		size    int
		wantErr bool
	}{
		{"accepted at limit", http.StatusOK, accepted, MaxRunnerResponseBodyBytes, false},
		{"accepted one byte over limit", http.StatusOK, accepted, MaxRunnerResponseBodyBytes + 1, true},
		{"conflict at limit", http.StatusConflict, conflict, MaxRunnerResponseBodyBytes, false},
		{"conflict one byte over limit", http.StatusConflict, conflict, MaxRunnerResponseBodyBytes + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := seedOnce(t, tt.status, seedBodyOfSize(t, tt.prefix, tt.size))
			if tt.wantErr {
				if !errors.Is(err, ErrRunnerResponseTooLarge) {
					t.Fatalf("SeedExecutionFromEntry() error = %v, want ErrRunnerResponseTooLarge", err)
				}
				// An oversized body must never be reported as handled, or the
				// caller would commit the offset.
				if resp.Accepted || resp.Conflict {
					t.Fatalf("SeedExecutionFromEntry() resp = %+v, want neither accepted nor conflict", resp)
				}
				return
			}
			if err != nil {
				t.Fatalf("SeedExecutionFromEntry() error = %v, want nil", err)
			}
			if !resp.Accepted && !resp.Conflict {
				t.Fatalf("SeedExecutionFromEntry() resp = %+v, want accepted or conflict", resp)
			}
		})
	}
}

func TestHTTPEntrySeedBoundsFenceReasonInError(t *testing.T) {
	body := `{"error":"` + strings.Repeat("e", 1<<20) + `"}`
	_, err := seedOnce(t, http.StatusConflict, body)
	if err == nil {
		t.Fatal("SeedExecutionFromEntry() error = nil, want fence rejection")
	}
	if n := strings.Count(err.Error(), "e"); n > maxRunnerErrorBodyBytes+64 {
		t.Fatalf("SeedExecutionFromEntry() error quotes %d bytes of reason, want at most %d", n, maxRunnerErrorBodyBytes)
	}
}
