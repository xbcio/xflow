package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// pollBodyOfSize returns a well-formed PollTaskResponse body of exactly size
// bytes, so only its length decides whether the client accepts it.
func pollBodyOfSize(t *testing.T, size int) string {
	t.Helper()
	const prefix, suffix = `{"wait":1,"pad":"`, `"}`
	pad := size - len(prefix) - len(suffix)
	if pad < 0 {
		t.Fatalf("size %d too small for a poll body", size)
	}
	return prefix + strings.Repeat("x", pad) + suffix
}

func TestClientCapsResponseBody(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"at limit", MaxRunnerResponseBodyBytes, false},
		{"one byte over limit", MaxRunnerResponseBodyBytes + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := pollBodyOfSize(t, tt.size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			t.Cleanup(server.Close)

			got, err := NewClient(server.URL, server.Client()).Poll(context.Background(), PollTaskRequest{RunnerID: "runner-1", SessionID: "s", Capacity: 1})
			if tt.wantErr {
				if !errors.Is(err, ErrRunnerResponseTooLarge) {
					t.Fatalf("Poll() with %d-byte body error = %v, want ErrRunnerResponseTooLarge", tt.size, err)
				}
				if !strings.Contains(err.Error(), PollTaskPath) || !strings.Contains(err.Error(), fmt.Sprint(MaxRunnerResponseBodyBytes)) {
					t.Errorf("Poll() error = %q, want the path and limit named", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Poll() with %d-byte body error = %v, want nil", tt.size, err)
			}
			if got.Wait != 1 {
				t.Fatalf("Poll() wait = %v, want 1", got.Wait)
			}
		})
	}
}

func TestClientBoundsErrorBodyInError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, strings.Repeat("e", 1<<20))
	}))
	t.Cleanup(server.Close)

	_, err := NewClient(server.URL, server.Client()).Heartbeat(context.Background(), HeartbeatRequest{RunnerID: "runner-1"})
	if err == nil {
		t.Fatal("Heartbeat() error = nil, want status error")
	}
	if n := strings.Count(err.Error(), "e"); n > maxRunnerErrorBodyBytes+64 {
		t.Fatalf("Heartbeat() error quotes %d bytes of body, want at most %d", n, maxRunnerErrorBodyBytes)
	}
}
