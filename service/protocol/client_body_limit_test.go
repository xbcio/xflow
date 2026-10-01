package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
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

// reportOfEncodedSize returns a report whose encoded request body, as post
// writes it, is exactly size bytes.
func reportOfEncodedSize(t *testing.T, size int) ReportResultRequest {
	t.Helper()
	build := func(pad int) ReportResultRequest {
		return ReportResultRequest{
			RunnerID:  "runner-1",
			SessionID: "s",
			Lease:     &engine.TaskLease{LeaseID: "lease-1", Task: engine.Task{ExecutionID: "exec-1", NodeName: "n"}},
			Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"pad": strings.Repeat("x", pad)}}},
		}
	}
	encodedLen := func(req ReportResultRequest) int {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(req); err != nil {
			t.Fatalf("encode report: %v", err)
		}
		return buf.Len()
	}
	pad := size - encodedLen(build(0))
	if pad < 0 {
		t.Fatalf("size %d too small for a report body", size)
	}
	req := build(pad)
	if got := encodedLen(req); got != size {
		t.Fatalf("report fixture encodes to %d bytes, want %d", got, size)
	}
	return req
}

func TestClientRefusesOversizeRequestBody(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		wantSent bool
	}{
		{"at limit", MaxRunnerRequestBodyBytes, true},
		{"one byte over limit", MaxRunnerRequestBodyBytes + 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sent atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sent.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = io.WriteString(w, `{"accepted":true}`)
			}))
			t.Cleanup(server.Close)

			_, err := NewClient(server.URL, server.Client()).ReportResult(context.Background(), reportOfEncodedSize(t, tt.size))
			if tt.wantSent {
				if err != nil || sent.Load() != 1 {
					t.Fatalf("ReportResult() with %d-byte body error = %v, requests = %d, want nil and 1", tt.size, err, sent.Load())
				}
				return
			}
			if !errors.Is(err, ErrRunnerRequestTooLarge) {
				t.Fatalf("ReportResult() with %d-byte body error = %v, want ErrRunnerRequestTooLarge", tt.size, err)
			}
			if sent.Load() != 0 {
				t.Fatalf("ReportResult() with %d-byte body sent %d requests, want 0", tt.size, sent.Load())
			}
		})
	}
}

func TestClientMapsStatus413ToRequestTooLarge(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = io.WriteString(w, `{"error":"runner request too large"}`)
	}))
	t.Cleanup(server.Close)

	_, err := NewClient(server.URL, server.Client()).ReportResult(context.Background(), reportOfEncodedSize(t, 1<<10))
	if !errors.Is(err, ErrRunnerRequestTooLarge) {
		t.Fatalf("ReportResult() against a 413 server error = %v, want ErrRunnerRequestTooLarge", err)
	}
}
