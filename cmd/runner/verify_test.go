package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestVerifyCommandRegistersAndHeartbeats(t *testing.T) {
	registeredReqs := make(chan protocol.RegisterRunnerRequest, 1)
	heartbeatReqs := make(chan protocol.HeartbeatRequest, 1)
	handlerErrs := make(chan error, 4)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			var registered protocol.RegisterRunnerRequest
			if err := json.NewDecoder(r.Body).Decode(&registered); err != nil {
				handlerErrs <- fmt.Errorf("decode register request: %w", err)
				http.Error(w, "bad register request", http.StatusBadRequest)
				return
			}
			registeredReqs <- registered
			if err := json.NewEncoder(w).Encode(protocol.RegisterRunnerResponse{RunnerID: registered.RunnerID}); err != nil {
				handlerErrs <- fmt.Errorf("encode register response: %w", err)
			}
		case protocol.HeartbeatPath:
			var heartbeat protocol.HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
				handlerErrs <- fmt.Errorf("decode heartbeat request: %w", err)
				http.Error(w, "bad heartbeat request", http.StatusBadRequest)
				return
			}
			heartbeatReqs <- heartbeat
			if err := json.NewEncoder(w).Encode(protocol.HeartbeatResponse{ServerTime: 1}); err != nil {
				handlerErrs <- fmt.Errorf("encode heartbeat response: %w", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var out bytes.Buffer
	cmd := newRootCommand(commandOptions{out: &out, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--server", server.URL,
		"--id", "runner-verify",
		"--concurrency", "2",
		"--cap", "xflow.function,xflow.http",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	close(handlerErrs)
	for err := range handlerErrs {
		if err != nil {
			t.Fatal(err)
		}
	}

	var registered protocol.RegisterRunnerRequest
	select {
	case registered = <-registeredReqs:
	default:
		t.Fatal("register request was not received")
	}
	if registered.RunnerID != "runner-verify" || registered.Concurrency != 2 {
		t.Fatalf("registered = %+v", registered)
	}
	if len(registered.Capabilities) != 2 {
		t.Fatalf("registered capabilities = %+v", registered.Capabilities)
	}

	var heartbeat protocol.HeartbeatRequest
	select {
	case heartbeat = <-heartbeatReqs:
	default:
		t.Fatal("heartbeat request was not received")
	}
	if heartbeat.RunnerID != "runner-verify" || heartbeat.Capacity != 2 {
		t.Fatalf("heartbeat = %+v", heartbeat)
	}
	if !strings.Contains(out.String(), "runner verified") {
		t.Fatalf("output = %q", out.String())
	}
}
