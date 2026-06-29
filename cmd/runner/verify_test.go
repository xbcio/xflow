package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestVerifyCommandRegistersAndHeartbeats(t *testing.T) {
	var registered protocol.RegisterRunnerRequest
	var heartbeat protocol.HeartbeatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			if err := json.NewDecoder(r.Body).Decode(&registered); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(protocol.RegisterRunnerResponse{RunnerID: registered.RunnerID})
		case protocol.HeartbeatPath:
			if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(protocol.HeartbeatResponse{ServerTime: 1})
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
	if registered.RunnerID != "runner-verify" || registered.Concurrency != 2 {
		t.Fatalf("registered = %+v", registered)
	}
	if len(registered.Capabilities) != 2 {
		t.Fatalf("registered capabilities = %+v", registered.Capabilities)
	}
	if heartbeat.RunnerID != "runner-verify" || heartbeat.Capacity != 2 {
		t.Fatalf("heartbeat = %+v", heartbeat)
	}
	if !strings.Contains(out.String(), "runner verified") {
		t.Fatalf("output = %q", out.String())
	}
}
