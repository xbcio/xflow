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

// TestVerifyCommandPrintsResolvedRunnerID pins that `verify`'s success
// message names the runner it actually registered as.
// TestVerifyCommandRegistersAndHeartbeats only greps for "runner verified",
// which stays present even if the ID were dropped from the message entirely,
// so an operator verifying several runners from a script could not tell which
// one the success line refers to.
//
// --id is deliberately NOT passed: with the flag set, the printed ID and the
// expected ID would both be the string the test itself supplied, and the
// assertion could not distinguish "prints the resolved ID" from "echoes the
// flag". Letting resolveRunnerConfig derive the default ID means the test has
// no way to know the value up front, so it has to take it from what the server
// was actually told — which also pins that the ID printed to the operator is
// the same one now holding a registration on the server.
func TestVerifyCommandPrintsResolvedRunnerID(t *testing.T) {
	var registeredID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			var req protocol.RegisterRunnerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode register request: %v", err)
			}
			registeredID = req.RunnerID
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"runner_id":"` + req.RunnerID + `"}`))
		case protocol.HeartbeatPath:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"server_time":1}`))
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
		// --transport http is required, not incidental: the default is grpc
		// (defaultRunnerConfig), and verify now honours it. Before it did,
		// this test passed while probing a transport the runner would not
		// have used.
		"--transport", "http",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	if registeredID == "" {
		t.Fatal("server received an empty runner id on register; nothing was resolved")
	}
	want := "runner verified: " + registeredID + " (supply encryption: false)\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

// TestVerifyCommandFailsWhenRequireSupplyEncryptionIsUnmet is the preflight
// counterpart to lifecycle.go's runtime check: --require-supply-encryption
// must fail verify on an operator's terminal, not surface only after the
// runner is already deployed and heartbeating against a control plane that
// never issued a key.
func TestVerifyCommandFailsWhenRequireSupplyEncryptionIsUnmet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			var req protocol.RegisterRunnerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode register request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			// No supply_key: the control plane in this test never issues one.
			_, _ = w.Write([]byte(`{"runner_id":"` + req.RunnerID + `"}`))
		case protocol.HeartbeatPath:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"server_time":1}`))
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
		"--transport", "http",
		"--id", "runner-require-supply",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
		"--require-supply-encryption",
	})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("cmd.Execute() = nil, want an error: the control plane issued no supply key")
	}
	if !strings.Contains(err.Error(), "--require-supply-encryption") {
		t.Fatalf("error = %q, want it to mention --require-supply-encryption", err.Error())
	}
}

// TestVerifyCommandPassesWhenRequireSupplyEncryptionIsMet is the passing
// counterpart to TestVerifyCommandFailsWhenRequireSupplyEncryptionIsUnmet.
// Without it, the requireSupplyEncryption branch in verifyRunner could
// degrade to "fail whenever the flag is set" — ignoring res.SupplyKeyIssued
// entirely — and the failing-direction test alone would not catch it: it
// never exercises a control plane that actually issues a key.
func TestVerifyCommandPassesWhenRequireSupplyEncryptionIsMet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			var req protocol.RegisterRunnerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode register request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"runner_id":"` + req.RunnerID + `","supply_key":"c3VwcGx5LWtleQ=="}`))
		case protocol.HeartbeatPath:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"server_time":1}`))
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
		"--transport", "http",
		"--id", "runner-require-supply-met",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
		"--require-supply-encryption",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("cmd.Execute() = %v, want nil: the control plane issued a supply key", err)
	}
	want := "runner verified: runner-require-supply-met (supply encryption: true)\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}
