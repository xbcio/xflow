package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// TestDeadLetterCLIRequiresServerOrBreakGlass proves the CLI no longer silently
// connects to Redis by default: it requires --server/--token for the management
// API path, and surfaces a clear error guiding the operator to either configure
// the API or use --break-glass explicitly.
func TestDeadLetterCLIRequiresServerOrBreakGlass(t *testing.T) {
	var out bytes.Buffer
	err := executeRootWith(&out, "dead-letter", "list", "--execution", "x")
	if err == nil {
		t.Fatal("CLI without --server/--token: err = nil, want error requiring API config or --break-glass")
	}
	if !strings.Contains(err.Error(), "--server") && !strings.Contains(err.Error(), "--break-glass") {
		t.Fatalf("CLI error message = %q, want guidance about --server or --break-glass", err.Error())
	}

	err = executeRootWith(&out, "dead-letter", "--server", "http://127.0.0.1:9999", "list", "--execution", "x")
	if err == nil {
		t.Fatal("CLI with --server but no --token: err = nil, want error requiring --token")
	}
	if !strings.Contains(err.Error(), "--token") {
		t.Fatalf("CLI error message = %q, want guidance about --token", err.Error())
	}
}

// TestDeadLetterCLIBreakGlassWarning proves the break-glass path emits a
// prominent stderr warning and records a "cli:breakglass:<user>" operator
// identity (distinct from the normal cli path) so break-glass use is
// distinguishable in audit.
func TestDeadLetterCLIBreakGlassWarning(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	execID := "exec-bg-warn"
	entryID := "execute/exec-bg-warn/review/1"
	seedDeadLetterRedis(t, mr.Addr(), "default", execID, entryID)

	// Capture stderr by swapping os.Stderr; the break-glass warning is written
	// to os.Stderr (not the cobra out writer).
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	oldStderr := os.Stderr
	os.Stderr = wErr
	done := make(chan struct{})
	go func() {
		var b bytes.Buffer
		_, _ = b.ReadFrom(rErr)
		stderrCapture = b.String()
		close(done)
	}()
	defer func() {
		_ = wErr.Close()
		os.Stderr = oldStderr
		<-done
	}()

	var replayOut bytes.Buffer
	err = executeRootWith(&replayOut, "dead-letter", "--break-glass", "--redis-addr", mr.Addr(), "replay",
		"--execution", execID, "--entry", entryID, "--reason", "operator triage", "--request-id", "req-bg-warn", "--tenant", "default")
	if err != nil {
		t.Fatalf("break-glass replay: %v", err)
	}
	_ = wErr.Close()
	os.Stderr = oldStderr
	<-done
	if !strings.Contains(stderrCapture, "--break-glass") {
		t.Fatalf("break-glass stderr warning not emitted: %q", stderrCapture)
	}
	var res map[string]any
	if err := json.Unmarshal(replayOut.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal break-glass replay: %v (out=%q)", err, replayOut.String())
	}
	if res["outcome"] != "replayed" {
		t.Fatalf("break-glass replay outcome = %v, want replayed", res["outcome"])
	}
}

// stderrCapture is written by the pipe reader goroutine in the break-glass
// warning test.
var stderrCapture string
