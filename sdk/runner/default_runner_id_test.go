package runner

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestDefaultRunnerIDIncludesHostname pins the multi-replica fix: a PID-only
// default collides across containers (every replica is usually PID 1), so the
// hostname — the pod name under Kubernetes — must be part of the default.
func TestDefaultRunnerIDIncludesHostname(t *testing.T) {
	got := defaultRunnerID()
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		if want := fmt.Sprintf("runner-%d", os.Getpid()); got != want {
			t.Fatalf("defaultRunnerID() = %q, want %q when hostname is unavailable", got, want)
		}
		return
	}
	if want := fmt.Sprintf("runner-%s-%d", host, os.Getpid()); got != want {
		t.Fatalf("defaultRunnerID() = %q, want %q", got, want)
	}
	if got := defaultRunnerConfig().runnerID; got != defaultRunnerID() {
		t.Fatalf("defaultRunnerConfig().runnerID = %q, want defaultRunnerID()", got)
	}
}
