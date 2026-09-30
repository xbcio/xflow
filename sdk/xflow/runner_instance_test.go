package xflow

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestDefaultRunnerInstanceUIDPrefersPodUID(t *testing.T) {
	t.Setenv("POD_UID", " 3f0c-uid ")
	if got := DefaultRunnerInstanceUID(); got != "pod:3f0c-uid" {
		t.Fatalf("DefaultRunnerInstanceUID() = %q, want pod:3f0c-uid", got)
	}
	if got := runnerInstanceUIDOrDefault(" explicit "); got != "explicit" {
		t.Fatalf("explicit uid = %q, want it preserved", got)
	}
	if got := runnerInstanceUIDOrDefault(""); got != "pod:3f0c-uid" {
		t.Fatalf("empty uid = %q, want the default", got)
	}
}

func TestDefaultRunnerInstanceUIDFallsBackToHostname(t *testing.T) {
	t.Setenv("POD_UID", "")
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		t.Skip("hostname unavailable")
	}
	if got := DefaultRunnerInstanceUID(); got != "host:"+strings.TrimSpace(host) {
		t.Fatalf("DefaultRunnerInstanceUID() = %q, want host:%s", got, host)
	}
}

func TestDefaultRunnerInstanceUIDFallsBackToProcessUUID(t *testing.T) {
	t.Setenv("POD_UID", "")
	oldHostname := runnerHostname
	oldUID := processRunnerInstanceUID
	runnerHostname = func() (string, error) { return "", errors.New("unavailable") }
	processRunnerInstanceUIDOnce = sync.Once{}
	processRunnerInstanceUID = ""
	t.Cleanup(func() {
		runnerHostname = oldHostname
		processRunnerInstanceUIDOnce = sync.Once{}
		processRunnerInstanceUID = oldUID
	})

	first := DefaultRunnerInstanceUID()
	second := DefaultRunnerInstanceUID()
	if !strings.HasPrefix(first, "proc:") || strings.TrimPrefix(first, "proc:") == "" {
		t.Fatalf("fallback UID = %q, want proc:<uuid>", first)
	}
	if second != first {
		t.Fatalf("process fallback changed from %q to %q", first, second)
	}
}
