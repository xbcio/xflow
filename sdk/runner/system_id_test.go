package runner

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultRunnerSystemIDFallbacks(t *testing.T) {
	t.Run("pod name", func(t *testing.T) {
		t.Setenv("POD_NAME", " pod-a ")
		if got := defaultRunnerSystemID(); got != "pod-a" {
			t.Fatalf("defaultRunnerSystemID() = %q, want pod-a", got)
		}
	})

	t.Run("hostname", func(t *testing.T) {
		t.Setenv("POD_NAME", "")
		hostname, err := os.Hostname()
		if err != nil || strings.TrimSpace(hostname) == "" {
			t.Skip("hostname unavailable")
		}
		if got, want := defaultRunnerSystemID(), strings.TrimSpace(hostname); got != want {
			t.Fatalf("defaultRunnerSystemID() = %q, want %q", got, want)
		}
	})
}

func TestRunnerSystemIDPrecedence(t *testing.T) {
	t.Setenv("POD_NAME", "pod-fallback")
	oldSystemID, hadSystemID := os.LookupEnv("XFLOW_RUNNER_SYSTEM_ID")
	if err := os.Unsetenv("XFLOW_RUNNER_SYSTEM_ID"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if hadSystemID {
			_ = os.Setenv("XFLOW_RUNNER_SYSTEM_ID", oldSystemID)
		} else {
			_ = os.Unsetenv("XFLOW_RUNNER_SYSTEM_ID")
		}
	})

	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte("runner:\n  system_id: file-system\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	fileCfg, err := loadRunnerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if fileCfg.systemID != "file-system" {
		t.Fatalf("file systemID = %q, want file-system", fileCfg.systemID)
	}

	t.Setenv("XFLOW_RUNNER_SYSTEM_ID", "env-system")
	base := defaultRunnerConfig()
	base.configPath = path
	base.allowPlaintext = true
	base.changed = map[string]bool{"allow-plaintext": true}
	envCfg, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if envCfg.systemID != "env-system" {
		t.Fatalf("env systemID = %q, want env-system", envCfg.systemID)
	}

	var ran runnerConfig
	err = executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--config", path, "--system-id", "flag-system", "--allow-plaintext")
	if err != nil {
		t.Fatal(err)
	}
	if ran.systemID != "flag-system" {
		t.Fatalf("flag systemID = %q, want flag-system", ran.systemID)
	}
}
