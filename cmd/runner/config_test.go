package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRunnerConfigFromYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  id: file-runner
  concurrency: 3
  capabilities:
    - xflow.http
server:
  url: http://file-server:8080
poll:
  wait: 2s
heartbeat:
  interval: 7s
logging:
  level: debug
  format: json
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadRunnerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.serverURL != "http://file-server:8080" || cfg.runnerID != "file-runner" || cfg.concurrency != 3 {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.capRaw != "xflow.http" {
		t.Fatalf("capRaw = %q", cfg.capRaw)
	}
	if cfg.pollWait != "2s" || cfg.heartbeatInterval != "7s" || cfg.logLevel != "debug" || cfg.logFormat != "json" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestRunCommandUsesConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  id: file-runner
  concurrency: 3
  capabilities:
    - xflow.http
server:
  url: http://file-server:8080
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var ran runnerConfig
	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{"run", "--config", path})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if ran.serverURL != "http://file-server:8080" || ran.runnerID != "file-runner" || ran.concurrency != 3 {
		t.Fatalf("config = %+v", ran)
	}
	if len(ran.capabilities) != 1 || ran.capabilities[0].NodeType != "xflow.http" {
		t.Fatalf("capabilities = %+v", ran.capabilities)
	}
}

func TestEnvOverridesFileConfig(t *testing.T) {
	cfg := runnerConfig{
		serverURL:         "http://file-server:8080",
		runnerID:          "file-runner",
		concurrency:       2,
		capRaw:            "xflow.http",
		heartbeatInterval: "5s",
		pollWait:          "1s",
		logLevel:          "info",
		logFormat:         "text",
	}
	env := map[string]string{
		"XFLOW_RUNNER_SERVER":             "http://env-server:8080",
		"XFLOW_RUNNER_ID":                 "env-runner",
		"XFLOW_RUNNER_CONCURRENCY":        "4",
		"XFLOW_RUNNER_CAP":                "xflow.function,xflow.http",
		"XFLOW_RUNNER_HEARTBEAT_INTERVAL": "9s",
		"XFLOW_RUNNER_POLL_WAIT":          "3s",
		"XFLOW_RUNNER_LOG_LEVEL":          "warn",
		"XFLOW_RUNNER_LOG_FORMAT":         "json",
	}
	got, err := applyEnvOverrides(cfg, func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if got.serverURL != "http://env-server:8080" || got.runnerID != "env-runner" || got.concurrency != 4 {
		t.Fatalf("config = %+v", got)
	}
	if got.capRaw != "xflow.function,xflow.http" || got.heartbeatInterval != "9s" || got.pollWait != "3s" {
		t.Fatalf("config = %+v", got)
	}
	if got.logLevel != "warn" || got.logFormat != "json" {
		t.Fatalf("config = %+v", got)
	}
}

func TestResolveRunnerConfigPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  id: file-runner
  concurrency: 2
  capabilities:
    - xflow.http
server:
  url: http://file-server:8080
poll:
  wait: 2s
heartbeat:
  interval: 7s
logging:
  level: debug
  format: json
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("XFLOW_RUNNER_SERVER", "http://env-server:8080")
	t.Setenv("XFLOW_RUNNER_ID", "env-runner")
	t.Setenv("XFLOW_RUNNER_CONCURRENCY", "4")
	t.Setenv("XFLOW_RUNNER_CAP", "xflow.function,xflow.http")
	t.Setenv("XFLOW_RUNNER_HEARTBEAT_INTERVAL", "9s")
	t.Setenv("XFLOW_RUNNER_POLL_WAIT", "3s")
	t.Setenv("XFLOW_RUNNER_LOG_LEVEL", "warn")
	t.Setenv("XFLOW_RUNNER_LOG_FORMAT", "text")

	base := defaultRunnerConfig()
	base.configPath = path
	base.serverURL = "http://cli-server:8080"
	base.runnerID = "cli-runner"
	base.concurrency = 6
	base.capRaw = "xflow.function"
	base.heartbeatInterval = "11s"
	base.pollWait = "4s"
	base.logLevel = "error"
	base.logFormat = "json"
	base.changed = map[string]bool{
		"server":             true,
		"id":                 true,
		"concurrency":        true,
		"cap":                true,
		"heartbeat-interval": true,
		"poll-wait":          true,
		"log-level":          true,
		"log-format":         true,
	}

	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.serverURL != "http://cli-server:8080" || got.runnerID != "cli-runner" || got.concurrency != 6 {
		t.Fatalf("config = %+v", got)
	}
	if got.capRaw != "xflow.function" || got.heartbeatInterval != "11s" || got.pollWait != "4s" {
		t.Fatalf("config = %+v", got)
	}
	if got.logLevel != "error" || got.logFormat != "json" {
		t.Fatalf("config = %+v", got)
	}
	if len(got.capabilities) != 1 || got.capabilities[0].NodeType != "xflow.function" {
		t.Fatalf("capabilities = %+v", got.capabilities)
	}
}

func TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  concurrency: 0
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	base := defaultRunnerConfig()
	base.configPath = path

	_, err := resolveRunnerConfig(base)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("error = %v, want containing %q", err, "concurrency")
	}
}

func TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  concurrency: 3
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("XFLOW_RUNNER_CONCURRENCY", "bad")

	base := defaultRunnerConfig()
	base.configPath = path

	_, err := resolveRunnerConfig(base)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("error = %v, want containing %q", err, "concurrency")
	}
}

func TestResolveRunnerConfigUsesCLIConcurrencyWhenEnvConcurrencyIsInvalid(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_CONCURRENCY", "bad")

	base := defaultRunnerConfig()
	base.concurrency = 2
	base.changed = map[string]bool{
		"concurrency": true,
	}

	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.concurrency != 2 {
		t.Fatalf("concurrency = %d, want 2", got.concurrency)
	}
}

func TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "runner id",
			data: `
runner:
  id: ""
`,
			want: "runner id",
		},
		{
			name: "server url",
			data: `
server:
  url: ""
`,
			want: "server URL",
		},
		{
			name: "poll wait",
			data: `
poll:
  wait: ""
`,
			want: "poll wait",
		},
		{
			name: "log level",
			data: `
logging:
  level: ""
`,
			want: "log level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runner.yaml")
			if err := os.WriteFile(path, []byte(tt.data), 0o600); err != nil {
				t.Fatal(err)
			}

			base := defaultRunnerConfig()
			base.configPath = path

			_, err := resolveRunnerConfig(base)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  capabilities: []
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			t.Fatalf("run should not execute, got %+v", cfg)
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{"run", "--config", path})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "capabilities") {
		t.Fatalf("error = %v, want containing %q", err, "capabilities")
	}
}

func TestValidateRunnerConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*runnerConfig)
		want string
	}{
		{name: "server", mut: func(c *runnerConfig) { c.serverURL = "localhost:8080" }, want: "server URL"},
		{name: "id", mut: func(c *runnerConfig) { c.runnerID = "" }, want: "runner id"},
		{name: "concurrency", mut: func(c *runnerConfig) { c.concurrency = 0 }, want: "concurrency"},
		{name: "capabilities", mut: func(c *runnerConfig) { c.capRaw = "," }, want: "capabilities"},
		{name: "heartbeat", mut: func(c *runnerConfig) { c.heartbeatInterval = "-1s" }, want: "heartbeat interval"},
		{name: "poll", mut: func(c *runnerConfig) { c.pollWait = "bad" }, want: "poll wait"},
		{name: "level", mut: func(c *runnerConfig) { c.logLevel = "trace" }, want: "log level"},
		{name: "format", mut: func(c *runnerConfig) { c.logFormat = "pretty" }, want: "log format"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultRunnerConfig()
			tt.mut(&cfg)
			err := validateRunnerConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
