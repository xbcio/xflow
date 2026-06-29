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
	if cfg.pollWait != "2s" || cfg.heartbeatInterval != "7s" {
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

func TestConfigValidateUsesGlobalConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
runner:
  id: file-runner
server:
  url: http://file-server:8080
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err := executeRootWithOptions(commandOptions{
		out: &out,
		err: &bytes.Buffer{},
	}, "--config", path, "config", "validate")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "runner config valid: file-runner") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestConfigValidateRejectsInvalidGlobalConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
server:
  url: localhost:8080
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	err := executeRootWithOptions(commandOptions{
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "--config", path, "config", "validate")
	if err == nil || !strings.Contains(err.Error(), "server URL") {
		t.Fatalf("error = %v, want server URL validation", err)
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
	}
	env := map[string]string{
		"XFLOW_RUNNER_SERVER":             "http://env-server:8080",
		"XFLOW_RUNNER_ID":                 "env-runner",
		"XFLOW_RUNNER_CONCURRENCY":        "4",
		"XFLOW_RUNNER_CAP":                "xflow.function,xflow.http",
		"XFLOW_RUNNER_HEARTBEAT_INTERVAL": "9s",
		"XFLOW_RUNNER_POLL_WAIT":          "3s",
	}
	got := applyEnvOverrides(cfg, func(key string) string { return env[key] })
	if got.serverURL != "http://env-server:8080" || got.runnerID != "env-runner" || got.concurrency != 4 {
		t.Fatalf("config = %+v", got)
	}
	if got.capRaw != "xflow.function,xflow.http" || got.heartbeatInterval != "9s" || got.pollWait != "3s" {
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
	base.changed = map[string]bool{
		"server":             true,
		"id":                 true,
		"concurrency":        true,
		"cap":                true,
		"heartbeat-interval": true,
		"poll-wait":          true,
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

func TestResolveRunnerConfigRejectsExplicitEmptyEnvValuesOverValidFileConfig(t *testing.T) {
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
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		envKey string
		want   string
	}{
		{name: "server", envKey: "XFLOW_RUNNER_SERVER", want: "server URL"},
		{name: "runner id", envKey: "XFLOW_RUNNER_ID", want: "runner id"},
		{name: "capabilities", envKey: "XFLOW_RUNNER_CAP", want: "capabilities"},
		{name: "poll wait", envKey: "XFLOW_RUNNER_POLL_WAIT", want: "poll wait"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envKey, "")

			base := defaultRunnerConfig()
			base.configPath = path

			_, err := resolveRunnerConfig(base)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestResolveRunnerConfigUsesCLIFlagWhenEnvOverrideIsEmptyOrInvalid(t *testing.T) {
	tests := []struct {
		name   string
		envKey string
		envVal string
		apply  func(*runnerConfig)
		check  func(t *testing.T, got runnerConfig)
	}{
		{
			name:   "server",
			envKey: "XFLOW_RUNNER_SERVER",
			envVal: "",
			apply: func(cfg *runnerConfig) {
				cfg.serverURL = "http://cli-server:8080"
				cfg.changed = map[string]bool{"server": true}
			},
			check: func(t *testing.T, got runnerConfig) {
				t.Helper()
				if got.serverURL != "http://cli-server:8080" {
					t.Fatalf("serverURL = %q, want %q", got.serverURL, "http://cli-server:8080")
				}
			},
		},
		{
			name:   "runner id",
			envKey: "XFLOW_RUNNER_ID",
			envVal: "",
			apply: func(cfg *runnerConfig) {
				cfg.runnerID = "cli-runner"
				cfg.changed = map[string]bool{"id": true}
			},
			check: func(t *testing.T, got runnerConfig) {
				t.Helper()
				if got.runnerID != "cli-runner" {
					t.Fatalf("runnerID = %q, want %q", got.runnerID, "cli-runner")
				}
			},
		},
		{
			name:   "concurrency",
			envKey: "XFLOW_RUNNER_CONCURRENCY",
			envVal: "",
			apply: func(cfg *runnerConfig) {
				cfg.concurrency = 2
				cfg.changed = map[string]bool{"concurrency": true}
			},
			check: func(t *testing.T, got runnerConfig) {
				t.Helper()
				if got.concurrency != 2 {
					t.Fatalf("concurrency = %d, want %d", got.concurrency, 2)
				}
			},
		},
		{
			name:   "concurrency invalid",
			envKey: "XFLOW_RUNNER_CONCURRENCY",
			envVal: "bad",
			apply: func(cfg *runnerConfig) {
				cfg.concurrency = 2
				cfg.changed = map[string]bool{"concurrency": true}
			},
			check: func(t *testing.T, got runnerConfig) {
				t.Helper()
				if got.concurrency != 2 {
					t.Fatalf("concurrency = %d, want %d", got.concurrency, 2)
				}
			},
		},
		{
			name:   "capabilities",
			envKey: "XFLOW_RUNNER_CAP",
			envVal: "",
			apply: func(cfg *runnerConfig) {
				cfg.capRaw = "xflow.http"
				cfg.changed = map[string]bool{"cap": true}
			},
			check: func(t *testing.T, got runnerConfig) {
				t.Helper()
				if got.capRaw != "xflow.http" {
					t.Fatalf("capRaw = %q, want %q", got.capRaw, "xflow.http")
				}
				if len(got.capabilities) != 1 || got.capabilities[0].NodeType != "xflow.http" {
					t.Fatalf("capabilities = %+v", got.capabilities)
				}
			},
		},
		{
			name:   "poll wait",
			envKey: "XFLOW_RUNNER_POLL_WAIT",
			envVal: "",
			apply: func(cfg *runnerConfig) {
				cfg.pollWait = "4s"
				cfg.changed = map[string]bool{"poll-wait": true}
			},
			check: func(t *testing.T, got runnerConfig) {
				t.Helper()
				if got.pollWait != "4s" {
					t.Fatalf("pollWait = %q, want %q", got.pollWait, "4s")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envKey, tt.envVal)

			base := defaultRunnerConfig()
			tt.apply(&base)

			got, err := resolveRunnerConfig(base)
			if err != nil {
				t.Fatal(err)
			}
			tt.check(t, got)
		})
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
