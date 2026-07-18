package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
  transport: "http"
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
  transport: "http"
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
  transport: "http"
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
		{name: "server", mut: func(c *runnerConfig) { c.transport = transportHTTP; c.serverURL = "localhost:8080" }, want: "server URL"},
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

func TestLoadRunnerConfigFromYAML_Credentials(t *testing.T) {
	t.Setenv("XFLOW_DB_PASSWORD", "s3cret")
	t.Setenv("XFLOW_API_TOKEN", "tok-123")
	data := []byte(`
credentials:
  db:
    driver: mysql
    dsn: "user:${XFLOW_DB_PASSWORD}@tcp(db:3306)/xflow?parseTime=true"
  api:
    token: "${XFLOW_API_TOKEN}"
    base_url: "https://api.example.com"
`)
	cfg, err := loadRunnerConfigFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.credentials == nil {
		t.Fatal("credentials = nil, want parsed map")
	}
	db := cfg.credentials["db"]
	if db["driver"] != "mysql" {
		t.Fatalf("db.driver = %v, want mysql", db["driver"])
	}
	wantDSN := "user:s3cret@tcp(db:3306)/xflow?parseTime=true"
	if db["dsn"] != wantDSN {
		t.Fatalf("db.dsn = %q, want %q (env-expanded)", db["dsn"], wantDSN)
	}
	api := cfg.credentials["api"]
	if api["token"] != "tok-123" {
		t.Fatalf("api.token = %v, want tok-123 (env-expanded)", api["token"])
	}
	if api["base_url"] != "https://api.example.com" {
		t.Fatalf("api.base_url = %v, want unchanged (no env ref)", api["base_url"])
	}
}

func TestLoadRunnerConfigFromYAML_CredentialsMissingEnvFails(t *testing.T) {
	// Ensure the var is genuinely unset so the missing reference is detected.
	t.Setenv("XFLOW_DB_PASSWORD", "")
	os.Unsetenv("XFLOW_DB_PASSWORD")

	data := []byte(`
credentials:
  db:
    dsn: "user:${XFLOW_DB_PASSWORD}@tcp(db:3306)/xflow"
`)
	_, err := loadRunnerConfigFromBytes(data)
	if err == nil {
		t.Fatal("error = nil, want a fail-closed error for missing env var")
	}
	if !strings.Contains(err.Error(), "XFLOW_DB_PASSWORD") {
		t.Fatalf("error = %v, want substring %q", err, "XFLOW_DB_PASSWORD")
	}
	// Security: error must not include the placeholder's surrounding context
	// (i.e. must not echo the partial DSN); it lists only the variable NAME.
	if strings.Contains(err.Error(), "user:") {
		t.Fatalf("error leaked credential context: %v", err)
	}
}

func TestLoadRunnerConfigFromYAML_CredentialsDollarVarExpanded(t *testing.T) {
	// $VAR (no braces) form is supported via os.Expand semantics.
	t.Setenv("XFLOW_API_TOKEN", "tok-dollar")
	data := []byte(`
credentials:
  api:
    token: "$XFLOW_API_TOKEN"
`)
	cfg, err := loadRunnerConfigFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.credentials["api"]["token"]; got != "tok-dollar" {
		t.Fatalf("api.token = %v, want tok-dollar ($VAR form)", got)
	}
}

func TestLoadRunnerConfigFromYAML_CredentialsNonStringLeavesUnchanged(t *testing.T) {
	t.Setenv("XFLOW_DB_PASSWORD", "s3cret")
	data := []byte(`
credentials:
  db:
    driver: mysql
    dsn: "user:${XFLOW_DB_PASSWORD}@tcp(db:3306)/xflow"
    max_open_conns: 25
    nested:
      inner: "${XFLOW_DB_PASSWORD}"
      count: 3
`)
	cfg, err := loadRunnerConfigFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	db := cfg.credentials["db"]
	if got, ok := db["max_open_conns"].(int); !ok || got != 25 {
		t.Fatalf("max_open_conns = %v (%T), want int 25 (non-string leaves unchanged)", db["max_open_conns"], db["max_open_conns"])
	}
	nested, ok := db["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %T, want map[string]any", db["nested"])
	}
	if nested["inner"] != "s3cret" {
		t.Fatalf("nested.inner = %v, want s3cret (env-expanded string leaf in nested map)", nested["inner"])
	}
	if got, ok := nested["count"].(int); !ok || got != 3 {
		t.Fatalf("nested.count = %v (%T), want int 3 (non-string leaf in nested map)", nested["count"], nested["count"])
	}
}

func TestLoadRunnerConfigFromYAML_ResourcePoolDurations(t *testing.T) {
	data := []byte(`
resource_pool:
  sql:
    max_open_conns: 10
    max_idle_conns: 2
    conn_max_lifetime: "30m"
  grpc:
    keepalive_time: "30s"
    keepalive_timeout: "10s"
`)
	cfg, err := loadRunnerConfigFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.resourcePoolConfig.SQL.MaxOpenConns != 10 {
		t.Fatalf("sql.MaxOpenConns = %d, want 10", cfg.resourcePoolConfig.SQL.MaxOpenConns)
	}
	if cfg.resourcePoolConfig.SQL.MaxIdleConns != 2 {
		t.Fatalf("sql.MaxIdleConns = %d, want 2", cfg.resourcePoolConfig.SQL.MaxIdleConns)
	}
	if cfg.resourcePoolConfig.SQL.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("sql.ConnMaxLifetime = %v, want 30m", cfg.resourcePoolConfig.SQL.ConnMaxLifetime)
	}
	if cfg.resourcePoolConfig.GRPC.KeepaliveTime != 30*time.Second {
		t.Fatalf("grpc.KeepaliveTime = %v, want 30s", cfg.resourcePoolConfig.GRPC.KeepaliveTime)
	}
	if cfg.resourcePoolConfig.GRPC.KeepaliveTimeout != 10*time.Second {
		t.Fatalf("grpc.KeepaliveTimeout = %v, want 10s", cfg.resourcePoolConfig.GRPC.KeepaliveTimeout)
	}
}

func TestLoadRunnerConfigFromYAML_ResourcePoolAbsentLeavesZero(t *testing.T) {
	data := []byte(`
runner:
  id: "r"
  capabilities:
    - xflow.function
server:
  url: http://localhost:8080
`)
	cfg, err := loadRunnerConfigFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.resourcePoolConfig.SQL.MaxOpenConns != 0 {
		t.Fatalf("sql.MaxOpenConns = %d, want 0 (absent resource_pool)", cfg.resourcePoolConfig.SQL.MaxOpenConns)
	}
	if cfg.resourcePoolConfig.SQL.ConnMaxLifetime != 0 {
		t.Fatalf("sql.ConnMaxLifetime = %v, want 0 (absent resource_pool)", cfg.resourcePoolConfig.SQL.ConnMaxLifetime)
	}
	if cfg.resourcePoolConfig.GRPC.KeepaliveTime != 0 {
		t.Fatalf("grpc.KeepaliveTime = %v, want 0 (absent resource_pool)", cfg.resourcePoolConfig.GRPC.KeepaliveTime)
	}
}

func TestLoadRunnerConfigFromYAML_ResourcePoolInvalidDurationFails(t *testing.T) {
	data := []byte(`
resource_pool:
  sql:
    conn_max_lifetime: "not-a-duration"
`)
	_, err := loadRunnerConfigFromBytes(data)
	if err == nil || !strings.Contains(err.Error(), "conn_max_lifetime") {
		t.Fatalf("error = %v, want containing %q", err, "conn_max_lifetime")
	}
}

func TestShouldConstructPool(t *testing.T) {
	tests := []struct {
		name string
		cfg  runnerConfig
		want bool
	}{
		{name: "no credentials, no db/grpc cap", cfg: runnerConfig{capabilities: parseCapabilities("xflow.function")}, want: false},
		{name: "credentials present", cfg: runnerConfig{capabilities: parseCapabilities("xflow.function"), credentials: map[string]map[string]any{"db": {"dsn": "x"}}}, want: true},
		{name: "xflow.database capability", cfg: runnerConfig{capabilities: parseCapabilities("xflow.function,xflow.database")}, want: true},
		{name: "xflow.grpc capability", cfg: runnerConfig{capabilities: parseCapabilities("xflow.function,xflow.grpc")}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldConstructPool(tt.cfg); got != tt.want {
				t.Fatalf("shouldConstructPool = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRunnerServiceConfig_ConstructsPoolAndResolverWhenConfigured(t *testing.T) {
	t.Setenv("XFLOW_DB_PASSWORD", "s3cret")
	cfg := defaultRunnerConfig()
	cfg.capabilities = parseCapabilities("xflow.database")
	cfg.credentials = map[string]map[string]any{
		"db": {"dsn": "user:${XFLOW_DB_PASSWORD}@tcp(db:3306)/xflow", "driver": "mysql"},
	}
	// Expand env leaves (loadRunnerConfigFromBytes does this; emulate here so
	// the unit test exercises the resolver closure directly).
	if err := expandEnvCredentialValues(cfg.credentials); err != nil {
		t.Fatal(err)
	}

	svcCfg, err := runnerServiceConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if svcCfg.ResourcePool == nil {
		t.Fatal("ResourcePool = nil, want a constructed pool")
	}
	if svcCfg.CredentialResolver == nil {
		t.Fatal("CredentialResolver = nil, want a resolver closure")
	}
	got := svcCfg.CredentialResolver("db")
	if got["dsn"] != "user:s3cret@tcp(db:3306)/xflow" {
		t.Fatalf("resolver returned dsn = %v, want env-expanded dsn", got["dsn"])
	}
	if svcCfg.CredentialResolver("missing") != nil {
		t.Fatal("resolver returned non-nil for unknown credential name")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = svcCfg.ResourcePool.Close(ctx)
	})
}

func TestRunnerServiceConfig_NoPoolWhenNotConfigured(t *testing.T) {
	cfg := defaultRunnerConfig()
	cfg.capabilities = parseCapabilities("xflow.function")
	svcCfg, err := runnerServiceConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if svcCfg.ResourcePool != nil {
		t.Fatalf("ResourcePool = %T, want nil (no credentials and no db/grpc capability)", svcCfg.ResourcePool)
	}
	if svcCfg.CredentialResolver != nil {
		t.Fatal("CredentialResolver = non-nil, want nil (no credentials)")
	}
}
