package xbcinfra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func TestLoadEmptyPathIsInactive(t *testing.T) {
	cfg, err := Load("   ")
	if err != nil {
		t.Fatalf("Load(\"\") = %v", err)
	}
	if cfg != nil {
		t.Fatalf("Load(\"\") = %#v, want nil", cfg)
	}
}

func TestLoadReadsOnlyConfiguredCapabilities(t *testing.T) {
	path := writeConfig(t, "xflow.yml", `
log:
  level: debug
  console:
    format: json
plugins:
  metrics:
    namespace: xflow
  gorm:
    default:
      driver: sqlite
      dsn: "file:test?mode=memory&cache=shared"
      translate_error: true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if !cfg.HasLog() || !cfg.HasMetrics() || !cfg.HasGORM() {
		t.Fatalf("HasLog/HasMetrics/HasGORM = %t/%t/%t, want true/true/true",
			cfg.HasLog(), cfg.HasMetrics(), cfg.HasGORM())
	}
	if cfg.HasTracing() {
		t.Fatal("HasTracing = true for a file without plugins.tracing")
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("log level = %q, want debug", cfg.Log.Level)
	}
	if cfg.Log.Console.Format != "json" {
		t.Fatalf("console format = %q, want json", cfg.Log.Console.Format)
	}
	if cfg.Metrics.Namespace != "xflow" {
		t.Fatalf("metrics namespace = %q, want xflow", cfg.Metrics.Namespace)
	}
	if cfg.GORM.Driver != "sqlite" || cfg.GORM.DSN != "file:test?mode=memory&cache=shared" {
		t.Fatalf("gorm = %#v", cfg.GORM)
	}
	if !cfg.GORM.TranslateError {
		t.Fatal("translate_error was not bound")
	}
	if cfg.GORM.MaxOpenConn != 20 {
		t.Fatalf("max_open_conn = %d, want the xbc default 20", cfg.GORM.MaxOpenConn)
	}
}

func TestLoadEnvironmentOverridesTheFile(t *testing.T) {
	t.Setenv("XFLOW_XBC_LOG_LEVEL", "warn")
	t.Setenv("XFLOW_XBC_PLUGINS_GORM_DEFAULT_DSN", "file:env?mode=memory&cache=shared")
	path := writeConfig(t, "xflow.yml", `
log:
  level: debug
plugins:
  gorm:
    default:
      driver: sqlite
      dsn: "file:file?mode=memory&cache=shared"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if cfg.Log.Level != "warn" {
		t.Fatalf("log level = %q, want the environment's warn", cfg.Log.Level)
	}
	if cfg.GORM.DSN != "file:env?mode=memory&cache=shared" {
		t.Fatalf("dsn = %q, want the environment's value", cfg.GORM.DSN)
	}
}

func TestLoadProfileOverlay(t *testing.T) {
	t.Setenv("XFLOW_XBC_PROFILE", "prod")
	dir := t.TempDir()
	base := filepath.Join(dir, "xflow.yml")
	overlay := filepath.Join(dir, "xflow-prod.yml")
	if err := os.WriteFile(base, []byte("log:\n  level: info\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(overlay, []byte("log:\n  level: error\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(base)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if cfg.Log.Level != "error" {
		t.Fatalf("log level = %q, want the profile overlay's error", cfg.Log.Level)
	}
}

func TestLoadAcceptsNestedSectionKeysAndEnvironment(t *testing.T) {
	t.Setenv("XFLOW_XBC_PLUGINS_TRACING_EXPORTER_ENDPOINT", "collector:4317")
	path := writeConfig(t, "xflow.yml", `
plugins:
  metrics:
    namespace: xflow
    runtime:
      go_collector: true
      process_collector: false
  tracing:
    service_name: xflow-server
    sample_ratio: 0.25
    exporter:
      protocol: grpc
      endpoint: file-configured:4317
      insecure: true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if !cfg.HasMetrics() || !cfg.HasTracing() {
		t.Fatalf("HasMetrics/HasTracing = %t/%t", cfg.HasMetrics(), cfg.HasTracing())
	}
	if cfg.Metrics.Runtime.ProcessCollector {
		t.Fatal("process_collector = true, want the file's false")
	}
	if !cfg.Metrics.Runtime.GoCollector {
		t.Fatal("go_collector = false, want true")
	}
	if cfg.Tracing.ServiceName != "xflow-server" || cfg.Tracing.SampleRatio != 0.25 {
		t.Fatalf("tracing = %#v", cfg.Tracing)
	}
	if cfg.Tracing.Exporter.Endpoint != "collector:4317" {
		t.Fatalf("endpoint = %q, want the environment override", cfg.Tracing.Exporter.Endpoint)
	}
	if len(cfg.Tracing.Propagation) == 0 {
		t.Fatal("nested defaults were lost: propagation is empty")
	}
}

func TestLoadIgnoresXFlowEnvironment(t *testing.T) {
	// xflow's own variables share no prefix with the xbc tree. They must not
	// be mistaken for configuration: xbc rejects an unknown variable under the
	// prefix it owns, and a deployment always has XFLOW_MASTER_KEY set.
	t.Setenv("XFLOW_MASTER_KEY", "not-a-configuration-key")
	t.Setenv("XFLOW_API_RATE", "10")
	t.Setenv("XFLOW_PROFILE", "prod")
	path := writeConfig(t, "xflow.yml", "log:\n  level: info\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() rejected xflow's own environment: %v", err)
	}
	if cfg.Log.Level != "info" {
		t.Fatalf("log level = %q, want info", cfg.Log.Level)
	}
}

func TestLoadRejectsUnknownXBCEnvironment(t *testing.T) {
	t.Setenv("XFLOW_XBC_UNKNOWN", "1")
	path := writeConfig(t, "xflow.yml", "log:\n  level: info\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() accepted a variable under XFLOW_XBC_ that names no declared section")
	}
	if !strings.Contains(err.Error(), "XFLOW_XBC_UNKNOWN") {
		t.Fatalf("error = %v, want it to name the unknown variable", err)
	}
}

func TestLoadRejectsUnownedKeys(t *testing.T) {
	path := writeConfig(t, "xflow.yml", "web:\n  addr: \":8080\"\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() accepted an unowned top-level section")
	}
	if !strings.Contains(err.Error(), "web") {
		t.Fatalf("error = %v, want it to name the unowned key", err)
	}
}

func TestLoadRejectsNonDefaultGORMInstance(t *testing.T) {
	path := writeConfig(t, "xflow.yml", `
plugins:
  gorm:
    primary:
      driver: sqlite
      dsn: "file:primary?mode=memory&cache=shared"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() accepted plugins.gorm.primary; xflow reads only plugins.gorm.default")
	}
	if !strings.Contains(err.Error(), "primary") {
		t.Fatalf("error = %v, want it to name the unsupported instance", err)
	}
}

func TestLoadRejectsMetricsHTTPEndpoint(t *testing.T) {
	path := writeConfig(t, "xflow.yml", `
plugins:
  metrics:
    namespace: xflow
    http:
      enabled: true
      path: /metrics
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() accepted plugins.metrics.http; xflow serves through --metrics-addr")
	}
	if !strings.Contains(err.Error(), "plugins.metrics.http") {
		t.Fatalf("error = %v, want it to name plugins.metrics.http", err)
	}
}

func TestLoadRequiresTheNamedFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yml"))
	if err == nil {
		t.Fatal("Load() accepted a missing file")
	}
}
