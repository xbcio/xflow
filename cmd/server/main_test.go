package main

import (
	"testing"

	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/service/apiserver"
)

func TestParseServerConfigSupportsMemoryMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":9090", "-memory", "-concurrency", "3"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.addr != ":9090" || !cfg.memory || cfg.concurrency != 3 {
		t.Fatalf("config = %+v, want addr :9090 memory concurrency 3", cfg)
	}
}

func TestParseServerConfigSupportsRedisMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":8080", "-redis", "localhost:6379"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redis != "localhost:6379" || cfg.memory {
		t.Fatalf("config = %+v, want redis localhost:6379", cfg)
	}
}

func TestParseServerConfigSupportsObservabilityFlags(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-memory",
		"-log-format", "json",
		"-metrics-addr", "127.0.0.1:0",
		"-metrics-path", "/custom-metrics",
		"-trace", "disabled",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.logFormat != "json" || cfg.metricsAddr != "127.0.0.1:0" || cfg.metricsPath != "/custom-metrics" || cfg.traceMode != "disabled" {
		t.Fatalf("config = %+v, want observability flags parsed", cfg)
	}
}

func TestParseServerConfigRejectsUnsupportedTraceMode(t *testing.T) {
	if _, err := parseServerConfig([]string{"-trace", "bogus"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for unsupported trace mode")
	}
}

func TestParseServerConfigSupportsTraceModes(t *testing.T) {
	for _, mode := range []string{"disabled", "stdout", "otlp"} {
		cfg, err := parseServerConfig([]string{"-memory", "-trace", mode})
		if err != nil {
			t.Fatalf("parseServerConfig(-trace %s) error = %v", mode, err)
		}
		if cfg.traceMode != mode {
			t.Fatalf("traceMode = %q, want %q", cfg.traceMode, mode)
		}
	}
}

func TestBuildLoggerRejectsUnknownFormat(t *testing.T) {
	if _, err := buildLogger(serverConfig{logFormat: "xml"}); err == nil {
		t.Fatal("buildLogger() error = nil, want error for unknown format")
	}
}

func TestBuildLoggerUsesZap(t *testing.T) {
	log, err := buildLogger(serverConfig{logFormat: "json"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := log.(obslogger.ZapLogger); !ok {
		t.Fatalf("buildLogger() = %T, want observability/logger.ZapLogger", log)
	}
}

func TestRunServerBuildsFromMemoryConfig(t *testing.T) {
	srv, err := apiserver.New(apiserver.Config{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if srv == nil {
		t.Fatal("apiserver.New returned nil")
	}
}

func TestParseServerConfigSupportsAPIAuthToken(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-api-auth-token", "mysecrettoken"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.apiAuthToken != "mysecrettoken" {
		t.Fatalf("apiAuthToken = %q, want mysecrettoken", cfg.apiAuthToken)
	}
}

func TestParseServerConfigRequireAPIAuthFlag(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-require-api-auth"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.requireAPIAuth {
		t.Fatal("requireAPIAuth = false, want true")
	}
}

func TestParseServerConfigSupportsManagementFlag(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-management"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.management {
		t.Fatal("management = false, want true")
	}
}
