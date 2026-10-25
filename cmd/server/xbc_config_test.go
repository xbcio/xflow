package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbcio/xflow/cmd/internal/xbcinfra"
)

func writeXBCConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "xflow-xbc.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write xbc config: %v", err)
	}
	return path
}

func TestParseServerConfigRecordsTheXBCConfigFlag(t *testing.T) {
	cfg, err := parseServerConfig([]string{"--xbc-config", "/etc/xflow/xbc.yml"})
	if err != nil {
		t.Fatalf("parseServerConfig() = %v", err)
	}
	if cfg.xbcConfig != "/etc/xflow/xbc.yml" {
		t.Fatalf("xbcConfig = %q", cfg.xbcConfig)
	}
	if !cfg.setFlags["xbc-config"] {
		t.Fatal("setFlags did not record --xbc-config")
	}
}

func TestValidateXBCConfigRejectsConflictingFlags(t *testing.T) {
	cases := []struct {
		name string
		body string
		flag string
		want string
	}{
		{
			name: "log format",
			body: "log:\n  level: info\n",
			flag: "log-format",
			want: "log.console.format",
		},
		{
			name: "trace mode",
			body: "plugins:\n  tracing:\n    service_name: xflow-server\n",
			flag: "trace",
			want: "plugins.tracing",
		},
		{
			name: "trace endpoint",
			body: "plugins:\n  tracing:\n    service_name: xflow-server\n",
			flag: "trace-endpoint",
			want: "plugins.tracing",
		},
		{
			name: "mysql dsn",
			body: "plugins:\n  gorm:\n    default:\n      driver: mysql\n      dsn: user:pass@tcp(localhost:3306)/xflow\n",
			flag: "mysql-dsn",
			want: "plugins.gorm.default",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			infra, err := xbcinfra.Load(writeXBCConfig(t, tc.body))
			if err != nil {
				t.Fatalf("xbcinfra.Load() = %v", err)
			}
			cfg := serverConfig{setFlags: map[string]bool{tc.flag: true}}
			err = validateXBCConfig(cfg, infra)
			if err == nil {
				t.Fatal("validateXBCConfig() accepted a conflicting flag")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateXBCConfigAllowsListenerAndUnconfiguredCapabilities(t *testing.T) {
	infra, err := xbcinfra.Load(writeXBCConfig(t, "plugins:\n  metrics:\n    namespace: xflow\n"))
	if err != nil {
		t.Fatalf("xbcinfra.Load() = %v", err)
	}
	cfg := serverConfig{setFlags: map[string]bool{
		"metrics-addr": true,
		"mysql-dsn":    true,
		"trace":        true,
		"log-format":   true,
	}}
	if err := validateXBCConfig(cfg, infra); err != nil {
		t.Fatalf("validateXBCConfig() rejected flags whose capabilities the file does not configure: %v", err)
	}
}

func TestValidateXBCConfigWithoutFileIsInert(t *testing.T) {
	if err := validateXBCConfig(serverConfig{setFlags: map[string]bool{"mysql-dsn": true}}, nil); err != nil {
		t.Fatalf("validateXBCConfig(nil) = %v", err)
	}
}
