package runner

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

func TestLoadRunnerConfigFromYAMLBrowserCDP(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
browser_cdp:
  endpoints:
    - chrome-a.test.internal
    - chrome-b.test.internal
  max_contexts: 3
  queue_timeout: "7s"
  connect_timeout: "8s"
`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.browserCDPEndpoints, []string{"chrome-a.test.internal", "chrome-b.test.internal"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("browser CDP endpoints = %v, want %v", got, want)
	}
	if cfg.browserCDPMaxContexts != 3 || cfg.browserCDPQueueTimeout != "7s" || cfg.browserCDPConnectTimeout != "8s" {
		t.Fatalf("browser CDP config = %+v, want max_contexts=3 queue_timeout=7s connect_timeout=8s", cfg)
	}
}

func TestApplyLookupEnvOverridesBrowserCDP(t *testing.T) {
	cfg := defaultRunnerConfig()
	got := applyLookupEnvOverrides(cfg, func(key string) (string, bool) {
		env := map[string]string{
			"XFLOW_BROWSER_CDP_ENDPOINTS":       "chrome-env-a.test.internal, chrome-env-b.test.internal",
			"XFLOW_BROWSER_CDP_MAX_CONTEXTS":    "4",
			"XFLOW_BROWSER_CDP_QUEUE_TIMEOUT":   "9s",
			"XFLOW_BROWSER_CDP_CONNECT_TIMEOUT": "10s",
		}
		value, ok := env[key]
		return value, ok
	})
	if got, want := got.browserCDPEndpoints, []string{"chrome-env-a.test.internal", "chrome-env-b.test.internal"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("browser CDP endpoints = %v, want %v", got, want)
	}
	if got.browserCDPMaxContexts != 4 || got.browserCDPQueueTimeout != "9s" || got.browserCDPConnectTimeout != "10s" {
		t.Fatalf("browser CDP config = %+v, want max_contexts=4 queue_timeout=9s connect_timeout=10s", got)
	}
}

func TestResolveRunnerConfigBrowserCDPFlagPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte(`
server:
  url: http://file-server:8080
browser_cdp:
  endpoints: [chrome-file.test.internal]
  max_contexts: 2
  queue_timeout: "2s"
  connect_timeout: "3s"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XFLOW_BROWSER_CDP_ENDPOINTS", "chrome-env.test.internal")
	t.Setenv("XFLOW_BROWSER_CDP_MAX_CONTEXTS", "4")
	t.Setenv("XFLOW_BROWSER_CDP_QUEUE_TIMEOUT", "4s")
	t.Setenv("XFLOW_BROWSER_CDP_CONNECT_TIMEOUT", "5s")

	base := defaultRunnerConfig()
	base.configPath = path
	base.allowPlaintext = true
	base.browserCDPEndpoints = []string{"chrome-flag-a.test.internal", "chrome-flag-b.test.internal"}
	base.browserCDPMaxContexts = 6
	base.browserCDPQueueTimeout = "6s"
	base.browserCDPConnectTimeout = "7s"
	base.changed = map[string]bool{
		"allow-plaintext":             true,
		"browser-cdp-endpoints":       true,
		"browser-cdp-max-contexts":    true,
		"browser-cdp-queue-timeout":   true,
		"browser-cdp-connect-timeout": true,
	}

	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"chrome-flag-a.test.internal", "chrome-flag-b.test.internal"}; !reflect.DeepEqual(got.browserCDPEndpoints, want) {
		t.Fatalf("endpoint allowlist = %v, want changed flag value %v", got.browserCDPEndpoints, want)
	}
	if got.browserCDPMaxContexts != 6 || got.browserCDPQueueTimeout != "6s" || got.browserCDPConnectTimeout != "7s" {
		t.Fatalf("browser CDP config = %+v, want changed flag values", got)
	}
}

func TestResolveRunnerConfigExplicitEmptyBrowserCDPFlagDeniesAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte(`
browser_cdp:
  endpoints: [chrome-file.test.internal]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XFLOW_BROWSER_CDP_ENDPOINTS", "chrome-env.test.internal")

	base := defaultRunnerConfig()
	base.configPath = path
	base.allowPlaintext = true
	base.changed = map[string]bool{
		"allow-plaintext":       true,
		"browser-cdp-endpoints": true,
	}
	// pflag.StringArray represents --browser-cdp-endpoints= as one
	// empty value. Resolution normalizes that explicit value to a non-nil empty
	// slice rather than letting the env or YAML allowlist survive.
	base.browserCDPEndpoints = []string{""}
	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.browserCDPEndpoints == nil || len(got.browserCDPEndpoints) != 0 {
		t.Fatalf("endpoint allowlist = %#v, want explicit deny-all empty slice", got.browserCDPEndpoints)
	}
}

func TestBrowserCDPEndpointsTrimsHostOnlyValues(t *testing.T) {
	cfg := defaultRunnerConfig()
	cfg.allowPlaintext = true
	cfg.browserCDPEndpoints = []string{"  ChRoMe.Test.Internal  "}
	if err := validateRunnerConfig(cfg); err != nil {
		t.Fatalf("validateRunnerConfig: %v", err)
	}
	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("toSDKRunnerConfig: %v", err)
	}
	if want := []string{"ChRoMe.Test.Internal"}; !reflect.DeepEqual(sdkCfg.BrowserCDP.Endpoints, want) {
		t.Fatalf("endpoint allowlist = %v, want %v", sdkCfg.BrowserCDP.Endpoints, want)
	}
}

func TestRunCommandPropagatesBrowserCDPFlagsToSDK(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if want := []string{"chrome-a.test.internal", "chrome-b.test.internal"}; !reflect.DeepEqual(cfg.BrowserCDP.Endpoints, want) {
			t.Errorf("BrowserCDP.Endpoints = %v, want %v", cfg.BrowserCDP.Endpoints, want)
		}
		if cfg.BrowserCDP.MaxContexts != 3 {
			t.Errorf("BrowserCDP.MaxContexts = %d, want 3", cfg.BrowserCDP.MaxContexts)
		}
		if cfg.BrowserCDP.QueueTimeout != 7*time.Second {
			t.Errorf("BrowserCDP.QueueTimeout = %s, want 7s", cfg.BrowserCDP.QueueTimeout)
		}
		if cfg.BrowserCDP.ConnectTimeout != 8*time.Second {
			t.Errorf("BrowserCDP.ConnectTimeout = %s, want 8s", cfg.BrowserCDP.ConnectTimeout)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--allow-plaintext",
		"--browser-cdp-endpoints", "chrome-a.test.internal",
		"--browser-cdp-endpoints", "chrome-b.test.internal",
		"--browser-cdp-max-contexts", "3",
		"--browser-cdp-queue-timeout", "7s",
		"--browser-cdp-connect-timeout", "8s")
}

func TestValidateRunnerConfigAcceptsBrowserCDPHostRuleForms(t *testing.T) {
	cfg := defaultRunnerConfig()
	cfg.allowPlaintext = true
	cfg.browserCDPEndpoints = []string{
		"chrome.example.test",
		".browser.example.test",
		"*.worker.example.test",
	}
	if err := validateRunnerConfig(cfg); err != nil {
		t.Fatalf("validateRunnerConfig() = %v, want supported exact/suffix/wildcard rules accepted", err)
	}
}

func TestValidateRunnerConfigRejectsInvalidBrowserCDPValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*runnerConfig)
		want string
	}{
		{
			name: "max contexts",
			edit: func(cfg *runnerConfig) { cfg.browserCDPMaxContexts = 0 },
			want: "browser CDP max contexts",
		},
		{
			name: "blank browser CDP endpoints entry",
			edit: func(cfg *runnerConfig) { cfg.browserCDPEndpoints = []string{" "} },
			want: "browser CDP endpoints",
		},
		{
			name: "endpoint scheme",
			edit: func(cfg *runnerConfig) { cfg.browserCDPEndpoints = []string{"ws://chrome.test.internal"} },
			want: "host-only",
		},
		{
			name: "endpoint port",
			edit: func(cfg *runnerConfig) { cfg.browserCDPEndpoints = []string{"chrome.test.internal:9222"} },
			want: "host-only",
		},
		{
			name: "endpoint path",
			edit: func(cfg *runnerConfig) { cfg.browserCDPEndpoints = []string{"chrome.test.internal/devtools"} },
			want: "host-only",
		},
		{
			name: "endpoint userinfo",
			edit: func(cfg *runnerConfig) { cfg.browserCDPEndpoints = []string{"user@chrome.test.internal"} },
			want: "host-only",
		},
		{
			name: "zero queue timeout",
			edit: func(cfg *runnerConfig) { cfg.browserCDPQueueTimeout = "0s" },
			want: "browser CDP queue timeout",
		},
		{
			name: "invalid connect timeout",
			edit: func(cfg *runnerConfig) { cfg.browserCDPConnectTimeout = "not-a-duration" },
			want: "browser CDP connect timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultRunnerConfig()
			cfg.allowPlaintext = true
			tt.edit(&cfg)
			if err := validateRunnerConfig(cfg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateRunnerConfig error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestResolveRunnerConfigRejectsMalformedBrowserCDPMaxContextsEnv(t *testing.T) {
	t.Setenv("XFLOW_BROWSER_CDP_MAX_CONTEXTS", "not-a-number")

	base := defaultRunnerConfig()
	base.allowPlaintext = true
	base.changed = map[string]bool{"allow-plaintext": true}
	if _, err := resolveRunnerConfig(base); err == nil || !strings.Contains(err.Error(), "XFLOW_BROWSER_CDP_MAX_CONTEXTS") {
		t.Fatalf("resolveRunnerConfig error = %v, want malformed Browser CDP max-contexts environment error", err)
	}
}

func TestRunnerConfigSamplesIncludeBrowserCDP(t *testing.T) {
	profile := Profile{
		Defaults: &Defaults{
			ServerURL:    "https://control.test.internal",
			Transport:    TransportHTTP,
			GRPCTarget:   "control.test.internal:9090",
			Capabilities: []string{"xflow.function"},
			AutoLabels:   true,
		},
	}
	for name, sample := range map[string]string{
		"generic": genericSampleRunnerConfigYAML(),
		"profile": profileSampleRunnerConfigYAML(profile),
	} {
		t.Run(name, func(t *testing.T) {
			for _, fragment := range []string{
				"browser_cdp:",
				"endpoints:",
				"max_contexts:",
				"queue_timeout:",
				"connect_timeout:",
				"denies all endpoint",
				"Browser CDP work requires the \"xflow.browser.cdp\" capability.",
			} {
				if !strings.Contains(sample, fragment) {
					t.Fatalf("sample config missing %q:\n%s", fragment, sample)
				}
			}
		})
	}
}

func TestBrowserCDPFlagsAreAcceptedByConfigValidate(t *testing.T) {
	var out bytes.Buffer
	err := executeRootWithOptions(commandOptions{
		out: &out,
		err: &bytes.Buffer{},
	}, "config", "validate", "--allow-plaintext",
		"--browser-cdp-endpoints", "chrome.test.internal",
		"--browser-cdp-max-contexts", "2",
		"--browser-cdp-queue-timeout", "2s",
		"--browser-cdp-connect-timeout", "3s")
	if err != nil {
		t.Fatalf("config validate: %v", err)
	}
	if !strings.Contains(out.String(), "runner config valid") {
		t.Fatalf("config validate output = %q", out.String())
	}
}
