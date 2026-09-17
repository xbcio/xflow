package runner

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	xnode "github.com/xbcio/xflow/node"
)

func TestLoadRunnerConfigFromYAMLHTTPHostPolicy(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
http_host_policy:
  allow: [app-a.test.internal, app-b.test.internal]
  deny: [metadata.test.internal]
`))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"app-a.test.internal", "app-b.test.internal"}; !reflect.DeepEqual(cfg.httpHostPolicyAllow, want) {
		t.Fatalf("HTTP host allowlist = %v, want %v", cfg.httpHostPolicyAllow, want)
	}
	if want := []string{"metadata.test.internal"}; !reflect.DeepEqual(cfg.httpHostPolicyDeny, want) {
		t.Fatalf("HTTP host denylist = %v, want %v", cfg.httpHostPolicyDeny, want)
	}
}

func TestResolveRunnerConfigHTTPHostPolicyPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte(`
http_host_policy:
  allow: [allow-file.test.internal]
  deny: [deny-file.test.internal]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XFLOW_HTTP_HOST_POLICY_ALLOW", "allow-env.test.internal")
	t.Setenv("XFLOW_HTTP_HOST_POLICY_DENY", "deny-env.test.internal")

	base := defaultRunnerConfig()
	base.configPath = path
	base.allowPlaintext = true
	base.httpHostPolicyAllow = []string{"allow-flag.test.internal"}
	base.httpHostPolicyDeny = []string{"deny-flag.test.internal"}
	base.changed = map[string]bool{"allow-plaintext": true, "http-host-allow": true, "http-host-deny": true}
	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"allow-flag.test.internal"}; !reflect.DeepEqual(got.httpHostPolicyAllow, want) {
		t.Fatalf("HTTP host allowlist = %v, want %v", got.httpHostPolicyAllow, want)
	}
	if want := []string{"deny-flag.test.internal"}; !reflect.DeepEqual(got.httpHostPolicyDeny, want) {
		t.Fatalf("HTTP host denylist = %v, want %v", got.httpHostPolicyDeny, want)
	}
}

func TestApplyLookupEnvOverridesHTTPHostPolicy(t *testing.T) {
	cfg := applyLookupEnvOverrides(defaultRunnerConfig(), func(key string) (string, bool) {
		values := map[string]string{
			"XFLOW_HTTP_HOST_POLICY_ALLOW": "allow-a.test.internal, allow-b.test.internal",
			"XFLOW_HTTP_HOST_POLICY_DENY":  "deny.test.internal",
		}
		value, ok := values[key]
		return value, ok
	})
	if want := []string{"allow-a.test.internal", "allow-b.test.internal"}; !reflect.DeepEqual(cfg.httpHostPolicyAllow, want) {
		t.Fatalf("HTTP host allowlist = %v, want %v", cfg.httpHostPolicyAllow, want)
	}
	if want := []string{"deny.test.internal"}; !reflect.DeepEqual(cfg.httpHostPolicyDeny, want) {
		t.Fatalf("HTTP host denylist = %v, want %v", cfg.httpHostPolicyDeny, want)
	}
}

func TestValidateRunnerConfigRejectsMalformedHTTPHostPolicyHosts(t *testing.T) {
	tests := []struct {
		name string
		host string
	}{
		{name: "blank", host: " "},
		{name: "scheme", host: "https://app.test.internal"},
		{name: "port", host: "app.test.internal:443"},
		{name: "path", host: "app.test.internal/path"},
		{name: "userinfo", host: "user@app.test.internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultRunnerConfig()
			cfg.allowPlaintext = true
			cfg.httpHostPolicyAllow = []string{tt.host}
			if err := validateRunnerConfig(cfg); err == nil || !strings.Contains(err.Error(), "HTTP host policy allowlist") {
				t.Fatalf("validateRunnerConfig error = %v, want HTTP host policy allowlist error", err)
			}
		})
	}
}

func TestInstallRunnerHTTPHostPolicyConfiguredForStockRunnerAndRestores(t *testing.T) {
	originalSetter := setRunnerHTTPHostPolicy
	defer func() { setRunnerHTTPHostPolicy = originalSetter }()

	var installed []xnode.HTTPHostPolicy
	setRunnerHTTPHostPolicy = func(policy xnode.HTTPHostPolicy) {
		installed = append(installed, policy)
	}
	cfg := defaultRunnerConfig()
	cfg.httpHostPolicyAllow = []string{" App.Test.Internal "}
	cfg.httpHostPolicyDeny = []string{"blocked.test.internal"}
	restore := installRunnerHTTPHostPolicy(cfg)
	if len(installed) != 1 || installed[0] == nil {
		t.Fatalf("installed policies = %v, want one non-nil policy", installed)
	}
	if err := installed[0]("app.test.internal"); err != nil {
		t.Fatalf("allowlisted host rejected: %v", err)
	}
	if err := installed[0]("blocked.test.internal"); err == nil {
		t.Fatal("denied host was permitted")
	}
	if err := installed[0]("other.test.internal"); err == nil {
		t.Fatal("host outside allowlist was permitted")
	}
	restore()
	if len(installed) != 2 || installed[1] != nil {
		t.Fatalf("restore policies = %v, want final nil policy", installed)
	}
}

func TestInstallRunnerHTTPHostPolicyBrowserDefaultsToDenyAll(t *testing.T) {
	originalSetter := setRunnerHTTPHostPolicy
	defer func() { setRunnerHTTPHostPolicy = originalSetter }()

	var installed []xnode.HTTPHostPolicy
	setRunnerHTTPHostPolicy = func(policy xnode.HTTPHostPolicy) {
		installed = append(installed, policy)
	}
	cfg := defaultRunnerConfig()
	cfg.capRaw = "xflow.browser.cdp"
	cfg.capabilities = parseCapabilities(cfg.capRaw)
	restore := installRunnerHTTPHostPolicy(cfg)
	if len(installed) != 1 || installed[0] == nil {
		t.Fatalf("installed policies = %v, want Browser deny-all policy", installed)
	}
	if err := installed[0]("app.test.internal"); err == nil {
		t.Fatal("Browser runner without an HTTP host policy permitted navigation")
	}
	restore()
	if len(installed) != 2 || installed[1] != nil {
		t.Fatalf("restore policies = %v, want final nil policy", installed)
	}
}

func TestInstallRunnerHTTPHostPolicyLeavesUnconfiguredStockRunnerUntouched(t *testing.T) {
	originalSetter := setRunnerHTTPHostPolicy
	defer func() { setRunnerHTTPHostPolicy = originalSetter }()

	calls := 0
	setRunnerHTTPHostPolicy = func(xnode.HTTPHostPolicy) { calls++ }
	restore := installRunnerHTTPHostPolicy(defaultRunnerConfig())
	restore()
	if calls != 0 {
		t.Fatalf("SetHTTPHostPolicy called %d times for an unconfigured stock runner, want 0", calls)
	}
}
