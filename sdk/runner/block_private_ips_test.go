package runner

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	xnode "github.com/xbcio/xflow/node"
)

func TestDefaultRunnerConfigBlocksPrivateIPs(t *testing.T) {
	if !defaultRunnerConfig().httpBlockPrivateIPs {
		t.Fatal("http_host_policy.block_private_ips must default to true")
	}
}

func TestLoadRunnerConfigBlockPrivateIPs(t *testing.T) {
	for _, tt := range []struct {
		name string
		yaml string
		want bool
	}{
		{
			name: "unset keeps the secure default",
			yaml: "http_host_policy:\n  allow: [app.test.internal]\n",
			want: true,
		},
		{
			name: "explicit true",
			yaml: "http_host_policy:\n  block_private_ips: true\n",
			want: true,
		},
		{
			name: "explicit false restores the old behavior",
			yaml: "http_host_policy:\n  block_private_ips: false\n",
			want: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := loadRunnerConfigFromBytes([]byte(tt.yaml))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.httpBlockPrivateIPs != tt.want {
				t.Fatalf("httpBlockPrivateIPs = %t, want %t", cfg.httpBlockPrivateIPs, tt.want)
			}
		})
	}
}

// TestLoadRunnerConfigBlockPrivateIPsAllow pins the separate YAML field: the
// exemption list parses into its own field and leaves the name-level lists
// untouched.
func TestLoadRunnerConfigBlockPrivateIPsAllow(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
http_host_policy:
  allow: [app.test.internal]
  block_private_ips_allow: ["internal-db.corp.test", "10.0.0.7"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"internal-db.corp.test", "10.0.0.7"}; !reflect.DeepEqual(cfg.httpBlockPrivateIPsAllow, want) {
		t.Fatalf("block_private_ips_allow = %v, want %v", cfg.httpBlockPrivateIPsAllow, want)
	}
	if want := []string{"app.test.internal"}; !reflect.DeepEqual(cfg.httpHostPolicyAllow, want) {
		t.Fatalf("HTTP host allowlist = %v, want %v", cfg.httpHostPolicyAllow, want)
	}
	if !cfg.httpBlockPrivateIPs {
		t.Fatal("block_private_ips must stay true when only the exemption list is set")
	}
}

// TestApplyLookupEnvOverridesBlockPrivateIPs pins the environment surface:
// the boolean defaults to enabled and the exemption list is CSV, both
// independent of the name-level host lists.
func TestApplyLookupEnvOverridesBlockPrivateIPs(t *testing.T) {
	cfg := applyLookupEnvOverrides(defaultRunnerConfig(), func(key string) (string, bool) {
		values := map[string]string{
			"XFLOW_HTTP_HOST_POLICY_BLOCK_PRIVATE_IPS":       "false",
			"XFLOW_HTTP_HOST_POLICY_BLOCK_PRIVATE_IPS_ALLOW": "internal-db.corp.test, 10.0.0.7",
		}
		value, ok := values[key]
		return value, ok
	})
	if cfg.httpBlockPrivateIPs {
		t.Fatal("XFLOW_HTTP_HOST_POLICY_BLOCK_PRIVATE_IPS=false did not disable the guard")
	}
	if want := []string{"internal-db.corp.test", "10.0.0.7"}; !reflect.DeepEqual(cfg.httpBlockPrivateIPsAllow, want) {
		t.Fatalf("block_private_ips_allow = %v, want %v", cfg.httpBlockPrivateIPsAllow, want)
	}
	if len(cfg.httpHostPolicyAllow) != 0 {
		t.Fatalf("HTTP host allowlist = %v, want empty: the env vars are independent", cfg.httpHostPolicyAllow)
	}

	// An unset boolean keeps the secure default.
	cfg = applyLookupEnvOverrides(defaultRunnerConfig(), func(string) (string, bool) { return "", false })
	if !cfg.httpBlockPrivateIPs {
		t.Fatal("unset block_private_ips did not keep the enabled default")
	}
}

// TestResolveRunnerConfigBlockPrivateIPsFlagPrecedence pins the CLI override:
// an explicitly passed flag wins over the file value, and the exemption list
// flag has its own changed key.
func TestResolveRunnerConfigBlockPrivateIPsFlagPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte(`
http_host_policy:
  block_private_ips: true
  block_private_ips_allow: [file.test.internal]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	base := defaultRunnerConfig()
	base.configPath = path
	base.allowPlaintext = true
	base.httpBlockPrivateIPs = false
	base.httpBlockPrivateIPsAllow = []string{"flag.test.internal"}
	base.changed = map[string]bool{
		"allow-plaintext":              true,
		"http-block-private-ips":       true,
		"http-block-private-ips-allow": true,
	}
	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.httpBlockPrivateIPs {
		t.Fatal("flag value did not override the file")
	}
	if want := []string{"flag.test.internal"}; !reflect.DeepEqual(got.httpBlockPrivateIPsAllow, want) {
		t.Fatalf("block_private_ips_allow = %v, want %v", got.httpBlockPrivateIPsAllow, want)
	}
}

type privateIPBlockingCall struct {
	enabled bool
	allow   []string
}

// swapInstallerSeams replaces both process-wide install seams for one test.
func swapInstallerSeams(t *testing.T) (*[]xnode.HTTPHostPolicy, *[]privateIPBlockingCall) {
	t.Helper()
	originalPolicy, originalBlocking := setRunnerHTTPHostPolicy, setRunnerHTTPPrivateIPBlocking
	t.Cleanup(func() {
		setRunnerHTTPHostPolicy, setRunnerHTTPPrivateIPBlocking = originalPolicy, originalBlocking
	})
	var policies []xnode.HTTPHostPolicy
	var blockings []privateIPBlockingCall
	setRunnerHTTPHostPolicy = func(policy xnode.HTTPHostPolicy) {
		policies = append(policies, policy)
	}
	setRunnerHTTPPrivateIPBlocking = func(enabled bool, allow []string) {
		blockings = append(blockings, privateIPBlockingCall{enabled: enabled, allow: allow})
	}
	return &policies, &blockings
}

// TestInstallRunnerHTTPHostPolicyInstallsPrivateIPBlockingByDefault pins the
// runner default: the address guard is installed even though the host policy
// is a separate system, and the dedicated block_private_ips_allow list is
// passed through as its exemption list.
func TestInstallRunnerHTTPHostPolicyInstallsPrivateIPBlockingByDefault(t *testing.T) {
	policies, blockings := swapInstallerSeams(t)

	cfg := defaultRunnerConfig()
	cfg.httpHostPolicyAllow = []string{" App.Test.Internal "}
	cfg.httpBlockPrivateIPsAllow = []string{" internal-db.corp.test ", "10.0.0.5"}
	restore := installRunnerHTTPHostPolicy(cfg)

	if len(*blockings) != 1 {
		t.Fatalf("private-IP blocking installs = %d, want 1", len(*blockings))
	}
	if !(*blockings)[0].enabled {
		t.Fatal("default blocking install is disabled")
	}
	if want := []string{"internal-db.corp.test", "10.0.0.5"}; !reflect.DeepEqual((*blockings)[0].allow, want) {
		t.Fatalf("blocking allowlist = %v, want %v", (*blockings)[0].allow, want)
	}
	if len(*policies) != 1 || (*policies)[0] == nil {
		t.Fatalf("installed host policies = %v, want one non-nil policy", *policies)
	}
	// The name-level allowlist keeps its own meaning.
	if err := (*policies)[0]("app.test.internal"); err != nil {
		t.Fatalf("host policy rejected its allowlisted host: %v", err)
	}

	restore()
	if len(*blockings) != 2 {
		t.Fatalf("private-IP blocking calls after restore = %d, want 2", len(*blockings))
	}
	if (*blockings)[1].enabled {
		t.Fatal("restore left private-IP blocking enabled")
	}
	if (*blockings)[1].allow != nil {
		t.Fatalf("restore allowlist = %v, want nil", (*blockings)[1].allow)
	}
	if len(*policies) != 2 || (*policies)[1] != nil {
		t.Fatalf("restore policies = %v, want final nil policy", *policies)
	}
}

// TestInstallRunnerHTTPHostPolicyNameAllowlistIsNotAnAddressExemption pins the
// separation: allowing a host by name must not exempt it (or anything else)
// from the address guard. Reusing the name list here used to switch the policy
// into allowlist-only mode, so one private-host exemption broke every public
// destination that was not listed.
func TestInstallRunnerHTTPHostPolicyNameAllowlistIsNotAnAddressExemption(t *testing.T) {
	_, blockings := swapInstallerSeams(t)

	cfg := defaultRunnerConfig()
	cfg.httpHostPolicyAllow = []string{"app.internal.test"}
	restore := installRunnerHTTPHostPolicy(cfg)
	defer restore()

	if len(*blockings) != 1 || !(*blockings)[0].enabled {
		t.Fatalf("private-IP blocking calls = %+v, want one enabled install", *blockings)
	}
	if len((*blockings)[0].allow) != 0 {
		t.Fatalf("blocking allowlist = %v, want empty: http_host_policy.allow must not exempt addresses",
			(*blockings)[0].allow)
	}
}

// TestInstallRunnerHTTPHostPolicyUnconfiguredStockRunnerStillBlocks pins that
// an ordinary runner with no host lists now gets the address guard, while the
// name-level policy stays untouched.
func TestInstallRunnerHTTPHostPolicyUnconfiguredStockRunnerStillBlocks(t *testing.T) {
	policies, blockings := swapInstallerSeams(t)

	restore := installRunnerHTTPHostPolicy(defaultRunnerConfig())
	if len(*policies) != 0 {
		t.Fatalf("host policy installs = %d for an unconfigured stock runner, want 0", len(*policies))
	}
	if len(*blockings) != 1 || !(*blockings)[0].enabled {
		t.Fatalf("private-IP blocking calls = %+v, want one enabled install", *blockings)
	}
	if len((*blockings)[0].allow) != 0 {
		t.Fatalf("blocking allowlist = %v, want empty", (*blockings)[0].allow)
	}

	restore()
	if len(*blockings) != 2 || (*blockings)[1].enabled {
		t.Fatalf("private-IP blocking calls after restore = %+v, want a disabled restore", *blockings)
	}
}

// TestInstallRunnerHTTPHostPolicySkipsBlockingWhenDisabled pins the opt-out:
// block_private_ips=false installs nothing, preserving the historical dial
// behavior.
func TestInstallRunnerHTTPHostPolicySkipsBlockingWhenDisabled(t *testing.T) {
	policies, blockings := swapInstallerSeams(t)

	cfg := defaultRunnerConfig()
	cfg.httpBlockPrivateIPs = false
	restore := installRunnerHTTPHostPolicy(cfg)
	if len(*policies) != 0 {
		t.Fatalf("host policy installs = %d, want 0", len(*policies))
	}
	if len(*blockings) != 0 {
		t.Fatalf("private-IP blocking calls = %+v, want none", *blockings)
	}

	restore()
	if len(*blockings) != 0 {
		t.Fatalf("private-IP blocking calls after restore = %+v, want none", *blockings)
	}
}
