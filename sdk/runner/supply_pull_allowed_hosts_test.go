package runner

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

// TestRunCommandExposesSupplyPullAllowedHostsFlag pins the CLI input for the
// supply-pull fetcher's operator allowlist. The flag name is also the key
// resolveRunnerConfig copies the value under, so a rename on either side would
// drop the setting silently and leave the runner dialing only public sources
// while the operator believes their private registry is permitted.
func TestRunCommandExposesSupplyPullAllowedHostsFlag(t *testing.T) {
	cmd, err := NewCommand(Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Flags().Lookup("supply-pull-allowed-hosts") == nil {
		t.Fatal("--supply-pull-allowed-hosts is not registered; the standalone runner " +
			"cannot permit any host the default pull policy blocks")
	}

	cfg := defaultRunnerConfig()
	cfg.allowPlaintext = true
	cfg.supplyPullAllowedHosts = []string{"registry.internal"}
	cfg.changed = map[string]bool{"allow-plaintext": true, "supply-pull-allowed-hosts": true}

	got, err := resolveRunnerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.supplyPullAllowedHosts, []string{"registry.internal"}) {
		t.Fatalf("resolved supplyPullAllowedHosts = %v, want %v: the flag did not "+
			"survive precedence resolution", got.supplyPullAllowedHosts, []string{"registry.internal"})
	}
}

// TestSupplyPullAllowedHostsFromEnv pins the environment override. It is the
// form most deployments use, since the value lives in the same deployment
// manifest as the pull node's URL.
func TestSupplyPullAllowedHostsFromEnv(t *testing.T) {
	cfg := defaultRunnerConfig()
	got := applyLookupEnvOverrides(cfg, func(key string) (string, bool) {
		if key == "XFLOW_SUPPLY_PULL_ALLOWED_HOSTS" {
			return "registry.internal, .corp.example.com", true
		}
		return "", false
	})
	want := []string{"registry.internal", ".corp.example.com"}
	if !reflect.DeepEqual(got.supplyPullAllowedHosts, want) {
		t.Fatalf("supplyPullAllowedHosts = %v, want %v from XFLOW_SUPPLY_PULL_ALLOWED_HOSTS",
			got.supplyPullAllowedHosts, want)
	}
}

// TestLoadRunnerConfigSupplyPullAllowedHostsFromYAML covers the file surface,
// including the wildcard form.
func TestLoadRunnerConfigSupplyPullAllowedHostsFromYAML(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
supply_pull_allowed_hosts:
  - registry.internal
  - ".corp.example.com"
  - "*.edge.example.com"
`))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"registry.internal", ".corp.example.com", "*.edge.example.com"}
	if !reflect.DeepEqual(cfg.supplyPullAllowedHosts, want) {
		t.Fatalf("supplyPullAllowedHosts = %v, want %v from the file", cfg.supplyPullAllowedHosts, want)
	}
}

// TestSampleRunnerConfigDocumentsSupplyPullAllowedHosts keeps the generated
// sample honest: the sample is what an operator reads to learn the knob exists,
// and a sample that omits it means the only discoverable way to learn about the
// allowlist is to watch a fetch get refused.
func TestSampleRunnerConfigDocumentsSupplyPullAllowedHosts(t *testing.T) {
	sample := sampleRunnerConfigYAML(Profile{})
	if !strings.Contains(sample, "supply_pull_allowed_hosts") {
		t.Fatal("the sample runner config does not mention supply_pull_allowed_hosts")
	}
}

// TestValidateRunnerConfigChecksSupplyPullAllowedHosts holds the config
// surface to the host-rule grammar the fetcher compiles: an entry that cannot
// compile must fail startup, not be silently turned into an allowlist that
// matches nothing (every pull then refused) or, worse, matches the wrong host.
func TestValidateRunnerConfigChecksSupplyPullAllowedHosts(t *testing.T) {
	valid := [][]string{
		{"registry.internal"},
		{".corp.example.com"},
		{"*.edge.example.com"},
		{"10.0.0.7"}, // an exact IP entry is the explicit private-address opt-in
	}
	for _, hosts := range valid {
		cfg := defaultRunnerConfig()
		cfg.allowPlaintext = true
		cfg.supplyPullAllowedHosts = hosts
		if err := validateRunnerConfig(cfg); err != nil {
			t.Fatalf("validateRunnerConfig(%v): %v, want accepted", hosts, err)
		}
	}

	invalid := [][]string{
		{"https://registry.internal/path"}, // URL, not a host rule
		{"registry.internal:8080"},         // port
		{""},                               // blank entry
		{"*.10.0.0.0"},                     // IP pattern has no DNS-label meaning
	}
	for _, hosts := range invalid {
		cfg := defaultRunnerConfig()
		cfg.allowPlaintext = true
		cfg.supplyPullAllowedHosts = hosts
		err := validateRunnerConfig(cfg)
		if err == nil {
			t.Fatalf("validateRunnerConfig(%v) = nil, want a rejection", hosts)
		}
		if !strings.Contains(err.Error(), "supply pull allowed hosts") {
			t.Fatalf("validateRunnerConfig(%v) error = %q, want it to name the field so "+
				"an operator can find it", hosts, err)
		}
	}
}

// TestRunCommandPropagatesSupplyPullAllowedHostsToTheSDK is the pass-through
// end to end: the allowlist a YAML sets must arrive at
// xflow.RunnerConfig.SupplyPullAllowedHosts, which is where the pull fetcher
// compiles it into the destination policy. Parsed-but-not-passed is the silent
// failure shape here too — the runner starts, registers, verifies clean, and
// dials the operator's private source under the default public-only policy,
// with a per-fetch refusal as the only evidence.
func TestRunCommandPropagatesSupplyPullAllowedHostsToTheSDK(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
server:
  url: http://server:8080
supply_pull_allowed_hosts:
  - registry.internal
  - "*.corp.example"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		want := []string{"registry.internal", "*.corp.example"}
		if !reflect.DeepEqual(cfg.SupplyPullAllowedHosts, want) {
			t.Errorf("SupplyPullAllowedHosts = %v, want %v: the configured allowlist never "+
				"reached the SDK, so the listed sources are refused by the default "+
				"public-only pull policy", cfg.SupplyPullAllowedHosts, want)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--config", path, "--allow-plaintext")
}

// TestSupplyPullAllowedHostsGrammarMatchesTheFetcherValidator tethers the
// CLI-side syntax check to the validator the SDK runs on the same values
// (service/runner.ValidateSupplyPullAllowedHosts): a rule accepted here but
// refused by the fetcher's compiler turns a config typo into a late
// construction error, and a rule refused here but acceptable there is a knob
// value that can never be set. The grammar is written twice on purpose —
// sdk/runner's validator also serves its three other host-list fields and
// mirrors the action layer's rules, which it cannot import — so this table is
// the tether that keeps the two acceptors from drifting apart.
func TestSupplyPullAllowedHostsGrammarMatchesTheFetcherValidator(t *testing.T) {
	rules := []string{
		"registry.internal",
		"corp.example.com",
		".corp.example.com",
		"*.edge.example.com",
		"*.corp",
		"10.0.0.7",
		"[2001:db8::1]",
		"2001:db8::1",
		"mirror_1.internal",
		"REGISTRY.INTERNAL",
		"https://registry.internal/path",
		"registry.internal:8080",
		"*.10.0.0.0",
		"10.0.0.0/8",
		"user@registry.internal",
		"registry.internal?x=1",
		"reg*istry.internal",
		"..leading.dots",
		"trailing.dot.",
		"-leading.dash",
		"",
	}
	for _, rule := range rules {
		localErr := validateHostOnlyList("supply pull allowed hosts", []string{rule})
		serviceErr := runnersvc.ValidateSupplyPullAllowedHosts([]string{rule})
		if (localErr == nil) != (serviceErr == nil) {
			t.Errorf("rule %q: config-side validateHostOnlyList = %v, service/runner "+
				"validator = %v; the two acceptors have drifted", rule, localErr, serviceErr)
		}
	}

	// The one deliberate difference, pinned so it stays deliberate: this layer
	// trims surrounding whitespace before validating (its callers normalize
	// every value with copyTrimmedHosts anyway), while the fetcher's compiler
	// rejects untrimmed input outright.
	if err := validateHostOnlyList("supply pull allowed hosts", []string{" registry.internal "}); err != nil {
		t.Fatalf("config-side validator rejected a padded rule: %v", err)
	}
	if err := runnersvc.ValidateSupplyPullAllowedHosts([]string{" registry.internal "}); err == nil {
		t.Fatal("the fetcher validator now accepts a padded rule; fold the trimming " +
			"into this test's table if that is intentional")
	}
}
