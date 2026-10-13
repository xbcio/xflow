package runner

import (
	"reflect"
	"strings"
	"testing"
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
