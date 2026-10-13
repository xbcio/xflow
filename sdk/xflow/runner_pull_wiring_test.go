package xflow

import (
	"reflect"
	"strings"
	"testing"
)

// TestRunnerWiringInstallsThePullFetcher pins that a trigger-capable runner's
// gate actually has pull mode wired — the config field existing is not the
// property that matters, the gate having the fetcher is: without it every pull
// requirement is declined with ErrSupplyPullUnavailable at activation time, and
// the deployment reads as "the source is down".
func TestRunnerWiringInstallsThePullFetcher(t *testing.T) {
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:              "http://127.0.0.1:1",
		Capabilities:           []string{"xflow.trigger.kafka"},
		SupplyPullAllowedHosts: []string{".corp.example", "config.example.com"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.SupplyGate == nil {
		t.Fatal("cfg.SupplyGate is nil — a trigger capability should have wired it")
	}
	pf := svcCfg.SupplyGate.PullFetcher()
	if pf == nil {
		t.Fatal("the gate has no pull fetcher; every xflow.supply.http requirement would " +
			"be declined as unavailable regardless of whether its source is reachable")
	}
	if want := []string{".corp.example", "config.example.com"}; !reflect.DeepEqual(pf.AllowedHosts, want) {
		t.Fatalf("pull fetcher AllowedHosts = %v, want %v", pf.AllowedHosts, want)
	}
}

// A runner that hosts no triggers never admits an activation, so it needs no
// pull fetcher — and must not get one, because building it would suggest
// pull mode is reachable from a runner that cannot receive the requirement.
func TestRunnerWithoutTriggersHasNoPullFetcher(t *testing.T) {
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://127.0.0.1:1",
		Capabilities: []string{"xflow.function"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.SupplyGate != nil {
		t.Fatalf("a runner without trigger hosting wired a supply gate")
	}
}

// A malformed allowlist entry is refused at construction: silently degrading to
// a policy that denies everything would surface later as an unreachable source,
// which is a much harder diagnosis than a config error naming the entry.
func TestNewRunnerRefusesAMalformedSupplyPullAllowedHost(t *testing.T) {
	for _, bad := range []string{"http://config.example.com", "config.example.com/path", "", "*.10.0.0.1"} {
		_, err := NewRunner(RunnerConfig{
			ServerURL:              "http://server:8080",
			Capabilities:           []string{"xflow.function"},
			SupplyPullAllowedHosts: []string{bad},
		})
		if err == nil {
			t.Fatalf("NewRunner accepted SupplyPullAllowedHosts = %q, want a config error", bad)
		}
		if !strings.Contains(err.Error(), "SupplyPullAllowedHosts") {
			t.Fatalf("error = %v, want it to name SupplyPullAllowedHosts", err)
		}
	}

	for _, ok := range [][]string{nil, {"config.example.com"}, {".corp.example"}, {"*.corp.example"}, {"127.0.0.1"}} {
		r, err := NewRunner(RunnerConfig{
			ServerURL:              "http://server:8080",
			Capabilities:           []string{"xflow.function"},
			SupplyPullAllowedHosts: ok,
		})
		// A later construction step may legitimately fail in this bare config;
		// what must not happen is the allowlist being refused.
		if err != nil && strings.Contains(err.Error(), "SupplyPullAllowedHosts") {
			t.Fatalf("NewRunner refused SupplyPullAllowedHosts = %v: %v", ok, err)
		}
		if r != nil {
			t.Cleanup(func() { _ = r.Close() })
		}
	}
}
