// Package runner provides the standalone-process front end for an XFlow
// runner. It resolves the documented YAML, environment, and CLI inputs before
// creating the execution runtime through xflow.NewRunner.
package runner

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/service/protocol"
)

// Defaults replaces the generic standalone runner defaults for a Profile. A
// nil Defaults preserves the xflow-runner defaults. Fields in a non-nil value
// are deliberate defaults, including empty strings: that lets a deployment
// profile require an explicit server URL instead of silently dialing the
// generic localhost address.
type Defaults struct {
	ServerURL    string
	Transport    string
	GRPCTarget   string
	Capabilities []string
	AutoLabels   bool
}

// Profile defines a reusable, non-business-specific standalone runner shape.
// It changes only process policy; node handlers and runtime assembly remain in
// xflow.NewRunner.
//
// RequiredLabels are added when absent and reject conflicting labels from a
// config file, environment, or command line. FixedCapabilities both supply the
// default capability set (unless Defaults.Capabilities is set) and reject any
// different set. RunnerIDPrefix derives the default ID as
// <prefix><hostname>-<pid> while preserving an explicit ID. RequiredRunnerIDPrefix
// validates every static, stored, or enrolled ID before it is used. RequireToken
// is useful for profiles whose server endpoint always authenticates runner
// registration.
type Profile struct {
	CommandName            string
	Short                  string
	Defaults               *Defaults
	RunnerIDPrefix         string
	RequiredRunnerIDPrefix string
	RequiredLabels         map[string]string
	FixedCapabilities      []string
	RequireToken           bool
}

func normalizeProfile(profile Profile) (Profile, error) {
	profile.CommandName = strings.TrimSpace(profile.CommandName)
	if profile.CommandName == "" {
		profile.CommandName = "xflow-runner"
	}
	profile.Short = strings.TrimSpace(profile.Short)
	if profile.Short == "" {
		profile.Short = "XFlow task runner"
	}
	if profile.Defaults != nil {
		defaults := *profile.Defaults
		if defaults.Capabilities != nil {
			defaults.Capabilities = normalizedStrings(defaults.Capabilities)
			if len(defaults.Capabilities) == 0 {
				return Profile{}, fmt.Errorf("runner profile %q has no default capabilities", profile.CommandName)
			}
		}
		profile.Defaults = &defaults
	}
	if len(profile.RequiredLabels) > 0 {
		profile.RequiredLabels = cloneStringMap(profile.RequiredLabels)
		for key, value := range profile.RequiredLabels {
			if strings.TrimSpace(key) == "" {
				return Profile{}, fmt.Errorf("runner profile %q has an empty required label key", profile.CommandName)
			}
			if value == "" {
				return Profile{}, fmt.Errorf("runner profile %q requires label %q with an empty value", profile.CommandName, key)
			}
		}
	}
	if profile.FixedCapabilities != nil {
		profile.FixedCapabilities = normalizedStrings(profile.FixedCapabilities)
		if len(profile.FixedCapabilities) == 0 {
			return Profile{}, fmt.Errorf("runner profile %q has no fixed capabilities", profile.CommandName)
		}
		if profile.Defaults != nil && profile.Defaults.Capabilities != nil &&
			!sameStrings(profile.Defaults.Capabilities, profile.FixedCapabilities) {
			return Profile{}, fmt.Errorf(
				"runner profile %q has defaults capabilities that differ from fixed capabilities",
				profile.CommandName,
			)
		}
	}
	profile.RunnerIDPrefix = strings.TrimSpace(profile.RunnerIDPrefix)
	profile.RequiredRunnerIDPrefix = strings.TrimSpace(profile.RequiredRunnerIDPrefix)
	if profile.RequiredRunnerIDPrefix != "" && profile.RunnerIDPrefix != "" &&
		!strings.HasPrefix(profile.RunnerIDPrefix, profile.RequiredRunnerIDPrefix) {
		return Profile{}, fmt.Errorf(
			"runner profile %q default ID prefix %q does not satisfy required ID prefix %q",
			profile.CommandName, profile.RunnerIDPrefix, profile.RequiredRunnerIDPrefix,
		)
	}
	return profile, nil
}

func defaultRunnerConfigForProfile(profile Profile) runnerConfig {
	cfg := defaultRunnerConfig()
	cfg.profile = profile
	if profile.Defaults != nil {
		cfg.serverURL = profile.Defaults.ServerURL
		cfg.transport = profile.Defaults.Transport
		cfg.grpcTarget = profile.Defaults.GRPCTarget
		if profile.Defaults.Capabilities != nil {
			cfg.capRaw = strings.Join(profile.Defaults.Capabilities, ",")
			cfg.capabilities = parseCapabilities(cfg.capRaw)
		}
		cfg.autoLabels = profile.Defaults.AutoLabels
	}
	if profile.FixedCapabilities != nil && (profile.Defaults == nil || profile.Defaults.Capabilities == nil) {
		cfg.capRaw = strings.Join(profile.FixedCapabilities, ",")
		cfg.capabilities = parseCapabilities(cfg.capRaw)
	}
	if profile.RunnerIDPrefix != "" {
		cfg.runnerID = profiledRunnerID(profile.RunnerIDPrefix)
	}
	return cfg
}

func profiledRunnerID(prefix string) string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return fmt.Sprintf("%s%d", prefix, os.Getpid())
	}
	return fmt.Sprintf("%s%s-%d", prefix, hostname, os.Getpid())
}

func applyProfile(cfg runnerConfig) (runnerConfig, error) {
	profile := cfg.profile
	for key, required := range profile.RequiredLabels {
		if actual, ok := cfg.labels[key]; ok && actual != required {
			return runnerConfig{}, fmt.Errorf(
				"runner profile %q requires label %s=%s, got %s",
				profile.CommandName, key, required, actual,
			)
		}
		if cfg.labels == nil {
			cfg.labels = make(map[string]string, len(profile.RequiredLabels))
		}
		cfg.labels[key] = required
	}
	if profile.FixedCapabilities != nil {
		actual := normalizedStrings(capabilityNames(cfg.capabilities))
		if !sameStrings(actual, profile.FixedCapabilities) {
			return runnerConfig{}, fmt.Errorf(
				"runner profile %q requires capabilities %s, got %s",
				profile.CommandName,
				strings.Join(profile.FixedCapabilities, ","),
				strings.Join(actual, ","),
			)
		}
	}
	return cfg, nil
}

func validateProfileRunnerID(profile Profile, runnerID string) error {
	prefix := profile.RequiredRunnerIDPrefix
	if prefix == "" {
		return nil
	}
	if !strings.HasPrefix(runnerID, prefix) {
		return fmt.Errorf(
			"runner profile %q requires runner ID prefix %q, got %q",
			profile.CommandName, prefix, runnerID,
		)
	}
	return nil
}

// requireProfileToken enforces a profile's credential requirement only after
// identity resolution. A file-backed identity or enrollment can supply the
// token after YAML/environment/flag resolution, so enforcing this in
// applyProfile would reject a valid restart before the identity store is read.
func requireProfileToken(cfg runnerConfig) error {
	if !cfg.profile.RequireToken || strings.TrimSpace(cfg.token) != "" {
		return nil
	}
	return fmt.Errorf("runner profile %q requires XFLOW_RUNNER_TOKEN or --token", cfg.profile.CommandName)
}

func capabilityNames(capabilities []protocol.Capability) []string {
	names := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		names = append(names, capability.NodeType)
	}
	return names
}

func normalizedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// profileSampleRunnerConfigYAML emits a safe, structurally valid example for
// a non-default profile. It deliberately leaves the bearer token in the
// environment, never in a config file. The caller already normalized profile.
func profileSampleRunnerConfigYAML(profile Profile) string {
	capabilities := profile.FixedCapabilities
	if capabilities == nil && profile.Defaults != nil && profile.Defaults.Capabilities != nil {
		capabilities = profile.Defaults.Capabilities
	}
	if len(capabilities) == 0 {
		capabilities = []string{"xflow.function"}
	}

	labels := cloneStringMap(profile.RequiredLabels)
	if len(labels) == 0 {
		labels = map[string]string{"mode": "remote"}
	}
	labelKeys := make([]string, 0, len(labels))
	for key := range labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)

	transport := TransportHTTP
	grpcTarget := "localhost:9090"
	serverURL := "https://REPLACE-ME:8080"
	autoLabels := true
	if profile.Defaults != nil {
		if strings.TrimSpace(profile.Defaults.ServerURL) != "" {
			serverURL = profile.Defaults.ServerURL
		}
		if strings.TrimSpace(profile.Defaults.Transport) != "" {
			transport = profile.Defaults.Transport
		}
		if strings.TrimSpace(profile.Defaults.GRPCTarget) != "" {
			grpcTarget = profile.Defaults.GRPCTarget
		}
		autoLabels = profile.Defaults.AutoLabels
	}

	var sample strings.Builder
	sample.WriteString("runner:\n")
	if profile.RunnerIDPrefix == "" {
		fmt.Fprintf(&sample, "  id: %s\n", strconv.Quote("runner-1"))
	} else {
		fmt.Fprintf(&sample, "  # id is omitted to use the %s<hostname>-<pid> profile default.\n", profile.RunnerIDPrefix)
	}
	sample.WriteString("  concurrency: 2\n")
	sample.WriteString("  labels:\n")
	for _, key := range labelKeys {
		fmt.Fprintf(&sample, "    %s: %s\n", strconv.Quote(key), strconv.Quote(labels[key]))
	}
	sample.WriteString("  capabilities:\n")
	sample.WriteString("    # Browser CDP work requires the \"xflow.browser.cdp\" capability.\n")
	for _, capability := range capabilities {
		fmt.Fprintf(&sample, "    - %s\n", strconv.Quote(capability))
	}
	fmt.Fprintf(&sample, "  auto_labels: %t\n\n", autoLabels)

	sample.WriteString("server:\n")
	fmt.Fprintf(&sample, "  transport: %s\n", strconv.Quote(transport))
	sample.WriteString("  # Replace the host while retaining https unless the deployment explicitly permits plaintext.\n")
	fmt.Fprintf(&sample, "  url: %s\n", strconv.Quote(serverURL))
	fmt.Fprintf(&sample, "  grpc_target: %s\n\n", strconv.Quote(grpcTarget))

	sample.WriteString("poll:\n  wait: \"1s\"\n\nheartbeat:\n  interval: \"5s\"\n")
	sample.WriteString(`

# Shared destination policy for xflow.http requests and Browser navigation.
# Hosts only: schemes, ports, paths, and userinfo are rejected at validation.
# http_host_policy:
#   allow: ["app.example.internal"]
#   deny: ["metadata.google.internal"]

# Browser CDP uses an existing remote-debugging endpoint; it never starts a
# browser. An empty endpoint_allowlist (the secure default) denies all endpoint
# connections until the exact hosts are listed. Browser navigation also denies
# all hosts unless http_host_policy is explicitly configured.
browser_cdp:
  endpoint_allowlist: []
  max_contexts: 1
  queue_timeout: "5s"
  connect_timeout: "5s"
`)
	if profile.RequireToken {
		sample.WriteString("\n# This profile requires a runner token. Use XFLOW_RUNNER_TOKEN (or --token),\n# a persisted identity, or a registration code; keep static tokens out of this file.\n")
	}
	return sample.String()
}
