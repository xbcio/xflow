package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
	"gopkg.in/yaml.v3"
)

type runnerConfigFile struct {
	Runner struct {
		ID           *string           `yaml:"id"`
		Concurrency  *int              `yaml:"concurrency"`
		Capabilities *[]string         `yaml:"capabilities"`
		Labels       map[string]string `yaml:"labels"`
		Namespaces   *[]string         `yaml:"namespaces"`
		AutoLabels   *bool             `yaml:"auto_labels"`
	} `yaml:"runner"`
	Server struct {
		URL        *string `yaml:"url"`
		Transport  *string `yaml:"transport"`
		GRPCTarget *string `yaml:"grpc_target"`
	} `yaml:"server"`
	Poll struct {
		Wait *string `yaml:"wait"`
	} `yaml:"poll"`
	Heartbeat struct {
		Interval *string `yaml:"interval"`
	} `yaml:"heartbeat"`
	Metrics struct {
		Addr           *string `yaml:"addr"`            // Prometheus scrape listen address
		Report         *bool   `yaml:"report"`          // ship metrics to the server
		ReportInterval *string `yaml:"report_interval"` // local reporting cadence
	} `yaml:"metrics"`
	Identity struct {
		Store *string `yaml:"store"` // "ephemeral" (default) or "file"
		File  *string `yaml:"file"`
	} `yaml:"identity"`
	Security struct {
		AllowPlaintext          *bool `yaml:"allow_plaintext"`
		RequireSupplyEncryption *bool `yaml:"require_supply_encryption"`
	} `yaml:"security"`
	// Credentials holds named credential maps (driver/dsn, token/base_url, …)
	// consumed by resource-aware nodes via input.Credential(name). String leaves
	// are expanded via os.Expand at load time so secrets are sourced from the
	// environment, not the config file. A referenced-but-unset env var fails
	// closed (see expandEnvCredentialValues).
	Credentials map[string]map[string]any `yaml:"credentials"`
	// ResourcePool, when present, overrides the default pool tunables. Absent
	// means the runner uses types.DefaultResourcePoolConfig().
	ResourcePool *resourcePoolFile `yaml:"resource_pool"`
}

// resourcePoolFile mirrors the resource_pool YAML section. Durations are
// parsed from their string form so YAML's implicit typing does not coerce
// them into ints (a bare "30m" is a string; YAML only treats unquoted 30 as
// an int).
type resourcePoolFile struct {
	SQL  *sqlPoolFile  `yaml:"sql"`
	GRPC *grpcPoolFile `yaml:"grpc"`
}

type sqlPoolFile struct {
	MaxOpenConns    *int    `yaml:"max_open_conns"`
	MaxIdleConns    *int    `yaml:"max_idle_conns"`
	ConnMaxLifetime *string `yaml:"conn_max_lifetime"`
}

type grpcPoolFile struct {
	KeepaliveTime    *string `yaml:"keepalive_time"`
	KeepaliveTimeout *string `yaml:"keepalive_timeout"`
}

func defaultRunnerConfig() runnerConfig {
	return runnerConfig{
		serverURL:             "http://localhost:8080",
		transport:             transportGRPC,
		grpcTarget:            "localhost:9090",
		runnerID:              fmt.Sprintf("runner-%d", os.Getpid()),
		concurrency:           1,
		capRaw:                "xflow.function",
		capabilities:          parseCapabilities("xflow.function"),
		heartbeatInterval:     "5s",
		pollWait:              "1s",
		reportMetricsInterval: "15s",
		identityStoreKind:     identityStoreEphemeral,
		autoLabels:            true,
	}
}

func loadRunnerConfig(path string) (runnerConfig, error) {
	if path == "" {
		return defaultRunnerConfig(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return runnerConfig{}, err
	}

	cfg, err := loadRunnerConfigFromBytes(data)
	if err != nil {
		return runnerConfig{}, err
	}
	cfg.configPath = path
	return cfg, nil
}

func loadRunnerConfigFromBytes(data []byte) (runnerConfig, error) {
	cfg := defaultRunnerConfig()

	var file runnerConfigFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return runnerConfig{}, err
	}

	if file.Server.URL != nil {
		cfg.serverURL = *file.Server.URL
	}
	if file.Server.Transport != nil {
		cfg.transport = *file.Server.Transport
	}
	if file.Server.GRPCTarget != nil {
		cfg.grpcTarget = *file.Server.GRPCTarget
	}
	if file.Runner.ID != nil {
		cfg.runnerID = *file.Runner.ID
	}
	if file.Runner.Concurrency != nil {
		cfg.concurrency = *file.Runner.Concurrency
	}
	if file.Runner.Capabilities != nil {
		cfg.capRaw = strings.Join(*file.Runner.Capabilities, ",")
	}
	if file.Runner.Labels != nil {
		cfg.labels = cloneStringMap(file.Runner.Labels)
		cfg.labelRaw = labelsToRaw(cfg.labels)
	}
	if file.Runner.Namespaces != nil {
		cfg.namespaceRaw = *file.Runner.Namespaces
	}
	if file.Runner.AutoLabels != nil {
		cfg.autoLabels = *file.Runner.AutoLabels
	}
	if file.Poll.Wait != nil {
		cfg.pollWait = *file.Poll.Wait
	}
	if file.Heartbeat.Interval != nil {
		cfg.heartbeatInterval = *file.Heartbeat.Interval
	}
	if file.Metrics.Addr != nil {
		cfg.metricsAddr = *file.Metrics.Addr
	}
	if file.Metrics.Report != nil {
		cfg.reportMetrics = *file.Metrics.Report
	}
	if file.Metrics.ReportInterval != nil {
		cfg.reportMetricsInterval = *file.Metrics.ReportInterval
	}
	if file.Identity.Store != nil {
		cfg.identityStoreKind = *file.Identity.Store
	}
	if file.Identity.File != nil {
		cfg.identityFile = *file.Identity.File
	}
	if file.Security.AllowPlaintext != nil {
		cfg.allowPlaintext = *file.Security.AllowPlaintext
	}
	if file.Security.RequireSupplyEncryption != nil {
		cfg.requireSupplyEncryption = *file.Security.RequireSupplyEncryption
	}

	if len(file.Credentials) > 0 {
		// Copy first so we never mutate the yaml-parsed map.
		creds := make(map[string]map[string]any, len(file.Credentials))
		for name, m := range file.Credentials {
			cp := make(map[string]any, len(m))
			for k, v := range m {
				cp[k] = v
			}
			creds[name] = cp
		}
		if err := expandEnvCredentialValues(creds); err != nil {
			return runnerConfig{}, err
		}
		cfg.credentials = creds
	}

	if file.ResourcePool != nil {
		rpc, err := parseResourcePoolConfig(file.ResourcePool)
		if err != nil {
			return runnerConfig{}, err
		}
		cfg.resourcePoolConfig = rpc
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	cfg.labels = parseLabels(cfg.labelRaw)
	cfg.namespaces = parseNamespaces(cfg.namespaceRaw)
	return cfg, nil
}

func parseNamespaces(raw []string) []namespace.Namespace {
	if len(raw) == 0 {
		return nil
	}
	out := make([]namespace.Namespace, 0, len(raw))
	seen := make(map[namespace.Namespace]struct{})
	for _, part := range raw {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		t := namespace.Namespace(part)
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var runnerConfigIssueOrder = []string{
	"server",
	"transport",
	"grpc-target",
	"id",
	"concurrency",
	"cap",
	"label",
	"namespace",
	"heartbeat-interval",
	"poll-wait",
	"allow-plaintext",
	"require-supply-encryption",
	"auto-labels",
}

func applyEnvOverrides(cfg runnerConfig, getenv func(string) string) runnerConfig {
	return applyLookupEnvOverrides(cfg, func(key string) (string, bool) {
		v := getenv(key)
		return v, v != ""
	})
}

func applyLookupEnvOverrides(cfg runnerConfig, lookupEnv func(string) (string, bool)) runnerConfig {
	if v, ok := lookupEnv("XFLOW_RUNNER_SERVER"); ok {
		cfg.serverURL = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_TRANSPORT"); ok {
		cfg.transport = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_GRPC_TARGET"); ok {
		cfg.grpcTarget = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_ID"); ok {
		cfg.runnerID = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_CONCURRENCY"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			setRunnerConfigIssue(&cfg, "concurrency", fmt.Errorf("concurrency from XFLOW_RUNNER_CONCURRENCY must be a valid integer: %w", err))
		} else {
			clearRunnerConfigIssue(&cfg, "concurrency")
			cfg.concurrency = n
		}
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_CAP"); ok {
		cfg.capRaw = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_LABELS"); ok {
		cfg.labelRaw = splitCSV(v)
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_TENANTS"); ok {
		cfg.namespaceRaw = splitCSV(v)
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_HEARTBEAT_INTERVAL"); ok {
		cfg.heartbeatInterval = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_POLL_WAIT"); ok {
		cfg.pollWait = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_TOKEN"); ok {
		cfg.token = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_TLS_SERVER_CA"); ok {
		cfg.tlsServerCA = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_TLS_CLIENT_CERT"); ok {
		cfg.tlsClientCert = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_TLS_CLIENT_KEY"); ok {
		cfg.tlsClientKey = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_IDENTITY_STORE"); ok {
		cfg.identityStoreKind = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_IDENTITY_FILE"); ok {
		cfg.identityFile = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_REGISTRATION_CODE"); ok {
		cfg.registrationCode = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_ALLOW_PLAINTEXT"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			setRunnerConfigIssue(&cfg, "allow-plaintext",
				fmt.Errorf("XFLOW_RUNNER_ALLOW_PLAINTEXT must be a valid boolean: %w", err))
		} else {
			clearRunnerConfigIssue(&cfg, "allow-plaintext")
			cfg.allowPlaintext = b
		}
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_REQUIRE_SUPPLY_ENCRYPTION"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			setRunnerConfigIssue(&cfg, "require-supply-encryption",
				fmt.Errorf("XFLOW_RUNNER_REQUIRE_SUPPLY_ENCRYPTION must be a valid boolean: %w", err))
		} else {
			clearRunnerConfigIssue(&cfg, "require-supply-encryption")
			cfg.requireSupplyEncryption = b
		}
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_AUTO_LABELS"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			setRunnerConfigIssue(&cfg, "auto-labels",
				fmt.Errorf("XFLOW_RUNNER_AUTO_LABELS must be a valid boolean: %w", err))
		} else {
			clearRunnerConfigIssue(&cfg, "auto-labels")
			cfg.autoLabels = b
		}
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	cfg.labels = parseLabels(cfg.labelRaw)
	cfg.namespaces = parseNamespaces(cfg.namespaceRaw)
	return cfg
}

func parseLabels(raw []string) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	labels := make(map[string]string, len(raw))
	for _, item := range raw {
		key, value, ok := strings.Cut(item, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !ok {
			labels[key] = ""
			continue
		}
		labels[key] = value
	}
	return labels
}

// detectRunnerLabels derives labels from the process environment so a fleet is
// selectable by where it runs without every deployment restating it in its
// config. Standard library only — no cloud metadata call, no new dependency,
// nothing that can hang at startup.
//
// The xflow.io/ prefix keeps these out of the flat namespace an operator's own
// labels live in, so an auto key can be added later without colliding with a
// name someone already used.
func detectRunnerLabels() map[string]string {
	labels := map[string]string{
		"xflow.io/os":   runtime.GOOS,
		"xflow.io/arch": runtime.GOARCH,
		"xflow.io/env":  "bare",
	}
	// The kubelet injects KUBERNETES_SERVICE_HOST into every pod, so its
	// presence is the cheapest in-cluster signal that needs no API access.
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		labels["xflow.io/env"] = "kubernetes"
	}
	// A hostname that cannot be read is not a startup failure: it costs one
	// label, not the process.
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		labels["xflow.io/hostname"] = host
	}
	return labels
}

// mergeRunnerLabels overlays manual labels on detected ones. Manual wins: an
// operator who writes a label meant to override what the environment says.
// An explicit empty value (e.g. --label xflow.io/os=) still wins the merge,
// but validateRunnerConfig then rejects the empty value outright rather than
// clearing the label — refuse-to-start, not silently-dropped-label, is the
// safer failure here. Neither argument is mutated.
func mergeRunnerLabels(auto, manual map[string]string) map[string]string {
	if len(auto) == 0 && len(manual) == 0 {
		return nil
	}
	out := make(map[string]string, len(auto)+len(manual))
	for k, v := range auto {
		out[k] = v
	}
	for k, v := range manual {
		out[k] = v
	}
	return out
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func labelsToRaw(labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := make([]string, 0, len(labels))
	for key, value := range labels {
		out = append(out, key+"="+value)
	}
	return out
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func validateRunnerConfig(cfg runnerConfig) error {
	switch cfg.transport {
	case transportHTTP:
		u, err := url.Parse(cfg.serverURL)
		if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("server URL must be an absolute http or https URL: %q", cfg.serverURL)
		}
	case transportGRPC:
		if strings.TrimSpace(cfg.grpcTarget) == "" {
			return errors.New("grpc target is required for grpc transport")
		}
	default:
		return fmt.Errorf("transport must be %q or %q: %q", transportHTTP, transportGRPC, cfg.transport)
	}
	if strings.TrimSpace(cfg.runnerID) == "" {
		return errors.New("runner id is required")
	}
	if cfg.concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than zero: %d", cfg.concurrency)
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	if len(cfg.capabilities) == 0 {
		return errors.New("capabilities must contain at least one node type")
	}
	cfg.namespaces = parseNamespaces(cfg.namespaceRaw)
	for _, t := range cfg.namespaces {
		if err := namespace.Validate(t); err != nil {
			return fmt.Errorf("invalid namespace %q: %w", t, err)
		}
	}
	for key, value := range cfg.labels {
		if strings.TrimSpace(key) == "" {
			return errors.New("labels must not contain an empty key")
		}
		if value == "" {
			return fmt.Errorf("label %q must not have an empty value", key)
		}
	}
	if err := validatePositiveDuration("heartbeat interval", cfg.heartbeatInterval); err != nil {
		return err
	}
	if err := validatePositiveDuration("poll wait", cfg.pollWait); err != nil {
		return err
	}

	if err := validateTransportSecurity(cfg); err != nil {
		return err
	}

	// Build the store to validate its configuration; the value is discarded.
	// Doing it here means a bad --identity-store/--identity-file combination
	// fails at config resolution, the same place every other malformed value
	// fails, rather than at the first enrollment attempt.
	if _, err := newIdentityStore(cfg); err != nil {
		return err
	}

	return nil
}

// validateTransportSecurity refuses a control-plane connection that carries no
// transport encryption unless the operator opted in.
//
// The gate lives here, not in sdk/xflow's buildRunnerTLSConfig, on purpose:
// an embedded runner shares its host's connection policy and its host's
// judgement, while a standalone runner process is the one that ships a bearer
// token to a remote control plane with nothing else guarding it. Only the
// standalone profile gets the hard stop.
//
// "Encrypted" means either an https server URL (http transport) or at least one
// piece of TLS material (either transport). A gRPC runner has no URL scheme to
// read, so the material is its only signal.
func validateTransportSecurity(cfg runnerConfig) error {
	if cfg.allowPlaintext {
		return nil
	}
	hasTLSMaterial := strings.TrimSpace(cfg.tlsServerCA) != "" ||
		strings.TrimSpace(cfg.tlsClientCert) != "" ||
		strings.TrimSpace(cfg.tlsClientKey) != ""
	if cfg.transport == transportHTTP {
		if u, err := url.Parse(cfg.serverURL); err == nil && u.Scheme == "https" {
			return nil
		}
		return fmt.Errorf(
			"refusing to start: --server %q is plaintext and no TLS material is configured, "+
				"so the runner token would cross the network in the clear; "+
				"configure --tls-server-ca (and --tls-client-cert/--tls-client-key for mTLS), "+
				"use an https:// URL, or pass --allow-plaintext to accept the risk",
			cfg.serverURL)
	}
	if hasTLSMaterial {
		return nil
	}
	return errors.New(
		"refusing to start: no TLS material is configured for the grpc transport, " +
			"so the runner token would cross the network in the clear; " +
			"configure --tls-server-ca (and --tls-client-cert/--tls-client-key for mTLS), " +
			"or pass --allow-plaintext to accept the risk")
}

func validatePositiveDuration(name, raw string) error {
	_, err := parsePositiveDuration(name, raw)
	return err
}

func parsePositiveDuration(name, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", name, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive: %s", name, raw)
	}
	return d, nil
}

func resolveRunnerConfig(base runnerConfig) (runnerConfig, error) {
	cfg, err := loadRunnerConfig(base.configPath)
	if err != nil {
		return runnerConfig{}, err
	}

	cfg = applyLookupEnvOverrides(cfg, os.LookupEnv)
	cfg.configPath = base.configPath
	cfg.changed = base.changed

	if base.changed["server"] {
		clearRunnerConfigIssue(&cfg, "server")
		cfg.serverURL = base.serverURL
	}
	if base.changed["transport"] {
		clearRunnerConfigIssue(&cfg, "transport")
		cfg.transport = base.transport
	}
	if base.changed["grpc-target"] {
		clearRunnerConfigIssue(&cfg, "grpc-target")
		cfg.grpcTarget = base.grpcTarget
	}
	if base.changed["id"] {
		clearRunnerConfigIssue(&cfg, "id")
		cfg.runnerID = base.runnerID
	}
	if base.changed["concurrency"] {
		clearRunnerConfigIssue(&cfg, "concurrency")
		cfg.concurrency = base.concurrency
	}
	if base.changed["cap"] {
		clearRunnerConfigIssue(&cfg, "cap")
		cfg.capRaw = base.capRaw
	}
	if base.changed["label"] {
		clearRunnerConfigIssue(&cfg, "label")
		cfg.labelRaw = append([]string(nil), base.labelRaw...)
	}
	if base.changed["namespace"] {
		clearRunnerConfigIssue(&cfg, "namespace")
		cfg.namespaceRaw = append([]string(nil), base.namespaceRaw...)
	}
	if base.changed["heartbeat-interval"] {
		clearRunnerConfigIssue(&cfg, "heartbeat-interval")
		cfg.heartbeatInterval = base.heartbeatInterval
	}
	if base.changed["poll-wait"] {
		clearRunnerConfigIssue(&cfg, "poll-wait")
		cfg.pollWait = base.pollWait
	}
	// The credential-bearing flags. They were missing from this list, so they
	// bound, parsed and validated and were then dropped here — only
	// XFLOW_RUNNER_TOKEN and XFLOW_RUNNER_TLS_* ever took effect. Both halves
	// fail closed without naming a cause: no CA means every client falls back to
	// DefaultTransport, so supply fetch fails and the readiness gate declines
	// every activation; no token means the server rejects registration outright.
	if base.changed["token"] {
		cfg.token = base.token
	}
	if base.changed["tls-server-ca"] {
		cfg.tlsServerCA = base.tlsServerCA
	}
	if base.changed["tls-client-cert"] {
		cfg.tlsClientCert = base.tlsClientCert
	}
	if base.changed["tls-client-key"] {
		cfg.tlsClientKey = base.tlsClientKey
	}
	if base.changed["identity-store"] {
		cfg.identityStoreKind = base.identityStoreKind
	}
	if base.changed["identity-file"] {
		cfg.identityFile = base.identityFile
	}
	if base.changed["registration-code"] {
		cfg.registrationCode = base.registrationCode
	}
	if base.changed["allow-plaintext"] {
		clearRunnerConfigIssue(&cfg, "allow-plaintext")
		cfg.allowPlaintext = base.allowPlaintext
	}
	if base.changed["require-supply-encryption"] {
		clearRunnerConfigIssue(&cfg, "require-supply-encryption")
		cfg.requireSupplyEncryption = base.requireSupplyEncryption
	}
	if base.changed["auto-labels"] {
		clearRunnerConfigIssue(&cfg, "auto-labels")
		cfg.autoLabels = base.autoLabels
	}
	if base.changed["metrics-addr"] {
		cfg.metricsAddr = base.metricsAddr
	}
	if base.changed["report-metrics"] {
		cfg.reportMetrics = base.reportMetrics
	}
	if base.changed["report-metrics-interval"] {
		cfg.reportMetricsInterval = base.reportMetricsInterval
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	cfg.labels = parseLabels(cfg.labelRaw)
	if cfg.autoLabels {
		// Merged here, not in toSDKRunnerConfig: validateRunnerConfig below
		// checks every label key/value, and a detected label must face the
		// same check a hand-written one does.
		cfg.labels = mergeRunnerLabels(detectRunnerLabels(), cfg.labels)
	}
	cfg.namespaces = parseNamespaces(cfg.namespaceRaw)
	if err := firstRunnerConfigIssue(cfg); err != nil {
		return runnerConfig{}, err
	}
	if err := validateRunnerConfig(cfg); err != nil {
		return runnerConfig{}, err
	}
	return cfg, nil
}

func setRunnerConfigIssue(cfg *runnerConfig, key string, err error) {
	if cfg.resolutionIssues == nil {
		cfg.resolutionIssues = make(map[string]error)
	}
	cfg.resolutionIssues[key] = err
}

func clearRunnerConfigIssue(cfg *runnerConfig, key string) {
	if cfg.resolutionIssues == nil {
		return
	}
	delete(cfg.resolutionIssues, key)
	if len(cfg.resolutionIssues) == 0 {
		cfg.resolutionIssues = nil
	}
}

func firstRunnerConfigIssue(cfg runnerConfig) error {
	if len(cfg.resolutionIssues) == 0 {
		return nil
	}
	for _, key := range runnerConfigIssueOrder {
		if err := cfg.resolutionIssues[key]; err != nil {
			return err
		}
	}
	for _, err := range cfg.resolutionIssues {
		return err
	}
	return nil
}

func sampleRunnerConfigYAML() string {
	return `runner:
  id: "runner-1"
  concurrency: 2
  labels:
    mode: "remote"
  capabilities:
    - "xflow.function"
  # auto_labels: true   # adds xflow.io/os, /arch, /env, /hostname

server:
  # transport: "http" or "grpc" (default: grpc, see defaultRunnerConfig)
  transport: "http"
  url: "http://localhost:8080"
  grpc_target: "localhost:9090"

poll:
  wait: "1s"

heartbeat:
  interval: "5s"

# identity:
#   # "ephemeral" (default) keeps the enrolled identity in memory only;
#   # "file" persists it so a restart reuses the same runner ID and token
#   # instead of consuming another registration code.
#   store: "file"
#   file: "/var/lib/xflow/runner-identity.json"

# security:
#   # A plaintext control-plane connection ships the runner token in the clear.
#   # The runner refuses to start on one unless this is set.
#   allow_plaintext: false
#   # Exit if the control plane issues no supply encryption key at
#   # registration, rather than fetching supply content in the clear.
#   require_supply_encryption: false

# Credentials: named maps consumed by resource-aware nodes via
# input.Credential(name). String leaves are env-expanded at load time
# (${VAR} and $VAR) so secrets live in the environment, not the file. A
# referenced-but-unset env var fails closed at config load.
#
# credentials:
#   db:
#     driver: mysql
#     dsn: "user:${XFLOW_DB_PASSWORD}@tcp(db:3306)/xflow?parseTime=true"
#   api:
#     token: "${XFLOW_API_TOKEN}"
#     base_url: "https://api.example.com"
#
# resource_pool:
#   sql:
#     max_open_conns: 25
#     max_idle_conns: 5
#     conn_max_lifetime: "30m"
#   grpc:
#     keepalive_time: "30s"      # minimum 10s; grpc-go ignores anything smaller
#     keepalive_timeout: "10s"
`
}

// expandEnvCredentialValues walks each credential map and applies os.Expand to
// every string leaf. ${VAR} and $VAR are both supported. A referenced env var
// that is not set in the environment causes a fail-closed error so a
// misconfigured secret surfaces at load time rather than producing an empty
// DSN deep in a node. Non-string leaves (ints, bools, nested maps/slices) are
// left unchanged — only string leaves can carry ${VAR} placeholders.
//
// Security: expanded values are held only in the returned map (in memory,
// behind the resolver closure); they are never logged or written back to
// disk. The error message lists only the missing variable NAMES, never
// credential values.
func expandEnvCredentialValues(creds map[string]map[string]any) error {
	var missing []string
	seenMissing := make(map[string]struct{})
	expandMapping := func(name string) string {
		v, ok := os.LookupEnv(name)
		if !ok {
			if _, dup := seenMissing[name]; !dup {
				seenMissing[name] = struct{}{}
				missing = append(missing, name)
			}
			return ""
		}
		return v
	}
	for _, m := range creds {
		expandStringLeaves(m, expandMapping)
	}
	if len(missing) > 0 {
		return fmt.Errorf("credentials reference unset environment variables: %s", strings.Join(missing, ", "))
	}
	return nil
}

// expandStringLeaves recursively applies os.Expand to string leaves of v in
// place. Map and slice values are recursed; non-string scalars are left as-is.
func expandStringLeaves(v any, mapping func(string) string) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if s, ok := child.(string); ok {
				t[k] = os.Expand(s, mapping)
				continue
			}
			expandStringLeaves(child, mapping)
		}
	case []any:
		for i, child := range t {
			if s, ok := child.(string); ok {
				t[i] = os.Expand(s, mapping)
				continue
			}
			expandStringLeaves(child, mapping)
		}
	}
}

// parseResourcePoolConfig translates the YAML resource_pool section into a
// types.ResourcePoolConfig. Absent sub-sections leave the corresponding
// zero-valued config, so resource.NewDefaultResourcePool applies its own
// normalizeConfig defaults for those tunables (matching
// types.DefaultResourcePoolConfig).
func parseResourcePoolConfig(file *resourcePoolFile) (types.ResourcePoolConfig, error) {
	var cfg types.ResourcePoolConfig
	if file.SQL != nil {
		if file.SQL.MaxOpenConns != nil {
			cfg.SQL.MaxOpenConns = *file.SQL.MaxOpenConns
		}
		if file.SQL.MaxIdleConns != nil {
			cfg.SQL.MaxIdleConns = *file.SQL.MaxIdleConns
		}
		if file.SQL.ConnMaxLifetime != nil {
			d, err := parsePositiveDuration("resource_pool.sql.conn_max_lifetime", *file.SQL.ConnMaxLifetime)
			if err != nil {
				return types.ResourcePoolConfig{}, err
			}
			cfg.SQL.ConnMaxLifetime = d
		}
	}
	if file.GRPC != nil {
		if file.GRPC.KeepaliveTime != nil {
			d, err := parsePositiveDuration("resource_pool.grpc.keepalive_time", *file.GRPC.KeepaliveTime)
			if err != nil {
				return types.ResourcePoolConfig{}, err
			}
			// Refuse a value grpc-go would silently raise. Accepting it would
			// leave the operator with a config file that states an interval
			// the process does not use.
			if d < types.MinGRPCKeepaliveTime {
				return types.ResourcePoolConfig{}, fmt.Errorf(
					"resource_pool.grpc.keepalive_time must be at least %s, got %s: "+
						"grpc-go raises any smaller ping interval to that floor, so this "+
						"value would not take effect",
					types.MinGRPCKeepaliveTime, *file.GRPC.KeepaliveTime)
			}
			cfg.GRPC.KeepaliveTime = d
		}
		if file.GRPC.KeepaliveTimeout != nil {
			d, err := parsePositiveDuration("resource_pool.grpc.keepalive_timeout", *file.GRPC.KeepaliveTimeout)
			if err != nil {
				return types.ResourcePoolConfig{}, err
			}
			cfg.GRPC.KeepaliveTimeout = d
		}
	}
	return cfg, nil
}
