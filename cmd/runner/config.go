package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type runnerConfigFile struct {
	Runner struct {
		ID           *string           `yaml:"id"`
		Concurrency  *int              `yaml:"concurrency"`
		Capabilities *[]string         `yaml:"capabilities"`
		Labels       map[string]string `yaml:"labels"`
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
}

func defaultRunnerConfig() runnerConfig {
	return runnerConfig{
		serverURL:         "http://localhost:8080",
		transport:         transportGRPC,
		grpcTarget:        "localhost:9090",
		runnerID:          fmt.Sprintf("runner-%d", os.Getpid()),
		concurrency:       1,
		capRaw:            "xflow.function",
		capabilities:      parseCapabilities("xflow.function"),
		heartbeatInterval: "5s",
		pollWait:          "1s",
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
	if file.Poll.Wait != nil {
		cfg.pollWait = *file.Poll.Wait
	}
	if file.Heartbeat.Interval != nil {
		cfg.heartbeatInterval = *file.Heartbeat.Interval
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	cfg.labels = parseLabels(cfg.labelRaw)
	return cfg, nil
}

var runnerConfigIssueOrder = []string{
	"server",
	"transport",
	"grpc-target",
	"id",
	"concurrency",
	"cap",
	"label",
	"heartbeat-interval",
	"poll-wait",
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

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	cfg.labels = parseLabels(cfg.labelRaw)
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

	return nil
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
	if base.changed["heartbeat-interval"] {
		clearRunnerConfigIssue(&cfg, "heartbeat-interval")
		cfg.heartbeatInterval = base.heartbeatInterval
	}
	if base.changed["poll-wait"] {
		clearRunnerConfigIssue(&cfg, "poll-wait")
		cfg.pollWait = base.pollWait
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	cfg.labels = parseLabels(cfg.labelRaw)
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

server:
  # transport: "http" (default) or "grpc"
  transport: "http"
  url: "http://localhost:8080"
  grpc_target: "localhost:9090"

poll:
  wait: "1s"

heartbeat:
  interval: "5s"
`
}
