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
		ID           *string   `yaml:"id"`
		Concurrency  *int      `yaml:"concurrency"`
		Capabilities *[]string `yaml:"capabilities"`
	} `yaml:"runner"`
	Server struct {
		URL *string `yaml:"url"`
	} `yaml:"server"`
	Poll struct {
		Wait *string `yaml:"wait"`
	} `yaml:"poll"`
	Heartbeat struct {
		Interval *string `yaml:"interval"`
	} `yaml:"heartbeat"`
	Logging struct {
		Level  *string `yaml:"level"`
		Format *string `yaml:"format"`
	} `yaml:"logging"`
}

func defaultRunnerConfig() runnerConfig {
	return runnerConfig{
		serverURL:         "http://localhost:8080",
		runnerID:          fmt.Sprintf("runner-%d", os.Getpid()),
		concurrency:       1,
		capRaw:            "xflow.function",
		capabilities:      parseCapabilities("xflow.function"),
		heartbeatInterval: "5s",
		pollWait:          "1s",
		logLevel:          "info",
		logFormat:         "text",
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
	if file.Runner.ID != nil {
		cfg.runnerID = *file.Runner.ID
	}
	if file.Runner.Concurrency != nil {
		cfg.concurrency = *file.Runner.Concurrency
	}
	if file.Runner.Capabilities != nil {
		cfg.capRaw = strings.Join(*file.Runner.Capabilities, ",")
	}
	if file.Poll.Wait != nil {
		cfg.pollWait = *file.Poll.Wait
	}
	if file.Heartbeat.Interval != nil {
		cfg.heartbeatInterval = *file.Heartbeat.Interval
	}
	if file.Logging.Level != nil {
		cfg.logLevel = *file.Logging.Level
	}
	if file.Logging.Format != nil {
		cfg.logFormat = *file.Logging.Format
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	return cfg, nil
}

var runnerConfigIssueOrder = []string{
	"server",
	"id",
	"concurrency",
	"cap",
	"heartbeat-interval",
	"poll-wait",
	"log-level",
	"log-format",
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
	if v, ok := lookupEnv("XFLOW_RUNNER_HEARTBEAT_INTERVAL"); ok {
		cfg.heartbeatInterval = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_POLL_WAIT"); ok {
		cfg.pollWait = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_LOG_LEVEL"); ok {
		cfg.logLevel = v
	}
	if v, ok := lookupEnv("XFLOW_RUNNER_LOG_FORMAT"); ok {
		cfg.logFormat = v
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	return cfg
}

func validateRunnerConfig(cfg runnerConfig) error {
	u, err := url.Parse(cfg.serverURL)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("server URL must be an absolute http or https URL: %q", cfg.serverURL)
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
	if err := validatePositiveDuration("heartbeat interval", cfg.heartbeatInterval); err != nil {
		return err
	}
	if err := validatePositiveDuration("poll wait", cfg.pollWait); err != nil {
		return err
	}

	switch cfg.logLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log level must be debug, info, warn, or error: %q", cfg.logLevel)
	}

	switch cfg.logFormat {
	case "text", "json":
	default:
		return fmt.Errorf("log format must be text or json: %q", cfg.logFormat)
	}

	return nil
}

func validatePositiveDuration(name, raw string) error {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s must be a valid duration: %w", name, err)
	}
	if d <= 0 {
		return fmt.Errorf("%s must be positive: %s", name, raw)
	}
	return nil
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
	if base.changed["heartbeat-interval"] {
		clearRunnerConfigIssue(&cfg, "heartbeat-interval")
		cfg.heartbeatInterval = base.heartbeatInterval
	}
	if base.changed["poll-wait"] {
		clearRunnerConfigIssue(&cfg, "poll-wait")
		cfg.pollWait = base.pollWait
	}
	if base.changed["log-level"] {
		clearRunnerConfigIssue(&cfg, "log-level")
		cfg.logLevel = base.logLevel
	}
	if base.changed["log-format"] {
		clearRunnerConfigIssue(&cfg, "log-format")
		cfg.logFormat = base.logFormat
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
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
  capabilities:
    - "xflow.function"

server:
  url: "http://localhost:8080"

poll:
  wait: "1s"

heartbeat:
  interval: "5s"

logging:
  level: "info"
  format: "text"
`
}
