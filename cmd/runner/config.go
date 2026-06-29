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
	cfg := defaultRunnerConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return runnerConfig{}, err
	}

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
	cfg.configPath = path
	return cfg, nil
}

func applyEnvOverrides(cfg runnerConfig, getenv func(string) string) runnerConfig {
	if v := getenv("XFLOW_RUNNER_SERVER"); v != "" {
		cfg.serverURL = v
	}
	if v := getenv("XFLOW_RUNNER_ID"); v != "" {
		cfg.runnerID = v
	}
	if v := getenv("XFLOW_RUNNER_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.concurrency = n
		}
	}
	if v := getenv("XFLOW_RUNNER_CAP"); v != "" {
		cfg.capRaw = v
	}
	if v := getenv("XFLOW_RUNNER_HEARTBEAT_INTERVAL"); v != "" {
		cfg.heartbeatInterval = v
	}
	if v := getenv("XFLOW_RUNNER_POLL_WAIT"); v != "" {
		cfg.pollWait = v
	}
	if v := getenv("XFLOW_RUNNER_LOG_LEVEL"); v != "" {
		cfg.logLevel = v
	}
	if v := getenv("XFLOW_RUNNER_LOG_FORMAT"); v != "" {
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

	cfg = applyEnvOverrides(cfg, os.Getenv)
	cfg.configPath = base.configPath
	cfg.changed = base.changed

	if base.changed["server"] {
		cfg.serverURL = base.serverURL
	}
	if base.changed["id"] {
		cfg.runnerID = base.runnerID
	}
	if base.changed["concurrency"] {
		cfg.concurrency = base.concurrency
	}
	if base.changed["cap"] {
		cfg.capRaw = base.capRaw
	}
	if base.changed["heartbeat-interval"] {
		cfg.heartbeatInterval = base.heartbeatInterval
	}
	if base.changed["poll-wait"] {
		cfg.pollWait = base.pollWait
	}
	if base.changed["log-level"] {
		cfg.logLevel = base.logLevel
	}
	if base.changed["log-format"] {
		cfg.logFormat = base.logFormat
	}

	cfg.capabilities = parseCapabilities(cfg.capRaw)
	if err := validateRunnerConfig(cfg); err != nil {
		return runnerConfig{}, err
	}
	return cfg, nil
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
