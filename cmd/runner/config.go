package main

import (
	"fmt"
	"os"
)

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

func resolveRunnerConfig(cfg runnerConfig) (runnerConfig, error) {
	cfg.capabilities = parseCapabilities(cfg.capRaw)
	return cfg, nil
}
