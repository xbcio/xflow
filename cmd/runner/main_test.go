package main

import "testing"

func TestParseRunnerConfig(t *testing.T) {
	cfg, err := parseRunnerConfig([]string{
		"-server", "http://localhost:8080",
		"-id", "runner-1",
		"-concurrency", "2",
		"-cap", "xflow.function,xflow.http",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.serverURL != "http://localhost:8080" || cfg.runnerID != "runner-1" || cfg.concurrency != 2 {
		t.Fatalf("config = %+v", cfg)
	}
	if len(cfg.capabilities) != 2 || cfg.capabilities[0].NodeType != "xflow.function" || cfg.capabilities[1].NodeType != "xflow.http" {
		t.Fatalf("capabilities = %+v", cfg.capabilities)
	}
}
