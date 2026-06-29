package main

import (
	"bytes"
	"testing"
)

func TestExecuteRootRunCommandParsesLegacySingleDashFlags(t *testing.T) {
	var ran runnerConfig
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	},
		"run",
		"-server", "http://localhost:8080",
		"-id", "runner-1",
		"-concurrency", "2",
		"-cap", "xflow.function,xflow.http",
	)
	if err != nil {
		t.Fatal(err)
	}
	if ran.serverURL != "http://localhost:8080" || ran.runnerID != "runner-1" || ran.concurrency != 2 {
		t.Fatalf("config = %+v", ran)
	}
	if len(ran.capabilities) != 2 || ran.capabilities[0].NodeType != "xflow.function" || ran.capabilities[1].NodeType != "xflow.http" {
		t.Fatalf("capabilities = %+v", ran.capabilities)
	}
}

func TestExecuteRootDefaultsToRunCommandWithLegacySingleDashFlag(t *testing.T) {
	var ran runnerConfig
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "-id", "runner-root")
	if err != nil {
		t.Fatal(err)
	}
	if ran.runnerID != "runner-root" {
		t.Fatalf("runner id = %q, want runner-root", ran.runnerID)
	}
}
