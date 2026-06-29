package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

var _ func(commandOptions) *cobra.Command = newRootCommand

func TestNewRootCommandRunCommandParsesExistingFlags(t *testing.T) {
	var ran runnerConfig
	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{
		"run",
		"--server", "http://localhost:8080",
		"--id", "runner-1",
		"--concurrency", "2",
		"--cap", "xflow.function,xflow.http",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if ran.serverURL != "http://localhost:8080" || ran.runnerID != "runner-1" || ran.concurrency != 2 {
		t.Fatalf("config = %+v", ran)
	}
	if len(ran.capabilities) != 2 || ran.capabilities[0].NodeType != "xflow.function" || ran.capabilities[1].NodeType != "xflow.http" {
		t.Fatalf("capabilities = %+v", ran.capabilities)
	}
}

func TestNewRootCommandRunCommandParsesLegacySingleDashFlags(t *testing.T) {
	var ran runnerConfig
	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{
		"run",
		"-server", "http://localhost:8080",
		"-id", "runner-1",
		"-concurrency", "2",
		"-cap", "xflow.function,xflow.http",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if ran.serverURL != "http://localhost:8080" || ran.runnerID != "runner-1" || ran.concurrency != 2 {
		t.Fatalf("config = %+v", ran)
	}
	if len(ran.capabilities) != 2 || ran.capabilities[0].NodeType != "xflow.function" || ran.capabilities[1].NodeType != "xflow.http" {
		t.Fatalf("capabilities = %+v", ran.capabilities)
	}
}

func TestNewRootCommandDefaultsToRunCommandWithLegacySingleDashFlag(t *testing.T) {
	var ran runnerConfig
	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{"-id", "runner-root"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if ran.runnerID != "runner-root" {
		t.Fatalf("runner id = %q, want runner-root", ran.runnerID)
	}
}

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

func TestCLIFlagsOverrideEnvironment(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_SERVER", "http://env-server:8080")
	t.Setenv("XFLOW_RUNNER_ID", "env-runner")
	t.Setenv("XFLOW_RUNNER_CONCURRENCY", "5")
	t.Setenv("XFLOW_RUNNER_CAP", "xflow.http")

	var ran runnerConfig
	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{
		"run",
		"--server", "http://flag-server:8080",
		"--id", "flag-runner",
		"--concurrency", "2",
		"--cap", "xflow.function",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if ran.serverURL != "http://flag-server:8080" || ran.runnerID != "flag-runner" || ran.concurrency != 2 {
		t.Fatalf("config = %+v", ran)
	}
	if len(ran.capabilities) != 1 || ran.capabilities[0].NodeType != "xflow.function" {
		t.Fatalf("capabilities = %+v", ran.capabilities)
	}
}

func TestConfigSamplePrintsYAML(t *testing.T) {
	var out bytes.Buffer
	cmd := newRootCommand(commandOptions{out: &out, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{"config", "sample"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "runner:") || !strings.Contains(out.String(), "server:") {
		t.Fatalf("sample output = %q", out.String())
	}
	if _, err := loadRunnerConfigFromBytes([]byte(out.String())); err != nil {
		t.Fatalf("sample is not parseable: %v", err)
	}
}

func TestConfigValidateRejectsInvalidFlagConfig(t *testing.T) {
	cmd := newRootCommand(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{"config", "validate", "--server", "localhost:8080"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "server URL") {
		t.Fatalf("error = %v, want server URL validation", err)
	}
}
