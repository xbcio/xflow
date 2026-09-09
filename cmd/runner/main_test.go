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
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	},
		"run",
		"--server", "http://localhost:8080",
		"--id", "runner-1",
		"--concurrency", "2",
		"--cap", "xflow.function,xflow.http",
		"--allow-plaintext",
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

func TestNewRootCommandRunCommandParsesLabels(t *testing.T) {
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
		"--server", "http://localhost:8080",
		"--id", "runner-1",
		"--label", "mode=remote",
		"--label", "env=prod",
		"--allow-plaintext",
	)
	if err != nil {
		t.Fatal(err)
	}
	if ran.labels["mode"] != "remote" || ran.labels["env"] != "prod" {
		t.Fatalf("labels = %+v, want mode/env", ran.labels)
	}
}

func TestNewRootCommandRunCommandParsesLegacySingleDashFlags(t *testing.T) {
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
		"--allow-plaintext",
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

func TestNewRootCommandDefaultsToRunCommandWithLegacySingleDashFlag(t *testing.T) {
	var ran runnerConfig
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "-id", "runner-root", "--allow-plaintext")
	if err != nil {
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
		"--allow-plaintext",
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
	}, "-id", "runner-root", "--allow-plaintext")
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
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	},
		"run",
		"--server", "http://flag-server:8080",
		"--id", "flag-runner",
		"--concurrency", "2",
		"--cap", "xflow.function",
		"--allow-plaintext",
	)
	if err != nil {
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
	cfg, err := loadRunnerConfigFromBytes(out.Bytes())
	if err != nil {
		t.Fatalf("sample is not parseable: %v", err)
	}
	// Parseable is not the bar. A sample that parses but is refused at startup
	// is worse than no sample: the reader copies it verbatim, gets a hard stop,
	// and has no reason to suspect the sample itself. That is exactly what
	// happened -- validateTransportSecurity landed while the sample still
	// advertised a plaintext localhost url with allow_plaintext commented out.
	//
	// This assertion is what keeps the two locked together: changing the gate
	// or the sample without the other reddens here.
	if err := validateTransportSecurity(cfg); err != nil {
		t.Fatalf("sample would be refused at startup by validateTransportSecurity: %v", err)
	}
	// Passing the gate is not enough either, because there are two ways to pass
	// it and only one of them is safe to ship. The sample previously passed by
	// carrying allow_plaintext: true next to a plaintext url -- a reader who
	// edited the url to a real host and missed the adjacent line would have
	// shipped the runner token in the clear, and validateTransportSecurity has
	// no loopback exemption that would catch the mismatch. So assert the sample
	// passes on the https scheme rather than on the opt-out: allow_plaintext
	// must not be live in shipped copy, whatever the url happens to say.
	if cfg.allowPlaintext {
		t.Fatal("sample ships a live allow_plaintext: true; it must stay commented out so enabling it is an active choice")
	}
}

func TestConfigValidateRejectsInvalidFlagConfig(t *testing.T) {
	cmd := newRootCommand(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}})
	// --transport http with an invalid server URL should fail URL validation
	cmd.SetArgs([]string{"config", "validate", "--transport", "http", "--server", "localhost:8080"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "server URL") {
		t.Fatalf("error = %v, want server URL validation", err)
	}
}

func TestRootHelpDoesNotRunRunner(t *testing.T) {
	var out bytes.Buffer
	runCalls := 0

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			runCalls++
			return nil
		},
		out: &out,
		err: &bytes.Buffer{},
	}, "--help")
	if err != nil {
		t.Fatal(err)
	}
	if runCalls != 0 {
		t.Fatalf("runFunc calls = %d, want 0", runCalls)
	}
	if !strings.Contains(out.String(), "Usage:") || !strings.Contains(out.String(), "xflow-runner") {
		t.Fatalf("help output = %q", out.String())
	}
}

func TestRunHelpDoesNotRunRunner(t *testing.T) {
	var out bytes.Buffer
	runCalls := 0

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			runCalls++
			return nil
		},
		out: &out,
		err: &bytes.Buffer{},
	}, "run", "--help")
	if err != nil {
		t.Fatal(err)
	}
	if runCalls != 0 {
		t.Fatalf("runFunc calls = %d, want 0", runCalls)
	}
	if !strings.Contains(out.String(), "Run the xflow task runner") || !strings.Contains(out.String(), "Usage:") {
		t.Fatalf("help output = %q", out.String())
	}
}

// The reconnect loop this file used to test now lives in sdk/xflow, where
// Runner.Run owns it, and is covered by
// sdk/xflow.TestRunnerReconnectsAfterATransportError. That test is the stronger
// one: it drives a real HTTP control plane that starts returning 500 and proves
// recovery by a SECOND Register, where the test here fed the loop an errStop
// sentinel that no production path ever produced.
