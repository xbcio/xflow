package runnerapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/protocol"
)

func testWorkloadProfile() Profile {
	return Profile{
		CommandName:    "dedicated-runner",
		Short:          "Dedicated XFlow workload runner",
		RunnerIDPrefix: "dedicated-runner-",
		Defaults: &Defaults{
			ServerURL:  "",
			Transport:  TransportHTTP,
			GRPCTarget: "",
			AutoLabels: false,
		},
		RequiredLabels: map[string]string{
			"workload": "dedicated-runner",
		},
		FixedCapabilities: []string{
			"xflow.trigger.timer",
			"xflow.http",
			"xflow.function",
		},
		RequireToken: true,
	}
}

func executeProfileForTest(t *testing.T, profile Profile, args ...string) (runnerConfig, error) {
	t.Helper()
	var ran runnerConfig
	err := executeRootWithOptionsAndProfile(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, profile, args...)
	return ran, err
}

func TestProfileUsesFixedCapabilitiesAsDefaults(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TOKEN", "test-token")

	ran, err := executeProfileForTest(t, testWorkloadProfile(),
		"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext", "--label", "region=test")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ran.labels["workload"], "dedicated-runner"; got != want {
		t.Fatalf("workload label = %q, want %q", got, want)
	}
	if got, want := ran.labels["region"], "test"; got != want {
		t.Fatalf("extra label = %q, want %q", got, want)
	}
	if len(ran.labels) != 2 {
		t.Fatalf("labels = %#v, want exactly required and explicit labels", ran.labels)
	}
	if got, want := normalizedStrings(capabilityNames(ran.capabilities)), normalizedStrings(testWorkloadProfile().FixedCapabilities); !sameStrings(got, want) {
		t.Fatalf("capabilities = %v, want %v", got, want)
	}
	if ran.transport != TransportHTTP {
		t.Fatalf("transport = %q, want %q", ran.transport, TransportHTTP)
	}
	if !strings.HasPrefix(ran.runnerID, "dedicated-runner-") {
		t.Fatalf("default runner ID = %q, want dedicated-runner prefix", ran.runnerID)
	}
}

func TestProfileRequiresRunnerIDPrefixAcrossConfigAndIdentitySources(t *testing.T) {
	profile := testWorkloadProfile()
	profile.RequiredRunnerIDPrefix = "dedicated-runner-"

	t.Run("static command line", func(t *testing.T) {
		t.Setenv("XFLOW_RUNNER_TOKEN", "test-token")
		err := executeRootWithOptionsAndProfile(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}}, profile,
			"config", "validate", "--server", "http://127.0.0.1:8080", "--allow-plaintext", "--id", "wrong-runner")
		if err == nil || !strings.Contains(err.Error(), `requires runner ID prefix "dedicated-runner-"`) {
			t.Fatalf("error = %v, want required ID prefix error", err)
		}
	})

	t.Run("stored identity", func(t *testing.T) {
		identityPath := filepath.Join(t.TempDir(), "identity.json")
		if err := os.WriteFile(identityPath, []byte(`{"runner_id":"wrong-runner","token":"stored-token"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		restore := stubRunnerServiceFactory(func(xflowsdk.RunnerConfig) error {
			t.Fatal("runner constructed with an identity outside the required prefix")
			return nil
		})
		defer restore()

		err := executeRootWithOptionsAndProfile(commandOptions{
			runFunc: func(cfg runnerConfig) error { return runRunner(context.Background(), cfg) },
			out:     &bytes.Buffer{},
			err:     &bytes.Buffer{},
		}, profile,
			"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext",
			"--identity-store", "file", "--identity-file", identityPath)
		if err == nil || !strings.Contains(err.Error(), `requires runner ID prefix "dedicated-runner-"`) {
			t.Fatalf("error = %v, want stored identity prefix error", err)
		}
	})

	t.Run("valid stored identity takes precedence over an invalid explicit ID", func(t *testing.T) {
		identityPath := filepath.Join(t.TempDir(), "identity.json")
		if err := os.WriteFile(identityPath, []byte(`{"runner_id":"dedicated-runner-stored","token":"stored-token"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
			if got, want := cfg.RunnerID, "dedicated-runner-stored"; got != want {
				t.Fatalf("runner ID = %q, want stored identity %q", got, want)
			}
			return nil
		})
		defer restore()

		err := executeRootWithOptionsAndProfile(commandOptions{
			runFunc: func(cfg runnerConfig) error { return runRunner(context.Background(), cfg) },
			out:     &bytes.Buffer{},
			err:     &bytes.Buffer{},
		}, profile,
			"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext", "--id", "wrong-runner",
			"--identity-store", "file", "--identity-file", identityPath)
		if err != nil {
			t.Fatalf("run with valid stored identity: %v", err)
		}
	})

	t.Run("enrollment response is not persisted", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != protocol.EnrollPath {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{RunnerID: "wrong-runner", Token: "issued-token"})
		}))
		defer server.Close()

		identityPath := filepath.Join(t.TempDir(), "identity.json")
		restore := stubRunnerServiceFactory(func(xflowsdk.RunnerConfig) error {
			t.Fatal("runner constructed with an enrolled identity outside the required prefix")
			return nil
		})
		defer restore()

		err := executeRootWithOptionsAndProfile(commandOptions{
			runFunc: func(cfg runnerConfig) error { return runRunner(context.Background(), cfg) },
			out:     &bytes.Buffer{},
			err:     &bytes.Buffer{},
		}, profile,
			"run", "--server", server.URL, "--allow-plaintext", "--registration-code", "one-time-code",
			"--identity-store", "file", "--identity-file", identityPath)
		if err == nil || !strings.Contains(err.Error(), `requires runner ID prefix "dedicated-runner-"`) {
			t.Fatalf("error = %v, want enrollment identity prefix error", err)
		}
		if _, err := os.Stat(identityPath); !os.IsNotExist(err) {
			t.Fatalf("identity file stat error = %v, want no persisted invalid identity", err)
		}
	})
}

func TestProfileRejectsIncompatibleRequiredAndDefaultIDPrefixes(t *testing.T) {
	_, err := NewCommand(Profile{
		CommandName:            "bad-profile",
		RunnerIDPrefix:         "dedicated-",
		RequiredRunnerIDPrefix: "sas-runner-",
	})
	if err == nil || !strings.Contains(err.Error(), "does not satisfy required ID prefix") {
		t.Fatalf("error = %v, want incompatible ID prefix error", err)
	}
}

func TestProfileRejectsConflictingRequiredLabelFromEveryInput(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) []string
	}{
		{
			name: "command line",
			setup: func(_ *testing.T) []string {
				return []string{"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext", "--label", "workload=local"}
			},
		},
		{
			name: "environment",
			setup: func(t *testing.T) []string {
				t.Setenv("XFLOW_RUNNER_LABELS", "workload=local")
				return []string{"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext"}
			},
		},
		{
			name: "yaml",
			setup: func(t *testing.T) []string {
				path := filepath.Join(t.TempDir(), "runner.yaml")
				if err := os.WriteFile(path, []byte("runner:\n  labels:\n    workload: local\nserver:\n  url: http://127.0.0.1:8080\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				return []string{"run", "--config", path, "--allow-plaintext"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XFLOW_RUNNER_TOKEN", "test-token")
			_, err := executeProfileForTest(t, testWorkloadProfile(), tt.setup(t)...)
			if err == nil || !strings.Contains(err.Error(), "requires label workload=dedicated-runner") {
				t.Fatalf("error = %v, want required label conflict", err)
			}
		})
	}
}

func TestProfileRejectsDifferentCapabilitiesFromEveryInput(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) []string
	}{
		{
			name: "command line",
			setup: func(_ *testing.T) []string {
				return []string{"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext", "--cap", "xflow.http"}
			},
		},
		{
			name: "environment",
			setup: func(t *testing.T) []string {
				t.Setenv("XFLOW_RUNNER_CAP", "xflow.http")
				return []string{"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext"}
			},
		},
		{
			name: "yaml",
			setup: func(t *testing.T) []string {
				path := filepath.Join(t.TempDir(), "runner.yaml")
				if err := os.WriteFile(path, []byte("runner:\n  capabilities:\n    - xflow.http\nserver:\n  url: http://127.0.0.1:8080\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				return []string{"run", "--config", path, "--allow-plaintext"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XFLOW_RUNNER_TOKEN", "test-token")
			_, err := executeProfileForTest(t, testWorkloadProfile(), tt.setup(t)...)
			if err == nil || !strings.Contains(err.Error(), "requires capabilities") {
				t.Fatalf("error = %v, want fixed capability conflict", err)
			}
		})
	}
}

func TestProfileConfigValidateAppliesPolicy(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TOKEN", "test-token")
	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte("runner:\n  labels:\n    workload: local\nserver:\n  url: https://runner.example.test\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := executeRootWithOptionsAndProfile(commandOptions{
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, testWorkloadProfile(), "--config", path, "config", "validate")
	if err == nil || !strings.Contains(err.Error(), "requires label workload=dedicated-runner") {
		t.Fatalf("error = %v, want profile validation error", err)
	}
}

func TestProfileRequiresCredentialBeforeRunnerCreation(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TOKEN", "")
	constructed := false
	previous := newRunnerService
	newRunnerService = func(xflowsdk.RunnerConfig, ...xflowsdk.RunnerOption) (runnerService, error) {
		constructed = true
		return nil, nil
	}
	t.Cleanup(func() { newRunnerService = previous })

	err := executeRootWithOptionsAndProfile(commandOptions{
		runFunc: func(cfg runnerConfig) error { return runRunner(context.Background(), cfg) },
		out:     &bytes.Buffer{},
		err:     &bytes.Buffer{},
	}, testWorkloadProfile(),
		"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext")
	if err == nil || !strings.Contains(err.Error(), "requires XFLOW_RUNNER_TOKEN or --token") {
		t.Fatalf("error = %v, want missing token error", err)
	}
	if constructed {
		t.Fatal("runner was constructed without a static token, stored identity, or registration code")
	}
}

func TestProfileConfigValidateAcceptsEveryCredentialSource(t *testing.T) {
	identityPath := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(identityPath, []byte(`{"runner_id":"stored-runner","token":"stored-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "static token",
			args: []string{"--token", "static-token"},
		},
		{
			name: "stored identity",
			args: []string{"--identity-store", "file", "--identity-file", identityPath},
		},
		{
			name: "registration code",
			args: []string{"--registration-code", "one-time-code"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--server", "https://runner.example.test", "config", "validate"}, tt.args...)
			if err := executeRootWithOptionsAndProfile(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}}, testWorkloadProfile(), args...); err != nil {
				t.Fatalf("config validate: %v", err)
			}
		})
	}
}

func TestProfileRunRestoresStoredIdentityBeforeRunnerCreation(t *testing.T) {
	identityPath := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(identityPath, []byte(`{"runner_id":"stored-runner","token":"stored-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.RunnerID != "stored-runner" || cfg.Token != "stored-token" {
			t.Fatalf("SDK identity = (%q, %q), want stored identity", cfg.RunnerID, cfg.Token)
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptionsAndProfile(commandOptions{
		runFunc: func(cfg runnerConfig) error { return runRunner(context.Background(), cfg) },
		out:     &bytes.Buffer{},
		err:     &bytes.Buffer{},
	}, testWorkloadProfile(),
		"run", "--server", "http://127.0.0.1:8080", "--allow-plaintext",
		"--identity-store", "file", "--identity-file", identityPath)
	if err != nil {
		t.Fatalf("run with stored identity: %v", err)
	}
}

func TestProfileRunEnrollsBeforeRunnerCreation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.EnrollPath {
			t.Errorf("path = %q, want enrollment path", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var req protocol.EnrollRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode enrollment request: %v", err)
		}
		if req.RegistrationCode != "one-time-code" {
			t.Errorf("registration code = %q, want one-time-code", req.RegistrationCode)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{RunnerID: "issued-runner", Token: "issued-token"})
	}))
	defer server.Close()

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.RunnerID != "issued-runner" || cfg.Token != "issued-token" {
			t.Fatalf("SDK identity = (%q, %q), want enrollment identity", cfg.RunnerID, cfg.Token)
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptionsAndProfile(commandOptions{
		runFunc: func(cfg runnerConfig) error { return runRunner(context.Background(), cfg) },
		out:     &bytes.Buffer{},
		err:     &bytes.Buffer{},
	}, testWorkloadProfile(),
		"run", "--server", server.URL, "--allow-plaintext", "--registration-code", "one-time-code")
	if err != nil {
		t.Fatalf("run with registration code: %v", err)
	}
}

func TestProfileSampleConformsToProfile(t *testing.T) {
	profile := testWorkloadProfile()
	var out bytes.Buffer
	cmd, err := newRootCommandForProfile(commandOptions{out: &out, err: &bytes.Buffer{}}, profile)
	if err != nil {
		t.Fatal(err)
	}
	cmd.SetArgs([]string{"config", "sample"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "dedicated-runner") || !strings.Contains(out.String(), "xflow.trigger.timer") {
		t.Fatalf("sample = %q, want profile labels and capabilities", out.String())
	}

	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XFLOW_RUNNER_TOKEN", "test-token")
	err = executeRootWithOptionsAndProfile(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}}, profile,
		"--config", path, "config", "validate")
	if err != nil {
		t.Fatalf("profile sample did not validate: %v", err)
	}
}

func TestProfileSamplesUseConfiguredServerAndValidateAcrossTransports(t *testing.T) {
	tests := []struct {
		name         string
		transport    string
		serverURL    string
		grpcTarget   string
		validateArgs []string
	}{
		{
			name:       "http",
			transport:  TransportHTTP,
			serverURL:  "https://runner-http.example.test",
			grpcTarget: "runner-http.example.test:9090",
		},
		{
			name:         "grpc",
			transport:    TransportGRPC,
			serverURL:    "https://runner-grpc.example.test",
			grpcTarget:   "runner-grpc.example.test:9090",
			validateArgs: []string{"--allow-plaintext"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile := testWorkloadProfile()
			profile.Defaults = &Defaults{
				ServerURL:  tt.serverURL,
				Transport:  tt.transport,
				GRPCTarget: tt.grpcTarget,
				AutoLabels: false,
			}
			var out bytes.Buffer
			cmd, err := newRootCommandForProfile(commandOptions{out: &out, err: &bytes.Buffer{}}, profile)
			if err != nil {
				t.Fatal(err)
			}
			cmd.SetArgs([]string{"config", "sample"})
			if err := cmd.Execute(); err != nil {
				t.Fatalf("sample: %v", err)
			}
			if !strings.Contains(out.String(), tt.serverURL) {
				t.Fatalf("sample = %q, want configured server URL %q", out.String(), tt.serverURL)
			}

			path := filepath.Join(t.TempDir(), "runner.yaml")
			if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
			args := append([]string{"--config", path, "config", "validate", "--token", "test-token"}, tt.validateArgs...)
			if err := executeRootWithOptionsAndProfile(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}}, profile, args...); err != nil {
				t.Fatalf("sample validation: %v", err)
			}
		})
	}
}

func TestProfileVerifyUsesResolvedStoredAndEnrolledIdentities(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, serverURL string) []string
		wantID    string
		wantToken string
	}{
		{
			name: "stored identity",
			setup: func(t *testing.T, _ string) []string {
				path := filepath.Join(t.TempDir(), "identity.json")
				if err := os.WriteFile(path, []byte(`{"runner_id":"stored-verify","token":"stored-verify-token"}`), 0o600); err != nil {
					t.Fatal(err)
				}
				return []string{"--identity-store", "file", "--identity-file", path}
			},
			wantID:    "stored-verify",
			wantToken: "stored-verify-token",
		},
		{
			name: "registration code",
			setup: func(_ *testing.T, _ string) []string {
				return []string{"--registration-code", "verify-code"}
			},
			wantID:    "issued-verify",
			wantToken: "issued-verify-token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var registered protocol.RegisterRunnerRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case protocol.EnrollPath:
					if tt.name != "registration code" {
						t.Errorf("stored identity unexpectedly enrolled")
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{RunnerID: "issued-verify", Token: "issued-verify-token"})
				case protocol.RegisterRunnerPath:
					if got, want := r.Header.Get("Authorization"), "Bearer "+tt.wantToken; got != want {
						t.Errorf("Authorization = %q, want %q", got, want)
					}
					if err := json.NewDecoder(r.Body).Decode(&registered); err != nil {
						t.Errorf("decode register request: %v", err)
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(protocol.RegisterRunnerResponse{RunnerID: registered.RunnerID, SessionID: "verify-session"})
				case protocol.HeartbeatPath:
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(protocol.HeartbeatResponse{})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			args := append([]string{"verify", "--server", server.URL, "--allow-plaintext"}, tt.setup(t, server.URL)...)
			if err := executeRootWithOptionsAndProfile(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}}, testWorkloadProfile(), args...); err != nil {
				t.Fatalf("verify: %v", err)
			}
			if registered.RunnerID != tt.wantID {
				t.Fatalf("registered runner ID = %q, want %q", registered.RunnerID, tt.wantID)
			}
		})
	}
}

func TestEmptyProfilePreservesGenericRunnerBehavior(t *testing.T) {
	var ran runnerConfig
	err := executeRootWithOptionsAndProfile(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, Profile{}, "run", "--server", "http://127.0.0.1:8080", "--allow-plaintext", "--cap", "xflow.http")
	if err != nil {
		t.Fatal(err)
	}
	if ran.labels["workload"] != "" {
		t.Fatalf("generic profile injected workload label: %#v", ran.labels)
	}
	if got, want := capabilityNames(ran.capabilities), []string{"xflow.http"}; !sameStrings(normalizedStrings(got), normalizedStrings(want)) {
		t.Fatalf("generic capabilities = %v, want %v", got, want)
	}

	cmd, err := NewCommand(Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cmd.Use, "xflow-runner"; got != want {
		t.Fatalf("generic command use = %q, want %q", got, want)
	}
}

func TestProfileRejectsConflictingDefaultAndFixedCapabilities(t *testing.T) {
	_, err := NewCommand(Profile{
		Defaults:          &Defaults{Capabilities: []string{"xflow.http"}},
		FixedCapabilities: []string{"xflow.function"},
	})
	if err == nil || !strings.Contains(err.Error(), "differ from fixed capabilities") {
		t.Fatalf("error = %v, want conflicting profile capabilities", err)
	}
}
