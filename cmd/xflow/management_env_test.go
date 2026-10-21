package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestManagementHelpNeverPrintsTheAPIToken is the regression guard for the
// pflag default-value leak: reading XFLOW_API_TOKEN (and XFLOW_API_ADDR) into
// a flag default makes cobra render `(default "<value>")` in --help, which
// printed the bearer token to stdout. The defaults are now empty and the
// environment is read after parsing instead.
//
// Both bare commands and --help are covered: a parent without a Run function
// prints its own help, and every subcommand inherits the persistent flags whose
// defaults previously carried the secret.
func TestManagementHelpNeverPrintsTheAPIToken(t *testing.T) {
	const token = "tok-help-secret-do-not-print"
	const addr = "http://env-marker.invalid:9"
	t.Setenv("XFLOW_API_TOKEN", token)
	t.Setenv("XFLOW_API_ADDR", addr)

	for _, args := range [][]string{
		{"runner"},
		{"runner", "--help"},
		{"runner", "list", "--help"},
		{"runner", "drain", "--help"},
		{"execution"},
		{"execution", "cancel", "--help"},
		{"execution", "rerun", "--help"},
		{"dead-letter"},
		{"dead-letter", "--help"},
		{"dead-letter", "list", "--help"},
		{"dead-letter", "replay", "--help"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var out bytes.Buffer
			if err := executeRootWith(&out, args...); err != nil {
				t.Fatalf("executeRootWith(%v): %v", args, err)
			}
			rendered := out.String()
			if rendered == "" {
				t.Fatalf("executeRootWith(%v) produced no help output", args)
			}
			// stdout and stderr share the buffer; the token must not reach either.
			if strings.Contains(rendered, token) {
				t.Fatalf("help for %v printed the XFLOW_API_TOKEN value; a non-empty pflag "+
					"default is echoed in --help:\n%s", args, rendered)
			}
			if strings.Contains(rendered, addr) {
				t.Fatalf("help for %v printed the XFLOW_API_ADDR value; the address is not "+
					"secret but it is help noise:\n%s", args, rendered)
			}
		})
	}
}

// TestRunnerAndExecutionResolveConfigFromTheEnvironment proves the post-parse
// fallback actually reaches the wire, not just the required-flag check: the
// commands are invoked with no --server/--token at all.
func TestRunnerAndExecutionResolveConfigFromTheEnvironment(t *testing.T) {
	t.Run("runner list", func(t *testing.T) {
		var rec recordedRequest
		srv := managementTestServer(t, []map[string]any{}, http.StatusOK, &rec)
		t.Setenv("XFLOW_API_ADDR", srv.URL)
		t.Setenv("XFLOW_API_TOKEN", "env-token-runner")

		var out bytes.Buffer
		if err := executeRootWith(&out, "runner", "list"); err != nil {
			t.Fatalf("runner list with config from the environment: %v", err)
		}
		if rec.path != "/v1/management/runners" {
			t.Fatalf("path = %q, want /v1/management/runners", rec.path)
		}
		if rec.auth != "Bearer env-token-runner" {
			t.Fatalf("Authorization = %q, want the XFLOW_API_TOKEN value: the token has "+
				"to reach the header, not just satisfy the required check", rec.auth)
		}
	})

	t.Run("execution cancel", func(t *testing.T) {
		var rec recordedRequest
		srv := managementTestServer(t, map[string]any{"accepted": true}, http.StatusOK, &rec)
		t.Setenv("XFLOW_API_ADDR", srv.URL)
		t.Setenv("XFLOW_API_TOKEN", "env-token-exec")

		var out bytes.Buffer
		if err := executeRootWith(&out, "execution", "cancel", "exec-1"); err != nil {
			t.Fatalf("execution cancel with config from the environment: %v", err)
		}
		if rec.path != "/v1/executions/exec-1/cancel" {
			t.Fatalf("path = %q, want /v1/executions/exec-1/cancel", rec.path)
		}
		if rec.auth != "Bearer env-token-exec" {
			t.Fatalf("Authorization = %q, want Bearer env-token-exec", rec.auth)
		}
		var result map[string]any
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
		}
		if result["status"] != "canceled" {
			t.Fatalf("status = %v, want canceled", result["status"])
		}
	})
}

// TestRunnerReadCommandsRejectUnknownOutputFormat covers the -o validation that
// guards the two read commands; an unknown format must fail before any request.
func TestRunnerReadCommandsRejectUnknownOutputFormat(t *testing.T) {
	for _, args := range [][]string{
		{"runner", "--server", "http://127.0.0.1:1", "--token", "tok", "list", "-o", "yaml"},
		{"runner", "--server", "http://127.0.0.1:1", "--token", "tok", "get", "r1", "-o", "yaml"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var out bytes.Buffer
			err := executeRootWith(&out, args...)
			if err == nil {
				t.Fatalf("executeRootWith(%v) returned nil, want an -o rejection", args)
			}
			if !strings.Contains(err.Error(), `--output must be "table" or "json"`) {
				t.Fatalf("error = %v, want it to name the two accepted output formats", err)
			}
		})
	}
}
