//go:build integration

// TestRunnerRefusesPlaintextByDefault is the only coverage validateTransportSecurity
// has on a real runner process. cmd/runner's own tests drive the cobra command
// in-process, which proves the flag parses but not that a shipped binary acts on
// it; the three process harnesses here (r8, subgraph, metrics proxy) all pass
// --allow-plaintext, so without this test the gate could be deleted outright and
// every suite would stay green.
//
// It exists because the gate was introduced in f721e1e without touching those
// three harnesses, and the resulting breakage surfaced as a 60s timeout waiting
// for a runner that had already exited -- a shape that reads like a flake. That
// misdiagnosis is what this test is meant to prevent next time.
//
// Run it with -count=1 when you have touched cmd/runner. This package does not
// import cmd/runner -- the binary is produced by an exec'd `go build` at test
// time -- so go test's cache sees no changed dependency and will replay a stale
// PASS. Verified: mutating validateTransportSecurity to `return nil` left this
// test green at a byte-identical 16.59s until -count=1 was added, at which point
// it failed on exactly the one arm that asserts refusal. Every process harness
// in this package has the same blind spot.

package integration

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

// plaintextGateWindow bounds how long an accepted config is given to prove it
// did NOT hit the gate. The check runs synchronously at startup, before any
// dial, so a process still alive after this window has passed it. The window
// only needs to cover process spawn, not a network round trip.
const plaintextGateWindow = 3 * time.Second

func TestRunnerRefusesPlaintextByDefault(t *testing.T) {
	_, runnerBin := buildR8Binaries(t)

	// A closed address. The accepted arms must fail to connect, so their
	// survival past plaintextGateWindow can only mean the gate let them
	// through -- not that they found a control plane.
	addr := freeAddr(t)

	tests := []struct {
		name       string
		server     string
		extra      []string
		wantRefuse bool
	}{
		{
			// The shape all three harnesses had, and the one that broke them.
			name:       "✗ plain http with no opt-out is refused",
			server:     "http://" + addr,
			wantRefuse: true,
		},
		{
			// The opt-out the harnesses now pass. Pins that the flag actually
			// reaches the gate in a built binary.
			name:       "✓ --allow-plaintext is accepted",
			server:     "http://" + addr,
			extra:      []string{"--allow-plaintext"},
			wantRefuse: false,
		},
		{
			// The gate's other release path: an https URL needs no opt-out.
			// Without this arm, a mutation that made the gate refuse every
			// http transport regardless of scheme would still look correct.
			name:       "✓ https needs no opt-out",
			server:     "https://" + addr,
			wantRefuse: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{
				"run",
				"--server", tt.server,
				"--transport", "http",
				"--id", "plaintext-gate-probe",
				"--token", r8RunnerToken,
				"--cap", "xflow.function",
			}, tt.extra...)

			out := &safeBuffer{}
			cmd := exec.Command(runnerBin, args...)
			cmd.Stdout, cmd.Stderr = out, out
			if err := cmd.Start(); err != nil {
				t.Fatalf("✗ 起 runner 失败: %v", err)
			}

			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()

			var exited bool
			select {
			case <-done:
				exited = true
			case <-time.After(plaintextGateWindow):
				_ = cmd.Process.Kill()
				<-done
			}

			logs := out.String()
			refused := strings.Contains(logs, "refusing to start") &&
				strings.Contains(logs, "--allow-plaintext")

			// Both halves are asserted. Exit alone would also be satisfied by a
			// crash for an unrelated reason, and the message alone says nothing
			// about whether the process actually stopped.
			if tt.wantRefuse {
				if !refused {
					t.Fatalf("✗ 期望被门禁拒绝，但日志里没有拒绝信息:\n%s", logs)
				}
				if !exited {
					t.Fatalf("✗ 打印了拒绝信息却没有退出:\n%s", logs)
				}
				return
			}
			if refused {
				t.Fatalf("✗ 期望放行，却撞上了明文门禁:\n%s", logs)
			}
			if exited {
				// Reaching here means it died for some other reason. That is
				// still a failure -- the arm cannot prove the gate released it
				// if the process was never alive to be released.
				t.Fatalf("✗ 期望放行后进程存活至窗口结束，却提前退出:\n%s", logs)
			}
		})
	}
}
