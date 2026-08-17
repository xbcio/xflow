//go:build integration

package integration

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// alive reports whether pid still names a live (un-reaped) process. Signal 0
// performs the permission/existence check without delivering anything.
func alive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// waitGone polls until the pid disappears, so a slow exit is not read as a leak.
func waitGone(pid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !alive(pid) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return !alive(pid)
}

// TestTrackedR8ProcessDiesWithTheTest pins the harness contract that makes every
// other R8 subtest trustworthy: a tracked child is terminated by the test
// framework, not by a hand-written kill at the end of the happy path.
//
// This matters because r8KillMidFlightRestart's first runner is only reclaimed
// by a `runner.kill(t)` that sits *after* a g1WaitForNodeStatus which can
// t.Fatal. On that path the runner outlives the whole `go test` process, keeps
// polling the shared Redis for tasks, and steals work from later runs — a
// leaked runner from a failed run made a healthy tree look like a regression.
//
// `sleep` stands in for the runner binary on purpose: the contract under test is
// the harness's, and building the real binaries here would add a minute to prove
// nothing extra.
func TestTrackedR8ProcessDiesWithTheTest(t *testing.T) {
	var pid int
	t.Run("child", func(t *testing.T) {
		cmd := exec.Command("sleep", "120")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}
		trackR8Process(t, cmd, &safeBuffer{}, "harness-probe")
		pid = cmd.Process.Pid
		if !alive(pid) {
			t.Fatalf("child %d not alive right after start", pid)
		}
		// Deliberately no kill/stop: the subtest ends as if it had t.Fatal'd
		// before reaching its cleanup call.
	})
	if !waitGone(pid, 10*time.Second) {
		t.Fatalf("child %d survived its test; a leaked runner keeps consuming Redis tasks in later runs", pid)
	}
}

// TestTrackedR8ProcessTerminationIsIdempotent guards the other direction: the
// existing subtests call stop/kill explicitly, and the framework-owned cleanup
// must not turn that into a hang or a panic on an already-reaped child.
func TestTrackedR8ProcessTerminationIsIdempotent(t *testing.T) {
	cmd := exec.Command("sleep", "120")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	p := trackR8Process(t, cmd, &safeBuffer{}, "harness-idempotent")
	pid := cmd.Process.Pid

	done := make(chan struct{})
	go func() {
		p.stop(t)
		p.kill(t)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("explicit stop+kill of child %d hung", pid)
	}
	if alive(pid) {
		t.Fatalf("child %d still alive after explicit stop+kill", pid)
	}
}
