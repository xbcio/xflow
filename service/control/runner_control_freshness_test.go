package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

type runnerControlFreshnessDirectory interface {
	RunnerDirectory
	RunnerControlDirectory
}

type runnerControlFreshnessDirectoryFactory struct {
	name string
	new  func(t *testing.T, clock func() time.Time) runnerControlFreshnessDirectory
}

func runnerControlFreshnessDirectories() []runnerControlFreshnessDirectoryFactory {
	return []runnerControlFreshnessDirectoryFactory{
		{
			name: "memory",
			new: func(_ *testing.T, clock func() time.Time) runnerControlFreshnessDirectory {
				return NewMemoryRunnerDirectory(
					WithMemoryRunnerDirectoryClock(clock),
					WithMemoryRunnerDirectoryDrainObservationFreshness(5*time.Second),
					WithMemoryRunnerDirectoryDrainDeadline(10*time.Second),
				)
			},
		},
		{
			name: "redis",
			new: func(t *testing.T, clock func() time.Time) runnerControlFreshnessDirectory {
				_, rdb := newRedisRunnerDirectoryTestClient(t)
				return NewRedisRunnerDirectory(
					rdb,
					WithRedisRunnerDirectoryClock(clock),
					WithRedisRunnerDirectoryDrainObservationFreshness(5*time.Second),
					WithRedisRunnerDirectoryDrainDeadline(10*time.Second),
				)
			},
		},
	}
}

func registerRunnerControlFreshnessRunner(t *testing.T, ctx context.Context, directory runnerControlFreshnessDirectory, runnerID string, now time.Time) RunnerSession {
	t.Helper()

	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          now,
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

func runnerControlFreshnessRequest(runnerID, action, requestID, requestHash string, desired RunnerDesiredState, now time.Time) RunnerControlRequest {
	return RunnerControlRequest{
		RunnerID:     runnerID,
		DesiredState: desired,
		Actor:        "operator-freshness",
		Action:       action,
		Reason:       action + " reason",
		RequestID:    requestID,
		RequestHash:  requestHash,
		Now:          now,
	}
}

func runnerControlFreshnessHeartbeat(session RunnerSession, generation uint64, reportedAt time.Time) HeartbeatRequest {
	return HeartbeatRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Capacity:  1,
		InFlight:  0,
		Now:       reportedAt,
		DrainObservation: &protocol.RunnerDrainObservation{
			Generation: generation, RecoveryOnly: true,
		},
	}
}

func requireFreshnessDrain(t *testing.T, snapshot RunnerControlSnapshot, phase RunnerDrainPhase, deadline time.Time) {
	t.Helper()
	if snapshot.DesiredState != RunnerDesiredStateDraining || snapshot.Drain == nil {
		t.Fatalf("snapshot = %+v, want draining snapshot", snapshot)
	}
	if snapshot.Drain.Phase != phase {
		t.Fatalf("drain phase = %q, want %q (snapshot=%+v)", snapshot.Drain.Phase, phase, snapshot.Drain)
	}
	if snapshot.Drain.DeadlineAt == nil || !snapshot.Drain.DeadlineAt.Equal(deadline) {
		t.Fatalf("drain deadline = %v, want %v", snapshot.Drain.DeadlineAt, deadline)
	}
}

func TestRunnerControlDrainObservationFreshnessUsesServerClock(t *testing.T) {
	for _, factory := range runnerControlFreshnessDirectories() {
		t.Run(factory.name, func(t *testing.T) {
			ctx := context.Background()
			startedAt := time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)
			now := startedAt
			directory := factory.new(t, func() time.Time { return now })
			session := registerRunnerControlFreshnessRunner(t, ctx, directory, "runner-fresh", startedAt)
			drain, err := directory.SetRunnerControl(ctx, runnerControlFreshnessRequest(
				session.RunnerID, "drain", "freshness-drain", "freshness-drain-hash", RunnerDesiredStateDraining, startedAt,
			))
			if err != nil {
				t.Fatalf("SetRunnerControl(drain): %v", err)
			}
			deadline := startedAt.Add(10 * time.Second)

			// The runner's supplied heartbeat timestamp is intentionally absurdly
			// far in the future. A complete projection proves freshness was recorded
			// with the directory's server-owned clock rather than req.Now.
			if err := directory.Heartbeat(ctx, runnerControlFreshnessHeartbeat(session, drain.Generation, startedAt.Add(24*time.Hour))); err != nil {
				t.Fatalf("quiet heartbeat: %v", err)
			}
			current, found, err := directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseComplete, deadline)

			// The freshness boundary itself is inclusive, while the next nanosecond
			// makes the otherwise quiet observation insufficient for completion.
			now = startedAt.Add(5 * time.Second)
			current, found, err = directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() at boundary = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseComplete, deadline)
			now = now.Add(time.Nanosecond)
			current, found, err = directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() after boundary = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseQuiescing, deadline)

			// A generation mismatch must clear, rather than preserve, an earlier
			// quiet acknowledgement.
			wrongGeneration := runnerControlFreshnessHeartbeat(session, drain.Generation+1, startedAt)
			if err := directory.Heartbeat(ctx, wrongGeneration); err != nil {
				t.Fatalf("wrong-generation heartbeat: %v", err)
			}
			current, found, err = directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() after wrong generation = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseQuiescing, deadline)

			wrongSession := runnerControlFreshnessHeartbeat(session, drain.Generation, startedAt)
			wrongSession.SessionID = "obsolete-session"
			if err := directory.Heartbeat(ctx, wrongSession); !errors.Is(err, ErrRunnerSessionStale) {
				t.Fatalf("wrong-session heartbeat error = %v, want ErrRunnerSessionStale", err)
			}
		})
	}
}

func TestRunnerControlDeadlineTimesOutWithoutReopeningAdmission(t *testing.T) {
	for _, factory := range runnerControlFreshnessDirectories() {
		t.Run(factory.name, func(t *testing.T) {
			ctx := context.Background()
			startedAt := time.Date(2026, time.September, 16, 13, 0, 0, 0, time.UTC)
			now := startedAt
			directory := factory.new(t, func() time.Time { return now })
			session := registerRunnerControlFreshnessRunner(t, ctx, directory, "runner-deadline", startedAt)
			original := runnerControlFreshnessRequest(
				session.RunnerID, "drain", "deadline-drain", "deadline-drain-hash", RunnerDesiredStateDraining, startedAt,
			)
			first, err := directory.SetRunnerControl(ctx, original)
			if err != nil {
				t.Fatalf("SetRunnerControl(drain): %v", err)
			}
			deadline := startedAt.Add(10 * time.Second)
			requireFreshnessDrain(t, first, RunnerDrainPhaseQuiescing, deadline)

			// A new receipt which asks for the existing state cannot slide the
			// deadline; neither can a re-registration that replaces the session.
			now = startedAt.Add(2 * time.Second)
			sameState, err := directory.SetRunnerControl(ctx, runnerControlFreshnessRequest(
				session.RunnerID, "drain", "deadline-same", "deadline-same-hash", RunnerDesiredStateDraining, now,
			))
			if err != nil {
				t.Fatalf("SetRunnerControl(same drain): %v", err)
			}
			requireFreshnessDrain(t, sameState, RunnerDrainPhaseQuiescing, deadline)

			now = startedAt.Add(3 * time.Second)
			session = registerRunnerControlFreshnessRunner(t, ctx, directory, session.RunnerID, now)
			current, found, err := directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() after re-register = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseQuiescing, deadline)

			// Replaying the original receipt after the deadline must return its
			// first frozen response, while the live projection is timed out exactly
			// at the deadline.
			now = deadline
			retry := original
			retry.Now = now
			replayed, err := directory.SetRunnerControl(ctx, retry)
			if err != nil {
				t.Fatalf("SetRunnerControl(receipt replay): %v", err)
			}
			requireFreshnessDrain(t, replayed, RunnerDrainPhaseQuiescing, deadline)
			current, found, err = directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() at deadline = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseTimedOut, deadline)

			assignment := testAssignment("exec-deadline/queued/activation-1")
			if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil || !enqueued {
				t.Fatalf("EnqueueAssignment() = %v, %v; want enqueued", enqueued, err)
			}
			if claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}); err != nil || ok {
				t.Fatalf("timed-out drain claim = %+v, ok=%v, err=%v; want closed gate", claim, ok, err)
			}

			// Deadline expiry is diagnostic rather than terminal: a fresh, valid
			// quiet observation can still move the live draining projection to
			// complete, but it does not reopen the new-admission gate.
			if err := directory.Heartbeat(ctx, runnerControlFreshnessHeartbeat(session, first.Generation, now)); err != nil {
				t.Fatalf("quiet heartbeat after timeout: %v", err)
			}
			current, found, err = directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("RunnerControl() after quiet heartbeat = %+v, found=%v, err=%v", current, found, err)
			}
			requireFreshnessDrain(t, current, RunnerDrainPhaseComplete, deadline)
			if claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}); err != nil || ok {
				t.Fatalf("completed drain claim = %+v, ok=%v, err=%v; want closed gate", claim, ok, err)
			}

			now = now.Add(time.Second)
			resumed, err := directory.SetRunnerControl(ctx, runnerControlFreshnessRequest(
				session.RunnerID, "resume", "deadline-resume", "deadline-resume-hash", RunnerDesiredStateActive, now,
			))
			if err != nil {
				t.Fatalf("SetRunnerControl(resume): %v", err)
			}
			if resumed.DesiredState != RunnerDesiredStateActive || resumed.Drain != nil {
				t.Fatalf("resume snapshot = %+v, want active without drain deadline", resumed)
			}
		})
	}
}

func TestRedisRunnerControlDeadlineSurvivesDirectoryRecreation(t *testing.T) {
	ctx := context.Background()
	startedAt := time.Date(2026, time.September, 16, 15, 0, 0, 0, time.UTC)
	now := startedAt
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	newDirectory := func() *RedisRunnerDirectory {
		return NewRedisRunnerDirectory(
			rdb,
			WithRedisRunnerDirectoryClock(func() time.Time { return now }),
			WithRedisRunnerDirectoryDrainObservationFreshness(5*time.Second),
			WithRedisRunnerDirectoryDrainDeadline(10*time.Second),
		)
	}

	directory := newDirectory()
	session := registerRunnerControlFreshnessRunner(t, ctx, directory, "runner-recreated", startedAt)
	original := runnerControlFreshnessRequest(
		session.RunnerID, "drain", "recreated-drain", "recreated-drain-hash", RunnerDesiredStateDraining, startedAt,
	)
	first, err := directory.SetRunnerControl(ctx, original)
	if err != nil {
		t.Fatalf("SetRunnerControl(drain): %v", err)
	}
	deadline := startedAt.Add(10 * time.Second)
	requireFreshnessDrain(t, first, RunnerDrainPhaseQuiescing, deadline)

	// A process replacement has no local state to restore: both the current
	// deadline and the receipt's frozen deadline must come from Redis.
	now = startedAt.Add(4 * time.Second)
	recreated := newDirectory()
	current, found, err := recreated.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found {
		t.Fatalf("recreated RunnerControl() = %+v, found=%v, err=%v", current, found, err)
	}
	requireFreshnessDrain(t, current, RunnerDrainPhaseQuiescing, deadline)

	now = deadline
	retry := original
	retry.Now = now
	replayed, err := recreated.SetRunnerControl(ctx, retry)
	if err != nil {
		t.Fatalf("recreated receipt replay: %v", err)
	}
	requireFreshnessDrain(t, replayed, RunnerDrainPhaseQuiescing, deadline)
	current, found, err = recreated.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found {
		t.Fatalf("recreated RunnerControl() at deadline = %+v, found=%v, err=%v", current, found, err)
	}
	requireFreshnessDrain(t, current, RunnerDrainPhaseTimedOut, deadline)

	now = now.Add(time.Second)
	if _, err := recreated.SetRunnerControl(ctx, runnerControlFreshnessRequest(
		session.RunnerID, "resume", "recreated-resume", "recreated-resume-hash", RunnerDesiredStateActive, now,
	)); err != nil {
		t.Fatalf("SetRunnerControl(resume): %v", err)
	}
	restartedAgain := newDirectory()
	current, found, err = restartedAgain.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found {
		t.Fatalf("RunnerControl() after durable resume = %+v, found=%v, err=%v", current, found, err)
	}
	if current.DesiredState != RunnerDesiredStateActive || current.Drain != nil {
		t.Fatalf("durably resumed control = %+v, want active without a drain deadline", current)
	}
}

func TestDrainObservationQuiescentRejectsMissingAndFutureSamples(t *testing.T) {
	now := time.Date(2026, time.September, 16, 14, 0, 0, 0, time.UTC)
	quiet := &runnerDrainObservation{
		sessionID: "session-a", generation: 7, recoveryOnly: true, observedAt: now,
	}
	if !drainObservationQuiescent(quiet, "session-a", 7, now, time.Second) {
		t.Fatal("fresh quiet observation was not quiescent")
	}
	cases := []struct {
		name        string
		observation *runnerDrainObservation
		sessionID   string
		generation  uint64
		now         time.Time
	}{
		{name: "missing", sessionID: "session-a", generation: 7, now: now},
		{name: "future", observation: &runnerDrainObservation{sessionID: "session-a", generation: 7, recoveryOnly: true, observedAt: now.Add(time.Nanosecond)}, sessionID: "session-a", generation: 7, now: now},
		{name: "wrong session", observation: quiet, sessionID: "session-b", generation: 7, now: now},
		{name: "wrong generation", observation: quiet, sessionID: "session-a", generation: 8, now: now},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if drainObservationQuiescent(tc.observation, tc.sessionID, tc.generation, tc.now, time.Second) {
				t.Fatalf("drainObservationQuiescent(%+v) = true, want false", tc.observation)
			}
		})
	}
}
