package control

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const (
	defaultRunnerControlReceiptRetention   = 24 * time.Hour
	defaultRunnerDrainObservationFreshness = 30 * time.Second
	defaultRunnerDrainDeadline             = 30 * time.Minute
)

// RunnerDesiredState is the server-authoritative scheduling intent for a
// runner. ACTIVE permits new queue claims; DRAINING permits only replay of an
// already-finalized lease owned by the runner's current session.
type RunnerDesiredState string

const (
	RunnerDesiredStateActive   RunnerDesiredState = "active"
	RunnerDesiredStateDraining RunnerDesiredState = "draining"
)

// RunnerDrainPhase is a read-only progress projection. It is deliberately not
// a desired state: resume always transitions the desired state back to ACTIVE.
type RunnerDrainPhase string

const (
	RunnerDrainPhaseQuiescing RunnerDrainPhase = "quiescing"
	RunnerDrainPhaseComplete  RunnerDrainPhase = "complete"
	// RunnerDrainPhaseTimedOut means the drain deadline elapsed before the
	// complete predicate became true. It deliberately leaves the desired state
	// DRAINING: the admission gate stays closed until an operator resumes.
	RunnerDrainPhaseTimedOut RunnerDrainPhase = "timed_out"
)

var (
	// ErrRunnerControlRequestIDRequired prevents a mutating control request from
	// being retried as a fresh state transition after a transport failure.
	ErrRunnerControlRequestIDRequired = errors.New("runner control request id is required")
	// ErrRunnerControlRequestConflict means one idempotency receipt was reused
	// with a different request body.
	ErrRunnerControlRequestConflict = errors.New("runner control request id was reused with a different request")
	// ErrRunnerControlInvalidState reports an unsupported desired state.
	ErrRunnerControlInvalidState = errors.New("invalid runner control desired state")
)

// RunnerDrainSnapshot is the server-side progress view for one draining
// runner. UnsettledDebt is derived from the durable directory handoff ledger;
// the split counters diagnose whether work is reserved, uncertain, or already
// finalized. Completion additionally requires a session- and
// generation-fenced runner-local observation; it never stops work itself.
type RunnerDrainSnapshot struct {
	Phase                    RunnerDrainPhase `json:"phase"`
	DeadlineAt               *time.Time       `json:"deadline_at,omitempty"`
	ActiveClaims             int              `json:"active_claims"`
	LeasedTasks              int              `json:"leased_tasks"`
	UnsettledDebt            int              `json:"unsettled_drain_debt"`
	HandoffDebt              int              `json:"handoff_debt"`
	LeaseMayExistDebt        int              `json:"lease_may_exist_debt"`
	ReplayableDebt           int              `json:"replayable_debt"`
	PendingActivationCleanup int              `json:"pending_activation_cleanup"`
	ServerQuiescent          bool             `json:"server_quiescent"`
	RunnerQuiescent          bool             `json:"runner_quiescent"`
}

// RunnerControlSnapshot is durable desired state plus its current directory
// progress projection. Actor is retained internally for audit/receipts and is
// intentionally not returned by the public management snapshot.
type RunnerControlSnapshot struct {
	DesiredState RunnerDesiredState   `json:"desired_state"`
	Generation   uint64               `json:"generation"`
	RequestedAt  *time.Time           `json:"requested_at,omitempty"`
	Reason       string               `json:"reason,omitempty"`
	Drain        *RunnerDrainSnapshot `json:"drain,omitempty"`
}

// RunnerControlRequest describes one idempotent desired-state transition.
// RequestHash must be a canonical hash of the action body, not a caller-supplied
// opaque value. The management module constructs it from the validated request.
type RunnerControlRequest struct {
	RunnerID     string
	DesiredState RunnerDesiredState
	Actor        string
	Reason       string
	Action       string
	RequestID    string
	RequestHash  string
	Now          time.Time
}

// RunnerControlDirectory is an optional extension of RunnerDirectory. A
// directory exposes the management write surface only when it can atomically
// persist a desired-state transition and gate its own ClaimForRunner path.
type RunnerControlDirectory interface {
	SetRunnerControl(ctx context.Context, req RunnerControlRequest) (RunnerControlSnapshot, error)
	RunnerControl(ctx context.Context, runnerID string) (RunnerControlSnapshot, bool, error)
}

type runnerControlReceipt struct {
	RequestHash string
	Snapshot    RunnerControlSnapshot
	StoredAt    time.Time
	ExpiresAt   time.Time
	Status      int
}

type memoryRunnerControl struct {
	desired          RunnerDesiredState
	generation       uint64
	requestedAt      time.Time
	actor            string
	reason           string
	receipts         map[string]runnerControlReceipt
	drainObservation *runnerDrainObservation
	drainDeadline    time.Time
}

// runnerDrainObservation is persisted only after a heartbeat has passed its
// live session fence. It deliberately records the heartbeat's top-level
// in-flight count rather than inventing a second worker counter.
type runnerDrainObservation struct {
	sessionID         string
	generation        uint64
	recoveryOnly      bool
	inFlight          int
	activeActivations uint32
	observedAt        time.Time
}

func validateRunnerControlRequest(req RunnerControlRequest) error {
	if req.RunnerID == "" {
		return ErrRunnerIDRequired
	}
	if req.DesiredState != RunnerDesiredStateActive && req.DesiredState != RunnerDesiredStateDraining {
		return fmt.Errorf("%w: %q", ErrRunnerControlInvalidState, req.DesiredState)
	}
	if req.RequestID == "" || req.RequestHash == "" {
		return ErrRunnerControlRequestIDRequired
	}
	return nil
}

func runnerControlAction(req RunnerControlRequest) string {
	if req.Action != "" {
		return req.Action
	}
	if req.DesiredState == RunnerDesiredStateDraining {
		return "drain"
	}
	return "resume"
}

// runnerControlReceiptID has a fixed safe Redis-hash-field representation even
// when an authenticated subject includes punctuation. It intentionally binds
// the principal, route/action, runner and request id together.
func runnerControlReceiptID(req RunnerControlRequest) string {
	// Length-prefix every component rather than concatenating with a delimiter.
	// Principal subjects are server-issued but need not be restricted to the
	// request-ID alphabet, so delimiter joining would let embedded NULs create
	// ambiguous tuples.
	hash := sha256.New()
	for _, component := range []string{req.Actor, runnerControlAction(req), req.RunnerID, req.RequestID} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(component)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(component))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func activeRunnerControl() memoryRunnerControl {
	return memoryRunnerControl{
		desired:  RunnerDesiredStateActive,
		receipts: make(map[string]runnerControlReceipt),
	}
}

func cloneRunnerControlSnapshot(src RunnerControlSnapshot) RunnerControlSnapshot {
	out := src
	if src.RequestedAt != nil {
		at := *src.RequestedAt
		out.RequestedAt = &at
	}
	if src.Drain != nil {
		drain := *src.Drain
		if src.Drain.DeadlineAt != nil {
			deadline := *src.Drain.DeadlineAt
			drain.DeadlineAt = &deadline
		}
		out.Drain = &drain
	}
	return out
}

func runnerControlProjection(
	control memoryRunnerControl,
	sessionID string,
	stats handoffDebtStats,
	pendingActivationCleanup int,
	now time.Time,
	observationFreshness time.Duration,
) RunnerControlSnapshot {
	out := RunnerControlSnapshot{
		DesiredState: control.desired,
		Generation:   control.generation,
		Reason:       control.reason,
	}
	if !control.requestedAt.IsZero() {
		at := control.requestedAt
		out.RequestedAt = &at
	}
	if control.desired != RunnerDesiredStateDraining {
		return out
	}
	serverQuiescent := stats.activeClaims == 0 &&
		stats.leasedTasks == 0 &&
		stats.unsettledDebt == 0 &&
		pendingActivationCleanup == 0
	runnerQuiescent := drainObservationQuiescent(control.drainObservation, sessionID, control.generation, now, observationFreshness)
	phase := RunnerDrainPhaseQuiescing
	if serverQuiescent && runnerQuiescent {
		phase = RunnerDrainPhaseComplete
	} else if runnerDrainDeadlineElapsed(control.drainDeadline, now) {
		phase = RunnerDrainPhaseTimedOut
	}
	var deadlineAt *time.Time
	if !control.drainDeadline.IsZero() {
		deadline := control.drainDeadline
		deadlineAt = &deadline
	}
	out.Drain = &RunnerDrainSnapshot{
		Phase:                    phase,
		DeadlineAt:               deadlineAt,
		ActiveClaims:             stats.activeClaims,
		LeasedTasks:              stats.leasedTasks,
		UnsettledDebt:            stats.unsettledDebt,
		HandoffDebt:              stats.handoffDebt,
		LeaseMayExistDebt:        stats.leaseMayExistDebt,
		ReplayableDebt:           stats.replayableDebt,
		PendingActivationCleanup: pendingActivationCleanup,
		ServerQuiescent:          serverQuiescent,
		RunnerQuiescent:          runnerQuiescent,
	}
	return out
}

func newRunnerDrainObservation(req HeartbeatRequest) *runnerDrainObservation {
	return newRunnerDrainObservationAt(req, req.Now)
}

// newRunnerDrainObservationAt records the service-side observation time. The
// heartbeat timestamp is a liveness datum supplied by the runner; it must not
// be trusted as the freshness clock for a completion acknowledgement.
func newRunnerDrainObservationAt(req HeartbeatRequest, observedAt time.Time) *runnerDrainObservation {
	if req.DrainObservation == nil {
		return nil
	}
	return &runnerDrainObservation{
		sessionID:         req.SessionID,
		generation:        req.DrainObservation.Generation,
		recoveryOnly:      req.DrainObservation.RecoveryOnly,
		inFlight:          req.InFlight,
		activeActivations: req.DrainObservation.ActiveActivations,
		observedAt:        observedAt,
	}
}

func drainObservationQuiescent(observation *runnerDrainObservation, sessionID string, generation uint64, now time.Time, freshness time.Duration) bool {
	if observation == nil ||
		observation.sessionID == "" ||
		observation.sessionID != sessionID ||
		observation.generation != generation ||
		!observation.recoveryOnly ||
		observation.inFlight != 0 ||
		observation.activeActivations != 0 ||
		observation.observedAt.IsZero() ||
		now.IsZero() ||
		now.Before(observation.observedAt) {
		return false
	}
	if freshness <= 0 {
		freshness = defaultRunnerDrainObservationFreshness
	}
	// The exact freshness boundary remains valid; a sample only expires after
	// the age grows strictly beyond the configured window.
	return now.Sub(observation.observedAt) <= freshness
}

func runnerDrainDeadlineElapsed(deadline, now time.Time) bool {
	return !deadline.IsZero() && !now.IsZero() && !now.Before(deadline)
}
