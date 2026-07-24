//go:build soak

// Package soak provides the HA soak harness for the xflow control plane.
// See package doc in harness.go for scope / honesty boundaries.
package soak

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// SLO thresholds from ha-soak-plan §6. These are the baseline targets used by
// SLOReport.Evaluate. Real soak runs may tighten or loosen them, but the
// template and report type document the baseline honestly.
const (
	// TargetAPIAvailability is the minimum acceptable multi-replica API
	// availability outside maintenance windows (≥ 99.9%).
	TargetAPIAvailability = 0.999

	// TargetLeaderSwitchTime is the maximum acceptable leader failover latency,
	// expressed as 3 × the RedisLeaderElector lease TTL (3 × 15s = 45s).
	TargetLeaderSwitchTime = 45 * time.Second

	// TargetRecoveryTime is the maximum acceptable outbox / lease replay
	// convergence time after a fault (≤ 30s).
	TargetRecoveryTime = 30 * time.Second

	// TargetErrorRate is the maximum acceptable steady-state error rate (< 1%).
	TargetErrorRate = 0.01
)

// FaultOutcome records the result of one fault-injection row from the
// ha-soak-plan §4 matrix. The fields map directly to the report template
// columns: injection time, expected invariant, measured recovery time,
// duplicate delivery count, fenced commit outcome, and pass/fail verdict.
type FaultOutcome struct {
	Fault              string
	InjectedAt         time.Time
	ExpectedInvariant  string
	ActualRecoveryTime time.Duration
	DuplicateDelivery  int
	CommitOutcome      string
	Passed             bool
	Notes              string
}

// SLOReport captures the ha-soak-plan §6 SLO measurements and the §4 fault
// matrix outcomes. It satisfies the SLORecorder interface defined in harness.go
// and can be used as a drop-in replacement for the countingRecorder used in
// Task 5.2 fault tests.
//
// The exported fields are the computed / aggregated values suitable for
// rendering into the ha-soak-report template. All methods are safe for
// concurrent use.
type SLOReport struct {
	// APIAvailability is successful API calls / total API calls (0..1).
	APIAvailability float64

	// MaxLeaderSwitchTime is the worst-case (max) observed leader-switch
	// latency. The field cannot be named LeaderSwitchTime because that name is
	// already used by the SLORecorder method.
	MaxLeaderSwitchTime time.Duration

	// MaxRecoveryTime is the worst-case (max) observed recovery latency. The
	// field cannot be named RecoveryTime because that name is already used by
	// the SLORecorder method.
	MaxRecoveryTime time.Duration

	// DuplicateInvocationRate is duplicate invocations / total API samples.
	// The denominator uses API samples because the SLORecorder interface only
	// observes duplicate events, not total handler deliveries; real soak runs
	// should reconcile this against host idempotency-key logs.
	DuplicateInvocationRate float64

	// ErrorRate is failed API calls / total API calls.
	ErrorRate float64

	// Samples is the total number of API calls observed.
	Samples int

	// FaultMatrix is one row per injected fault class.
	FaultMatrix []FaultOutcome

	mu            sync.Mutex
	switchTimes   []time.Duration
	recoveryTimes []time.Duration
	duplicates    int
	apiTotal      int
	apiSuccess    int
	apiFailures   int
}

// Compile-time check: *SLOReport satisfies the SLORecorder interface declared
// in harness.go.
var _ SLORecorder = (*SLOReport)(nil)

// NewSLOReport returns an empty SLOReport ready to receive observations.
func NewSLOReport() *SLOReport {
	return &SLOReport{}
}

// RecordAPI records one API call result. success=true counts toward
// availability; success=false counts toward the error rate.
func (r *SLOReport) RecordAPI(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.apiTotal++
	if success {
		r.apiSuccess++
	} else {
		r.apiFailures++
	}
	r.recomputeLocked()
}

// RecordFaultOutcome appends one fault-matrix row. Injectors should call this
// after observing recovery for a given fault class.
func (r *SLOReport) RecordFaultOutcome(o FaultOutcome) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.FaultMatrix = append(r.FaultMatrix, o)
}

// --- SLORecorder implementation ---

// LeaderElected records that a replica won leadership.
func (r *SLOReport) LeaderElected(replicaIndex int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Topology event; not an SLO metric by itself. Keep the hook so the
	// recorder can be extended later (e.g. leader-index timeline).
	_ = replicaIndex
}

// LeaderLost records that a replica lost leadership.
func (r *SLOReport) LeaderLost(replicaIndex int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_ = replicaIndex
}

// LeaderSwitchTime records one leader-switch latency observation. The exported
// MaxLeaderSwitchTime field is updated to the max observed value.
func (r *SLOReport) LeaderSwitchTime(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.switchTimes = append(r.switchTimes, d)
	if d > r.MaxLeaderSwitchTime {
		r.MaxLeaderSwitchTime = d
	}
}

// RecoveryTime records one recovery-latency observation. The exported
// MaxRecoveryTime field is updated to the max observed value.
func (r *SLOReport) RecoveryTime(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.recoveryTimes = append(r.recoveryTimes, d)
	if d > r.MaxRecoveryTime {
		r.MaxRecoveryTime = d
	}
}

// DuplicateInvocation records one observed duplicate handler invocation.
func (r *SLOReport) DuplicateInvocation() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.duplicates++
	r.recomputeLocked()
}

// ReplicaStopped records a replica lifecycle event.
func (r *SLOReport) ReplicaStopped(replicaIndex int) { _ = replicaIndex }

// RunnerStarted records a runner lifecycle event.
func (r *SLOReport) RunnerStarted(runnerIndex int) { _ = runnerIndex }

// RunnerStopped records a runner lifecycle event.
func (r *SLOReport) RunnerStopped(runnerIndex int) { _ = runnerIndex }

// --- accessors ---

// LeaderSwitchTimeCount returns the number of leader-switch observations.
func (r *SLOReport) LeaderSwitchTimeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.switchTimes)
}

// RecoveryTimeCount returns the number of recovery-time observations.
func (r *SLOReport) RecoveryTimeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recoveryTimes)
}

// DuplicateInvocationCount returns the raw duplicate-invocation count.
func (r *SLOReport) DuplicateInvocationCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.duplicates
}

// --- evaluation / rendering ---

// EvaluationResult is the outcome of SLOReport.Evaluate.
type EvaluationResult struct {
	Passed   bool
	Failures []string
}

// Evaluate compares the aggregated SLO metrics against the ha-soak-plan §6
// thresholds and returns a pass/fail verdict with any failing items.
func (r *SLOReport) Evaluate() EvaluationResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evaluateLocked()
}

func (r *SLOReport) evaluateLocked() EvaluationResult {
	var failures []string
	if r.APIAvailability < TargetAPIAvailability {
		failures = append(failures, fmt.Sprintf("API availability %.4f < target %.4f", r.APIAvailability, TargetAPIAvailability))
	}
	if r.MaxLeaderSwitchTime > TargetLeaderSwitchTime {
		failures = append(failures, fmt.Sprintf("leader switch time %s > target %s", r.MaxLeaderSwitchTime, TargetLeaderSwitchTime))
	}
	if r.MaxRecoveryTime > TargetRecoveryTime {
		failures = append(failures, fmt.Sprintf("recovery time %s > target %s", r.MaxRecoveryTime, TargetRecoveryTime))
	}
	if r.ErrorRate >= TargetErrorRate {
		failures = append(failures, fmt.Sprintf("error rate %.4f >= target %.4f", r.ErrorRate, TargetErrorRate))
	}
	return EvaluationResult{Passed: len(failures) == 0, Failures: failures}
}

// Render returns a structured text representation of the SLO report, suitable
// for appending to the ha-soak-report template or emitting in test logs.
func (r *SLOReport) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "=== xflow HA Soak SLO Report ===\n")
	fmt.Fprintf(&b, "Samples (API calls): %d\n", r.Samples)
	fmt.Fprintf(&b, "API Availability: %.4f (target >= %.4f)\n", r.APIAvailability, TargetAPIAvailability)
	fmt.Fprintf(&b, "Leader Switch Time (worst): %s (target <= %s)\n", r.MaxLeaderSwitchTime, TargetLeaderSwitchTime)
	fmt.Fprintf(&b, "Recovery Time (worst): %s (target <= %s)\n", r.MaxRecoveryTime, TargetRecoveryTime)
	fmt.Fprintf(&b, "Duplicate Invocation Rate: %.4f (recorded, no SLO target)\n", r.DuplicateInvocationRate)
	fmt.Fprintf(&b, "Error Rate: %.4f (target < %.4f)\n", r.ErrorRate, TargetErrorRate)

	fmt.Fprintf(&b, "\nFault Matrix (%d rows):\n", len(r.FaultMatrix))
	if len(r.FaultMatrix) == 0 {
		fmt.Fprintf(&b, "  (no faults recorded)\n")
	} else {
		for _, o := range r.FaultMatrix {
			status := "PASS"
			if !o.Passed {
				status = "FAIL"
			}
			fmt.Fprintf(&b, "- %s @ %s [%s]\n", o.Fault, o.InjectedAt.Format(time.RFC3339), status)
			fmt.Fprintf(&b, "  Expected invariant: %s\n", o.ExpectedInvariant)
			fmt.Fprintf(&b, "  Actual recovery: %s, duplicate delivery: %d, commit outcome: %s\n", o.ActualRecoveryTime, o.DuplicateDelivery, o.CommitOutcome)
			if o.Notes != "" {
				fmt.Fprintf(&b, "  Notes: %s\n", o.Notes)
			}
		}
	}

	eval := r.evaluateLocked()
	fmt.Fprintf(&b, "\nSLO Verdict: ")
	if eval.Passed {
		fmt.Fprintf(&b, "PASS\n")
	} else {
		fmt.Fprintf(&b, "FAIL\n")
		for _, f := range eval.Failures {
			fmt.Fprintf(&b, "  - %s\n", f)
		}
	}
	return b.String()
}

// String delegates to Render so SLOReport can be printed directly.
func (r *SLOReport) String() string { return r.Render() }

func (r *SLOReport) recomputeLocked() {
	r.Samples = r.apiTotal
	if r.apiTotal > 0 {
		r.APIAvailability = float64(r.apiSuccess) / float64(r.apiTotal)
		r.ErrorRate = float64(r.apiFailures) / float64(r.apiTotal)
		r.DuplicateInvocationRate = float64(r.duplicates) / float64(r.apiTotal)
	} else {
		r.APIAvailability = 0
		r.ErrorRate = 0
		r.DuplicateInvocationRate = 0
	}
}
