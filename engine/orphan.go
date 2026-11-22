package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// OrphanReclaimReason is the execution-level reason recorded when a concurrency
// orphan is converged into the canceled status. It is a constant rather than a
// literal at each site because it is observable outside the process: the
// inspect payload, the SQL audit row and the per-execution reset response all
// carry the same text.
const OrphanReclaimReason = "orphan_reclaimed"

// OrphanReclaimReasonOperatorReset is the execution-level reason recorded when
// an operator reset converges a live execution through the include_live path.
// It is deliberately distinct from OrphanReclaimReason: an operator-forced
// termination of work that still had a graph must be distinguishable, after
// the fact, from the automatic repair of work that had already lost it.
const OrphanReclaimReasonOperatorReset = "operator_reset"

// OrphanReclaimTrigger* name the path that asked for a reclaim. The trigger is
// reported on every convergence (the structured log line and the
// xflow_orphan_reclaimed_total reason label), so an operator reset can always
// be told apart from a periodic sweep or an automatic promotion repair.
const (
	// OrphanTriggerSweep is the periodic maintenance path.
	OrphanTriggerSweep = "sweep"
	// OrphanTriggerPromotion is the concurrency promotion path reaping an
	// execution that cannot start.
	OrphanTriggerPromotion = "promotion"
	// OrphanTriggerCancel is Cancel's self-heal of the torn shape.
	OrphanTriggerCancel = "cancel"
	// OrphanTriggerReset is the explicit operator reset surface. Its value is
	// the documented audit reason, operator_reset.
	OrphanTriggerReset = OrphanReclaimReasonOperatorReset
)

// OrphanKind* are the bounded values of the xflow_orphan_reclaimed_total kind
// label: the status the execution was reclaimed from, or missing when the
// execution itself was already gone.
const (
	OrphanKindRunning = "running"
	OrphanKindQueued  = "queued"
	OrphanKindMissing = "missing"
)

// MaxResetWorkflowNameLen bounds the workflow name a reset request may carry.
// It matches the execution-list name filter's limit: a longer value cannot
// name a registered workflow, so it is refused as a bad request rather than
// hashed into a concurrency key that addresses nothing.
const MaxResetWorkflowNameLen = 128

// ErrConcurrencyOrphanReclaimUnsupported reports a state store with no
// ConcurrencyOrphanReclaimer capability. Callers must treat it as "this
// deployment cannot reclaim", never as "nothing to reclaim".
var ErrConcurrencyOrphanReclaimUnsupported = errors.New("state store does not support concurrency orphan reclaim")

// ErrInvalidWorkflowName reports a reset request that cannot name a workflow.
var ErrInvalidWorkflowName = errors.New("invalid workflow name")

// DefaultOrphanReclaimGrace is how old a candidate has to be before a sweep
// reclaims it. It exists for the same reason concurrencyStaleGrace does: the
// sweep must not race the normal admission window in which a slot or queue
// member is charged before its execution keys are written.
//
// The age is measured from the newer of two clocks: the execution's status
// transition clock (updated_at_ms) and its node activity clock (the latest node
// lease acquisition, lease renewal, or accepted node commit). The second clock
// is what keeps a long execution between two leases out of the sweep's reach:
// ordinary node progress never moves updated_at_ms, so status age alone would
// read a working execution as abandoned.
const DefaultOrphanReclaimGrace = 2 * time.Minute

// OrphanReclaimMode selects how the age guard is applied.
//
// Both modes apply every other guard unconditionally: the execution must be
// running or queued, its graph must be absent, no node lease may be live, and no
// node may be suspended. Sweep additionally requires the newer of the status
// and activity clocks to be older than the grace. Force bypasses the status
// clock (and nothing else), which lets an explicit reset converge an execution
// that just lost its graph without waiting out the grace — but it still refuses
// an execution whose activity is inside the grace, because a recent lease or
// commit is direct evidence that work is visibly running.
type OrphanReclaimMode string

const (
	// OrphanReclaimSweep is the periodic maintenance mode: only candidates
	// whose newer age clock — status transition or node activity — is older than
	// the grace are converged.
	OrphanReclaimSweep OrphanReclaimMode = "sweep"
	// OrphanReclaimForce ignores the status transition clock. It is for explicit
	// operator actions (Cancel on a torn execution, the reset API) where the
	// caller has already established that the execution cannot be routed. It
	// does not ignore recent node activity; only IncludeLive does that.
	OrphanReclaimForce OrphanReclaimMode = "force"
)

// Evidence values reported for a reclaimed (or, in dry-run mode, reclaimable)
// candidate. They are part of the observable surface (logs and the reset API),
// so callers should treat them as stable strings.
const (
	// OrphanEvidenceGraphMissing is the torn shape: the status key still reads
	// running or queued while the graph key is gone. Routing is broken on an
	// execution the store still calls live.
	OrphanEvidenceGraphMissing = "graph_missing"
	// OrphanEvidenceExecutionMissing is the retention-lapsed shape: the status
	// key is gone while a concurrency slot or queue member still names the
	// execution. Nothing can route it and nothing can terminalize it, so the
	// member is removed and the SQL mirror is converged directly.
	OrphanEvidenceExecutionMissing = "execution_missing"
)

// OrphanReclaimRequest describes one reclaim pass or one targeted convergence.
//
// A zero ExecutionID means "scan the concurrency state of the store's scopes"
// (all owned namespaces, or the single Namespace when set). A non-zero
// ExecutionID narrows the pass to that execution and ignores Limit.
//
// DryRun makes the store answer the same question without mutating anything:
// Candidates lists what would be reclaimed and Reclaimed stays 0. This is the
// shape the reset API's preview endpoint needs, and it shares the judgement
// path with the mutating call so a preview cannot disagree with the reset.
type OrphanReclaimRequest struct {
	Mode        OrphanReclaimMode
	DryRun      bool
	Limit       int
	ExecutionID types.ExecutionID
	Namespace   namespace.Namespace
	// Grace overrides DefaultOrphanReclaimGrace when positive.
	Grace time.Duration
	// Workflow narrows an ExecutionID-less pass to one workflow's concurrency
	// state. It is the workflow NAME, not an id: concurrency is keyed by
	// namespace plus name, so a rename starts a new slot set under the same
	// rule admissions already use. An empty Workflow scans every indexed
	// workflow in the pass's namespaces.
	Workflow string
	// IncludeLive lets force mode converge an execution whose graph still
	// exists. It exists for the operator reset surface only: an orphan has no
	// graph by definition, so without this flag a live execution is never
	// touched. The caller that sets it is responsible for the audit trail (the
	// convergence is recorded with Reason operator_reset).
	IncludeLive bool
	// Reason names the path that asked for the reclaim: OrphanTriggerSweep
	// when empty, or one of the OrphanTrigger* constants. It is reported on
	// the structured log line and the reason metric label.
	Reason string
}

// Trigger returns the reason to report for this request, defaulting to the
// periodic sweep when the caller named none.
func (r OrphanReclaimRequest) Trigger() string {
	if r.Reason == "" {
		return OrphanTriggerSweep
	}
	return r.Reason
}

// OrphanReclaimCandidate is one execution the pass judged (or would judge)
// reclaimable, with the evidence a structured log or an operator needs.
//
// Status is the surviving non-terminal status for OrphanEvidenceGraphMissing,
// and empty for OrphanEvidenceExecutionMissing. Age is measured from the newer
// of the execution's status transition clock and its node activity clock, or
// from the concurrency member's own admission time when the status key is gone.
type OrphanReclaimCandidate struct {
	ExecutionID types.ExecutionID
	Namespace   namespace.Namespace
	// Workflow is the persisted workflow name when the SQL mirror can supply
	// it, otherwise the workflow tag the concurrency state is keyed by.
	Workflow string
	Status   types.ExecutionStatus
	Age      time.Duration
	Evidence string
}

// Kind maps the candidate onto the bounded kind label of
// xflow_orphan_reclaimed_total: the status it was reclaimed from, or missing
// when the execution itself was already gone.
func (c OrphanReclaimCandidate) Kind() string {
	if c.Evidence == OrphanEvidenceExecutionMissing {
		return OrphanKindMissing
	}
	if c.Status == types.ExecutionStatusQueued {
		return OrphanKindQueued
	}
	return OrphanKindRunning
}

// OrphanReclaimResult reports one pass. Inspected counts the executions and
// dangling concurrency members the pass examined; Candidates lists the ones
// judged reclaimable (in dry-run mode) or actually converged; Reclaimed counts
// the converged executions (zero for a dry run).
type OrphanReclaimResult struct {
	Inspected  int
	Candidates []OrphanReclaimCandidate
	Reclaimed  int
}

// ConcurrencyOrphanReclaimer is the optional StateStore capability that
// converges executions the engine can no longer route: the status key still
// says running or queued while the compiled graph the engine needs is gone.
//
// The shape is real and recurring. Redis is authoritative for scheduling, so a
// graph key lost to eviction, expiry or a partial restore leaves an execution
// the engine refuses to route (loadActiveGraph answers inactive) and Cancel
// cannot converge (it refuses an inactive execution before writing anything).
// The concurrency slot stays charged, the SQL audit row stays running, and the
// workflow silently runs one short of its limit behind work that will never
// start.
//
// Implementations own the whole repair, in one pass:
//   - the candidate judgement is made on the authoritative state and re-checked
//     atomically with the convergence, so an execution that regains a live
//     lease, a graph, a suspension, or recent node activity between the scan and
//     the write is left alone;
//   - a converged execution is canceled with Reason OrphanReclaimReason, its
//     concurrency slot and queue place are removed, and the freed capacity is
//     offered to the head of the line in the same pass;
//   - the SQL mirror is projected with the terminal status even when the Redis
//     status key is already gone, which is the case an ordinary drift repair
//     skips as not_found;
//   - a candidate whose status key is gone has its dangling concurrency member
//     removed and its SQL row converged, but no Redis status is invented: a
//     bare status key without the rest of a creation would read as a live
//     execution with no time budget.
//
// The runner-facing half of the liveness question is the engine's own lease
// index: an execution with an unexpired node or group lease is never
// reclaimed. A control-plane claim that has not produced a lease yet is not
// consulted (the store cannot see it and it cannot execute work without
// passing AcquireTaskLease, which re-reads the execution status), and the
// handoff ledger resolves such claims against this same engine authority.
//
// The capability is optional so backends that have no concurrency state can
// omit it; callers must treat its absence as "cannot reclaim", not as "nothing
// to reclaim".
type ConcurrencyOrphanReclaimer interface {
	ReclaimConcurrencyOrphans(ctx context.Context, req OrphanReclaimRequest) (OrphanReclaimResult, error)
}

// OrphanReclaimObserver is the narrow boundary a backend publishes a
// convergence through. It exists so a state store can report the repair
// without importing a metrics exporter; observability/metrics implements it,
// and a nil observer simply reports nothing.
//
// kind is one of the OrphanKind* constants, reason one of the OrphanTrigger*
// constants. ctx carries the candidate's namespace. Implementations must be
// non-blocking.
type OrphanReclaimObserver interface {
	OnConcurrencyOrphanReclaimed(ctx context.Context, kind, reason string)
}

// ReclaimConcurrencyOrphans delegates one reclaim pass to the state store.
// When the store does not implement ConcurrencyOrphanReclaimer the call
// returns ErrConcurrencyOrphanReclaimUnsupported, which is "cannot reclaim",
// not "nothing to reclaim".
func (e *Engine) ReclaimConcurrencyOrphans(ctx context.Context, req OrphanReclaimRequest) (OrphanReclaimResult, error) {
	reclaimer, ok := e.state.(ConcurrencyOrphanReclaimer)
	if !ok {
		return OrphanReclaimResult{}, ErrConcurrencyOrphanReclaimUnsupported
	}
	return reclaimer.ReclaimConcurrencyOrphans(ctx, req)
}

// ResetConcurrencyOptions controls ResetWorkflowConcurrency.
//
// The zero value is a dry run. Apply is deliberately a positive opt-in rather
// than a DryRun flag defaulting to false: a caller that constructs the options
// and forgets a field must preview, not terminate work.
type ResetConcurrencyOptions struct {
	// Apply performs the convergence. False answers the same judgement without
	// writing anything.
	Apply bool
	// IncludeLive also converges executions whose graph still exists, or whose
	// node activity is inside the reclaim grace. It is honored in dry-run mode
	// too, so a preview can show exactly what an apply would terminate. Every
	// live convergence is recorded with OrphanTriggerReset, visible in the
	// structured log and the metric.
	IncludeLive bool
}

// ResetWorkflowConcurrency judges (and, with Apply, converges) the concurrency
// state of one workflow in one namespace.
//
// The default judgement is the concurrency-orphan judgement: only executions
// the engine can no longer route are converged (running/queued, graph absent,
// no live lease, no suspension, activity older than the grace). IncludeLive
// lifts the graph and activity guards for an explicit operator reset, and only
// then can a live execution holding a graph be terminated. The pass uses force
// mode because an operator asking to reset a named workflow has already
// established that its capacity is wrong; the other guards still decide what is
// safe to touch.
//
// The workflow name is validated before anything is read: an empty or
// over-long name cannot address a registered workflow, so it is refused rather
// than hashed into a concurrency key. The returned reason/evidence strings are
// the same ones xflow_orphan_reclaimed_total reports.
func (e *Engine) ResetWorkflowConcurrency(ctx context.Context, ns namespace.Namespace, workflowName string, opts ResetConcurrencyOptions) (OrphanReclaimResult, error) {
	name := strings.TrimSpace(workflowName)
	if name == "" || utf8.RuneCountInString(name) > MaxResetWorkflowNameLen {
		return OrphanReclaimResult{}, fmt.Errorf("%w: must be between 1 and %d characters", ErrInvalidWorkflowName, MaxResetWorkflowNameLen)
	}
	if ns == "" {
		ns = namespace.FromContext(ctx)
	}
	if err := namespace.Validate(ns); err != nil {
		return OrphanReclaimResult{}, err
	}
	return e.ReclaimConcurrencyOrphans(ctx, OrphanReclaimRequest{
		Mode:        OrphanReclaimForce,
		DryRun:      !opts.Apply,
		Namespace:   ns,
		Workflow:    name,
		IncludeLive: opts.IncludeLive,
		Reason:      OrphanTriggerReset,
	})
}

// reclaimGraphLostExecution converges the one torn shape Cancel is allowed to
// repair: the status key still reads running or queued while the graph key is
// gone. It is deliberately narrow — a terminal execution stays terminal, an
// absent one stays absent, and a backend that cannot reclaim keeps answering
// ErrExecutionInactive.
//
// Force is correct here rather than a grace-gated sweep: the caller is an
// explicit Cancel, the graph/status pair is written atomically at creation, so
// "graph absent, status live" cannot be the admission window the sweep's grace
// exists to protect. Force skips the status transition clock only: the store
// still refuses an execution whose node activity — a lease acquisition,
// renewal, or accepted commit — is inside the grace, so an execution that is
// visibly working is not converged by a mistaken Cancel. The lease and
// suspension guards apply as well.
func (e *Engine) reclaimGraphLostExecution(ctx context.Context, id types.ExecutionID) bool {
	reclaimer, ok := e.state.(ConcurrencyOrphanReclaimer)
	if !ok {
		return false
	}
	var inactive *ExecutionInactiveError
	if !errors.As(e.inactiveExecutionError(ctx, id, time.Time{}), &inactive) {
		return false
	}
	if inactive.Kind != ExecutionInactiveGone || inactive.Status == "" || types.IsTerminalExecutionStatus(inactive.Status) {
		return false
	}
	res, err := reclaimer.ReclaimConcurrencyOrphans(ctx, OrphanReclaimRequest{
		Mode:        OrphanReclaimForce,
		Limit:       1,
		ExecutionID: id,
		Reason:      OrphanTriggerCancel,
	})
	if err != nil || res.Reclaimed == 0 {
		return false
	}
	e.EvictExecution(id)
	return true
}
