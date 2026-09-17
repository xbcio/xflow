package control

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// MemoryRunnerDirectoryOption configures a MemoryRunnerDirectory.
type MemoryRunnerDirectoryOption func(*memoryRunnerDirectoryConfig)

type memoryRunnerDirectoryConfig struct {
	controlReceiptRetention   time.Duration
	drainObservationFreshness time.Duration
	drainDeadline             time.Duration
	clock                     func() time.Time
}

// WithMemoryRunnerDirectoryControlReceiptRetention sets how long completed
// runner-control requests remain idempotent. Non-positive values retain the
// default 24-hour period.
func WithMemoryRunnerDirectoryControlReceiptRetention(retention time.Duration) MemoryRunnerDirectoryOption {
	return func(cfg *memoryRunnerDirectoryConfig) {
		if retention > 0 {
			cfg.controlReceiptRetention = retention
		}
	}
}

// WithMemoryRunnerDirectoryDrainObservationFreshness sets the maximum age of
// a quiet runner observation that may satisfy the drain completion predicate.
// Non-positive values retain the default 30-second window.
func WithMemoryRunnerDirectoryDrainObservationFreshness(freshness time.Duration) MemoryRunnerDirectoryOption {
	return func(cfg *memoryRunnerDirectoryConfig) {
		if freshness > 0 {
			cfg.drainObservationFreshness = freshness
		}
	}
}

// WithMemoryRunnerDirectoryDrainDeadline sets the time after a real
// ACTIVE-to-DRAINING transition at which an incomplete drain projects as
// timed_out. Non-positive values retain the default 30-minute deadline.
func WithMemoryRunnerDirectoryDrainDeadline(deadline time.Duration) MemoryRunnerDirectoryOption {
	return func(cfg *memoryRunnerDirectoryConfig) {
		if deadline > 0 {
			cfg.drainDeadline = deadline
		}
	}
}

// WithMemoryRunnerDirectoryClock supplies the directory-owned clock used for
// live drain projections and observation timestamps. It is primarily useful
// to embedded deployments and deterministic tests.
func WithMemoryRunnerDirectoryClock(clock func() time.Time) MemoryRunnerDirectoryOption {
	return func(cfg *memoryRunnerDirectoryConfig) {
		if clock != nil {
			cfg.clock = clock
		}
	}
}

// MemoryRunnerDirectory keeps runner registration and assignment state in
// process for embedded and test deployments.
type MemoryRunnerDirectory struct {
	mu                        sync.RWMutex
	runners                   map[string]*memoryRunnerState
	queue                     []Assignment
	seen                      map[AssignmentID]struct{}
	claims                    map[ClaimID]memoryClaim
	handoffs                  map[ClaimID]memoryHandoff
	handoffByAssignment       map[AssignmentID]map[ClaimID]struct{}
	controls                  map[string]*memoryRunnerControl
	deactivationObligations   map[string]DeactivationObligation
	activationInventory       map[string]map[deactivationInventoryKey]uint64
	controlReceiptRetention   time.Duration
	drainObservationFreshness time.Duration
	drainDeadline             time.Duration
	clock                     func() time.Time
}

type memoryRunnerState struct {
	snapshot          RunnerSnapshot
	policy            RunnerPolicy
	sessionID         string
	namespaces        map[namespace.Namespace]struct{}
	activeClaims      map[ClaimID]AssignmentID
	activeOrder       []ClaimID
	finalizedLease    map[AssignmentID]engine.TaskLease
	leaseByID         map[engine.LeaseID]AssignmentID
	leaseByToken      map[engine.LeaseToken]AssignmentID
	leasedAssignments map[AssignmentID]Assignment
}

type memoryClaim struct {
	runnerID   string
	assignment Assignment
}

// memoryHandoff is the in-process equivalent of the Redis handoff ledger. The
// memory directory is not restart durable, but retaining the same state
// machine keeps embedded mode's safety semantics aligned with cluster mode.
type memoryHandoff struct {
	runnerID      string
	sessionID     string
	assignment    Assignment
	debt          HandoffDebt
	recoveryReady bool
}

var _ ActivationRunnerLister = (*MemoryRunnerDirectory)(nil)
var _ HandoffDebtDirectory = (*MemoryRunnerDirectory)(nil)
var _ FinalizedHandoffSettler = (*MemoryRunnerDirectory)(nil)
var _ DeactivationObligationDirectory = (*MemoryRunnerDirectory)(nil)

// NewMemoryRunnerDirectory constructs an empty in-memory runner directory.
func NewMemoryRunnerDirectory(opts ...MemoryRunnerDirectoryOption) *MemoryRunnerDirectory {
	cfg := memoryRunnerDirectoryConfig{
		controlReceiptRetention:   defaultRunnerControlReceiptRetention,
		drainObservationFreshness: defaultRunnerDrainObservationFreshness,
		drainDeadline:             defaultRunnerDrainDeadline,
		clock:                     time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &MemoryRunnerDirectory{
		runners:                   make(map[string]*memoryRunnerState),
		seen:                      make(map[AssignmentID]struct{}),
		claims:                    make(map[ClaimID]memoryClaim),
		handoffs:                  make(map[ClaimID]memoryHandoff),
		handoffByAssignment:       make(map[AssignmentID]map[ClaimID]struct{}),
		controls:                  make(map[string]*memoryRunnerControl),
		deactivationObligations:   make(map[string]DeactivationObligation),
		activationInventory:       make(map[string]map[deactivationInventoryKey]uint64),
		controlReceiptRetention:   cfg.controlReceiptRetention,
		drainObservationFreshness: cfg.drainObservationFreshness,
		drainDeadline:             cfg.drainDeadline,
		clock:                     cfg.clock,
	}
}

func (d *MemoryRunnerDirectory) runnerControlReceiptRetention() time.Duration {
	if d.controlReceiptRetention > 0 {
		return d.controlReceiptRetention
	}
	return defaultRunnerControlReceiptRetention
}

func (d *MemoryRunnerDirectory) runnerDrainObservationFreshness() time.Duration {
	if d.drainObservationFreshness > 0 {
		return d.drainObservationFreshness
	}
	return defaultRunnerDrainObservationFreshness
}

func (d *MemoryRunnerDirectory) runnerDrainDeadline() time.Duration {
	if d.drainDeadline > 0 {
		return d.drainDeadline
	}
	return defaultRunnerDrainDeadline
}

func (d *MemoryRunnerDirectory) clockNow() time.Time {
	if d.clock != nil {
		return d.clock().UTC()
	}
	return time.Now().UTC()
}

// Register installs or replaces a runner session. Re-registering the same
// runner ID fences older requests and requeues any unfinalized claims.
func (d *MemoryRunnerDirectory) Register(_ context.Context, req RegisterRunnerRequest) (RunnerSession, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if req.RunnerID == "" {
		return RunnerSession{}, ErrRunnerIDRequired
	}
	if req.Capacity <= 0 {
		return RunnerSession{}, ErrConcurrencyRequired
	}

	now := req.Now
	if now.IsZero() {
		now = d.clockNow()
	}
	session := RunnerSession{
		RunnerID:  req.RunnerID,
		SessionID: uuid.NewString(),
	}

	finalizedLease := make(map[AssignmentID]engine.TaskLease)
	leasedAssignments := make(map[AssignmentID]Assignment)
	inFlight := 0
	previous := d.runners[req.RunnerID]
	if previous != nil {
		finalizedLease = cloneFinalizedLeases(previous.finalizedLease)
		leasedAssignments = cloneLeasedAssignments(previous.leasedAssignments)
		inFlight = previous.snapshot.InFlight
	}
	if d.controls[req.RunnerID] == nil {
		control := activeRunnerControl()
		d.controls[req.RunnerID] = &control
	}
	// A registration always creates a new session fence, even when the process
	// reconnects immediately. An observation from the old session must never
	// carry completion into the replacement session.
	d.controls[req.RunnerID].drainObservation = nil

	state := &memoryRunnerState{
		snapshot: RunnerSnapshot{
			RunnerID:      req.RunnerID,
			SessionID:     session.SessionID,
			Capacity:      req.Capacity,
			Labels:        cloneLabels(req.Labels),
			Capabilities:  cloneCapabilities(req.Capabilities),
			InFlight:      inFlight,
			Namespaces:    normalizeRunnerNamespaces(req.Namespaces),
			LastHeartbeat: now,
		},
		policy:            req.Policy,
		sessionID:         session.SessionID,
		namespaces:        namespaceSet(req.Namespaces),
		activeClaims:      make(map[ClaimID]AssignmentID),
		finalizedLease:    finalizedLease,
		leaseByID:         indexLeaseIDs(finalizedLease),
		leaseByToken:      indexLeaseTokens(finalizedLease),
		leasedAssignments: leasedAssignments,
	}
	d.runners[req.RunnerID] = state
	// Registration carries the reconnect inventory into the same mutex
	// transition as session replacement. A control-plane crash after Register
	// therefore cannot lose the proof needed to rebind cleanup delivery.
	d.setActivationInventoryAndRebindLocked(req.RunnerID, session.SessionID, req.Activations)
	if previous != nil {
		d.rebindHandoffsLocked(previous, state)
	}
	return session, nil
}

// ValidateSession confirms the runner ID still points to the provided live
// session.
func (d *MemoryRunnerDirectory) ValidateSession(_ context.Context, runnerID, sessionID string) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, err := d.runnerForSessionLocked(runnerID, sessionID)
	return err
}

// Heartbeat updates runner liveness and advertised capacity for the current
// session.
func (d *MemoryRunnerDirectory) Heartbeat(_ context.Context, req HeartbeatRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	state, err := d.runnerForSessionLocked(req.RunnerID, req.SessionID)
	if err != nil {
		return err
	}

	state.snapshot.Capacity = req.Capacity
	state.snapshot.InFlight = req.InFlight
	if !req.Now.IsZero() {
		state.snapshot.LastHeartbeat = req.Now
	}
	control := d.controls[req.RunnerID]
	observation := newRunnerDrainObservationAt(req, d.clockNow())
	if control != nil && observation != nil &&
		control.desired == RunnerDesiredStateDraining &&
		observation.generation == control.generation {
		control.drainObservation = observation
	} else if control != nil {
		// A current draining heartbeat that omits, or carries a stale,
		// observation is evidence only of uncertainty. Clear rather than retain
		// a previously quiet sample.
		control.drainObservation = nil
	}
	return nil
}

// EnqueueAssignment queues an assignment, deduplicating against the seen set.
//
// A seen mark alone is not enough to reject: it says the assignment has been
// dispatched before, not that it is still live. An assignment released without
// clearing its mark — ReleaseLeased with RemoveSeen=false, the stale-token path
// — is exactly that combination, and rejecting it strands the task forever
// (Dispatcher.HandleTask treats a duplicate as success and drops it). So the
// guard is liveness, with the mark only deciding which bookkeeping needs
// creating. This mirrors the state check in redisEnqueueAssignmentLua; the two
// directories must not disagree about when a task can be re-dispatched.
func (d *MemoryRunnerDirectory) EnqueueAssignment(_ context.Context, assignment Assignment) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if assignment.AssignmentID == "" {
		return false, fmt.Errorf("assignment id is required")
	}
	if _, ok := d.seen[assignment.AssignmentID]; ok && d.assignmentLiveLocked(assignment.AssignmentID) {
		return false, nil
	}
	d.seen[assignment.AssignmentID] = struct{}{}
	d.removeQueuedAssignmentLocked(assignment.AssignmentID)
	d.queue = append(d.queue, assignment)
	return true, nil
}

// assignmentLiveLocked reports whether the assignment is still owned by the
// plane: waiting in the queue, reserved by an unfinalized claim, or held under
// a finalized lease. This is the in-memory equivalent of the Redis directory's
// queued/claimed/leased states; anything else is the released state.
func (d *MemoryRunnerDirectory) assignmentLiveLocked(assignmentID AssignmentID) bool {
	for _, queued := range d.queue {
		if queued.AssignmentID == assignmentID {
			return true
		}
	}
	for _, claim := range d.claims {
		if claim.assignment.AssignmentID == assignmentID {
			return true
		}
	}
	for _, state := range d.runners {
		if state == nil {
			continue
		}
		if _, ok := state.finalizedLease[assignmentID]; ok {
			return true
		}
	}
	return false
}

// ClaimForRunner reserves the first compatible assignment for the runner's
// current session.
func (d *MemoryRunnerDirectory) ClaimForRunner(_ context.Context, req ClaimRequest) (Claim, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state, err := d.runnerForSessionLocked(req.RunnerID, req.SessionID)
	if err != nil {
		return Claim{}, false, err
	}

	control := d.controls[req.RunnerID]
	draining := control != nil && control.desired == RunnerDesiredStateDraining
	// Resolve a previously marked handoff before considering either finalized
	// lease replay or a new queue admission. lease_created is immediately
	// recoverable; lease_may_exist becomes recoverable only after session
	// replacement, so a concurrent Build*Lease call is never mistaken for an
	// absent engine lease.
	if handoff, ok := d.recoverableHandoffLocked(req.RunnerID, req.SessionID); ok {
		return handoff, true, nil
	}

	// Lease replay is recovery of an already-admitted handoff, never a new
	// queue claim. Preserve the established ACTIVE behavior (no speculative
	// replay in the in-memory directory), but keep replay available while
	// draining so a lost poll response cannot strand a fenced engine lease until
	// TTL expiry.
	if draining || req.RecoveryOnly {
		if replay, ok := state.replayLease(req.ActiveLeaseIDs); ok {
			return replay, true, nil
		}
		// Recovery-only is intentionally a stricter local request even after a
		// newer ACTIVE directive exists server-side. It can only suppress work;
		// DRAINING itself remains the authoritative safety gate.
		return Claim{}, false, nil
	}

	// Register owns routing metadata and Heartbeat owns capacity observations.
	// Poll is deliberately not allowed to refresh either: labels and capabilities
	// decide which workload a runner may claim, so accepting them here would let
	// an authenticated runner impersonate a differently entitled workload.
	if state.headroom() <= 0 {
		return Claim{}, false, nil
	}

	for i, assignment := range d.queue {
		if !MatchCapabilities(state.snapshot.Capabilities, assignment.Routing) {
			continue
		}
		if !state.policy.Allows(assignment.Routing.NodeType) {
			continue
		}
		if !state.canServeNamespace(assignment.Namespace) {
			continue
		}
		if rs := assignment.Routing.RunnerSelector; rs != nil && !MatchLabels(state.snapshot.Labels, rs.MatchLabels) {
			continue
		}

		claimID := ClaimID(uuid.NewString())
		d.queue = append(d.queue[:i], d.queue[i+1:]...)
		d.claims[claimID] = memoryClaim{runnerID: req.RunnerID, assignment: assignment}
		generation := uint64(0)
		if control != nil {
			generation = control.generation
		}
		d.handoffs[claimID] = memoryHandoff{
			runnerID:   req.RunnerID,
			sessionID:  req.SessionID,
			assignment: assignment,
			debt: HandoffDebt{
				State:               HandoffDebtReserved,
				AdmissionGeneration: generation,
			},
		}
		d.addHandoffLocked(claimID, d.handoffs[claimID])
		state.activeClaims[claimID] = assignment.AssignmentID
		state.activeOrder = append(state.activeOrder, claimID)
		return Claim{
			ClaimID:    claimID,
			Assignment: assignment,
		}, true, nil
	}

	return Claim{}, false, nil
}

// FinalizeClaim moves an active claim into leased-capacity accounting. Its
// ledger update is in the same mutex transaction, so a finalized lease can no
// longer be mistaken for an untracked Build*Lease handoff.
func (d *MemoryRunnerDirectory) FinalizeClaim(_ context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	claim, ok := d.claims[claimID]
	if !ok {
		return nil
	}

	state := d.runners[claim.runnerID]
	if state == nil {
		return errClaimNotActive
	}
	handoff, ok := d.handoffs[claimID]
	if !ok {
		handoff = memoryHandoff{
			runnerID:   claim.runnerID,
			sessionID:  state.sessionID,
			assignment: claim.assignment,
			debt: HandoffDebt{
				State: HandoffDebtReserved,
			},
		}
		d.addHandoffLocked(claimID, handoff)
	}

	delete(d.claims, claimID)
	delete(state.activeClaims, claimID)
	state.activeOrder = removeClaimID(state.activeOrder, claimID)
	if existing, ok := state.finalizedLease[claim.assignment.AssignmentID]; ok {
		state.removeLeaseIndexes(claim.assignment.AssignmentID, existing)
	}
	state.leasedAssignments[claim.assignment.AssignmentID] = claim.assignment
	if lease != nil {
		leaseCopy := *lease
		state.finalizedLease[claim.assignment.AssignmentID] = leaseCopy
		state.addLeaseIndexes(claim.assignment.AssignmentID, leaseCopy)
		handoff.debt.Lease = &leaseCopy
	} else {
		state.finalizedLease[claim.assignment.AssignmentID] = engine.TaskLease{}
		handoff.debt.Lease = nil
	}
	handoff.sessionID = state.sessionID
	handoff.debt.State = HandoffDebtFinalized
	handoff.recoveryReady = false
	d.addHandoffLocked(claimID, handoff)
	return nil
}

// MarkClaimLeaseMayExist writes the crash fence before Core calls an engine
// Build*Lease method. It does not imply that a lease definitely exists.
func (d *MemoryRunnerDirectory) MarkClaimLeaseMayExist(_ context.Context, claimID ClaimID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.claims[claimID]; !ok {
		return errClaimNotActive
	}
	handoff, ok := d.handoffs[claimID]
	if !ok {
		return errClaimNotActive
	}
	switch handoff.debt.State {
	case HandoffDebtReserved, HandoffDebtLeaseMayExist:
		// Marking is idempotent while the engine call has not returned.
	default:
		return errClaimNotActive
	}
	handoff.debt.State = HandoffDebtLeaseMayExist
	handoff.recoveryReady = false
	d.handoffs[claimID] = handoff
	return nil
}

// RecordClaimLeaseCreated records a successfully returned engine lease before
// FinalizeClaim touches directory capacity. A later process can therefore
// recover the exact handoff instead of issuing a second lease.
func (d *MemoryRunnerDirectory) RecordClaimLeaseCreated(_ context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.claims[claimID]; !ok {
		return errClaimNotActive
	}
	handoff, ok := d.handoffs[claimID]
	if !ok {
		return errClaimNotActive
	}
	if lease == nil {
		return fmt.Errorf("record runner handoff %q: nil lease", claimID)
	}
	if handoff.debt.State != HandoffDebtLeaseMayExist && handoff.debt.State != HandoffDebtLeaseCreated {
		return errClaimNotActive
	}
	leaseCopy := *lease
	handoff.debt.State = HandoffDebtLeaseCreated
	handoff.debt.Lease = &leaseCopy
	handoff.recoveryReady = false
	d.handoffs[claimID] = handoff
	return nil
}

// MakeClaimHandoffRecoverable releases a resolver token after a known
// dispatch failure. It never changes the debt state, so recovery must still
// ask the engine whether a lease exists before requeueing or dropping it.
func (d *MemoryRunnerDirectory) MakeClaimHandoffRecoverable(_ context.Context, claimID ClaimID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	handoff, ok := d.handoffs[claimID]
	if !ok {
		return errClaimNotActive
	}
	switch handoff.debt.State {
	case HandoffDebtLeaseMayExist, HandoffDebtLeaseCreated:
		handoff.recoveryReady = true
		d.handoffs[claimID] = handoff
	}
	return nil
}

// SettleClaimHandoff resolves an unfinalized handoff only after Core has
// established that no live engine lease remains. It atomically removes the
// claim/debt and returns the assignment to its requested disposition.
func (d *MemoryRunnerDirectory) SettleClaimHandoff(_ context.Context, claimID ClaimID, disposition HandoffDisposition) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if disposition != HandoffDispositionRequeue && disposition != HandoffDispositionDrop {
		return fmt.Errorf("settle runner handoff %q: unsupported disposition %q", claimID, disposition)
	}
	claim, ok := d.claims[claimID]
	if !ok {
		return nil
	}
	handoff, ok := d.handoffs[claimID]
	if !ok || handoff.debt.State == HandoffDebtReserved || handoff.debt.State == HandoffDebtFinalized {
		return ErrHandoffResolutionRequired
	}
	delete(d.claims, claimID)
	if state := d.runners[claim.runnerID]; state != nil {
		delete(state.activeClaims, claimID)
		state.activeOrder = removeClaimID(state.activeOrder, claimID)
	}
	d.deleteHandoffLocked(claimID, handoff.assignment.AssignmentID)
	switch disposition {
	case HandoffDispositionRequeue:
		d.queue = append([]Assignment{claim.assignment}, d.queue...)
	case HandoffDispositionDrop:
		delete(d.seen, claim.assignment.AssignmentID)
	}
	return nil
}

// ReleaseClaim removes a claim and optionally requeues or clears the
// underlying assignment.
func (d *MemoryRunnerDirectory) ReleaseClaim(_ context.Context, claimID ClaimID, reason ReleaseClaimReason) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	claim, ok := d.claims[claimID]
	if !ok {
		return nil
	}
	if handoff, exists := d.handoffs[claimID]; exists && handoff.debt.State != HandoffDebtReserved {
		return ErrHandoffResolutionRequired
	}

	delete(d.claims, claimID)
	d.deleteHandoffLocked(claimID, claim.assignment.AssignmentID)
	if state := d.runners[claim.runnerID]; state != nil {
		delete(state.activeClaims, claimID)
		state.activeOrder = removeClaimID(state.activeOrder, claimID)
	}

	switch reason {
	case ReleaseClaimRequeue:
		d.queue = append([]Assignment{claim.assignment}, d.queue...)
	case ReleaseClaimDrop:
		delete(d.seen, claim.assignment.AssignmentID)
	case ReleaseClaimKeepSeen:
	}
	return nil
}

// ReleaseExpiredLease removes a finalized lease from the directory only when
// AssignmentID, LeaseID, and LeaseToken all match the stored record. It is
// used by the LeaseSweeper before engine reclaim to prevent a stale finalized
// lease from occupying runner capacity or suppressing redelivery via the seen
// marker after the engine has revoked and re-issued the lease.
func (d *MemoryRunnerDirectory) ReleaseExpiredLease(_ context.Context, req ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, state := range d.runners {
		lease, exists := state.finalizedLease[req.AssignmentID]
		if !exists {
			continue
		}
		if lease.LeaseID != req.LeaseID || lease.LeaseToken != req.LeaseToken {
			return ExpiredDirectoryLeaseTokenMismatch, nil
		}
		delete(state.finalizedLease, req.AssignmentID)
		delete(state.leasedAssignments, req.AssignmentID)
		state.removeLeaseIndexes(req.AssignmentID, lease)
		delete(d.seen, req.AssignmentID)
		return ExpiredDirectoryLeaseReleased, nil
	}
	return ExpiredDirectoryLeaseAlreadyReleased, nil
}

// SettleFinalizedHandoff clears a finalized debt only after the engine has
// conclusively reclaimed or otherwise retired the matching lease identity.
func (d *MemoryRunnerDirectory) SettleFinalizedHandoff(_ context.Context, assignmentID AssignmentID, leaseID engine.LeaseID, leaseToken engine.LeaseToken) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.deleteFinalizedHandoffLocked(assignmentID, leaseID, leaseToken)
	return nil
}

// ReleaseLeased removes leased-capacity accounting for a finalized assignment.
// It resolves the live finalized lease by lease identity, so cleanup remains
// safe if the runner re-registers after report validation but before commit
// cleanup runs.
func (d *MemoryRunnerDirectory) ReleaseLeased(_ context.Context, req ReleaseLeasedRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	state := d.runners[req.RunnerID]
	if state == nil {
		return ErrRunnerNotFound
	}

	assignmentID, ok := state.resolveAssignmentID(req)
	if !ok {
		return nil
	}
	current, ok := state.finalizedLease[assignmentID]
	if !ok || !matchesReleasedLease(current, req) {
		return nil
	}
	delete(state.finalizedLease, assignmentID)
	delete(state.leasedAssignments, assignmentID)
	state.removeLeaseIndexes(assignmentID, current)
	d.deleteFinalizedHandoffLocked(assignmentID, current.LeaseID, current.LeaseToken)
	if req.RemoveSeen {
		delete(d.seen, assignmentID)
	}
	return nil
}

// ClearAssignment removes every trace of the assignment from queue, claim,
// lease, and dedupe bookkeeping.
func (d *MemoryRunnerDirectory) ClearAssignment(_ context.Context, assignmentID AssignmentID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.removeQueuedAssignmentLocked(assignmentID)
	delete(d.seen, assignmentID)
	d.deleteHandoffByAssignmentLocked(assignmentID)
	for claimID, claim := range d.claims {
		if claim.assignment.AssignmentID != assignmentID {
			continue
		}
		delete(d.claims, claimID)
		d.deleteHandoffLocked(claimID, claim.assignment.AssignmentID)
		if state := d.runners[claim.runnerID]; state != nil {
			delete(state.activeClaims, claimID)
			state.activeOrder = removeClaimID(state.activeOrder, claimID)
		}
	}
	for _, state := range d.runners {
		if lease, ok := state.finalizedLease[assignmentID]; ok {
			delete(state.finalizedLease, assignmentID)
			delete(state.leasedAssignments, assignmentID)
			state.removeLeaseIndexes(assignmentID, lease)
		}
	}
	return nil
}

// Runner returns the current runner snapshot.
func (d *MemoryRunnerDirectory) Runner(_ context.Context, runnerID string) (RunnerSnapshot, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	state := d.runners[runnerID]
	if state == nil {
		return RunnerSnapshot{}, false
	}
	snapshot := state.snapshot
	snapshot.Labels = cloneLabels(snapshot.Labels)
	snapshot.Capabilities = cloneCapabilities(snapshot.Capabilities)
	snapshot.Namespaces = normalizeRunnerNamespaces(snapshot.Namespaces)
	snapshot.Control = d.controlSnapshotLocked(runnerID, state)
	return snapshot, true
}

// ListLiveRunners returns a snapshot of every registered runner. It implements
// ActivationRunnerLister so the EntryActivationReconciler can enumerate runners
// for assignment. Liveness (heartbeat TTL) is applied by the reconciler's
// selector, so this returns all registered runners and lets the caller filter.
func (d *MemoryRunnerDirectory) ListLiveRunners(_ context.Context) []RunnerSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make([]RunnerSnapshot, 0, len(d.runners))
	for _, state := range d.runners {
		if state == nil {
			continue
		}
		snapshot := state.snapshot
		snapshot.Labels = cloneLabels(snapshot.Labels)
		snapshot.Capabilities = cloneCapabilities(snapshot.Capabilities)
		snapshot.Namespaces = normalizeRunnerNamespaces(snapshot.Namespaces)
		snapshot.Control = d.controlSnapshotLocked(snapshot.RunnerID, state)
		out = append(out, snapshot)
	}
	return out
}

// ListRunners returns the IDs of every registered runner. It implements the
// apiserver's runnerLister for the runner-list management endpoint.
//
// Unlike ListLiveRunners this returns bare IDs and never fails — an in-memory
// map read cannot error — but the signature matches RedisRunnerDirectory's,
// whose backend can.
func (d *MemoryRunnerDirectory) ListRunners(_ context.Context) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.runners))
	for id, state := range d.runners {
		if state == nil {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

// LookupLease returns the server-authoritative finalized lease for one
// (runner, session, lease-identity) triple. It is the namespace authority on the
// report path: the lease JSON echoed by the runner is unsigned and mutable, so
// reportResult resolves the lease from server state here instead of trusting
// req.Lease.Namespace. ok=false means no finalized lease matches (not found,
// already released, wrong runner/session, or token/leaseID mismatch); err is
// non-nil only on internal failure.
func (d *MemoryRunnerDirectory) LookupLease(_ context.Context, runnerID, sessionID string, key LeaseLookupKey) (*engine.TaskLease, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	state := d.runners[runnerID]
	if state == nil {
		return nil, false, nil
	}
	// Session fence: a stale session (post re-register) must not resolve leases
	// for the current session. Re-register preserves finalizedLease across
	// sessions (see Register), so we gate on sessionID explicitly. This is the
	// session check ReleaseLeased deliberately omits (report cleanup races
	// re-register); LookupLease is a single-RPC authority read, not a cleanup,
	// so session fencing is correct here.
	if state.sessionID != sessionID {
		return nil, false, nil
	}
	req := ReleaseLeasedRequest{
		RunnerID:     runnerID,
		AssignmentID: key.AssignmentID,
		LeaseID:      key.LeaseID,
		LeaseToken:   key.LeaseToken,
	}
	assignmentID, ok := state.resolveAssignmentID(req)
	if !ok {
		return nil, false, nil
	}
	current, ok := state.finalizedLease[assignmentID]
	if !ok || !matchesReleasedLease(current, req) {
		return nil, false, nil
	}
	lease := current
	return &lease, true, nil
}

// SetRunnerControl atomically persists an idempotent desired-state transition
// with the same mutex ClaimForRunner uses for its new-admission gate.
func (d *MemoryRunnerDirectory) SetRunnerControl(_ context.Context, req RunnerControlRequest) (RunnerControlSnapshot, error) {
	if err := validateRunnerControlRequest(req); err != nil {
		return RunnerControlSnapshot{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := req.Now
	if now.IsZero() {
		now = d.clockNow()
	}

	state := d.runners[req.RunnerID]
	if state == nil {
		return RunnerControlSnapshot{}, ErrRunnerNotFound
	}
	control := d.controls[req.RunnerID]
	if control == nil {
		initial := activeRunnerControl()
		control = &initial
		d.controls[req.RunnerID] = control
	}
	d.cleanupExpiredRunnerControlReceiptsLocked(control, now)
	receiptID := runnerControlReceiptID(req)
	if receipt, ok := control.receipts[receiptID]; ok {
		if receipt.RequestHash != req.RequestHash {
			return RunnerControlSnapshot{}, ErrRunnerControlRequestConflict
		}
		return cloneRunnerControlSnapshot(receipt.Snapshot), nil
	}
	transitioned := control.desired != req.DesiredState
	if transitioned {
		control.desired = req.DesiredState
		control.generation++
		control.drainObservation = nil
		control.requestedAt = now
		control.actor = req.Actor
		control.reason = req.Reason
		// Keep the deadline bound to the real state transition. Same-state
		// requests and receipt replays must not perpetually extend a drain.
		if req.DesiredState == RunnerDesiredStateDraining {
			control.drainDeadline = now.Add(d.runnerDrainDeadline())
		} else {
			control.drainDeadline = time.Time{}
		}
	}
	snapshot := runnerControlProjection(*control, state.sessionID, d.handoffStatsLocked(req.RunnerID, state), d.pendingDeactivationCleanupLocked(req.RunnerID), now, d.runnerDrainObservationFreshness())
	control.receipts[receiptID] = runnerControlReceipt{
		RequestHash: req.RequestHash,
		Snapshot:    cloneRunnerControlSnapshot(snapshot),
		StoredAt:    now,
		ExpiresAt:   now.Add(d.runnerControlReceiptRetention()),
		Status:      http.StatusOK,
	}
	return snapshot, nil
}

func (d *MemoryRunnerDirectory) cleanupExpiredRunnerControlReceiptsLocked(control *memoryRunnerControl, now time.Time) {
	for receiptID, receipt := range control.receipts {
		if !receipt.ExpiresAt.IsZero() && !now.Before(receipt.ExpiresAt) {
			delete(control.receipts, receiptID)
		}
	}
}

// RunnerControl returns the current projection for one registered runner.
func (d *MemoryRunnerDirectory) RunnerControl(_ context.Context, runnerID string) (RunnerControlSnapshot, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	state := d.runners[runnerID]
	if state == nil {
		return RunnerControlSnapshot{}, false, nil
	}
	return *d.controlSnapshotLocked(runnerID, state), true, nil
}

func (d *MemoryRunnerDirectory) controlSnapshotLocked(runnerID string, state *memoryRunnerState) *RunnerControlSnapshot {
	control := d.controls[runnerID]
	if control == nil {
		initial := activeRunnerControl()
		control = &initial
	}
	snapshot := runnerControlProjection(*control, state.sessionID, d.handoffStatsLocked(runnerID, state), d.pendingDeactivationCleanupLocked(runnerID), d.clockNow(), d.runnerDrainObservationFreshness())
	return &snapshot
}

func (s *memoryRunnerState) replayLease(activeLeaseIDs []string) (Claim, bool) {
	active := make(map[string]struct{}, len(activeLeaseIDs))
	for _, id := range activeLeaseIDs {
		active[id] = struct{}{}
	}
	for assignmentID, lease := range s.finalizedLease {
		if _, executing := active[string(lease.LeaseID)]; executing {
			continue
		}
		assignment, ok := s.leasedAssignments[assignmentID]
		if !ok {
			continue
		}
		leaseCopy := lease
		return Claim{Assignment: assignment, Lease: &leaseCopy}, true
	}
	return Claim{}, false
}

func cloneLeasedAssignments(src map[AssignmentID]Assignment) map[AssignmentID]Assignment {
	out := make(map[AssignmentID]Assignment, len(src))
	for id, assignment := range src {
		out[id] = assignment
	}
	return out
}

func (d *MemoryRunnerDirectory) runnerForSessionLocked(runnerID, sessionID string) (*memoryRunnerState, error) {
	state := d.runners[runnerID]
	if state == nil {
		return nil, ErrRunnerNotFound
	}
	if sessionID == "" || state.sessionID != sessionID {
		return nil, ErrRunnerSessionStale
	}
	return state, nil
}

func (d *MemoryRunnerDirectory) requeueActiveClaimsLocked(state *memoryRunnerState) {
	if state == nil || len(state.activeClaims) == 0 {
		return
	}
	for _, claimID := range state.activeOrder {
		claim, ok := d.claims[claimID]
		if !ok {
			continue
		}
		delete(d.claims, claimID)
		d.deleteHandoffLocked(claimID, claim.assignment.AssignmentID)
		d.queue = append([]Assignment{claim.assignment}, d.queue...)
	}
	state.activeClaims = make(map[ClaimID]AssignmentID)
	state.activeOrder = nil
}

func (d *MemoryRunnerDirectory) rebindHandoffsLocked(previous, next *memoryRunnerState) {
	if previous == nil || next == nil {
		return
	}
	requeue := make([]Assignment, 0, len(previous.activeOrder))
	for _, claimID := range previous.activeOrder {
		claim, ok := d.claims[claimID]
		if !ok {
			continue
		}
		handoff, hasHandoff := d.handoffs[claimID]
		if hasHandoff && handoff.debt.State != HandoffDebtReserved && handoff.debt.State != HandoffDebtFinalized {
			handoff.sessionID = next.sessionID
			handoff.recoveryReady = true
			d.handoffs[claimID] = handoff
			next.activeClaims[claimID] = claim.assignment.AssignmentID
			next.activeOrder = append(next.activeOrder, claimID)
			continue
		}
		requeue = append(requeue, claim.assignment)
		delete(d.claims, claimID)
		d.deleteHandoffLocked(claimID, claim.assignment.AssignmentID)
	}
	for claimID, handoff := range d.handoffs {
		if handoff.runnerID != next.snapshot.RunnerID || handoff.debt.State != HandoffDebtFinalized {
			continue
		}
		handoff.sessionID = next.sessionID
		d.handoffs[claimID] = handoff
	}
	if len(requeue) > 0 {
		d.queue = append(requeue, d.queue...)
	}
}

func (d *MemoryRunnerDirectory) recoverableHandoffLocked(runnerID, sessionID string) (Claim, bool) {
	for _, claimID := range d.runners[runnerID].activeOrder {
		handoff, ok := d.handoffs[claimID]
		if !ok || handoff.runnerID != runnerID || handoff.sessionID != sessionID {
			continue
		}
		if (handoff.debt.State != HandoffDebtLeaseCreated && handoff.debt.State != HandoffDebtLeaseMayExist) || !handoff.recoveryReady {
			continue
		}
		// Take the resolver token before returning the handoff. A concurrent poll
		// cannot replay the same uncertain lease; failure paths explicitly release
		// the token again, while a crashed resolver is recovered on re-register.
		handoff.recoveryReady = false
		d.handoffs[claimID] = handoff
		debt := cloneHandoffDebt(handoff.debt)
		return Claim{ClaimID: claimID, Assignment: handoff.assignment, Handoff: &debt}, true
	}
	return Claim{}, false
}

func (d *MemoryRunnerDirectory) addHandoffLocked(claimID ClaimID, handoff memoryHandoff) {
	d.handoffs[claimID] = handoff
	claims := d.handoffByAssignment[handoff.assignment.AssignmentID]
	if claims == nil {
		claims = make(map[ClaimID]struct{})
		d.handoffByAssignment[handoff.assignment.AssignmentID] = claims
	}
	claims[claimID] = struct{}{}
}

func (d *MemoryRunnerDirectory) deleteHandoffLocked(claimID ClaimID, assignmentID AssignmentID) {
	delete(d.handoffs, claimID)
	claims := d.handoffByAssignment[assignmentID]
	if claims == nil {
		return
	}
	delete(claims, claimID)
	if len(claims) == 0 {
		delete(d.handoffByAssignment, assignmentID)
	}
}

func (d *MemoryRunnerDirectory) deleteHandoffByAssignmentLocked(assignmentID AssignmentID) {
	claims := d.handoffByAssignment[assignmentID]
	for claimID := range claims {
		delete(d.handoffs, claimID)
	}
	delete(d.handoffByAssignment, assignmentID)
}

func (d *MemoryRunnerDirectory) deleteFinalizedHandoffLocked(assignmentID AssignmentID, leaseID engine.LeaseID, leaseToken engine.LeaseToken) {
	claims := d.handoffByAssignment[assignmentID]
	for claimID := range claims {
		handoff, ok := d.handoffs[claimID]
		if !ok || handoff.debt.State != HandoffDebtFinalized || handoff.debt.Lease == nil {
			continue
		}
		if handoff.debt.Lease.LeaseID != leaseID || handoff.debt.Lease.LeaseToken != leaseToken {
			continue
		}
		d.deleteHandoffLocked(claimID, assignmentID)
		return
	}
}

// EnsureDeactivationObligation records a drain-owned cleanup intent before an
// activation fence. The same mutex serializes it with SetRunnerControl, so a
// resume that wins first prevents a stale reconcile from fencing an active
// owner. Repeated reconciliation is idempotent and never changes the original
// drain generation.
func (d *MemoryRunnerDirectory) EnsureDeactivationObligation(_ context.Context, obligation DeactivationObligation) (bool, error) {
	if err := validateDeactivationObligation(obligation); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	state := d.runners[obligation.RunnerID]
	control := d.controls[obligation.RunnerID]
	if state == nil || control == nil || control.desired != RunnerDesiredStateDraining || control.generation != obligation.DrainGeneration {
		return false, nil
	}
	if obligation.SessionID == "" || obligation.SessionID == state.sessionID || d.currentSessionReportedObligationLocked(obligation.RunnerID, obligation) {
		obligation.SessionID = state.sessionID
	}
	id := deactivationObligationID(obligation)
	if _, ok := d.deactivationObligations[id]; ok {
		return true, nil
	}
	obligation.State = DeactivationObligationPendingFence
	d.deactivationObligations[id] = obligation
	return true, nil
}

// MarkDeactivationObligationReady makes a pre-fence intent deliverable only
// after the activation authority fence has completed. A missing intent remains
// an error rather than silently claiming cleanup succeeded.
func (d *MemoryRunnerDirectory) MarkDeactivationObligationReady(_ context.Context, obligation DeactivationObligation) error {
	if err := validateDeactivationObligation(obligation); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	id := deactivationObligationID(obligation)
	existing, ok := d.deactivationObligations[id]
	if !ok {
		return ErrDeactivationObligationNotFound
	}
	if existing.State == DeactivationObligationReady {
		return nil
	}
	existing.State = DeactivationObligationReady
	d.deactivationObligations[id] = existing
	return nil
}

// CancelPendingDeactivationObligation compensates a failed activation fence.
// It intentionally cannot cancel ready work: once the old owner was fenced,
// losing the cleanup obligation would make a stale subscription unobservable.
func (d *MemoryRunnerDirectory) CancelPendingDeactivationObligation(_ context.Context, obligation DeactivationObligation) error {
	if err := validateDeactivationObligation(obligation); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	id := deactivationObligationID(obligation)
	if existing, ok := d.deactivationObligations[id]; ok && existing.State == DeactivationObligationPendingFence {
		delete(d.deactivationObligations, id)
	}
	return nil
}

// PendingDeactivationObligations returns every durable unfinished cleanup
// obligation. It is used by a leader after restart to resume a crash between
// intent persistence and activation fencing.
func (d *MemoryRunnerDirectory) PendingDeactivationObligations(_ context.Context) ([]DeactivationObligation, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make([]DeactivationObligation, 0, len(d.deactivationObligations))
	for _, obligation := range d.deactivationObligations {
		out = append(out, obligation)
	}
	return out, nil
}

// DeactivationDirectives derives delivery from durable ready obligations. It
// does not remove them, so a lost heartbeat response is retried on the next
// heartbeat and a control-plane restart retains the work.
func (d *MemoryRunnerDirectory) DeactivationDirectives(_ context.Context, runnerID, sessionID string) ([]protocol.DeactivateDirective, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	state, err := d.runnerForSessionLocked(runnerID, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]protocol.DeactivateDirective, 0)
	for _, obligation := range d.deactivationObligations {
		if obligation.RunnerID != runnerID || obligation.SessionID != state.sessionID || obligation.State != DeactivationObligationReady {
			continue
		}
		out = append(out, obligation.Directive())
	}
	return out, nil
}

// AcknowledgeDeactivation removes one cleanup obligation only when the current
// runner session and the full activation-generation identity match. A stale,
// duplicate, or unrelated acknowledgement is intentionally a successful no-op:
// it must not make a retrying runner fail its heartbeat loop, but it also cannot
// erase current cleanup debt.
func (d *MemoryRunnerDirectory) AcknowledgeDeactivation(ctx context.Context, ack protocol.ActivationAck) (bool, error) {
	if ack.Status != protocol.ActivationStatusDeactivated || ack.RunnerID == "" || ack.SessionID == "" || ack.WorkflowVersion == "" {
		return false, ErrInvalidDeactivationReceipt
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	state := d.runners[ack.RunnerID]
	if state == nil || state.sessionID != ack.SessionID {
		return false, nil
	}
	probe := deactivationObligationFromAck(namespace.FromContext(ctx), ack)
	id := deactivationObligationID(probe)
	obligation, ok := d.deactivationObligations[id]
	if !ok || obligation.State != DeactivationObligationReady || obligation.SessionID != ack.SessionID {
		return false, nil
	}
	delete(d.deactivationObligations, id)
	return true, nil
}

// RebindDeactivationObligations moves an existing delivery attempt to a
// replacement session only when that session reported hosting the exact old
// activation generation. This is the explicit reconnect proof required before
// a new session can acknowledge cleanup that was originally owed by an old one.
func (d *MemoryRunnerDirectory) RebindDeactivationObligations(_ context.Context, runnerID, sessionID string, reported []protocol.ActivationInventoryItem) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, err := d.runnerForSessionLocked(runnerID, sessionID); err != nil {
		return err
	}
	d.setActivationInventoryAndRebindLocked(runnerID, sessionID, reported)
	return nil
}

func (d *MemoryRunnerDirectory) setActivationInventoryAndRebindLocked(runnerID, sessionID string, reported []protocol.ActivationInventoryItem) {
	hosted := make(map[deactivationInventoryKey]uint64, len(reported))
	for _, item := range reported {
		hosted[deactivationInventoryKeyFromItem(item)] = item.Generation
	}
	d.activationInventory[runnerID] = hosted
	for id, obligation := range d.deactivationObligations {
		if obligation.RunnerID != runnerID || obligation.SessionID == sessionID {
			continue
		}
		if generation, ok := hosted[deactivationInventoryKeyFromObligation(obligation)]; !ok || generation != obligation.Generation {
			continue
		}
		obligation.SessionID = sessionID
		d.deactivationObligations[id] = obligation
	}
}

func (d *MemoryRunnerDirectory) currentSessionReportedObligationLocked(runnerID string, obligation DeactivationObligation) bool {
	generation, ok := d.activationInventory[runnerID][deactivationInventoryKeyFromObligation(obligation)]
	return ok && generation == obligation.Generation
}

func (d *MemoryRunnerDirectory) pendingDeactivationCleanupLocked(runnerID string) int {
	pending := 0
	for _, obligation := range d.deactivationObligations {
		if obligation.RunnerID == runnerID {
			pending++
		}
	}
	return pending
}

func (d *MemoryRunnerDirectory) handoffStatsLocked(runnerID string, state *memoryRunnerState) handoffDebtStats {
	stats := handoffDebtStats{}
	if state != nil {
		stats.activeClaims = len(state.activeClaims)
		stats.leasedTasks = len(state.finalizedLease)
	}
	for _, handoff := range d.handoffs {
		if handoff.runnerID != runnerID {
			continue
		}
		stats.unsettledDebt++
		switch handoff.debt.State {
		case HandoffDebtLeaseMayExist, HandoffDebtLeaseCreated:
			stats.handoffDebt++
			if handoff.debt.State == HandoffDebtLeaseMayExist {
				stats.leaseMayExistDebt++
			}
		case HandoffDebtFinalized:
			stats.replayableDebt++
		}
	}
	return stats
}

func (d *MemoryRunnerDirectory) removeQueuedAssignmentLocked(assignmentID AssignmentID) {
	filtered := d.queue[:0]
	for _, assignment := range d.queue {
		if assignment.AssignmentID == assignmentID {
			continue
		}
		filtered = append(filtered, assignment)
	}
	d.queue = filtered
}

// headroom is the number of additional tasks this runner can accept. It is the
// authoritative total Capacity (from Register/Heartbeat) minus the tasks
// already in flight — active claims plus finalized leases. snapshot.InFlight is
// a heartbeat observation only: the directory already tracks every in-flight
// task via activeClaims + finalizedLease, so subtracting InFlight as well would
// double-count and silently suppress effective concurrency.
func (s *memoryRunnerState) headroom() int {
	headroom := s.snapshot.Capacity - len(s.finalizedLease) - len(s.activeClaims)
	if headroom < 0 {
		return 0
	}
	return headroom
}

func removeClaimID(claims []ClaimID, claimID ClaimID) []ClaimID {
	for i, candidate := range claims {
		if candidate != claimID {
			continue
		}
		return append(claims[:i], claims[i+1:]...)
	}
	return claims
}

func matchesReleasedLease(current engine.TaskLease, req ReleaseLeasedRequest) bool {
	if req.LeaseToken != "" {
		return current.LeaseToken == req.LeaseToken
	}
	if req.LeaseID != "" {
		return current.LeaseID == req.LeaseID
	}
	return true
}

func (s *memoryRunnerState) resolveAssignmentID(req ReleaseLeasedRequest) (AssignmentID, bool) {
	if req.LeaseToken != "" {
		if assignmentID, ok := s.leaseByToken[req.LeaseToken]; ok {
			return assignmentID, true
		}
	}
	if req.LeaseID != "" {
		if assignmentID, ok := s.leaseByID[req.LeaseID]; ok {
			return assignmentID, true
		}
	}
	if req.AssignmentID == "" {
		return "", false
	}
	return req.AssignmentID, true
}

func (s *memoryRunnerState) addLeaseIndexes(assignmentID AssignmentID, lease engine.TaskLease) {
	if lease.LeaseID != "" {
		s.leaseByID[lease.LeaseID] = assignmentID
	}
	if lease.LeaseToken != "" {
		s.leaseByToken[lease.LeaseToken] = assignmentID
	}
}

func (s *memoryRunnerState) removeLeaseIndexes(assignmentID AssignmentID, lease engine.TaskLease) {
	if lease.LeaseID != "" {
		if current, ok := s.leaseByID[lease.LeaseID]; ok && current == assignmentID {
			delete(s.leaseByID, lease.LeaseID)
		}
	}
	if lease.LeaseToken != "" {
		if current, ok := s.leaseByToken[lease.LeaseToken]; ok && current == assignmentID {
			delete(s.leaseByToken, lease.LeaseToken)
		}
	}
}

func indexLeaseIDs(finalized map[AssignmentID]engine.TaskLease) map[engine.LeaseID]AssignmentID {
	index := make(map[engine.LeaseID]AssignmentID, len(finalized))
	for assignmentID, lease := range finalized {
		if lease.LeaseID == "" {
			continue
		}
		index[lease.LeaseID] = assignmentID
	}
	return index
}

func indexLeaseTokens(finalized map[AssignmentID]engine.TaskLease) map[engine.LeaseToken]AssignmentID {
	index := make(map[engine.LeaseToken]AssignmentID, len(finalized))
	for assignmentID, lease := range finalized {
		if lease.LeaseToken == "" {
			continue
		}
		index[lease.LeaseToken] = assignmentID
	}
	return index
}

func cloneFinalizedLeases(src map[AssignmentID]engine.TaskLease) map[AssignmentID]engine.TaskLease {
	if len(src) == 0 {
		return make(map[AssignmentID]engine.TaskLease)
	}

	cloned := make(map[AssignmentID]engine.TaskLease, len(src))
	for assignmentID, lease := range src {
		cloned[assignmentID] = lease
	}
	return cloned
}

func canRunRouting(capabilities []protocol.Capability, routing engine.TaskRouting) bool {
	for _, capability := range capabilities {
		if capability.NodeType != routing.NodeType {
			continue
		}
		if routing.NodeVersion == 0 || capability.NodeVersion == 0 || capability.NodeVersion == routing.NodeVersion {
			return true
		}
	}
	return false
}

func namespaceSet(namespaces []namespace.Namespace) map[namespace.Namespace]struct{} {
	set := make(map[namespace.Namespace]struct{}, len(namespaces))
	for _, t := range namespaces {
		set[t] = struct{}{}
	}
	return set
}

func normalizeRunnerNamespaces(namespaces []namespace.Namespace) []namespace.Namespace {
	if len(namespaces) == 0 {
		return []namespace.Namespace{namespace.Default}
	}
	out := make([]namespace.Namespace, len(namespaces))
	copy(out, namespaces)
	return out
}

func (s *memoryRunnerState) canServeNamespace(t namespace.Namespace) bool {
	if len(s.namespaces) == 0 {
		return t == namespace.Default || t == ""
	}
	if t == "" {
		t = namespace.Default
	}
	_, ok := s.namespaces[t]
	return ok
}
