package control

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// MemoryRunnerDirectory keeps runner registration and assignment state in
// process for embedded and test deployments.
type MemoryRunnerDirectory struct {
	mu      sync.RWMutex
	runners map[string]*memoryRunnerState
	queue   []Assignment
	seen    map[AssignmentID]struct{}
	claims  map[ClaimID]memoryClaim
}

type memoryRunnerState struct {
	snapshot       RunnerSnapshot
	policy         RunnerPolicy
	sessionID      string
	namespaces     map[namespace.Namespace]struct{}
	activeClaims   map[ClaimID]AssignmentID
	activeOrder    []ClaimID
	finalizedLease map[AssignmentID]engine.TaskLease
	leaseByID      map[engine.LeaseID]AssignmentID
	leaseByToken   map[engine.LeaseToken]AssignmentID
}

type memoryClaim struct {
	runnerID   string
	assignment Assignment
}

// NewMemoryRunnerDirectory constructs an empty in-memory runner directory.
func NewMemoryRunnerDirectory() *MemoryRunnerDirectory {
	return &MemoryRunnerDirectory{
		runners: make(map[string]*memoryRunnerState),
		seen:    make(map[AssignmentID]struct{}),
		claims:  make(map[ClaimID]memoryClaim),
	}
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

	finalizedLease := make(map[AssignmentID]engine.TaskLease)
	inFlight := 0
	if existing := d.runners[req.RunnerID]; existing != nil {
		d.requeueActiveClaimsLocked(existing)
		finalizedLease = cloneFinalizedLeases(existing.finalizedLease)
		inFlight = existing.snapshot.InFlight
	}

	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}
	session := RunnerSession{
		RunnerID:  req.RunnerID,
		SessionID: uuid.NewString(),
	}
	state := &memoryRunnerState{
		snapshot: RunnerSnapshot{
			RunnerID:      req.RunnerID,
			Capacity:      req.Capacity,
			Labels:        cloneLabels(req.Labels),
			Capabilities:  cloneCapabilities(req.Capabilities),
			InFlight:      inFlight,
			Namespaces:    normalizeRunnerNamespaces(req.Namespaces),
			LastHeartbeat: now,
		},
		policy:         req.Policy,
		sessionID:      session.SessionID,
		namespaces:     namespaceSet(req.Namespaces),
		activeClaims:   make(map[ClaimID]AssignmentID),
		activeOrder:    nil,
		finalizedLease: finalizedLease,
		leaseByID:      indexLeaseIDs(finalizedLease),
		leaseByToken:   indexLeaseTokens(finalizedLease),
	}
	d.runners[req.RunnerID] = state
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
	return nil
}

// EnqueueAssignment queues a new assignment exactly once until its seen marker
// is cleared.
func (d *MemoryRunnerDirectory) EnqueueAssignment(_ context.Context, assignment Assignment) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if assignment.AssignmentID == "" {
		return false, fmt.Errorf("assignment id is required")
	}
	if _, ok := d.seen[assignment.AssignmentID]; ok {
		return false, nil
	}
	d.seen[assignment.AssignmentID] = struct{}{}
	d.queue = append(d.queue, assignment)
	return true, nil
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

	// Capacity is the authoritative total, written only by Register/Heartbeat.
	// A poll must not overwrite it with a client-supplied remainder — that would
	// both corrupt the snapshot and double-count in-flight work already tracked
	// by activeClaims + finalizedLease. Capabilities and labels may still refresh per poll.
	if req.Capabilities != nil {
		state.snapshot.Capabilities = cloneCapabilities(req.Capabilities)
	}
	if req.Labels != nil {
		state.snapshot.Labels = cloneLabels(req.Labels)
	}
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

		claimID := ClaimID(uuid.NewString())
		d.queue = append(d.queue[:i], d.queue[i+1:]...)
		d.claims[claimID] = memoryClaim{runnerID: req.RunnerID, assignment: assignment}
		state.activeClaims[claimID] = assignment.AssignmentID
		state.activeOrder = append(state.activeOrder, claimID)
		return Claim{
			ClaimID:    claimID,
			Assignment: assignment,
		}, true, nil
	}

	return Claim{}, false, nil
}

// FinalizeClaim moves an active claim into leased-capacity accounting.
func (d *MemoryRunnerDirectory) FinalizeClaim(_ context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	claim, ok := d.claims[claimID]
	if !ok {
		return nil
	}

	state := d.runners[claim.runnerID]
	if state == nil {
		delete(d.claims, claimID)
		return nil
	}

	delete(d.claims, claimID)
	delete(state.activeClaims, claimID)
	state.activeOrder = removeClaimID(state.activeOrder, claimID)
	if existing, ok := state.finalizedLease[claim.assignment.AssignmentID]; ok {
		state.removeLeaseIndexes(claim.assignment.AssignmentID, existing)
	}
	if lease != nil {
		state.finalizedLease[claim.assignment.AssignmentID] = *lease
		state.addLeaseIndexes(claim.assignment.AssignmentID, *lease)
	} else {
		state.finalizedLease[claim.assignment.AssignmentID] = engine.TaskLease{}
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

	delete(d.claims, claimID)
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
		state.removeLeaseIndexes(req.AssignmentID, lease)
		delete(d.seen, req.AssignmentID)
		return ExpiredDirectoryLeaseReleased, nil
	}
	return ExpiredDirectoryLeaseAlreadyReleased, nil
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
	state.removeLeaseIndexes(assignmentID, current)
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
	for claimID, claim := range d.claims {
		if claim.assignment.AssignmentID != assignmentID {
			continue
		}
		delete(d.claims, claimID)
		if state := d.runners[claim.runnerID]; state != nil {
			delete(state.activeClaims, claimID)
			state.activeOrder = removeClaimID(state.activeOrder, claimID)
		}
	}
	for _, state := range d.runners {
		if lease, ok := state.finalizedLease[assignmentID]; ok {
			delete(state.finalizedLease, assignmentID)
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
	snapshot.Capabilities = cloneCapabilities(snapshot.Capabilities)
	snapshot.Namespaces = normalizeRunnerNamespaces(snapshot.Namespaces)
	return snapshot, true
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
	if len(state.activeClaims) == 0 {
		return
	}

	requeue := make([]Assignment, 0, len(state.activeOrder))
	for _, claimID := range state.activeOrder {
		claim, ok := d.claims[claimID]
		if !ok {
			continue
		}
		requeue = append(requeue, claim.assignment)
		delete(d.claims, claimID)
	}
	if len(requeue) == 0 {
		return
	}
	d.queue = append(requeue, d.queue...)
	state.activeOrder = nil
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
