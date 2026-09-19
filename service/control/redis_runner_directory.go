package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

const (
	defaultRedisRunnerDirectoryClaimTTL                 = 30 * time.Second
	defaultRedisRunnerDirectoryControlAuditMaxLen int64 = 10_000
	redisRunnerDirectoryKeyPrefix                       = "xflow:runner-directory:{control}"

	redisAssignmentQueued   = "queued"
	redisAssignmentClaimed  = "claimed"
	redisAssignmentLeased   = "leased"
	redisAssignmentReleased = "released"
)

var errClaimNotActive = errors.New("runner claim is no longer active")

// RedisRunnerDirectoryOption configures a RedisRunnerDirectory.
type RedisRunnerDirectoryOption func(*redisRunnerDirectoryConfig)

type redisRunnerDirectoryConfig struct {
	claimTTL                  time.Duration
	controlReceiptRetention   time.Duration
	controlAuditMaxLen        int64
	drainObservationFreshness time.Duration
	drainDeadline             time.Duration
	clock                     func() time.Time
	observer                  RunnerClaimObserver
}

// WithRedisRunnerDirectoryClaimTTL sets the maximum time a poll claim can
// remain unfinalized before it is returned to the durable queue.
func WithRedisRunnerDirectoryClaimTTL(ttl time.Duration) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if ttl > 0 {
			cfg.claimTTL = ttl
		}
	}
}

// WithRedisRunnerDirectoryControlReceiptRetention sets how long a completed
// runner-control request remains replayable. The retention period is enforced
// by the next control mutation so receipt cleanup stays in the same Redis Lua
// transition as idempotency checking.
func WithRedisRunnerDirectoryControlReceiptRetention(retention time.Duration) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if retention > 0 {
			cfg.controlReceiptRetention = retention
		}
	}
}

// WithRedisRunnerDirectoryControlAuditMaxLen bounds the Redis Stream that
// records real runner-control desired-state transitions.
func WithRedisRunnerDirectoryControlAuditMaxLen(maxLen int64) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if maxLen > 0 {
			cfg.controlAuditMaxLen = maxLen
		}
	}
}

// WithRedisRunnerDirectoryDrainObservationFreshness sets how long a
// server-recorded runner drain observation can establish runner quiescence.
// A non-positive value keeps the production default.
func WithRedisRunnerDirectoryDrainObservationFreshness(freshness time.Duration) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if freshness > 0 {
			cfg.drainObservationFreshness = freshness
		}
	}
}

// WithRedisRunnerDirectoryDrainDeadline sets the fixed deadline assigned to a
// real ACTIVE -> DRAINING transition. A non-positive value keeps the
// production default.
func WithRedisRunnerDirectoryDrainDeadline(deadline time.Duration) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if deadline > 0 {
			cfg.drainDeadline = deadline
		}
	}
}

// WithRedisRunnerDirectoryClock supplies the server-owned clock used for
// drain observation timestamps and live drain projections. It is primarily
// useful for deterministic tests; client-provided request timestamps never
// establish drain-observation freshness.
func WithRedisRunnerDirectoryClock(clock func() time.Time) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if clock != nil {
			cfg.clock = clock
		}
	}
}

// WithRedisRunnerDirectoryObserver installs an optional observer for durable
// claim reclamation and lease replay events.
func WithRedisRunnerDirectoryObserver(observer RunnerClaimObserver) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if observer != nil {
			cfg.observer = observer
		}
	}
}

// RedisRunnerDirectory persists runner registrations, pending assignments,
// claims, and leased capacity in Redis. It has no process-local scheduling
// state, so a replacement control-plane process can continue from the same
// durable records.
type RedisRunnerDirectory struct {
	rdb                       redis.Cmdable
	claimTTL                  time.Duration
	controlReceiptRetention   time.Duration
	controlAuditMaxLen        int64
	drainObservationFreshness time.Duration
	drainDeadline             time.Duration
	clock                     func() time.Time
	observer                  RunnerClaimObserver
	keys                      redisRunnerDirectoryKeys

	// claimCursorMu guards claimCursors, the per-runner resume position into
	// the shared assignment queue. It is process-local scheduling state, not
	// authority: losing it (restart, eviction) only means a runner restarts its
	// sweep from the head. See claimFromQueuePage.
	claimCursorMu sync.Mutex
	claimCursors  map[string]int
}

var _ RunnerDirectory = (*RedisRunnerDirectory)(nil)
var _ ClaimReclaimer = (*RedisRunnerDirectory)(nil)
var _ ActivationRunnerLister = (*RedisRunnerDirectory)(nil)
var _ ExpiredLeaseReleaser = (*RedisRunnerDirectory)(nil)
var _ HandoffDebtDirectory = (*RedisRunnerDirectory)(nil)
var _ FinalizedHandoffSettler = (*RedisRunnerDirectory)(nil)

// NewRedisRunnerDirectory constructs a Redis-backed RunnerDirectory. Every
// key used by its Lua transitions includes the same Redis Cluster hash tag.
func NewRedisRunnerDirectory(rdb redis.Cmdable, opts ...RedisRunnerDirectoryOption) *RedisRunnerDirectory {
	cfg := redisRunnerDirectoryConfig{
		claimTTL:                  defaultRedisRunnerDirectoryClaimTTL,
		controlReceiptRetention:   defaultRunnerControlReceiptRetention,
		controlAuditMaxLen:        defaultRedisRunnerDirectoryControlAuditMaxLen,
		drainObservationFreshness: defaultRunnerDrainObservationFreshness,
		drainDeadline:             defaultRunnerDrainDeadline,
		clock:                     time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &RedisRunnerDirectory{
		rdb:                       rdb,
		claimTTL:                  cfg.claimTTL,
		controlReceiptRetention:   cfg.controlReceiptRetention,
		controlAuditMaxLen:        cfg.controlAuditMaxLen,
		drainObservationFreshness: cfg.drainObservationFreshness,
		drainDeadline:             cfg.drainDeadline,
		clock:                     cfg.clock,
		observer:                  cfg.observer,
		keys:                      newRedisRunnerDirectoryKeys(redisRunnerDirectoryKeyPrefix),
		claimCursors:              make(map[string]int),
	}
}

// runnerControlReceiptRetention supplies defaults for package tests and
// integrations that construct a directory literal to choose a custom Redis
// prefix. Production construction always initializes the configured value.
func (d *RedisRunnerDirectory) runnerControlReceiptRetention() time.Duration {
	if d.controlReceiptRetention > 0 {
		return d.controlReceiptRetention
	}
	return defaultRunnerControlReceiptRetention
}

func (d *RedisRunnerDirectory) runnerControlAuditMaxLen() int64 {
	if d.controlAuditMaxLen > 0 {
		return d.controlAuditMaxLen
	}
	return defaultRedisRunnerDirectoryControlAuditMaxLen
}

// clockNow returns the directory's server-owned time. Directory literals are
// used by a few real-Redis tests with a custom key prefix, so retain a safe
// fallback when no constructor initialized the clock.
func (d *RedisRunnerDirectory) clockNow() time.Time {
	if d.clock != nil {
		return d.clock().UTC()
	}
	return time.Now().UTC()
}

func (d *RedisRunnerDirectory) runnerDrainObservationFreshness() time.Duration {
	if d.drainObservationFreshness > 0 {
		return d.drainObservationFreshness
	}
	return defaultRunnerDrainObservationFreshness
}

func (d *RedisRunnerDirectory) runnerDrainDeadline() time.Duration {
	if d.drainDeadline > 0 {
		return d.drainDeadline
	}
	return defaultRunnerDrainDeadline
}

// Register installs a fresh fenced session, returns ordinary unfinalized
// claims for the prior session to the durable queue, and rebinds uncertain
// handoffs and finalized leases to the replacement session. A lease_may_exist
// handoff is deliberately never requeued here: the new session must resolve it
// against the engine first.
func (d *RedisRunnerDirectory) Register(ctx context.Context, req RegisterRunnerRequest) (RunnerSession, error) {
	if req.RunnerID == "" {
		return RunnerSession{}, ErrRunnerIDRequired
	}
	if req.Capacity <= 0 {
		return RunnerSession{}, ErrConcurrencyRequired
	}
	if err := d.ReclaimExpiredClaims(ctx); err != nil {
		return RunnerSession{}, err
	}

	capabilities, err := json.Marshal(cloneCapabilities(req.Capabilities))
	if err != nil {
		return RunnerSession{}, fmt.Errorf("marshal runner capabilities: %w", err)
	}
	policy, err := json.Marshal(req.Policy)
	if err != nil {
		return RunnerSession{}, fmt.Errorf("marshal runner policy: %w", err)
	}
	namespaces, err := json.Marshal(normalizeRunnerNamespaces(req.Namespaces))
	if err != nil {
		return RunnerSession{}, fmt.Errorf("marshal runner namespaces: %w", err)
	}
	labels, err := json.Marshal(cloneLabels(req.Labels))
	if err != nil {
		return RunnerSession{}, fmt.Errorf("marshal runner labels: %w", err)
	}
	activations, err := marshalRedisActivationInventory(req.Activations)
	if err != nil {
		return RunnerSession{}, err
	}

	now := req.Now
	if now.IsZero() {
		now = d.clockNow()
	}
	session := RunnerSession{RunnerID: req.RunnerID, SessionID: uuid.NewString()}
	status, err := d.evalStatus(ctx, redisRegisterRunnerLua, []string{
		d.keys.queue,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.runnerSession,
		d.keys.runnerCapacity,
		d.keys.runnerInflight,
		d.keys.runnerCapabilities,
		d.keys.runnerPolicy,
		d.keys.runnerNamespaces,
		d.keys.runnerHeartbeat,
		d.keys.runnerLeaseCount,
		d.keys.claimsExpiry,
		d.keys.runnerLabels,
		d.keys.runnerControlDesired,
		d.keys.runnerControlGeneration,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
		d.keys.deactivationObligationRunner,
		d.keys.deactivationObligationSession,
		d.keys.deactivationObligationNamespace,
		d.keys.deactivationObligationWorkflowID,
		d.keys.deactivationObligationWorkflowVersion,
		d.keys.deactivationObligationEntryUnitID,
		d.keys.deactivationObligationReplicaIndex,
		d.keys.deactivationObligationGeneration,
		d.keys.deactivationObligationDrainGeneration,
		d.keys.deactivationObligationState,
		d.keys.runnerActivationInventory,
		d.keys.runnerDrainObservation,
	}, req.RunnerID, session.SessionID, strconv.Itoa(req.Capacity), string(capabilities), string(policy), string(namespaces), strconv.FormatInt(now.UnixMilli(), 10), string(labels), activations)
	if err != nil {
		return RunnerSession{}, fmt.Errorf("register redis runner: %w", err)
	}
	if status != "registered" {
		return RunnerSession{}, fmt.Errorf("register redis runner: unexpected result %q", status)
	}
	return session, nil
}

// ValidateSession confirms that runnerID still identifies sessionID.
func (d *RedisRunnerDirectory) ValidateSession(ctx context.Context, runnerID, sessionID string) error {
	current, err := d.rdb.HGet(ctx, d.keys.runnerSession, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		return ErrRunnerNotFound
	}
	if err != nil {
		return fmt.Errorf("read runner session: %w", err)
	}
	if sessionID == "" || current != sessionID {
		return ErrRunnerSessionStale
	}
	return nil
}

// Heartbeat updates liveness and observed capacity only for the live session.
func (d *RedisRunnerDirectory) Heartbeat(ctx context.Context, req HeartbeatRequest) error {
	now := req.Now
	if now.IsZero() {
		return d.heartbeat(ctx, req, "")
	}
	return d.heartbeat(ctx, req, strconv.FormatInt(now.UnixMilli(), 10))
}

func (d *RedisRunnerDirectory) heartbeat(ctx context.Context, req HeartbeatRequest, heartbeatMillis string) error {
	// The runner may supply req.Now for legacy heartbeat/liveness bookkeeping,
	// but only this server-owned timestamp is allowed to establish drain
	// observation freshness.
	observation := newRunnerDrainObservationAt(req, d.clockNow())
	observationPayload, err := marshalRedisRunnerDrainObservation(observation)
	if err != nil {
		return err
	}
	hasObservation := "0"
	observationGeneration := ""
	if observation != nil {
		hasObservation = "1"
		observationGeneration = strconv.FormatUint(observation.generation, 10)
	}
	status, err := d.evalStatus(ctx, redisHeartbeatLua, []string{
		d.keys.runnerSession,
		d.keys.runnerCapacity,
		d.keys.runnerInflight,
		d.keys.runnerHeartbeat,
		d.keys.runnerDrainObservation,
		d.keys.runnerControlDesired,
		d.keys.runnerControlGeneration,
	}, req.RunnerID, req.SessionID, strconv.Itoa(req.Capacity), strconv.Itoa(req.InFlight), heartbeatMillis,
		hasObservation, observationGeneration, observationPayload)
	if err != nil {
		return fmt.Errorf("heartbeat redis runner: %w", err)
	}
	return runnerSessionStatusError(status)
}

// EnqueueAssignment inserts a unique durable assignment. Repeated attempts
// keep the original queue position and return false.
func (d *RedisRunnerDirectory) EnqueueAssignment(ctx context.Context, assignment Assignment) (bool, error) {
	if assignment.AssignmentID == "" {
		return false, fmt.Errorf("assignment id is required")
	}
	payload, err := marshalRedisAssignment(assignment)
	if err != nil {
		return false, err
	}
	status, err := d.evalStatus(ctx, redisEnqueueAssignmentLua, []string{
		d.keys.queue,
		d.keys.seen,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID)),
	}, string(assignment.AssignmentID), payload)
	if err != nil {
		return false, fmt.Errorf("enqueue redis assignment: %w", err)
	}
	switch status {
	case "enqueued":
		return true, nil
	case "duplicate":
		return false, nil
	default:
		return false, fmt.Errorf("enqueue redis assignment: unexpected result %q", status)
	}
}

// ClaimForRunner first replays one unfinished lease owned by the current
// session and not already executing on the runner, then reserves the first
// compatible queued assignment with available runner headroom. Replaying is
// intentional at-least-once delivery: a control-plane crash after finalization
// or a lost poll response cannot make the assignment unreachable. Skipping the
// leases the runner reports in flight is what keeps that from also handing a
// live task to the same runner's other workers.
func (d *RedisRunnerDirectory) ClaimForRunner(ctx context.Context, req ClaimRequest) (Claim, bool, error) {
	if err := d.ReclaimExpiredClaims(ctx); err != nil {
		return Claim{}, false, err
	}

	runner, found, err := d.runnerForClaim(ctx, req.RunnerID)
	if err != nil {
		return Claim{}, false, err
	}
	if !found {
		// The runner is gone, so its resume position is stale state that would
		// otherwise linger in the process-local cursor map.
		d.storeClaimCursor(req.RunnerID, 0)
		return Claim{}, false, ErrRunnerNotFound
	}
	if req.SessionID == "" || runner.sessionID != req.SessionID {
		return Claim{}, false, ErrRunnerSessionStale
	}

	if handoff, ok, err := d.recoverableHandoff(ctx, req.RunnerID, req.SessionID); err != nil {
		return Claim{}, false, err
	} else if ok {
		return handoff, true, nil
	}
	if replay, ok, err := d.replayLease(ctx, req.RunnerID, req.SessionID, req.ActiveLeaseIDs); err != nil {
		return Claim{}, false, err
	} else if ok {
		return replay, true, nil
	}
	if req.RecoveryOnly {
		// This is an additional, runner-requested suppression only. The Lua
		// transition below still fences DRAINING atomically for old runners that
		// do not know how to set RecoveryOnly.
		return Claim{}, false, nil
	}

	// Labels and capabilities are registration-time authority. Poll carries the
	// fields only for wire compatibility with older servers; never let a poll
	// change what this authenticated runner is eligible to claim.
	capabilities := runner.capabilities
	labels := runner.labels

	// A runner with no headroom cannot claim anything, so skip the queue scan
	// entirely: reading it for a full runner was O(queue) work per poll that
	// always ended in the same 'none'. The claim transition below re-checks
	// headroom atomically, so this precheck only removes the read.
	if runner.hasHeadroom {
		claim, ok, resolved, err := d.claimFromQueuePage(ctx, req, runner, capabilities, labels)
		if err != nil {
			return Claim{}, false, err
		}
		if resolved {
			return claim, ok, nil
		}
	}

	status, err := d.claim(ctx, req.RunnerID, req.SessionID, "", "", "")
	if err != nil {
		return Claim{}, false, err
	}
	switch status {
	case "none", "draining":
		return Claim{}, false, nil
	case "not_found", "stale":
		return Claim{}, false, runnerSessionStatusError(status)
	default:
		return Claim{}, false, fmt.Errorf("claim redis assignment: unexpected result %q", status)
	}
}

// redisClaimQueuePage bounds how much of the shared assignment queue one poll
// examines. The queue is shared across every runner, namespace and workflow, so
// a whole-list read made each poll O(queue) and the fleet O(runners x queue).
const redisClaimQueuePage = 64

// claimFromQueuePage reads one bounded page of the assignment queue starting at
// this runner's persisted cursor and attempts to claim the first candidate it is
// eligible for. It reports resolved=true when it reached a definite answer
// (a claim, or "none"/"draining"); resolved=false means the page held nothing
// this runner could claim and the caller should still run the empty transition
// so draining and session fencing keep their meaning.
//
// The cursor is deliberately anchored rather than free-running. LREM (in the
// claim and requeue transitions) deletes by value, so a removal ahead of a
// positional cursor shifts the tail left and would skip an element. Resetting
// the cursor to the head on a claim, on a short (end-of-queue) page, and once it
// has walked past the length sampled at entry guarantees a skipped position is
// revisited on the next sweep instead of being stranded. If a skipped element is
// still 'queued' when the sweep wraps, it is re-examined then.
func (d *RedisRunnerDirectory) claimFromQueuePage(
	ctx context.Context,
	req ClaimRequest,
	runner redisClaimRunner,
	capabilities []protocol.Capability,
	labels map[string]string,
) (Claim, bool, bool, error) {
	total, err := d.rdb.LLen(ctx, d.keys.queue).Result()
	if err != nil {
		return Claim{}, false, false, fmt.Errorf("read redis assignment queue length: %w", err)
	}
	cursor := d.loadClaimCursor(req.RunnerID)
	if total == 0 || cursor < 0 || cursor >= int(total) {
		cursor = 0
	}
	assignmentIDs, err := d.rdb.LRange(ctx, d.keys.queue, int64(cursor), int64(cursor+redisClaimQueuePage-1)).Result()
	if err != nil {
		return Claim{}, false, false, fmt.Errorf("read redis assignment queue: %w", err)
	}
	if len(assignmentIDs) == 0 {
		d.storeClaimCursor(req.RunnerID, 0)
		return Claim{}, false, false, nil
	}

	raws, err := d.rdb.HMGet(ctx, d.keys.assignmentData, assignmentIDs...).Result()
	if err != nil {
		return Claim{}, false, false, fmt.Errorf("read redis assignments: %w", err)
	}

	for i, assignmentID := range assignmentIDs {
		raw, _ := raws[i].(string)
		if raw == "" {
			// The payload expired between LRange and HMGet, or was never written;
			// either way there is nothing claimable here.
			continue
		}
		assignment, err := unmarshalRedisAssignment(raw)
		if err != nil {
			return Claim{}, false, false, err
		}
		if !MatchCapabilities(capabilities, assignment.Routing) || !runner.policy.Allows(assignment.Routing.NodeType) {
			continue
		}
		if !canServeNamespace(runner.namespaces, assignment.Namespace) {
			continue
		}
		if rs := assignment.Routing.RunnerSelector; rs != nil && !MatchLabels(labels, rs.MatchLabels) {
			continue
		}

		claimID := ClaimID(uuid.NewString())
		status, err := d.claim(ctx, req.RunnerID, req.SessionID, assignmentID, raw, claimID)
		if err != nil {
			return Claim{}, false, false, err
		}
		switch status {
		case "claimed":
			d.storeClaimCursor(req.RunnerID, 0)
			return Claim{ClaimID: claimID, Assignment: assignment}, true, true, nil
		case "retry":
			continue
		case "none", "draining":
			d.storeClaimCursor(req.RunnerID, 0)
			return Claim{}, false, true, nil
		case "not_found", "stale":
			return Claim{}, false, false, runnerSessionStatusError(status)
		default:
			return Claim{}, false, false, fmt.Errorf("claim redis assignment: unexpected result %q", status)
		}
	}

	// Nothing on this page was claimable. Wrap to the head at end-of-queue or
	// once the cursor has passed the length sampled at entry; otherwise resume
	// from the next page on the following poll.
	next := cursor + len(assignmentIDs)
	if len(assignmentIDs) < redisClaimQueuePage || next >= int(total) {
		next = 0
	}
	d.storeClaimCursor(req.RunnerID, next)
	return Claim{}, false, false, nil
}

func (d *RedisRunnerDirectory) loadClaimCursor(runnerID string) int {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	return d.claimCursors[runnerID]
}

func (d *RedisRunnerDirectory) storeClaimCursor(runnerID string, cursor int) {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	if cursor <= 0 {
		delete(d.claimCursors, runnerID)
		return
	}
	if d.claimCursors == nil {
		d.claimCursors = make(map[string]int)
	}
	d.claimCursors[runnerID] = cursor
}

// MarkClaimLeaseMayExist writes the durable pre-Build*Lease crash fence. The
// Lua transition refuses stale/missing claims and leaves capacity unchanged.
func (d *RedisRunnerDirectory) MarkClaimLeaseMayExist(ctx context.Context, claimID ClaimID) error {
	status, err := d.evalStatus(ctx, redisMarkHandoffLeaseMayExistLua, []string{
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.claimsAssignment,
		d.keys.handoffState,
		d.keys.handoffClaim,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, string(claimID), strconv.FormatInt(time.Now().UTC().UnixMilli(), 10), strconv.FormatInt(d.claimTTLMillis(), 10))
	if err != nil {
		return fmt.Errorf("mark redis handoff lease may exist: %w", err)
	}
	if status != "marked" {
		return errClaimNotActive
	}
	return nil
}

// RecordClaimLeaseCreated persists the exact lease returned by an engine
// Build*Lease call before directory finalization, closing the torn handoff
// window if the caller dies before FinalizeClaim.
func (d *RedisRunnerDirectory) RecordClaimLeaseCreated(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	if lease == nil {
		return fmt.Errorf("record redis handoff %q: nil lease", claimID)
	}
	meta, err := marshalRedisLeaseMeta(lease)
	if err != nil {
		return err
	}
	status, err := d.evalStatus(ctx, redisRecordHandoffLeaseCreatedLua, []string{
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.claimsAssignment,
		d.keys.handoffState,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, string(claimID), meta, string(lease.LeaseID), string(lease.LeaseToken))
	if err != nil {
		return fmt.Errorf("record redis handoff lease created: %w", err)
	}
	if status != "recorded" {
		return errClaimNotActive
	}
	return nil
}

// MakeClaimHandoffRecoverable releases a resolver token after a known dispatch
// failure. A crash before this call is made recoverable by claim expiry or
// session replacement, never by silently requeueing unknown work.
func (d *RedisRunnerDirectory) MakeClaimHandoffRecoverable(ctx context.Context, claimID ClaimID) error {
	status, err := d.evalStatus(ctx, redisMakeHandoffRecoverableLua, []string{
		d.keys.handoffState,
		d.keys.handoffClaim,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, string(claimID), strconv.FormatInt(time.Now().UTC().UnixMilli(), 10), strconv.FormatInt(d.claimTTLMillis(), 10))
	if err != nil {
		return fmt.Errorf("make redis handoff recoverable: %w", err)
	}
	if status != "ready" && status != "noop" {
		return errClaimNotActive
	}
	return nil
}

func (d *RedisRunnerDirectory) SettleClaimHandoff(ctx context.Context, claimID ClaimID, disposition HandoffDisposition) error {
	if disposition != HandoffDispositionRequeue && disposition != HandoffDispositionDrop {
		return fmt.Errorf("settle redis handoff %q: unsupported disposition %q", claimID, disposition)
	}
	assignmentID, ok, err := d.claimAssignmentID(ctx, claimID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	status, err := d.evalStatus(ctx, redisSettleHandoffLua, []string{
		d.keys.queue,
		d.keys.seen,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.claimsExpiry,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
		d.keys.assignmentLeaseMetaKey(assignmentID),
	}, string(claimID), string(disposition), assignmentID)
	if err != nil {
		return fmt.Errorf("settle redis handoff: %w", err)
	}
	switch status {
	case "settled", "noop":
		return nil
	case "unresolved":
		return ErrHandoffResolutionRequired
	default:
		return fmt.Errorf("settle redis handoff: unexpected result %q", status)
	}
}

func (d *RedisRunnerDirectory) recoverableHandoff(ctx context.Context, runnerID, sessionID string) (Claim, bool, error) {
	claims, err := d.rdb.HGetAll(ctx, d.keys.handoffRunner).Result()
	if err != nil {
		return Claim{}, false, fmt.Errorf("read redis handoff runners: %w", err)
	}
	claimIDs := make([]string, 0, len(claims))
	for claimID, owner := range claims {
		if owner == runnerID {
			claimIDs = append(claimIDs, claimID)
		}
	}
	sort.Strings(claimIDs)
	for _, rawClaimID := range claimIDs {
		session, err := d.rdb.HGet(ctx, d.keys.handoffSession, rawClaimID).Result()
		if errors.Is(err, redis.Nil) || session != sessionID {
			continue
		}
		if err != nil {
			return Claim{}, false, fmt.Errorf("read redis handoff session %q: %w", rawClaimID, err)
		}
		stateRaw, err := d.rdb.HGet(ctx, d.keys.handoffState, rawClaimID).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return Claim{}, false, fmt.Errorf("read redis handoff state %q: %w", rawClaimID, err)
		}
		state := HandoffDebtState(stateRaw)
		if state != HandoffDebtLeaseMayExist && state != HandoffDebtLeaseCreated {
			continue
		}
		status, err := d.evalStatus(ctx, redisTakeHandoffRecoveryLua, []string{
			d.keys.handoffState,
			d.keys.handoffClaim,
			d.keys.handoffRecoveryReady,
			d.keys.handoffRecoveryDeadline,
		}, rawClaimID, strconv.FormatInt(time.Now().UTC().UnixMilli(), 10), strconv.FormatInt(d.claimTTLMillis(), 10))
		if err != nil {
			return Claim{}, false, fmt.Errorf("take redis handoff recovery %q: %w", rawClaimID, err)
		}
		if status != "taken" {
			continue
		}
		assignmentID, err := d.rdb.HGet(ctx, d.keys.handoffClaim, rawClaimID).Result()
		if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
			if errors.Is(err, redis.Nil) {
				continue
			}
			return Claim{}, false, fmt.Errorf("read redis handoff assignment %q: %w", rawClaimID, err)
		}
		rawAssignment, err := d.rdb.HGet(ctx, d.keys.assignmentData, assignmentID).Result()
		if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
			if errors.Is(err, redis.Nil) {
				continue
			}
			return Claim{}, false, fmt.Errorf("read redis handoff assignment payload %q: %w", assignmentID, err)
		}
		assignment, err := unmarshalRedisAssignment(rawAssignment)
		if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
			return Claim{}, false, err
		}
		generationRaw, err := d.rdb.HGet(ctx, d.keys.handoffGeneration, rawClaimID).Result()
		if errors.Is(err, redis.Nil) {
			generationRaw = "0"
		} else if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
			return Claim{}, false, fmt.Errorf("read redis handoff generation %q: %w", rawClaimID, err)
		}
		generation, err := strconv.ParseUint(generationRaw, 10, 64)
		if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
			return Claim{}, false, fmt.Errorf("decode redis handoff generation %q: %w", rawClaimID, err)
		}
		debt := &HandoffDebt{State: state, AdmissionGeneration: generation}
		if state == HandoffDebtLeaseCreated {
			rawLease, err := d.rdb.HGet(ctx, d.keys.handoffLeaseMeta, rawClaimID).Result()
			if err != nil {
				d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
				return Claim{}, false, fmt.Errorf("read redis handoff lease %q: %w", rawClaimID, err)
			}
			lease, err := unmarshalRedisLeaseMeta(rawLease, assignment.Task)
			if err != nil {
				d.restoreHandoffRecovery(ctx, ClaimID(rawClaimID))
				return Claim{}, false, fmt.Errorf("decode redis handoff lease %q: %w", rawClaimID, err)
			}
			debt.Lease = lease
		}
		return Claim{ClaimID: ClaimID(rawClaimID), Assignment: assignment, Handoff: debt}, true, nil
	}
	return Claim{}, false, nil
}

func (d *RedisRunnerDirectory) restoreHandoffRecovery(ctx context.Context, claimID ClaimID) {
	if directory, ok := any(d).(HandoffDebtDirectory); ok {
		_ = directory.MakeClaimHandoffRecoverable(ctx, claimID)
	}
}

func (d *RedisRunnerDirectory) replayLease(ctx context.Context, runnerID, sessionID string, activeLeaseIDs []string) (Claim, bool, error) {
	var active map[string]struct{}
	if len(activeLeaseIDs) > 0 {
		active = make(map[string]struct{}, len(activeLeaseIDs))
		for _, id := range activeLeaseIDs {
			active[id] = struct{}{}
		}
	}
	// Pass 0 reads the per-runner index; pass 1, reached only when the index is
	// known to be short, reads the whole assignment-state hash. A pass that
	// replays a lease returns, so a shortfall is noticed by the poll that finds
	// nothing left to replay rather than by every poll.
	visited := make(map[string]struct{})
	for pass := 0; ; pass++ {
		assignmentIDs, err := d.leasedAssignmentCandidates(ctx, runnerID, pass > 0)
		if err != nil {
			return Claim{}, false, err
		}
		sort.Strings(assignmentIDs)
		live := 0
		for _, assignmentID := range assignmentIDs {
			if _, done := visited[assignmentID]; done {
				continue
			}
			visited[assignmentID] = struct{}{}
			// The candidates are a superset of this runner's leases — the index
			// can hold an entry whose lease has since been released, and the
			// fallback pass holds every leased assignment in the directory — so
			// each one is re-checked here rather than trusted.
			state, err := d.rdb.HGet(ctx, d.keys.assignmentState, assignmentID).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return Claim{}, false, fmt.Errorf("read lease state %q: %w", assignmentID, err)
			}
			if state != redisAssignmentLeased {
				d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
				continue
			}
			owner, err := d.rdb.HGet(ctx, d.keys.assignmentRunner, assignmentID).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return Claim{}, false, fmt.Errorf("read lease owner %q: %w", assignmentID, err)
			}
			if owner != runnerID {
				d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
				continue
			}
			live++
			if pass > 0 {
				// This candidate came from the full scan, so the index did not know
				// about it. Recording a lease that was just confirmed live and owned
				// is the safe direction for the index to be wrong in: a later poll
				// re-checks it, and an extra entry costs one bounded read where a
				// missing one would cost the lease its replay.
				d.indexLeasedAssignment(ctx, runnerID, assignmentID)
			}
			session, err := d.rdb.HGet(ctx, d.keys.assignmentSession, assignmentID).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return Claim{}, false, fmt.Errorf("read lease session %q: %w", assignmentID, err)
			}
			if session != sessionID {
				// Still leased and still this runner's, just not this session's. The
				// index entry is accurate, so it is left in place for the session
				// that does own it.
				continue
			}
			rawAssignment, err := d.rdb.HGet(ctx, d.keys.assignmentData, assignmentID).Result()
			if errors.Is(err, redis.Nil) {
				// Released between the reads above — by the sweeper reclaiming a
				// dead runner's lease, or by a report committing. The assignment is
				// simply not this runner's to replay. Reporting it as an error would
				// be fatal out of proportion: Runner.pollLoop returns on a poll
				// error, so the runner stops claiming work altogether and a queue
				// with waiting tasks goes unserved.
				d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
				continue
			}
			if err != nil {
				return Claim{}, false, fmt.Errorf("read leased assignment %q: %w", assignmentID, err)
			}
			assignment, err := unmarshalRedisAssignment(rawAssignment)
			if err != nil {
				return Claim{}, false, err
			}
			rawLease, err := d.rdb.Get(ctx, d.keys.assignmentLeaseMetaKey(assignmentID)).Result()
			if errors.Is(err, redis.Nil) {
				// The metadata is armed for the lease window plus one recovery margin
				// and is now extended on every renewal, so its absence here means the
				// lease has expired out of the directory: LookupLease can no longer
				// find it, no renew and no report will ever succeed, and the
				// assignment would hold this runner's capacity for the life of the
				// process. Neither reclaim path sees that shape — the claim reclaimer
				// only scans 'claimed', and the lease sweeper enumerates the
				// execution's own lease index, which shares the metadata's transient
				// TTL. Release the record here, token-fenced on the identity it still
				// carries, and keep scanning.
				//
				// This is safe against a lost execution rather than a lost lease: an
				// engine that still holds the lease reclaims it and re-enqueues the
				// task through its outbox, and an execution that is gone has nothing
				// left to re-enqueue.
				d.releaseStrandedLease(ctx, assignmentID)
				d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
				continue
			}
			if err != nil {
				return Claim{}, false, fmt.Errorf("read persisted lease %q: %w", assignmentID, err)
			}
			lease, err := unmarshalRedisLeaseMeta(rawLease, assignment.Task)
			if err != nil {
				return Claim{}, false, fmt.Errorf("decode persisted lease %q: %w", assignmentID, err)
			}
			if _, executing := active[string(lease.LeaseID)]; executing {
				// The runner is running this one. Handing it back would start a
				// second execution of a node the first worker has not finished.
				continue
			}
			d.observeLeaseReplay(ctx)
			return Claim{Assignment: assignment, Lease: lease}, true, nil
		}
		if pass > 0 {
			return Claim{}, false, nil
		}
		// The poll is about to report no work, and that answer is only as good as
		// the index behind it: entries that no longer name a live lease were
		// pruned above, so the entries counted live here are exactly the leases
		// this runner's index can account for. Comparing them against the count
		// every version maintains exposes the one way the index can be short —
		// leases finalized by a control plane that predates it — and reading the
		// whole hash once rebuilds it.
		count, err := d.rdb.HGet(ctx, d.keys.runnerLeaseCount, runnerID).Int()
		if err != nil && !errors.Is(err, redis.Nil) {
			return Claim{}, false, fmt.Errorf("read runner lease count %q: %w", runnerID, err)
		}
		if live >= count {
			return Claim{}, false, nil
		}
	}
}

// leasedAssignmentCandidates returns the assignment IDs that may be leased by
// runnerID. scan selects the full assignment-state hash over the per-runner
// index.
//
// The index is what keeps a poll proportional to the runner's own leases rather
// than to every assignment in the directory. It is not trusted on its own: the
// caller counts the candidates it turns out to hold live leases for and compares
// that against runnerLeaseCount, which every version maintains. FinalizeClaim is
// the sole transition into 'leased' and writes the index in the same step, so an
// index that names fewer live leases than the count cannot be complete — the
// entries it is missing were finalized by a control plane that predates the
// index — and the full hash is read once to rebuild it.
func (d *RedisRunnerDirectory) leasedAssignmentCandidates(ctx context.Context, runnerID string, scan bool) ([]string, error) {
	if !scan {
		indexed, err := d.rdb.SMembers(ctx, d.keys.runnerLeasedAssignmentsKey(runnerID)).Result()
		if err != nil {
			return nil, fmt.Errorf("read runner leased assignment index: %w", err)
		}
		return indexed, nil
	}
	states, err := d.rdb.HGetAll(ctx, d.keys.assignmentState).Result()
	if err != nil {
		return nil, fmt.Errorf("read leased assignment states: %w", err)
	}
	assignmentIDs := make([]string, 0, len(states))
	for assignmentID, state := range states {
		if state == redisAssignmentLeased {
			assignmentIDs = append(assignmentIDs, assignmentID)
		}
	}
	return assignmentIDs, nil
}

// pruneLeasedAssignmentIndex drops an index entry that no longer names a live
// lease for its runner. It is best effort on purpose: a stale entry costs one
// bounded read on the next poll, and a poll that failed over index hygiene would
// stop the runner from claiming work altogether.
func (d *RedisRunnerDirectory) pruneLeasedAssignmentIndex(ctx context.Context, runnerID, assignmentID string) {
	_, _ = d.rdb.SRem(ctx, d.keys.runnerLeasedAssignmentsKey(runnerID), assignmentID).Result()
}

// indexLeasedAssignment records a lease the full scan confirmed, so the poll
// after this one can use the bounded index path again. Like the prune, it is
// best effort: the next poll that finds the index short simply scans again.
func (d *RedisRunnerDirectory) indexLeasedAssignment(ctx context.Context, runnerID, assignmentID string) {
	_, _ = d.rdb.SAdd(ctx, d.keys.runnerLeasedAssignmentsKey(runnerID), assignmentID).Result()
}

// releaseStrandedLease clears a directory record whose lease metadata is gone.
//
// The release is token-fenced on the lease identity the assignment still
// carries, so a newer lease generation is never disturbed, and it is
// idempotent: a release that raced this one turns the Lua's state check into a
// no-op. Failures are left to the next poll — the condition is durable, and
// returning an error would be fatal out of proportion, since Runner.pollLoop
// stops claiming work altogether on a poll error.
func (d *RedisRunnerDirectory) releaseStrandedLease(ctx context.Context, assignmentID string) {
	leaseID, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseID, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return
	}
	leaseToken, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseToken, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return
	}
	_, _ = d.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: AssignmentID(assignmentID),
		LeaseID:      engine.LeaseID(leaseID),
		LeaseToken:   engine.LeaseToken(leaseToken),
	})
}

// claim materializes one queued assignment into a claim owned by this runner.
func (d *RedisRunnerDirectory) claim(ctx context.Context, runnerID, sessionID, assignmentID, expectedData string, claimID ClaimID) (string, error) {
	status, err := d.evalStatus(ctx, redisClaimAssignmentLua, []string{
		d.keys.queue,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.runnerLeaseCount,
		d.keys.runnerSession,
		d.keys.runnerCapacity,
		d.keys.claimsExpiry,
		d.keys.runnerControlDesired,
		d.keys.runnerControlGeneration,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
	}, runnerID, sessionID, assignmentID, expectedData, string(claimID), strconv.FormatInt(d.claimTTLMillis(), 10), strconv.FormatInt(time.Now().UTC().UnixMilli(), 10))
	if err != nil {
		return "", fmt.Errorf("claim redis assignment: %w", err)
	}
	return status, nil
}

// FinalizeClaim records a fully materialized task lease against its durable
// assignment after BuildTaskLease succeeds. It validates that the claim still
// belongs to the current runner session before converting claimed -> leased.
func (d *RedisRunnerDirectory) FinalizeClaim(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	if err := d.ReclaimExpiredClaims(ctx); err != nil {
		return err
	}
	assignmentID, ok, err := d.claimAssignmentID(ctx, claimID)
	if err != nil {
		return err
	}
	if !ok {
		return errClaimNotActive
	}
	// The per-runner lease index key has to be named in KEYS for Redis Cluster,
	// which means resolving the claim's runner here rather than inside the
	// script. The script re-checks the ID it resolved against this one, so a
	// claim that changed hands in between is refused instead of indexed under
	// the wrong runner.
	runnerID, ok, err := d.claimRunnerID(ctx, claimID)
	if err != nil {
		return err
	}
	if !ok {
		return errClaimNotActive
	}
	meta, err := marshalRedisLeaseMeta(lease)
	if err != nil {
		return err
	}
	leaseID := ""
	leaseToken := ""
	if lease != nil {
		leaseID = string(lease.LeaseID)
		leaseToken = string(lease.LeaseToken)
	}
	status, err := d.evalStatus(ctx, redisFinalizeClaimLua, []string{
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(assignmentID),
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.runnerLeaseCount,
		d.keys.leaseByID,
		d.keys.leaseByToken,
		d.keys.claimsExpiry,
		d.keys.runnerSession,
		d.keys.handoffState,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
		d.keys.runnerLeasedAssignmentsKey(runnerID),
	}, string(claimID), leaseID, leaseToken, meta, assignmentID,
		strconv.FormatInt(d.assignmentLeaseMetaTTLMillis(lease), 10), runnerID)
	if err != nil {
		return fmt.Errorf("finalize redis claim: %w", err)
	}
	switch status {
	case "finalized":
		return nil
	case "noop":
		return errClaimNotActive
	case "stale":
		return ErrRunnerSessionStale
	default:
		return fmt.Errorf("finalize redis claim: unexpected result %q", status)
	}
}

// ReleaseClaim discards or requeues an unfinalized durable claim according to
// reason. An uncertain lease_may_exist/lease_created handoff is deliberately
// rejected: only Core's engine-aware resolver may settle it.
func (d *RedisRunnerDirectory) ReleaseClaim(ctx context.Context, claimID ClaimID, reason ReleaseClaimReason) error {
	if err := d.ReclaimExpiredClaims(ctx); err != nil {
		return err
	}
	assignmentID, ok, err := d.claimAssignmentID(ctx, claimID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	status, err := d.evalStatus(ctx, redisReleaseClaimLua, []string{
		d.keys.queue,
		d.keys.seen,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(assignmentID),
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.claimsExpiry,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, string(claimID), string(reason), assignmentID)
	if err != nil {
		return fmt.Errorf("release redis claim: %w", err)
	}
	switch status {
	case "released", "noop":
		return nil
	case "resolution_required":
		return ErrHandoffResolutionRequired
	default:
		return fmt.Errorf("release redis claim: unexpected result %q", status)
	}
}

// ReleaseLeased removes leased capacity only when the durable lease identity
// still matches. It intentionally does not require the original session,
// because report cleanup may race a legitimate runner re-registration.
func (d *RedisRunnerDirectory) ReleaseLeased(ctx context.Context, req ReleaseLeasedRequest) error {
	assignmentID, _, err := d.resolveLeaseAssignmentID(ctx, LeaseLookupKey{
		AssignmentID: req.AssignmentID,
		LeaseID:      req.LeaseID,
		LeaseToken:   req.LeaseToken,
	})
	if err != nil {
		return err
	}
	status, err := d.evalStatus(ctx, redisReleaseLeasedLua, []string{
		d.keys.runnerSession,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(assignmentID),
		d.keys.leaseByID,
		d.keys.leaseByToken,
		d.keys.runnerLeaseCount,
		d.keys.seen,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, req.RunnerID, string(req.AssignmentID), string(req.LeaseID), string(req.LeaseToken),
		boolRedisArg(req.RemoveSeen), assignmentID)
	if err != nil {
		return fmt.Errorf("release redis lease: %w", err)
	}
	switch status {
	case "released", "noop":
		return nil
	case "not_found":
		return ErrRunnerNotFound
	default:
		return fmt.Errorf("release redis lease: unexpected result %q", status)
	}
}

// LookupLease returns the server-authoritative finalized lease for one
// (runner, session, lease-identity) triple. It is the namespace authority on the
// report path: the lease JSON echoed by the runner is unsigned and mutable, so
// reportResult resolves the lease from server state here instead of trusting
// req.Lease.Namespace. Resolution mirrors ReleaseLeased (token > leaseID >
// assignmentID via the lease:by-token / lease:by-id indexes), then validates
// the assignment is still leased to (runnerID, sessionID). ok=false means no
// match (not found, already released, wrong runner/session, or identity
// mismatch); err is non-nil only on internal failure.
func (d *RedisRunnerDirectory) LookupLease(ctx context.Context, runnerID, sessionID string, key LeaseLookupKey) (*engine.TaskLease, bool, error) {
	assignmentID, ok, err := d.resolveLeaseAssignmentID(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	state, err := d.rdb.HGet(ctx, d.keys.assignmentState, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, fmt.Errorf("lookup lease state %q: %w", assignmentID, err)
	}
	if errors.Is(err, redis.Nil) || state != redisAssignmentLeased {
		return nil, false, nil
	}
	owner, err := d.rdb.HGet(ctx, d.keys.assignmentRunner, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, fmt.Errorf("lookup lease owner %q: %w", assignmentID, err)
	}
	if errors.Is(err, redis.Nil) || owner != runnerID {
		return nil, false, nil
	}
	session, err := d.rdb.HGet(ctx, d.keys.assignmentSession, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, fmt.Errorf("lookup lease session %q: %w", assignmentID, err)
	}
	if errors.Is(err, redis.Nil) || session != sessionID {
		return nil, false, nil
	}
	rawAssignment, err := d.rdb.HGet(ctx, d.keys.assignmentData, assignmentID).Result()
	if err != nil {
		return nil, false, fmt.Errorf("read leased assignment %q: %w", assignmentID, err)
	}
	assignment, err := unmarshalRedisAssignment(rawAssignment)
	if err != nil {
		return nil, false, err
	}
	rawLease, err := d.rdb.Get(ctx, d.keys.assignmentLeaseMetaKey(assignmentID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read persisted lease %q: %w", assignmentID, err)
	}
	lease, err := unmarshalRedisLeaseMeta(rawLease, assignment.Task)
	if err != nil {
		return nil, false, fmt.Errorf("decode persisted lease %q: %w", assignmentID, err)
	}
	return lease, true, nil
}

// RefreshLeaseMeta re-arms one finalized lease's metadata expiry.
//
// FinalizeClaim arms that expiry once, for the lease's own TTL plus one
// claim-recovery margin — about 90s for a default 60s lease — and nothing used
// to extend it. A node that legitimately outlives that window then loses the
// metadata its own renewals and reports are resolved through: LookupLease
// reads redis.Nil, renew is refused with "lease not found", the runner cancels
// its handler, and the assignment is stranded in 'leased' where no reclaim path
// can see it. The renewal path calls this after each successful engine-side
// extension so the directory expiry tracks the lease the engine actually
// granted.
//
// Absent metadata reports "expired" rather than an error: the lease is already
// unrecoverable by then, and the renewal that led here has already succeeded,
// so failing it would only widen the damage.
func (d *RedisRunnerDirectory) RefreshLeaseMeta(ctx context.Context, runnerID, sessionID string, key LeaseLookupKey, live time.Duration) error {
	assignmentID, ok, err := d.resolveLeaseAssignmentID(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	status, err := d.evalStatus(ctx, redisRefreshLeaseMetaLua, []string{
		d.keys.assignmentState,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseMetaKey(assignmentID),
	}, assignmentID, runnerID, sessionID, strconv.FormatInt(d.assignmentLeaseMetaTTLMillisFor(live), 10))
	if err != nil {
		return fmt.Errorf("refresh redis lease metadata: %w", err)
	}
	switch status {
	case "refreshed", "noop", "expired":
		return nil
	default:
		return fmt.Errorf("refresh redis lease metadata: unexpected result %q", status)
	}
}

// resolveLeaseAssignmentID resolves a finalized assignment ID from a lease
// identity, mirroring ReleaseLeased's token > leaseID > assignmentID precedence
// but read-only. ok=false means no index entry matches.
func (d *RedisRunnerDirectory) resolveLeaseAssignmentID(ctx context.Context, key LeaseLookupKey) (string, bool, error) {
	if key.LeaseToken != "" {
		assignmentID, err := d.rdb.HGet(ctx, d.keys.leaseByToken, string(key.LeaseToken)).Result()
		if err == nil {
			return assignmentID, true, nil
		}
		if !errors.Is(err, redis.Nil) {
			return "", false, fmt.Errorf("resolve lease by token: %w", err)
		}
	}
	if key.LeaseID != "" {
		assignmentID, err := d.rdb.HGet(ctx, d.keys.leaseByID, string(key.LeaseID)).Result()
		if err == nil {
			return assignmentID, true, nil
		}
		if !errors.Is(err, redis.Nil) {
			return "", false, fmt.Errorf("resolve lease by id: %w", err)
		}
	}
	if key.AssignmentID != "" {
		return string(key.AssignmentID), true, nil
	}
	return "", false, nil
}

func (d *RedisRunnerDirectory) claimAssignmentID(ctx context.Context, claimID ClaimID) (string, bool, error) {
	assignmentID, err := d.rdb.HGet(ctx, d.keys.claimsAssignment, string(claimID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("resolve claim assignment %q: %w", claimID, err)
	}
	return assignmentID, true, nil
}

// claimRunnerID resolves the runner a claim belongs to. FinalizeClaim needs it
// to name the per-runner lease index key, which Redis Cluster requires to be
// known before the script runs.
func (d *RedisRunnerDirectory) claimRunnerID(ctx context.Context, claimID ClaimID) (string, bool, error) {
	runnerID, err := d.rdb.HGet(ctx, d.keys.claimsRunner, string(claimID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("resolve claim runner %q: %w", claimID, err)
	}
	if runnerID == "" {
		return "", false, nil
	}
	return runnerID, true, nil
}

// ReleaseExpiredLease removes a finalized lease from the directory only when
// AssignmentID, LeaseID, and LeaseToken all match the stored record. It removes
// capacity before engine reclaim but intentionally retains the finalized
// handoff debt until the sweeper observes a conclusive engine outcome.
func (d *RedisRunnerDirectory) ReleaseExpiredLease(ctx context.Context, req ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	status, err := d.evalStatus(ctx, redisReleaseExpiredLeaseLua, []string{
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(string(req.AssignmentID)),
		d.keys.leaseByID,
		d.keys.leaseByToken,
		d.keys.runnerLeaseCount,
		d.keys.seen,
		d.keys.queue,
		d.keys.handoffState,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, string(req.AssignmentID), string(req.LeaseID), string(req.LeaseToken))
	if err != nil {
		return "", fmt.Errorf("release expired redis lease: %w", err)
	}
	switch status {
	case "released":
		return ExpiredDirectoryLeaseReleased, nil
	case "already_released":
		return ExpiredDirectoryLeaseAlreadyReleased, nil
	case "token_mismatch":
		return ExpiredDirectoryLeaseTokenMismatch, nil
	default:
		return "", fmt.Errorf("release expired redis lease: unexpected result %q", status)
	}
}

// SettleFinalizedHandoff clears a durable finalized debt only after a
// token-fenced engine reclaim/commit outcome has made the lease terminal.
func (d *RedisRunnerDirectory) SettleFinalizedHandoff(ctx context.Context, assignmentID AssignmentID, leaseID engine.LeaseID, leaseToken engine.LeaseToken) error {
	status, err := d.evalStatus(ctx, redisSettleFinalizedHandoffLua, []string{
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, string(assignmentID), string(leaseID), string(leaseToken))
	if err != nil {
		return fmt.Errorf("settle redis finalized handoff: %w", err)
	}
	switch status {
	case "settled", "noop", "mismatch":
		return nil
	default:
		return fmt.Errorf("settle redis finalized handoff: unexpected result %q", status)
	}
}

// ClearAssignment removes the assignment from durable queue, claim, lease,
// dedupe, and matching handoff records.
func (d *RedisRunnerDirectory) ClearAssignment(ctx context.Context, assignmentID AssignmentID) error {
	status, err := d.evalStatus(ctx, redisClearAssignmentLua, []string{
		d.keys.queue,
		d.keys.seen,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(string(assignmentID)),
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.runnerLeaseCount,
		d.keys.leaseByID,
		d.keys.leaseByToken,
		d.keys.claimsExpiry,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
		d.keys.assignmentLeaseMetaLegacy,
	}, string(assignmentID))
	if err != nil {
		return fmt.Errorf("clear redis assignment: %w", err)
	}
	if status != "cleared" {
		return fmt.Errorf("clear redis assignment: unexpected result %q", status)
	}
	return nil
}

// runnerRawFields holds the raw string values fetched from Redis for a single
// runner. An empty-string value means the field was absent (redis.Nil).
type runnerRawFields struct {
	session      string
	capacity     string
	inflight     string
	capabilities string
	namespaces   string
	heartbeat    string
	labels       string
}

// decodeRunnerSnapshot converts raw field strings into a RunnerSnapshot.
// Required fields (session, capacity, capabilities) must be non-empty;
// optional fields (inflight, namespaces, heartbeat, labels) fall back to
// defaults when empty — matching the semantics of redis.Nil in the original
// per-field HGet path.
func decodeRunnerSnapshot(runnerID string, raw runnerRawFields) (RunnerSnapshot, bool) {
	if raw.session == "" {
		return RunnerSnapshot{}, false
	}
	if raw.capacity == "" {
		return RunnerSnapshot{}, false
	}
	if raw.capabilities == "" {
		return RunnerSnapshot{}, false
	}

	// Optional field defaults (equivalent to redis.Nil handling).
	if raw.inflight == "" {
		raw.inflight = "0"
	}
	if raw.heartbeat == "" {
		raw.heartbeat = "0"
	}

	capacity, err := strconv.Atoi(raw.capacity)
	if err != nil {
		return RunnerSnapshot{}, false
	}
	inFlight, err := strconv.Atoi(raw.inflight)
	if err != nil {
		return RunnerSnapshot{}, false
	}
	heartbeatMillis, err := strconv.ParseInt(raw.heartbeat, 10, 64)
	if err != nil {
		return RunnerSnapshot{}, false
	}
	var capabilities []protocol.Capability
	if err := json.Unmarshal([]byte(raw.capabilities), &capabilities); err != nil {
		return RunnerSnapshot{}, false
	}
	namespaces := normalizeRunnerNamespaces(nil)
	if raw.namespaces != "" {
		if err := json.Unmarshal([]byte(raw.namespaces), &namespaces); err != nil {
			return RunnerSnapshot{}, false
		}
	}
	var labels map[string]string
	if raw.labels != "" {
		if err := json.Unmarshal([]byte(raw.labels), &labels); err != nil {
			return RunnerSnapshot{}, false
		}
	}
	return RunnerSnapshot{
		RunnerID:      runnerID,
		SessionID:     raw.session,
		Capacity:      capacity,
		InFlight:      inFlight,
		Labels:        labels,
		Capabilities:  cloneCapabilities(capabilities),
		Namespaces:    namespaces,
		LastHeartbeat: time.UnixMilli(heartbeatMillis),
	}, true
}

// Runner returns the latest durable snapshot for runnerID.
func (d *RedisRunnerDirectory) Runner(ctx context.Context, runnerID string) (RunnerSnapshot, bool) {
	session, err := d.rdb.HGet(ctx, d.keys.runnerSession, runnerID).Result()
	if err != nil || session == "" {
		return RunnerSnapshot{}, false
	}
	capacityRaw, err := d.rdb.HGet(ctx, d.keys.runnerCapacity, runnerID).Result()
	if err != nil {
		return RunnerSnapshot{}, false
	}
	inFlightRaw, err := d.rdb.HGet(ctx, d.keys.runnerInflight, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		inFlightRaw = "0"
	} else if err != nil {
		return RunnerSnapshot{}, false
	}
	capabilitiesRaw, err := d.rdb.HGet(ctx, d.keys.runnerCapabilities, runnerID).Result()
	if err != nil {
		return RunnerSnapshot{}, false
	}
	namespacesRaw, err := d.rdb.HGet(ctx, d.keys.runnerNamespaces, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		namespacesRaw = ""
	} else if err != nil {
		return RunnerSnapshot{}, false
	}
	heartbeatRaw, err := d.rdb.HGet(ctx, d.keys.runnerHeartbeat, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		heartbeatRaw = "0"
	} else if err != nil {
		return RunnerSnapshot{}, false
	}
	labelsRaw, err := d.rdb.HGet(ctx, d.keys.runnerLabels, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		labelsRaw = ""
	} else if err != nil {
		return RunnerSnapshot{}, false
	}

	snapshot, ok := decodeRunnerSnapshot(runnerID, runnerRawFields{
		session:      session,
		capacity:     capacityRaw,
		inflight:     inFlightRaw,
		capabilities: capabilitiesRaw,
		namespaces:   namespacesRaw,
		heartbeat:    heartbeatRaw,
		labels:       labelsRaw,
	})
	if !ok {
		return RunnerSnapshot{}, false
	}
	if control, found, controlErr := d.RunnerControl(ctx, runnerID); controlErr == nil && found {
		snapshot.Control = &control
	}
	return snapshot, true
}

// ListLiveRunners returns a snapshot of every registered runner using a
// pipelined bulk fetch (HKeys + 7 HMGet calls in one round-trip) to avoid
// O(n) serial Redis calls.
func (d *RedisRunnerDirectory) ListLiveRunners(ctx context.Context) []RunnerSnapshot {
	runnerIDs, err := d.rdb.HKeys(ctx, d.keys.runnerSession).Result()
	if err != nil {
		return nil
	}
	if len(runnerIDs) == 0 {
		return nil
	}

	pipe := d.rdb.Pipeline()
	sessionCmd := pipe.HMGet(ctx, d.keys.runnerSession, runnerIDs...)
	capacityCmd := pipe.HMGet(ctx, d.keys.runnerCapacity, runnerIDs...)
	inflightCmd := pipe.HMGet(ctx, d.keys.runnerInflight, runnerIDs...)
	capabilitiesCmd := pipe.HMGet(ctx, d.keys.runnerCapabilities, runnerIDs...)
	namespacesCmd := pipe.HMGet(ctx, d.keys.runnerNamespaces, runnerIDs...)
	heartbeatCmd := pipe.HMGet(ctx, d.keys.runnerHeartbeat, runnerIDs...)
	labelsCmd := pipe.HMGet(ctx, d.keys.runnerLabels, runnerIDs...)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil
	}

	n := len(runnerIDs)
	sessions := sessionCmd.Val()
	capacities := capacityCmd.Val()
	inflights := inflightCmd.Val()
	capabilitiesList := capabilitiesCmd.Val()
	namespacesList := namespacesCmd.Val()
	heartbeats := heartbeatCmd.Val()
	labelsList := labelsCmd.Val()

	out := make([]RunnerSnapshot, 0, n)
	for i := 0; i < n; i++ {
		raw := runnerRawFields{
			session:      hmgetString(sessions, i),
			capacity:     hmgetString(capacities, i),
			inflight:     hmgetString(inflights, i),
			capabilities: hmgetString(capabilitiesList, i),
			namespaces:   hmgetString(namespacesList, i),
			heartbeat:    hmgetString(heartbeats, i),
			labels:       hmgetString(labelsList, i),
		}
		if snap, ok := decodeRunnerSnapshot(runnerIDs[i], raw); ok {
			if control, found, controlErr := d.RunnerControl(ctx, snap.RunnerID); controlErr == nil && found {
				snap.Control = &control
			}
			out = append(out, snap)
		}
	}
	return out
}

// ListRunners returns the IDs of every registered runner. It implements the
// apiserver's runnerLister for the runner-list management endpoint.
//
// It deliberately does not call ListLiveRunners: that method swallows every
// Redis error and returns nil either way, which would make a down Redis look
// identical to zero registered runners. The management endpoint has to draw
// that distinction (500 vs. an empty 200), so this issues its own HKeys and
// propagates the error.
func (d *RedisRunnerDirectory) ListRunners(ctx context.Context) ([]string, error) {
	ids, err := d.rdb.HKeys(ctx, d.keys.runnerSession).Result()
	if err != nil {
		return nil, fmt.Errorf("list runners: %w", err)
	}
	return ids, nil
}

// hmgetString safely extracts a string from an HMGet result slice. HMGet
// returns nil interface{} elements for fields that do not exist in the hash.
func hmgetString(vals []interface{}, i int) string {
	if i >= len(vals) || vals[i] == nil {
		return ""
	}
	s, _ := vals[i].(string)
	return s
}

type redisClaimRunner struct {
	sessionID    string
	capabilities []protocol.Capability
	policy       RunnerPolicy
	namespaces   []namespace.Namespace
	labels       map[string]string
	// hasHeadroom is capacity-claims-leases read in the same pipeline as the
	// registration fields. It is a precheck only: the claim Lua re-evaluates it
	// atomically, so a stale true costs one refused claim, never an over-claim.
	hasHeadroom bool
}

func (d *RedisRunnerDirectory) runnerForClaim(ctx context.Context, runnerID string) (redisClaimRunner, bool, error) {
	// One pipelined round-trip replaces the previous five serial HGet calls, and
	// folds in the capacity/claims/leases headroom so a poll for a full runner
	// can skip the assignment-queue read entirely.
	pipe := d.rdb.Pipeline()
	sessionCmd := pipe.HGet(ctx, d.keys.runnerSession, runnerID)
	capabilitiesCmd := pipe.HGet(ctx, d.keys.runnerCapabilities, runnerID)
	policyCmd := pipe.HGet(ctx, d.keys.runnerPolicy, runnerID)
	namespacesCmd := pipe.HGet(ctx, d.keys.runnerNamespaces, runnerID)
	labelsCmd := pipe.HGet(ctx, d.keys.runnerLabels, runnerID)
	capacityCmd := pipe.HGet(ctx, d.keys.runnerCapacity, runnerID)
	claimCountCmd := pipe.HGet(ctx, d.keys.runnerClaimCount, runnerID)
	leaseCountCmd := pipe.HGet(ctx, d.keys.runnerLeaseCount, runnerID)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return redisClaimRunner{}, false, fmt.Errorf("read runner claim registration: %w", err)
	}

	sessionID := sessionCmd.Val()
	if sessionID == "" {
		return redisClaimRunner{}, false, nil
	}
	capabilitiesRaw := capabilitiesCmd.Val()
	if capabilitiesRaw == "" {
		return redisClaimRunner{}, false, fmt.Errorf("read runner capabilities: %w", redis.Nil)
	}
	policyRaw := policyCmd.Val()
	if policyRaw == "" {
		return redisClaimRunner{}, false, fmt.Errorf("read runner policy: %w", redis.Nil)
	}
	namespacesRaw := namespacesCmd.Val()
	labelsRaw := labelsCmd.Val()

	var capabilities []protocol.Capability
	if err := json.Unmarshal([]byte(capabilitiesRaw), &capabilities); err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("decode runner capabilities: %w", err)
	}
	var policy RunnerPolicy
	if err := json.Unmarshal([]byte(policyRaw), &policy); err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("decode runner policy: %w", err)
	}
	namespaces := normalizeRunnerNamespaces(nil)
	if namespacesRaw != "" {
		if err := json.Unmarshal([]byte(namespacesRaw), &namespaces); err != nil {
			return redisClaimRunner{}, false, fmt.Errorf("decode runner namespaces: %w", err)
		}
	}
	var labels map[string]string
	if labelsRaw != "" {
		if err := json.Unmarshal([]byte(labelsRaw), &labels); err != nil {
			return redisClaimRunner{}, false, fmt.Errorf("decode runner labels: %w", err)
		}
	}
	headroom := atoiDefault(capacityCmd.Val()) - atoiDefault(claimCountCmd.Val()) - atoiDefault(leaseCountCmd.Val())
	return redisClaimRunner{
		sessionID:    sessionID,
		capabilities: capabilities,
		policy:       policy,
		namespaces:   namespaces,
		labels:       labels,
		hasHeadroom:  headroom > 0,
	}, true, nil
}

// atoiDefault parses a Redis integer field, treating a missing or malformed
// value as zero. The claim Lua reads the same fields with the same default, so
// the headroom precheck cannot disagree with the authoritative transition.
func atoiDefault(raw string) int {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return value
}

// ReclaimExpiredClaims returns ordinary expired claims to the durable queue.
// A lease_may_exist or lease_created record is never requeued here: expiry only
// makes it resolver-eligible, preserving the engine-side crash fence.
func (d *RedisRunnerDirectory) ReclaimExpiredClaims(ctx context.Context) error {
	reclaimed, err := d.rdb.Eval(ctx, redisRecoverExpiredClaimsLua, []string{
		d.keys.queue,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.claimsAssignment,
		d.keys.claimsRunner,
		d.keys.claimsSession,
		d.keys.runnerClaimCount,
		d.keys.claimsExpiry,
		d.keys.handoffState,
		d.keys.handoffGeneration,
		d.keys.handoffLeaseMeta,
		d.keys.handoffClaim,
		d.keys.handoffAssignment,
		d.keys.handoffRunner,
		d.keys.handoffSession,
		d.keys.handoffLeaseID,
		d.keys.handoffLeaseToken,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)).Int64()
	if err != nil {
		return fmt.Errorf("recover expired redis claims: %w", err)
	}
	d.observeClaimReclaimed(ctx, int(reclaimed))
	return nil
}

func (d *RedisRunnerDirectory) claimTTLMillis() int64 {
	if millis := d.claimTTL.Milliseconds(); millis > 0 {
		return millis
	}
	return 1
}

// assignmentLeaseMetaTTLMillis retains the complete runner-facing lease for
// the live lease interval plus one claim-recovery window. The metadata is
// sensitive (it can include task input), so a zero-TTL lease or a directory
// constructed directly by a test must still receive a finite positive expiry.
func (d *RedisRunnerDirectory) assignmentLeaseMetaTTLMillis(lease *engine.TaskLease) int64 {
	var live time.Duration
	if lease != nil {
		live = lease.TTL
	}
	return d.assignmentLeaseMetaTTLMillisFor(live)
}

// assignmentLeaseMetaTTLMillisFor applies the live-plus-recovery-margin policy
// to an explicit live window. The renewal path knows the window the engine just
// granted but not the original lease value, so it needs the same arithmetic
// without a lease.
func (d *RedisRunnerDirectory) assignmentLeaseMetaTTLMillisFor(live time.Duration) int64 {
	if live <= 0 {
		live = d.claimTTL
	}
	if live <= 0 {
		live = defaultRedisRunnerDirectoryClaimTTL
	}
	margin := d.claimTTL
	if margin <= 0 {
		margin = defaultRedisRunnerDirectoryClaimTTL
	}
	if millis := (live + margin).Milliseconds(); millis > 0 {
		return millis
	}
	return 1
}

func (d *RedisRunnerDirectory) evalStatus(ctx context.Context, script string, keys []string, args ...interface{}) (string, error) {
	value, err := d.rdb.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		return "", err
	}
	status, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("unexpected Redis Lua result type %T", value)
	}
	return status, nil
}

func runnerSessionStatusError(status string) error {
	switch status {
	case "ok", "registered":
		return nil
	case "not_found":
		return ErrRunnerNotFound
	case "stale":
		return ErrRunnerSessionStale
	default:
		return fmt.Errorf("unexpected runner session result %q", status)
	}
}

func canServeNamespace(namespaces []namespace.Namespace, t namespace.Namespace) bool {
	if len(namespaces) == 0 {
		return t == namespace.Default || t == ""
	}
	if t == "" {
		t = namespace.Default
	}
	for _, allowed := range namespaces {
		if allowed == t {
			return true
		}
	}
	return false
}

const redisRegisterRunnerLua = `
local oldRunner = ARGV[1]
local newSession = ARGV[2]
local preservedClaims = 0
for _, claimID in ipairs(redis.call('HKEYS', KEYS[8])) do
  if redis.call('HGET', KEYS[8], claimID) == oldRunner then
    local assignmentID = redis.call('HGET', KEYS[7], claimID)
    local handoffState = redis.call('HGET', KEYS[23], claimID)
    if not handoffState then
      -- A claim from a pre-ledger control plane may already have crossed into
      -- BuildTaskLease. Migrate it conservatively; never requeue on guesswork.
      handoffState = 'lease_may_exist'
      redis.call('HSET', KEYS[23], claimID, handoffState)
      redis.call('HSET', KEYS[24], claimID, '0')
      redis.call('HDEL', KEYS[25], claimID)
      redis.call('HSET', KEYS[26], claimID, assignmentID)
      redis.call('HSET', KEYS[27], assignmentID, claimID)
      redis.call('HSET', KEYS[28], claimID, oldRunner)
      redis.call('HDEL', KEYS[30], claimID)
      redis.call('HDEL', KEYS[31], claimID)
      redis.call('HSET', KEYS[32], claimID, '1')
      redis.call('HDEL', KEYS[33], claimID)
    end
    if handoffState == 'lease_may_exist' or handoffState == 'lease_created' then
      redis.call('HSET', KEYS[6], assignmentID, newSession)
      redis.call('HSET', KEYS[9], claimID, newSession)
      redis.call('HSET', KEYS[29], claimID, newSession)
      redis.call('HSET', KEYS[32], claimID, '1')
      redis.call('HDEL', KEYS[33], claimID)
      preservedClaims = preservedClaims + 1
    else
      if assignmentID and redis.call('HGET', KEYS[3], assignmentID) == 'claimed' and redis.call('HGET', KEYS[4], assignmentID) == claimID then
        redis.call('HSET', KEYS[3], assignmentID, 'queued')
        redis.call('HDEL', KEYS[4], assignmentID)
        redis.call('HDEL', KEYS[5], assignmentID)
        redis.call('HDEL', KEYS[6], assignmentID)
        redis.call('LREM', KEYS[1], 0, assignmentID)
        if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then redis.call('LPUSH', KEYS[1], assignmentID) end
      end
      redis.call('HDEL', KEYS[7], claimID)
      redis.call('HDEL', KEYS[8], claimID)
      redis.call('HDEL', KEYS[9], claimID)
      redis.call('ZREM', KEYS[19], claimID)
      redis.call('HDEL', KEYS[23], claimID)
      redis.call('HDEL', KEYS[24], claimID)
      redis.call('HDEL', KEYS[25], claimID)
      redis.call('HDEL', KEYS[26], claimID)
      if assignmentID then redis.call('HDEL', KEYS[27], assignmentID) end
      redis.call('HDEL', KEYS[28], claimID)
      redis.call('HDEL', KEYS[29], claimID)
      redis.call('HDEL', KEYS[30], claimID)
      redis.call('HDEL', KEYS[31], claimID)
      redis.call('HDEL', KEYS[32], claimID)
      redis.call('HDEL', KEYS[33], claimID)
    end
  end
end
local states = redis.call('HGETALL', KEYS[3])
for index = 1, #states, 2 do
  local assignmentID = states[index]
  if states[index + 1] == 'leased' and redis.call('HGET', KEYS[5], assignmentID) == oldRunner then redis.call('HSET', KEYS[6], assignmentID, newSession) end
end
for _, claimID in ipairs(redis.call('HKEYS', KEYS[28])) do
  if redis.call('HGET', KEYS[28], claimID) == oldRunner and redis.call('HGET', KEYS[23], claimID) == 'finalized' then redis.call('HSET', KEYS[29], claimID, newSession) end
end
-- The reconnect inventory is part of this registration transition. Only an
-- explicit report of the exact old activation generation may move a durable
-- cleanup receipt to this new session; an empty inventory leaves the old
-- session fenced and the obligation visible as a blocker.
local inventory = cjson.decode(ARGV[9])
for _, obligationID in ipairs(redis.call('HKEYS', KEYS[34])) do
  if redis.call('HGET', KEYS[34], obligationID) == oldRunner and redis.call('HGET', KEYS[35], obligationID) ~= newSession then
    for _, item in ipairs(inventory) do
      local replica = tostring(item.replica_index or 0)
      if (item.workflow_id == redis.call('HGET', KEYS[37], obligationID)) and
         (item.workflow_version == redis.call('HGET', KEYS[38], obligationID)) and
         (item.entry_unit_id == redis.call('HGET', KEYS[39], obligationID)) and
         (replica == redis.call('HGET', KEYS[40], obligationID)) and
         (item.generation == redis.call('HGET', KEYS[41], obligationID)) then
        redis.call('HSET', KEYS[35], obligationID, newSession)
        break
      end
    end
  end
end
redis.call('HSET', KEYS[11], oldRunner, newSession)
redis.call('HSET', KEYS[12], oldRunner, ARGV[3])
redis.call('HSET', KEYS[13], oldRunner, '0')
redis.call('HSET', KEYS[14], oldRunner, ARGV[4])
redis.call('HSET', KEYS[15], oldRunner, ARGV[5])
redis.call('HSET', KEYS[16], oldRunner, ARGV[6])
redis.call('HSET', KEYS[17], oldRunner, ARGV[7])
redis.call('HSET', KEYS[20], oldRunner, ARGV[8])
redis.call('HSETNX', KEYS[21], oldRunner, 'active')
redis.call('HSETNX', KEYS[22], oldRunner, '0')
redis.call('HSET', KEYS[10], oldRunner, tostring(preservedClaims))
redis.call('HSET', KEYS[44], oldRunner, ARGV[9])
redis.call('HDEL', KEYS[45], oldRunner)
return 'registered'
`

const redisHeartbeatLua = `
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then
  return 'not_found'
end
if ARGV[2] == '' or current ~= ARGV[2] then
  return 'stale'
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[4])
if ARGV[5] ~= '' then
  redis.call('HSET', KEYS[4], ARGV[1], ARGV[5])
end
if ARGV[6] == '1' and redis.call('HGET', KEYS[6], ARGV[1]) == 'draining' and redis.call('HGET', KEYS[7], ARGV[1]) == ARGV[7] then
  redis.call('HSET', KEYS[5], ARGV[1], ARGV[8])
else
  redis.call('HDEL', KEYS[5], ARGV[1])
end
return 'ok'
`

// redisEnqueueAssignmentLua inserts a durable assignment, deduplicating against
// the seen set.
//
// A seen mark alone is not enough to reject: it says "this assignment has been
// dispatched before", not "it is still live". The one state where both are true
// at once is 'released' — ReleaseLeased with RemoveSeen=false, which is what a
// stale-token commit produces. The plane has given up ownership but kept the
// mark, so rejecting on the mark alone strands the task forever: the caller
// (Dispatcher.HandleTask) treats 'duplicate' as success and drops it, and
// nothing re-queues it. A map expansion that lost one batch this way waits at
// its barrier for the life of the execution.
//
// So the guard is the STATE, with the seen mark only deciding which key needs
// creating. Every non-released state still rejects: 'queued' (already waiting),
// 'claimed' (a runner is materializing a lease), 'leased' (a runner is running
// it) — re-queueing any of those would hand the same task to a second runner.
const redisEnqueueAssignmentLua = `
local seen = redis.call('SADD', KEYS[2], ARGV[1]) == 0
if seen then
  local state = redis.call('HGET', KEYS[4], ARGV[1])
  if state and state ~= 'released' then
    return 'duplicate'
  end
end
redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[4], ARGV[1], 'queued')
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HDEL', KEYS[8], ARGV[1])
redis.call('HDEL', KEYS[9], ARGV[1])
redis.call('DEL', KEYS[10])
redis.call('LREM', KEYS[1], 0, ARGV[1])
redis.call('RPUSH', KEYS[1], ARGV[1])
return 'enqueued'
`

const redisRecoverExpiredClaimsLua = `
local function deleteHandoff(claimID, assignmentID)
  redis.call('HDEL', KEYS[12], claimID)
  redis.call('HDEL', KEYS[13], claimID)
  redis.call('HDEL', KEYS[14], claimID)
  redis.call('HDEL', KEYS[15], claimID)
  if assignmentID then redis.call('HDEL', KEYS[16], assignmentID) end
  redis.call('HDEL', KEYS[17], claimID)
  redis.call('HDEL', KEYS[18], claimID)
  redis.call('HDEL', KEYS[19], claimID)
  redis.call('HDEL', KEYS[20], claimID)
  redis.call('HDEL', KEYS[21], claimID)
  redis.call('HDEL', KEYS[22], claimID)
end
local function reclaim(claimID)
  local assignmentID = redis.call('HGET', KEYS[7], claimID)
  local runnerID = redis.call('HGET', KEYS[8], claimID)
  local handoffState = redis.call('HGET', KEYS[12], claimID)
  if not handoffState then
    -- Rolling-upgrade fence for claims created before the ledger existed.
    handoffState = 'lease_may_exist'
    redis.call('HSET', KEYS[12], claimID, handoffState)
    redis.call('HSET', KEYS[13], claimID, '0')
    redis.call('HDEL', KEYS[14], claimID)
    redis.call('HSET', KEYS[15], claimID, assignmentID)
    redis.call('HSET', KEYS[16], assignmentID, claimID)
    redis.call('HSET', KEYS[17], claimID, runnerID)
    redis.call('HSET', KEYS[18], claimID, redis.call('HGET', KEYS[9], claimID) or '')
    redis.call('HDEL', KEYS[19], claimID)
    redis.call('HDEL', KEYS[20], claimID)
    redis.call('HSET', KEYS[21], claimID, '1')
    redis.call('HDEL', KEYS[22], claimID)
    return false
  end
  if handoffState == 'lease_may_exist' or handoffState == 'lease_created' then
    local deadline = tonumber(redis.call('HGET', KEYS[22], claimID) or '0')
    if deadline <= tonumber(ARGV[1]) then
      redis.call('HSET', KEYS[21], claimID, '1')
      redis.call('HDEL', KEYS[22], claimID)
    end
    return false
  end
  local recovered = false
  if assignmentID and redis.call('HGET', KEYS[3], assignmentID) == 'claimed' and redis.call('HGET', KEYS[4], assignmentID) == claimID then
    redis.call('HSET', KEYS[3], assignmentID, 'queued')
    redis.call('HDEL', KEYS[4], assignmentID)
    redis.call('HDEL', KEYS[5], assignmentID)
    redis.call('HDEL', KEYS[6], assignmentID)
    redis.call('LREM', KEYS[1], 0, assignmentID)
    if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then redis.call('LPUSH', KEYS[1], assignmentID) end
    recovered = true
  end
  redis.call('HDEL', KEYS[7], claimID)
  redis.call('HDEL', KEYS[8], claimID)
  redis.call('HDEL', KEYS[9], claimID)
  redis.call('ZREM', KEYS[11], claimID)
  if runnerID then
    local count = tonumber(redis.call('HGET', KEYS[10], runnerID) or '0')
    if count > 0 then redis.call('HINCRBY', KEYS[10], runnerID, -1) end
  end
  deleteHandoff(claimID, assignmentID)
  return recovered
end
local recovered = 0
for _, claimID in ipairs(redis.call('ZRANGEBYSCORE', KEYS[11], '-inf', ARGV[1])) do if reclaim(claimID) then recovered = recovered + 1 end end
for _, claimID in ipairs(redis.call('HKEYS', KEYS[7])) do if redis.call('ZSCORE', KEYS[11], claimID) == false and reclaim(claimID) then recovered = recovered + 1 end end
return recovered
`

const redisClaimAssignmentLua = `
local current = redis.call('HGET', KEYS[12], ARGV[1])
if not current then return 'not_found' end
if ARGV[2] == '' or current ~= ARGV[2] then return 'stale' end
if (redis.call('HGET', KEYS[15], ARGV[1]) or 'active') == 'draining' then return 'draining' end
local capacity = tonumber(redis.call('HGET', KEYS[13], ARGV[1]) or '0')
local claims = tonumber(redis.call('HGET', KEYS[10], ARGV[1]) or '0')
local leases = tonumber(redis.call('HGET', KEYS[11], ARGV[1]) or '0')
if capacity - claims - leases <= 0 then return 'none' end
if ARGV[3] == '' then return 'none' end
if redis.call('HGET', KEYS[3], ARGV[3]) ~= 'queued' then return 'retry' end
if redis.call('HGET', KEYS[2], ARGV[3]) ~= ARGV[4] then return 'retry' end
if redis.call('LREM', KEYS[1], 0, ARGV[3]) == 0 then return 'retry' end

redis.call('HSET', KEYS[3], ARGV[3], 'claimed')
redis.call('HSET', KEYS[4], ARGV[3], ARGV[5])
redis.call('HSET', KEYS[5], ARGV[3], ARGV[1])
redis.call('HSET', KEYS[6], ARGV[3], ARGV[2])
redis.call('HSET', KEYS[7], ARGV[5], ARGV[3])
redis.call('HSET', KEYS[8], ARGV[5], ARGV[1])
redis.call('HSET', KEYS[9], ARGV[5], ARGV[2])
redis.call('ZADD', KEYS[14], tonumber(ARGV[7]) + tonumber(ARGV[6]), ARGV[5])
redis.call('HINCRBY', KEYS[10], ARGV[1], 1)

-- The reservation and durable debt record share this exact Lua transaction.
redis.call('HSET', KEYS[17], ARGV[5], 'reserved')
redis.call('HSET', KEYS[18], ARGV[5], redis.call('HGET', KEYS[16], ARGV[1]) or '0')
redis.call('HDEL', KEYS[19], ARGV[5])
redis.call('HSET', KEYS[20], ARGV[5], ARGV[3])
redis.call('HSET', KEYS[21], ARGV[3], ARGV[5])
redis.call('HSET', KEYS[22], ARGV[5], ARGV[1])
redis.call('HSET', KEYS[23], ARGV[5], ARGV[2])
redis.call('HDEL', KEYS[24], ARGV[5])
redis.call('HDEL', KEYS[25], ARGV[5])
redis.call('HDEL', KEYS[26], ARGV[5])
return 'claimed'
`

const redisFinalizeClaimLua = `
local claimID = ARGV[1]
local assignmentID = redis.call('HGET', KEYS[8], claimID)
local runnerID = redis.call('HGET', KEYS[9], claimID)
local sessionID = redis.call('HGET', KEYS[10], claimID)
if not assignmentID or not runnerID then return 'noop' end
if assignmentID ~= ARGV[5] then return 'noop' end
-- KEYS[27] is the per-runner leased-assignment index, and its key literal is
-- derived from the runner's ID by the caller. That derivation is the one part
-- of this transition that is not read inside the script, so it is re-checked
-- here: a claim rebound to another runner between the caller's read and this
-- call would otherwise add the assignment to a set the owner never reads,
-- leaving the owner's own replay blind to its own lease.
if runnerID ~= ARGV[7] then return 'stale' end
if redis.call('HGET', KEYS[16], runnerID) ~= sessionID then return 'stale' end
if redis.call('HGET', KEYS[1], assignmentID) ~= 'claimed' or redis.call('HGET', KEYS[2], assignmentID) ~= claimID then return 'noop' end
redis.call('HDEL', KEYS[8], claimID)
redis.call('HDEL', KEYS[9], claimID)
redis.call('HDEL', KEYS[10], claimID)
redis.call('ZREM', KEYS[15], claimID)
local claims = tonumber(redis.call('HGET', KEYS[11], runnerID) or '0')
if claims > 0 then redis.call('HINCRBY', KEYS[11], runnerID, -1) end
redis.call('HINCRBY', KEYS[12], runnerID, 1)
redis.call('HSET', KEYS[1], assignmentID, 'leased')
-- The index is written in the same step that makes the assignment leased, not
-- by a follow-up call: a lost write would leave a live lease unindexed, and an
-- unindexed lease is one this runner never replays after a reconnect.
redis.call('SADD', KEYS[27], assignmentID)
redis.call('HDEL', KEYS[2], assignmentID)
redis.call('HSET', KEYS[5], assignmentID, ARGV[2])
redis.call('HSET', KEYS[6], assignmentID, ARGV[3])
redis.call('SET', KEYS[7], ARGV[4], 'PX', tonumber(ARGV[6]))
if ARGV[2] ~= '' then redis.call('HSET', KEYS[13], ARGV[2], assignmentID) end
if ARGV[3] ~= '' then redis.call('HSET', KEYS[14], ARGV[3], assignmentID) end
redis.call('HSET', KEYS[17], claimID, 'finalized')
redis.call('HSET', KEYS[18], claimID, ARGV[4])
redis.call('HSET', KEYS[19], claimID, assignmentID)
redis.call('HSET', KEYS[20], assignmentID, claimID)
redis.call('HSET', KEYS[21], claimID, runnerID)
redis.call('HSET', KEYS[22], claimID, sessionID)
redis.call('HSET', KEYS[23], claimID, ARGV[2])
redis.call('HSET', KEYS[24], claimID, ARGV[3])
redis.call('HDEL', KEYS[25], claimID)
redis.call('HDEL', KEYS[26], claimID)
return 'finalized'
`

const redisMarkHandoffLeaseMayExistLua = `
local claimID = ARGV[1]
local assignmentID = redis.call('HGET', KEYS[3], claimID)
if not assignmentID or redis.call('HGET', KEYS[1], assignmentID) ~= 'claimed' or redis.call('HGET', KEYS[2], assignmentID) ~= claimID then return 'noop' end
local state = redis.call('HGET', KEYS[4], claimID)
if state == 'lease_may_exist' then return 'marked' end
if state ~= 'reserved' then return 'noop' end
redis.call('HSET', KEYS[4], claimID, 'lease_may_exist')
redis.call('HDEL', KEYS[6], claimID)
redis.call('HSET', KEYS[7], claimID, tostring(tonumber(ARGV[2]) + tonumber(ARGV[3])))
return 'marked'
`

const redisRecordHandoffLeaseCreatedLua = `
local claimID = ARGV[1]
local assignmentID = redis.call('HGET', KEYS[3], claimID)
if not assignmentID or redis.call('HGET', KEYS[1], assignmentID) ~= 'claimed' or redis.call('HGET', KEYS[2], assignmentID) ~= claimID then return 'noop' end
local state = redis.call('HGET', KEYS[4], claimID)
if state ~= 'lease_may_exist' and state ~= 'lease_created' then return 'noop' end
redis.call('HSET', KEYS[4], claimID, 'lease_created')
redis.call('HSET', KEYS[5], claimID, ARGV[2])
redis.call('HSET', KEYS[6], claimID, assignmentID)
redis.call('HSET', KEYS[7], claimID, ARGV[3])
redis.call('HSET', KEYS[8], claimID, ARGV[4])
redis.call('HDEL', KEYS[9], claimID)
-- Keep the pre-Build resolver deadline until finalization. A second polling
-- worker cannot take the same in-progress handoff just because Build returned.
return 'recorded'
`

const redisMakeHandoffRecoverableLua = `
local state = redis.call('HGET', KEYS[1], ARGV[1])
if state == 'lease_may_exist' or state == 'lease_created' then
  redis.call('HSET', KEYS[3], ARGV[1], '1')
  redis.call('HDEL', KEYS[4], ARGV[1])
  return 'ready'
end
return 'noop'
`

const redisTakeHandoffRecoveryLua = `
local state = redis.call('HGET', KEYS[1], ARGV[1])
if state ~= 'lease_may_exist' and state ~= 'lease_created' then return 'noop' end
if redis.call('HGET', KEYS[2], ARGV[1]) == false then return 'noop' end
local now = tonumber(ARGV[2])
local deadline = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
if deadline > now then return 'busy' end
if redis.call('HGET', KEYS[3], ARGV[1]) ~= '1' then return 'noop' end
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], tostring(now + tonumber(ARGV[3])))
return 'taken'
`

const redisSettleHandoffLua = `
local claimID = ARGV[1]
local assignmentID = redis.call('HGET', KEYS[8], claimID)
local runnerID = redis.call('HGET', KEYS[9], claimID)
local state = redis.call('HGET', KEYS[13], claimID)
if not assignmentID or not state then return 'noop' end
if assignmentID ~= ARGV[3] then return 'noop' end
if state ~= 'lease_may_exist' and state ~= 'lease_created' then return 'unresolved' end
if redis.call('HGET', KEYS[4], assignmentID) ~= 'claimed' or redis.call('HGET', KEYS[5], assignmentID) ~= claimID then return 'unresolved' end
if ARGV[2] == 'requeue' then
  redis.call('HSET', KEYS[4], assignmentID, 'queued')
  redis.call('HDEL', KEYS[5], assignmentID)
  redis.call('HDEL', KEYS[6], assignmentID)
  redis.call('HDEL', KEYS[7], assignmentID)
  redis.call('LREM', KEYS[1], 0, assignmentID)
  if redis.call('HEXISTS', KEYS[3], assignmentID) == 1 then redis.call('LPUSH', KEYS[1], assignmentID) end
elseif ARGV[2] == 'drop' then
  redis.call('LREM', KEYS[1], 0, assignmentID)
  redis.call('SREM', KEYS[2], assignmentID)
  redis.call('HDEL', KEYS[3], assignmentID)
  redis.call('HDEL', KEYS[4], assignmentID)
  redis.call('HDEL', KEYS[5], assignmentID)
  redis.call('HDEL', KEYS[6], assignmentID)
  redis.call('HDEL', KEYS[7], assignmentID)
else return 'unresolved' end
redis.call('DEL', KEYS[24])
redis.call('HDEL', KEYS[8], claimID)
redis.call('HDEL', KEYS[9], claimID)
redis.call('HDEL', KEYS[10], claimID)
redis.call('ZREM', KEYS[12], claimID)
if runnerID then
  local claims = tonumber(redis.call('HGET', KEYS[11], runnerID) or '0')
  if claims > 0 then redis.call('HINCRBY', KEYS[11], runnerID, -1) end
end
redis.call('HDEL', KEYS[13], claimID)
redis.call('HDEL', KEYS[14], claimID)
redis.call('HDEL', KEYS[15], claimID)
redis.call('HDEL', KEYS[16], claimID)
redis.call('HDEL', KEYS[17], assignmentID)
redis.call('HDEL', KEYS[18], claimID)
redis.call('HDEL', KEYS[19], claimID)
redis.call('HDEL', KEYS[20], claimID)
redis.call('HDEL', KEYS[21], claimID)
redis.call('HDEL', KEYS[22], claimID)
redis.call('HDEL', KEYS[23], claimID)
return 'settled'
`

const redisReleaseClaimLua = `
local claimID = ARGV[1]
local assignmentID = redis.call('HGET', KEYS[11], claimID)
local runnerID = redis.call('HGET', KEYS[12], claimID)
if not assignmentID then return 'noop' end
if assignmentID ~= ARGV[3] then return 'noop' end
local handoffState = redis.call('HGET', KEYS[16], claimID)
if not handoffState then
  -- Do not let a newly deployed error path erase a pre-ledger claim that may
  -- have an engine lease. Promote it into resolver-owned debt first.
  handoffState = 'lease_may_exist'
  redis.call('HSET', KEYS[16], claimID, handoffState)
  redis.call('HSET', KEYS[17], claimID, '0')
  redis.call('HDEL', KEYS[18], claimID)
  redis.call('HSET', KEYS[19], claimID, assignmentID)
  redis.call('HSET', KEYS[20], assignmentID, claimID)
  redis.call('HSET', KEYS[21], claimID, runnerID)
  redis.call('HSET', KEYS[22], claimID, redis.call('HGET', KEYS[13], claimID) or '')
  redis.call('HDEL', KEYS[23], claimID)
  redis.call('HDEL', KEYS[24], claimID)
  redis.call('HSET', KEYS[25], claimID, '1')
  redis.call('HDEL', KEYS[26], claimID)
  return 'resolution_required'
end
if handoffState == 'lease_may_exist' or handoffState == 'lease_created' then return 'resolution_required' end
if redis.call('HGET', KEYS[4], assignmentID) == 'claimed' and redis.call('HGET', KEYS[5], assignmentID) == claimID then
  if ARGV[2] == 'requeue' then
    redis.call('HSET', KEYS[4], assignmentID, 'queued')
    redis.call('HDEL', KEYS[5], assignmentID)
    redis.call('HDEL', KEYS[6], assignmentID)
    redis.call('HDEL', KEYS[7], assignmentID)
    redis.call('DEL', KEYS[10])
    redis.call('LREM', KEYS[1], 0, assignmentID)
    if redis.call('HEXISTS', KEYS[3], assignmentID) == 1 then redis.call('LPUSH', KEYS[1], assignmentID) end
  elseif ARGV[2] == 'drop' then
    redis.call('LREM', KEYS[1], 0, assignmentID)
    redis.call('SREM', KEYS[2], assignmentID)
    redis.call('HDEL', KEYS[3], assignmentID)
    redis.call('HDEL', KEYS[4], assignmentID)
    redis.call('HDEL', KEYS[5], assignmentID)
    redis.call('HDEL', KEYS[6], assignmentID)
    redis.call('HDEL', KEYS[7], assignmentID)
    redis.call('HDEL', KEYS[8], assignmentID)
    redis.call('HDEL', KEYS[9], assignmentID)
    redis.call('DEL', KEYS[10])
  else
    return 'noop'
  end
end
redis.call('HDEL', KEYS[11], claimID)
redis.call('HDEL', KEYS[12], claimID)
redis.call('HDEL', KEYS[13], claimID)
redis.call('ZREM', KEYS[15], claimID)
if runnerID then
  local claims = tonumber(redis.call('HGET', KEYS[14], runnerID) or '0')
  if claims > 0 then redis.call('HINCRBY', KEYS[14], runnerID, -1) end
end
redis.call('HDEL', KEYS[16], claimID)
redis.call('HDEL', KEYS[17], claimID)
redis.call('HDEL', KEYS[18], claimID)
redis.call('HDEL', KEYS[19], claimID)
redis.call('HDEL', KEYS[20], assignmentID)
redis.call('HDEL', KEYS[21], claimID)
redis.call('HDEL', KEYS[22], claimID)
redis.call('HDEL', KEYS[23], claimID)
redis.call('HDEL', KEYS[24], claimID)
redis.call('HDEL', KEYS[25], claimID)
redis.call('HDEL', KEYS[26], claimID)
return 'released'
`

const redisReleaseLeasedLua = `
if not redis.call('HGET', KEYS[1], ARGV[1]) then return 'not_found' end
local assignmentID = nil
if ARGV[4] ~= '' then assignmentID = redis.call('HGET', KEYS[10], ARGV[4]) end
if not assignmentID and ARGV[3] ~= '' then assignmentID = redis.call('HGET', KEYS[9], ARGV[3]) end
if not assignmentID or assignmentID == '' then assignmentID = ARGV[2] end
if assignmentID == '' then return 'noop' end
if assignmentID ~= ARGV[6] then return 'noop' end
if redis.call('HGET', KEYS[3], assignmentID) ~= 'leased' then return 'noop' end
if redis.call('HGET', KEYS[4], assignmentID) ~= ARGV[1] then return 'noop' end
local currentLeaseID = redis.call('HGET', KEYS[6], assignmentID) or ''
local currentLeaseToken = redis.call('HGET', KEYS[7], assignmentID) or ''
if ARGV[4] ~= '' and currentLeaseToken ~= ARGV[4] then return 'noop' end
if ARGV[4] == '' and ARGV[3] ~= '' and currentLeaseID ~= ARGV[3] then return 'noop' end
redis.call('DEL', KEYS[8])
if currentLeaseID ~= '' and redis.call('HGET', KEYS[9], currentLeaseID) == assignmentID then redis.call('HDEL', KEYS[9], currentLeaseID) end
if currentLeaseToken ~= '' and redis.call('HGET', KEYS[10], currentLeaseToken) == assignmentID then redis.call('HDEL', KEYS[10], currentLeaseToken) end
local leases = tonumber(redis.call('HGET', KEYS[11], ARGV[1]) or '0')
if leases > 0 then redis.call('HINCRBY', KEYS[11], ARGV[1], -1) end
if ARGV[5] == '1' then
  redis.call('SREM', KEYS[12], assignmentID)
  redis.call('HDEL', KEYS[2], assignmentID)
  redis.call('HDEL', KEYS[3], assignmentID)
  redis.call('HDEL', KEYS[4], assignmentID)
  redis.call('HDEL', KEYS[5], assignmentID)
  redis.call('HDEL', KEYS[6], assignmentID)
  redis.call('HDEL', KEYS[7], assignmentID)
else
  redis.call('HSET', KEYS[3], assignmentID, 'released')
  redis.call('HDEL', KEYS[4], assignmentID)
  redis.call('HDEL', KEYS[5], assignmentID)
  redis.call('HDEL', KEYS[6], assignmentID)
  redis.call('HDEL', KEYS[7], assignmentID)
end
local claimID = redis.call('HGET', KEYS[17], assignmentID)
if claimID and redis.call('HGET', KEYS[13], claimID) == 'finalized' and redis.call('HGET', KEYS[20], claimID) == currentLeaseID and redis.call('HGET', KEYS[21], claimID) == currentLeaseToken then
  redis.call('HDEL', KEYS[13], claimID)
  redis.call('HDEL', KEYS[14], claimID)
  redis.call('HDEL', KEYS[15], claimID)
  redis.call('HDEL', KEYS[16], claimID)
  redis.call('HDEL', KEYS[17], assignmentID)
  redis.call('HDEL', KEYS[18], claimID)
  redis.call('HDEL', KEYS[19], claimID)
  redis.call('HDEL', KEYS[20], claimID)
  redis.call('HDEL', KEYS[21], claimID)
  redis.call('HDEL', KEYS[22], claimID)
  redis.call('HDEL', KEYS[23], claimID)
end
return 'released'
`

const redisSettleFinalizedHandoffLua = `
local assignmentID = ARGV[1]
local leaseID = ARGV[2]
local leaseToken = ARGV[3]
local claimID = redis.call('HGET', KEYS[5], assignmentID)
if not claimID then return 'noop' end
if redis.call('HGET', KEYS[1], claimID) ~= 'finalized' then return 'noop' end
if redis.call('HGET', KEYS[8], claimID) ~= leaseID or redis.call('HGET', KEYS[9], claimID) ~= leaseToken then return 'mismatch' end
redis.call('HDEL', KEYS[1], claimID)
redis.call('HDEL', KEYS[2], claimID)
redis.call('HDEL', KEYS[3], claimID)
redis.call('HDEL', KEYS[4], claimID)
redis.call('HDEL', KEYS[5], assignmentID)
redis.call('HDEL', KEYS[6], claimID)
redis.call('HDEL', KEYS[7], claimID)
redis.call('HDEL', KEYS[8], claimID)
redis.call('HDEL', KEYS[9], claimID)
redis.call('HDEL', KEYS[10], claimID)
redis.call('HDEL', KEYS[11], claimID)
return 'settled'
`

const redisClearAssignmentLua = `
local assignmentID = ARGV[1]
local claimID = redis.call('HGET', KEYS[5], assignmentID)
local runnerID = redis.call('HGET', KEYS[6], assignmentID)
local state = redis.call('HGET', KEYS[4], assignmentID)
if claimID then
  local claimRunner = redis.call('HGET', KEYS[12], claimID)
  if claimRunner then runnerID = claimRunner end
  redis.call('HDEL', KEYS[11], claimID)
  redis.call('HDEL', KEYS[12], claimID)
  redis.call('HDEL', KEYS[13], claimID)
  redis.call('ZREM', KEYS[18], claimID)
  if runnerID then
    local claims = tonumber(redis.call('HGET', KEYS[14], runnerID) or '0')
    if claims > 0 then redis.call('HINCRBY', KEYS[14], runnerID, -1) end
  end
end
if state == 'leased' and runnerID then
  local leases = tonumber(redis.call('HGET', KEYS[15], runnerID) or '0')
  if leases > 0 then redis.call('HINCRBY', KEYS[15], runnerID, -1) end
end
local leaseID = redis.call('HGET', KEYS[8], assignmentID)
local leaseToken = redis.call('HGET', KEYS[9], assignmentID)
if leaseID and redis.call('HGET', KEYS[16], leaseID) == assignmentID then redis.call('HDEL', KEYS[16], leaseID) end
if leaseToken and redis.call('HGET', KEYS[17], leaseToken) == assignmentID then redis.call('HDEL', KEYS[17], leaseToken) end
redis.call('LREM', KEYS[1], 0, assignmentID)
redis.call('SREM', KEYS[2], assignmentID)
redis.call('HDEL', KEYS[3], assignmentID)
redis.call('HDEL', KEYS[4], assignmentID)
redis.call('HDEL', KEYS[5], assignmentID)
redis.call('HDEL', KEYS[6], assignmentID)
redis.call('HDEL', KEYS[7], assignmentID)
redis.call('HDEL', KEYS[8], assignmentID)
redis.call('HDEL', KEYS[9], assignmentID)
redis.call('DEL', KEYS[10])
-- KEYS[30] is the pre-U-7 shared lease-metadata hash. A clear is the one
-- point where this version has decided the assignment is terminally done, so
-- dropping its legacy field is exactly as safe as the HDELs above: the shared
-- per-assignment hashes this version and the previous one both read are being
-- erased in the same atomic step. Leaving the field behind would keep the
-- legacy hash alive forever, which is the leak the per-assignment keys fixed.
redis.call('HDEL', KEYS[30], assignmentID)
local handoffID = redis.call('HGET', KEYS[23], assignmentID)
if handoffID then
  redis.call('HDEL', KEYS[19], handoffID)
  redis.call('HDEL', KEYS[20], handoffID)
  redis.call('HDEL', KEYS[21], handoffID)
  redis.call('HDEL', KEYS[22], handoffID)
  redis.call('HDEL', KEYS[23], assignmentID)
  redis.call('HDEL', KEYS[24], handoffID)
  redis.call('HDEL', KEYS[25], handoffID)
  redis.call('HDEL', KEYS[26], handoffID)
  redis.call('HDEL', KEYS[27], handoffID)
  redis.call('HDEL', KEYS[28], handoffID)
  redis.call('HDEL', KEYS[29], handoffID)
end
return 'cleared'
`

const redisReleaseExpiredLeaseLua = `
local assignmentID = ARGV[1]
local leaseID = ARGV[2]
local leaseToken = ARGV[3]
if redis.call('HGET', KEYS[2], assignmentID) ~= 'leased' then return 'already_released' end
local currentLeaseID = redis.call('HGET', KEYS[5], assignmentID) or ''
local currentLeaseToken = redis.call('HGET', KEYS[6], assignmentID) or ''
if currentLeaseID ~= leaseID or currentLeaseToken ~= leaseToken then return 'token_mismatch' end
local runnerID = redis.call('HGET', KEYS[3], assignmentID)
redis.call('HDEL', KEYS[1], assignmentID)
redis.call('HDEL', KEYS[2], assignmentID)
redis.call('HDEL', KEYS[3], assignmentID)
redis.call('HDEL', KEYS[4], assignmentID)
redis.call('HDEL', KEYS[5], assignmentID)
redis.call('HDEL', KEYS[6], assignmentID)
redis.call('DEL', KEYS[7])
if leaseID ~= '' and redis.call('HGET', KEYS[8], leaseID) == assignmentID then redis.call('HDEL', KEYS[8], leaseID) end
if leaseToken ~= '' and redis.call('HGET', KEYS[9], leaseToken) == assignmentID then redis.call('HDEL', KEYS[9], leaseToken) end
if runnerID then
  local leases = tonumber(redis.call('HGET', KEYS[10], runnerID) or '0')
  if leases > 0 then redis.call('HINCRBY', KEYS[10], runnerID, -1) end
end
redis.call('SREM', KEYS[11], assignmentID)
redis.call('LREM', KEYS[12], 0, assignmentID)
-- Keep the finalized handoff record until engine reclaim proves the same token
-- is no longer live. Its assignment index survives this capacity cleanup.
return 'released'
`

// redisRefreshLeaseMetaLua pushes a live lease's metadata expiry forward.
//
// KEYS: 1=assignment:state 2=assignment:runner 3=assignment:session
//
//	4=this assignment's lease-metadata key
//
// ARGV: 1=assignmentID 2=runnerID 3=sessionID 4=ttl_ms
//
// The runner/session and state checks make the refresh a no-op for a lease that
// has already been released, taken over, or rebound to a newer session, so a
// late renewal from a superseded runner cannot resurrect an expiry a release
// chose to drop. 'expired' means the metadata is already gone: the lease is
// unrecoverable and the caller must not treat the refresh as having armed it.
const redisRefreshLeaseMetaLua = `
local assignmentID = ARGV[1]
if redis.call('HGET', KEYS[1], assignmentID) ~= 'leased' then return 'noop' end
if redis.call('HGET', KEYS[2], assignmentID) ~= ARGV[2] then return 'noop' end
if redis.call('HGET', KEYS[3], assignmentID) ~= ARGV[3] then return 'noop' end
if redis.call('EXISTS', KEYS[4]) == 0 then return 'expired' end
redis.call('PEXPIRE', KEYS[4], tonumber(ARGV[4]))
return 'refreshed'
`
