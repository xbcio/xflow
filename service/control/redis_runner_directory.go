package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
		d.keys.assignmentLeaseMeta,
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

	assignmentIDs, err := d.rdb.LRange(ctx, d.keys.queue, 0, -1).Result()
	if err != nil {
		return Claim{}, false, fmt.Errorf("read redis assignment queue: %w", err)
	}
	for _, assignmentID := range assignmentIDs {
		raw, err := d.rdb.HGet(ctx, d.keys.assignmentData, assignmentID).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return Claim{}, false, fmt.Errorf("read redis assignment %q: %w", assignmentID, err)
		}
		assignment, err := unmarshalRedisAssignment(raw)
		if err != nil {
			return Claim{}, false, err
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
			return Claim{}, false, err
		}
		switch status {
		case "claimed":
			return Claim{ClaimID: claimID, Assignment: assignment}, true, nil
		case "retry":
			continue
		case "none", "draining":
			return Claim{}, false, nil
		case "not_found", "stale":
			return Claim{}, false, runnerSessionStatusError(status)
		default:
			return Claim{}, false, fmt.Errorf("claim redis assignment: unexpected result %q", status)
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
	}, string(claimID), string(disposition))
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
	states, err := d.rdb.HGetAll(ctx, d.keys.assignmentState).Result()
	if err != nil {
		return Claim{}, false, fmt.Errorf("read leased assignment states: %w", err)
	}
	assignmentIDs := make([]string, 0, len(states))
	for assignmentID, state := range states {
		if state == redisAssignmentLeased {
			assignmentIDs = append(assignmentIDs, assignmentID)
		}
	}
	sort.Strings(assignmentIDs)
	for _, assignmentID := range assignmentIDs {
		owner, err := d.rdb.HGet(ctx, d.keys.assignmentRunner, assignmentID).Result()
		if errors.Is(err, redis.Nil) || owner != runnerID {
			continue
		}
		if err != nil {
			return Claim{}, false, fmt.Errorf("read lease owner %q: %w", assignmentID, err)
		}
		session, err := d.rdb.HGet(ctx, d.keys.assignmentSession, assignmentID).Result()
		if errors.Is(err, redis.Nil) || session != sessionID {
			continue
		}
		if err != nil {
			return Claim{}, false, fmt.Errorf("read lease session %q: %w", assignmentID, err)
		}
		rawAssignment, err := d.rdb.HGet(ctx, d.keys.assignmentData, assignmentID).Result()
		if errors.Is(err, redis.Nil) {
			// Released between the HGETALL above and this read — by the sweeper
			// reclaiming a dead runner's lease, or by a report committing. The
			// assignment is simply not this runner's to replay. Reporting it as
			// an error would be fatal out of proportion: Runner.pollLoop returns
			// on a poll error, so the runner stops claiming work altogether and
			// a queue with waiting tasks goes unserved.
			continue
		}
		if err != nil {
			return Claim{}, false, fmt.Errorf("read leased assignment %q: %w", assignmentID, err)
		}
		assignment, err := unmarshalRedisAssignment(rawAssignment)
		if err != nil {
			return Claim{}, false, err
		}
		rawLease, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseMeta, assignmentID).Result()
		if errors.Is(err, redis.Nil) {
			// Same race, one field later: the release deleted the lease metadata
			// while the payload was still readable.
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
	return Claim{}, false, nil
}

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
		d.keys.assignmentLeaseMeta,
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
	}, string(claimID), leaseID, leaseToken, meta)
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
		d.keys.assignmentLeaseMeta,
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
	}, string(claimID), string(reason))
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
	status, err := d.evalStatus(ctx, redisReleaseLeasedLua, []string{
		d.keys.runnerSession,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMeta,
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
	}, req.RunnerID, string(req.AssignmentID), string(req.LeaseID), string(req.LeaseToken), boolRedisArg(req.RemoveSeen))
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
	rawLease, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseMeta, assignmentID).Result()
	if err != nil {
		return nil, false, fmt.Errorf("read persisted lease %q: %w", assignmentID, err)
	}
	lease, err := unmarshalRedisLeaseMeta(rawLease, assignment.Task)
	if err != nil {
		return nil, false, fmt.Errorf("decode persisted lease %q: %w", assignmentID, err)
	}
	return lease, true, nil
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
		d.keys.assignmentLeaseMeta,
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
		d.keys.assignmentLeaseMeta,
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
}

func (d *RedisRunnerDirectory) runnerForClaim(ctx context.Context, runnerID string) (redisClaimRunner, bool, error) {
	sessionID, err := d.rdb.HGet(ctx, d.keys.runnerSession, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		return redisClaimRunner{}, false, nil
	}
	if err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("read runner session: %w", err)
	}
	capabilitiesRaw, err := d.rdb.HGet(ctx, d.keys.runnerCapabilities, runnerID).Result()
	if err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("read runner capabilities: %w", err)
	}
	policyRaw, err := d.rdb.HGet(ctx, d.keys.runnerPolicy, runnerID).Result()
	if err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("read runner policy: %w", err)
	}
	namespacesRaw, err := d.rdb.HGet(ctx, d.keys.runnerNamespaces, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		namespacesRaw = ""
	} else if err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("read runner namespaces: %w", err)
	}
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
	labelsRaw, err := d.rdb.HGet(ctx, d.keys.runnerLabels, runnerID).Result()
	if errors.Is(err, redis.Nil) {
		labelsRaw = ""
	} else if err != nil {
		return redisClaimRunner{}, false, fmt.Errorf("read runner labels: %w", err)
	}
	var labels map[string]string
	if labelsRaw != "" {
		if err := json.Unmarshal([]byte(labelsRaw), &labels); err != nil {
			return redisClaimRunner{}, false, fmt.Errorf("decode runner labels: %w", err)
		}
	}
	return redisClaimRunner{sessionID: sessionID, capabilities: capabilities, policy: policy, namespaces: namespaces, labels: labels}, true, nil
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
redis.call('HDEL', KEYS[10], ARGV[1])
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
redis.call('HDEL', KEYS[2], assignmentID)
redis.call('HSET', KEYS[5], assignmentID, ARGV[2])
redis.call('HSET', KEYS[6], assignmentID, ARGV[3])
redis.call('HSET', KEYS[7], assignmentID, ARGV[4])
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
    redis.call('HDEL', KEYS[10], assignmentID)
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
if redis.call('HGET', KEYS[3], assignmentID) ~= 'leased' then return 'noop' end
if redis.call('HGET', KEYS[4], assignmentID) ~= ARGV[1] then return 'noop' end
local currentLeaseID = redis.call('HGET', KEYS[6], assignmentID) or ''
local currentLeaseToken = redis.call('HGET', KEYS[7], assignmentID) or ''
if ARGV[4] ~= '' and currentLeaseToken ~= ARGV[4] then return 'noop' end
if ARGV[4] == '' and ARGV[3] ~= '' and currentLeaseID ~= ARGV[3] then return 'noop' end
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
  redis.call('HDEL', KEYS[8], assignmentID)
else
  redis.call('HSET', KEYS[3], assignmentID, 'released')
  redis.call('HDEL', KEYS[4], assignmentID)
  redis.call('HDEL', KEYS[5], assignmentID)
  redis.call('HDEL', KEYS[6], assignmentID)
  redis.call('HDEL', KEYS[7], assignmentID)
  redis.call('HDEL', KEYS[8], assignmentID)
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
redis.call('HDEL', KEYS[10], assignmentID)
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
redis.call('HDEL', KEYS[7], assignmentID)
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
