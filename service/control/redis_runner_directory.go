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
	executions                engine.ExecutionStatusReader
	lanes                     []string
	laneWriteMode             LaneWriteMode
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

// WithRedisRunnerDirectoryLanes enables node-type queue lanes for the listed
// routing node types, in priority order. An empty list (the default) keeps
// every assignment on the shared legacy queue, exactly as before lanes
// existed, and disables the lane write mode below entirely.
func WithRedisRunnerDirectoryLanes(lanes []string) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		cfg.lanes = append([]string(nil), lanes...)
	}
}

// WithRedisRunnerDirectoryLaneWriteMode selects which queue keys a new
// assignment is offered to while lanes are configured. It is meaningful only
// alongside WithRedisRunnerDirectoryLanes; without lanes every mode degenerates
// to the legacy single-queue write. An unrecognized mode also falls back to
// legacy-only, so a config typo can never split writes away from the queue
// every reader still walks.
func WithRedisRunnerDirectoryLaneWriteMode(mode LaneWriteMode) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		cfg.laneWriteMode = mode
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

// WithRedisRunnerDirectoryExecutionStatus installs the execution-liveness probe
// the queued-assignment reaper asks whether an assignment can still be leased.
// It is a StateStore capability rather than a key the directory knows, because
// the execution's keys belong to the engine's namespace-scoped key space: a
// directory reading that layout directly would silently stop matching the day
// it changed, and "not found" is the answer that removes work. A directory built
// without this option performs no queued reclamation at all; see
// DeadQueuedAssignmentReaper.
func WithRedisRunnerDirectoryExecutionStatus(reader engine.ExecutionStatusReader) RedisRunnerDirectoryOption {
	return func(cfg *redisRunnerDirectoryConfig) {
		if reader != nil {
			cfg.executions = reader
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
	executions                engine.ExecutionStatusReader
	lanes                     []string
	laneWriteMode             LaneWriteMode
	keys                      redisRunnerDirectoryKeys

	// claimCursorMu guards claimCursors and claimWalks, the per-runner resume
	// position into the shared assignment queue and whether the last scan for
	// that runner stopped at its scan budget with queue left unexamined. Both
	// are process-local scheduling state, not authority: losing them (restart,
	// eviction) only means a runner restarts its sweep from the head and at the
	// idle cadence. See claimFromQueuePage.
	claimCursorMu sync.Mutex
	claimCursors  map[string]int
	claimWalks    map[string]bool

	// queuedReap is the same kind of process-local state for the
	// queued-assignment reaper's walk of assignment:state, and laneMarkerReap
	// for the reconciliation lap it runs over the lane marker hash. Separate
	// cursors because they are separate structures: one position cannot
	// describe progress through both. See ReapDeadQueuedAssignments.
	queuedReap     queuedReapCursor
	laneMarkerReap queuedReapCursor
}

var _ RunnerDirectory = (*RedisRunnerDirectory)(nil)
var _ ClaimWalkReporter = (*RedisRunnerDirectory)(nil)
var _ RunnerRemover = (*RedisRunnerDirectory)(nil)
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
		executions:                cfg.executions,
		lanes:                     normalizeLaneNodeTypes(cfg.lanes),
		laneWriteMode:             cfg.laneWriteMode,
		keys:                      newRedisRunnerDirectoryKeys(redisRunnerDirectoryKeyPrefix),
		claimCursors:              make(map[string]int),
		claimWalks:                make(map[string]bool),
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

// laneWriteModeOrDefault resolves the effective lane write mode. A directory
// without lanes (including the literals a few real-Redis tests build by hand)
// and an unrecognized mode both resolve to legacy-only, so a config typo can
// never split writes onto a lane that no configured reader walks.
func (d *RedisRunnerDirectory) laneWriteModeOrDefault() LaneWriteMode {
	return resolveLaneWriteMode(d.lanes, d.laneWriteMode)
}

// laneQueueKeys resolves the queue keys a lane configuration names, in
// priority order, always ending with the legacy queue. Claim walks and
// enqueue target sets must both flow through this one function so the keys a
// writer offers an assignment to can never drift from the keys a reader walks.
func (d *RedisRunnerDirectory) laneQueueKeys() []string {
	if len(d.lanes) == 0 {
		return []string{d.keys.queue}
	}
	keys := make([]string, 0, len(d.lanes)+1)
	for _, lane := range d.lanes {
		keys = append(keys, d.keys.laneQueueKey(lane))
	}
	return append(keys, d.keys.queue)
}

// appendLaneRequeueKeys appends the lane requeue KEYS tail — every candidate
// queue key (lanes first, legacy last) and then the lane marker hash — to a
// script's key list. Callers must also pass laneRequeueCandidateCount and
// laneRequeueModeArg as the trailing ARGV values; redisLaneRequeueLua reads
// the candidate count from the second-to-last ARGV and the write mode from the
// last.
func (d *RedisRunnerDirectory) appendLaneRequeueKeys(keys []string) []string {
	keys = append(keys, d.laneQueueKeys()...)
	return append(keys, d.keys.assignmentLane)
}

// laneRequeueCandidateCount fixes where the candidate keys start in KEYS: the
// requeue helper derives laneBase as #KEYS minus this count.
func (d *RedisRunnerDirectory) laneRequeueCandidateCount() string {
	return strconv.Itoa(len(d.laneQueueKeys()))
}

// laneRequeueModeArg is the effective lane write mode the requeue helper
// applies. With no lanes configured it is legacy-only over a single legacy
// candidate — the pre-lane behavior.
func (d *RedisRunnerDirectory) laneRequeueModeArg() string {
	return string(d.laneWriteModeOrDefault())
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
	descriptors, err := marshalRedisRunnerDescriptors(req, now)
	if err != nil {
		return RunnerSession{}, err
	}
	session := RunnerSession{RunnerID: req.RunnerID, SessionID: uuid.NewString()}
	status, err := d.evalStatus(ctx, redisRegisterRunnerLua, d.appendLaneRequeueKeys([]string{
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
		d.keys.handoffClaimIndexKey(req.RunnerID),
		d.keys.runnerInstanceUID,
		d.keys.runnerDescriptors,
	}), req.RunnerID, session.SessionID, strconv.Itoa(req.Capacity), string(capabilities), string(policy), string(namespaces), strconv.FormatInt(now.UnixMilli(), 10), string(labels), activations,
		req.InstanceUID, strconv.FormatInt(DefaultRunnerLiveTTL.Milliseconds(), 10), descriptors,
		d.laneRequeueCandidateCount(), d.laneRequeueModeArg())
	if err != nil {
		return RunnerSession{}, fmt.Errorf("register redis runner: %w", err)
	}
	if status == "runner_id_conflict" {
		return RunnerSession{}, ErrRunnerIDConflict
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
	candidates, targets, marker := d.enqueueQueueKeys(assignment)
	keys := make([]string, 0, len(candidates)+len(targets)+10)
	keys = append(keys, candidates...)
	keys = append(keys, targets...)
	keys = append(keys,
		d.keys.seen,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID)),
		d.keys.assignmentLane,
	)
	status, err := d.evalStatus(ctx, redisEnqueueAssignmentLua, keys,
		string(assignment.AssignmentID), payload,
		strconv.Itoa(len(candidates)), strconv.Itoa(len(targets)), marker)
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

// enqueueQueueKeys resolves where a new assignment is offered: the LREM
// candidate set every queue key is cleared from, the RPUSH target set the
// write mode selects, and the lane marker value (empty = write no marker).
// The candidate set is every configured lane plus the legacy queue — a stale
// copy of this assignment can only ever sit on one of those keys, and clearing
// all of them here is what keeps a re-enqueue from stranding a duplicate that
// the claim walk would only skip. Resolution goes through resolveQueueLane,
// the same function the claim walk uses, so the keys a writer offers an
// assignment to can never drift from the keys a reader walks.
func (d *RedisRunnerDirectory) enqueueQueueKeys(assignment Assignment) (candidates, targets []string, marker string) {
	candidates = d.laneQueueKeys()
	placement := resolveLanePlacement(d.lanes, d.laneWriteModeOrDefault(), assignment.Routing.NodeType)
	targets = make([]string, 0, len(placement.targets))
	for _, lane := range placement.targets {
		if lane == "" {
			targets = append(targets, d.keys.queue)
			continue
		}
		targets = append(targets, d.keys.laneQueueKey(lane))
	}
	if !placement.hasMarker {
		return candidates, targets, ""
	}
	if placement.marker == "" {
		return candidates, targets, d.keys.queue
	}
	return candidates, targets, d.keys.laneQueueKey(placement.marker)
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
		// The runner is gone, so its resume positions and pending-walk marker
		// are stale state that would otherwise linger in the process-local maps.
		d.clearClaimCursors(req.RunnerID)
		d.storeClaimWalk(req.RunnerID, false)
		return Claim{}, false, ErrRunnerNotFound
	}
	if req.SessionID == "" || runner.sessionID != req.SessionID {
		return Claim{}, false, ErrRunnerSessionStale
	}

	// The ledger is keyed by claim across the whole fleet, so filtering it in
	// Redis would cost O(fleet handoff debt) on every poll. The per-runner index
	// makes the scan proportional to this runner's own debt instead; the runner
	// ID is known here, so no fleet-wide read is needed to find it.
	if handoff, ok, err := d.recoverableHandoff(ctx, req.RunnerID, req.SessionID, runner.handoffClaimIDs); err != nil {
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
		//
		// No scan happens on this path, so a pending-walk marker left by an
		// earlier poll is cleared rather than reported: it would otherwise keep
		// the poll loop at the short walk cadence while the runner is asking for
		// recovery only.
		d.storeClaimWalk(req.RunnerID, false)
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
	} else {
		// A full runner does not scan the queue, so any walk from an earlier
		// poll is not in progress for it: clear the marker so the poll loop
		// falls back to the idle long-poll cadence while it waits for headroom.
		d.storeClaimWalk(req.RunnerID, false)
	}

	// The empty transition asks about the runner, not about an entry: no
	// candidate is named, so the queue key is never touched.
	status, err := d.claim(ctx, d.keys.queue, req.RunnerID, req.SessionID, "", "", "")
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

// redisClaimScanBudget bounds how many queue entries one poll may walk past
// while every page it reads yields nothing this runner can claim. One page per
// poll is what made the unclaimable prefix expensive: the prefix is the
// residue of expired executions, the dead-queued reaper drains it at its own
// bounded rate, and while it is long the cursor crosses it at one page per
// pollWait — so a cursor reset to the head (which a queue shrink causes; see
// loadClaimCursor's guard) means minutes of claiming nothing, however many live
// entries sit behind the prefix. The budget is counted in entries rather than
// wall-clock time so a slow shared Redis stretches one page's cost but cannot
// shrink how far a poll reaches; it is a multiple of the page so the walk
// advances whole pages on the common path.
const redisClaimScanBudget = 16 * redisClaimQueuePage

// redisClaimAttemptsPerPoll bounds how many claim transitions one poll may
// attempt. A single-page scan bounded this implicitly; a walk that spans pages
// must bound it explicitly, or a queue whose candidates keep losing their races
// ('retry') could make one poll run an unbounded number of Lua transitions.
// Reaching the bound steps the cursor past the page being attempted — a retried
// entry stays retryable, because its state changed, its payload was replaced,
// or it is no longer in the list, so re-reading the same page would re-attempt
// the same entries — and positions left unattempted are revisited on the next
// sweep, like positions skipped by a concurrent removal.
const redisClaimAttemptsPerPoll = redisClaimQueuePage

// claimFromQueuePage walks the queue targets this runner can serve — the
// configured lanes it has a capability for, in configuration order, then the
// legacy queue — and attempts to claim the first eligible candidate, up to a
// scan budget's worth of entries per target per poll. It reports resolved=true
// when it reached a definite answer (a claim, or "none"/"draining" — both of
// which describe the runner rather than one target, so the walk stops there);
// resolved=false means no target yielded anything this runner could claim and
// the caller should still run the empty transition so draining and session
// fencing keep their meaning.
//
// The targets are probed in one round trip, and an empty target costs exactly
// that probe: no page is read from it and no cursor is kept for it. Lanes come
// before the legacy queue so an entry on a small, quiet lane is seen within one
// poll cycle instead of behind whatever backlog the legacy queue holds — which
// is the whole point of the split. The legacy queue is always walked, so
// entries written by an older control plane, or on another machine before lanes
// were configured, stay reachable.
//
// The cursor is deliberately anchored rather than free-running, and it is kept
// per target. LREM (in the claim and requeue transitions) deletes by value, so
// a removal ahead of a positional cursor shifts the tail left and would skip an
// element. Resetting the cursor to the head on a short (end-of-queue) page, and
// once it has walked past the length sampled at entry, guarantees a skipped
// position is revisited on the next sweep instead of being stranded. If a
// skipped element is still 'queued' when the sweep wraps, it is re-examined
// then.
func (d *RedisRunnerDirectory) claimFromQueuePage(
	ctx context.Context,
	req ClaimRequest,
	runner redisClaimRunner,
	capabilities []protocol.Capability,
	labels map[string]string,
) (Claim, bool, bool, error) {
	targets := d.claimTargets(capabilities)
	lengths, err := d.claimQueueLengths(ctx, targets)
	if err != nil {
		return Claim{}, false, false, err
	}

	pending := false
	for _, target := range targets {
		walk, err := d.walkClaimTarget(ctx, req, runner, capabilities, labels, target, lengths[target])
		if err != nil {
			return Claim{}, false, false, err
		}
		if walk.resolved {
			// A definite answer applies to the poll as a whole, so the remaining
			// targets are not worth another round trip. walkClaimTarget has
			// already cleared the pending-walk marker on these paths.
			return walk.claim, walk.claim.ClaimID != "", true, nil
		}
		pending = pending || walk.pending
	}
	// The scan budget ran out with queue left unexamined on at least one target.
	// Report the walk as pending so the poll loop resumes it at the short cadence
	// instead of the idle one.
	d.storeClaimWalk(req.RunnerID, pending)
	return Claim{}, false, false, nil
}

// claimTargets resolves the queue keys one poll walks, in order: every
// configured lane this runner has a capability for, then the legacy queue. A
// lane whose node type the runner cannot serve is skipped because every entry
// on it would fail MatchCapabilities in the walk anyway; the legacy queue is
// never skipped, so entries an older control plane wrote, or another machine
// wrote before lanes were configured, stay reachable.
func (d *RedisRunnerDirectory) claimTargets(capabilities []protocol.Capability) []string {
	targets := make([]string, 0, len(d.lanes)+1)
	for _, lane := range d.lanes {
		if !canRunRouting(capabilities, engine.TaskRouting{NodeType: lane}) {
			continue
		}
		targets = append(targets, d.keys.laneQueueKey(lane))
	}
	return append(targets, d.keys.queue)
}

// claimQueueLengths reads every target's depth in one round trip, so probing N
// targets costs one round trip rather than N. With a single target it is a
// plain LLEN: that is the pre-lane shape, and it is also the common one.
func (d *RedisRunnerDirectory) claimQueueLengths(ctx context.Context, targets []string) (map[string]int64, error) {
	if len(targets) == 1 {
		total, err := d.rdb.LLen(ctx, targets[0]).Result()
		if err != nil {
			return nil, fmt.Errorf("read redis assignment queue length: %w", err)
		}
		return map[string]int64{targets[0]: total}, nil
	}
	pipe := d.rdb.Pipeline()
	commands := make([]*redis.IntCmd, 0, len(targets))
	for _, target := range targets {
		commands = append(commands, pipe.LLen(ctx, target))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read redis assignment queue lengths: %w", err)
	}
	lengths := make(map[string]int64, len(targets))
	for i, target := range targets {
		lengths[target] = commands[i].Val()
	}
	return lengths, nil
}

// claimTargetWalk is what walking one queue target yielded for one poll.
type claimTargetWalk struct {
	// claim is the claimed assignment; zero when nothing was claimed.
	claim Claim
	// resolved is true when the walk reached a definite answer for the poll: a
	// claim, or a "none"/"draining" verdict.
	resolved bool
	// pending is true when the target has queue left unexamined, so the poll
	// loop should resume at the short cadence. It is only meaningful on an
	// unresolved walk.
	pending bool
}

// walkClaimTarget walks bounded pages of one queue target starting at the
// runner's persisted cursor for that target, and attempts to claim the first
// candidate it is eligible for. total is the depth probed for the target a
// moment earlier; it only bounds the wrap-around test, so a concurrent change
// is harmless.
func (d *RedisRunnerDirectory) walkClaimTarget(
	ctx context.Context,
	req ClaimRequest,
	runner redisClaimRunner,
	capabilities []protocol.Capability,
	labels map[string]string,
	target string,
	total int64,
) (claimTargetWalk, error) {
	if total <= 0 {
		// An empty target costs the probe and nothing else: no page is read, and
		// there is no cursor to reset because it is already at the head.
		d.storeClaimCursorForTarget(req.RunnerID, target, 0)
		return claimTargetWalk{}, nil
	}
	cursor := d.loadClaimCursorForTarget(req.RunnerID, target)
	if cursor < 0 || cursor >= int(total) {
		cursor = 0
	}

	attempts := 0
	scanned := 0
	for scanned < redisClaimScanBudget {
		page := redisClaimQueuePage
		if remaining := redisClaimScanBudget - scanned; remaining < page {
			page = remaining
		}
		assignmentIDs, err := d.rdb.LRange(ctx, target, int64(cursor), int64(cursor+page-1)).Result()
		if err != nil {
			return claimTargetWalk{}, fmt.Errorf("read redis assignment queue: %w", err)
		}
		if len(assignmentIDs) == 0 {
			d.storeClaimCursorForTarget(req.RunnerID, target, 0)
			return claimTargetWalk{}, nil
		}

		raws, err := d.rdb.HMGet(ctx, d.keys.assignmentData, assignmentIDs...).Result()
		if err != nil {
			return claimTargetWalk{}, fmt.Errorf("read redis assignments: %w", err)
		}

		// Resolve the whole page before claiming anything. The eligibility filters used
		// to run inside the claim loop, which made their cost invisible; hoisting them
		// out is what lets the liveness question below be asked once for the page
		// instead of once per attempt.
		type claimCandidate struct {
			assignmentID string
			raw          string
			assignment   Assignment
		}
		candidates := make([]claimCandidate, 0, len(assignmentIDs))
		assignments := make([]Assignment, 0, len(assignmentIDs))
		for i, assignmentID := range assignmentIDs {
			raw, _ := raws[i].(string)
			if raw == "" {
				// The payload expired between LRange and HMGet, or was never written;
				// either way there is nothing claimable here.
				continue
			}
			assignment, err := unmarshalRedisAssignment(raw)
			if err != nil {
				return claimTargetWalk{}, err
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
			candidates = append(candidates, claimCandidate{assignmentID: assignmentID, raw: raw, assignment: assignment})
			assignments = append(assignments, assignment)
		}

		stopped := false
		if len(candidates) > 0 {
			// Drop the assignments whose execution is already gone before claiming any of
			// them. Claiming one is not a no-op: it runs an entire Lua transition, allocates
			// a claim slot, and is then walked back by finalize/release when the dispatch
			// finds nothing to run. On a queue whose head has outlived its executions that
			// is nearly all of the page, so the claim path spent its whole budget
			// materializing leases for work that no longer existed — which is both wasted
			// work and, because a claim resets the resume cursor, the reason a live entry
			// further down the queue was never reached at all.
			//
			// Removal stays with the dead-queued reaper: it owns the atomic transitions
			// (queue, seen set, claim maps) and a wrong removal here would lose work.
			leaseable, err := d.leaseableExecutions(ctx, assignments)
			if err != nil {
				return claimTargetWalk{}, err
			}

		claimAttempts:
			for _, c := range candidates {
				if !leaseable[c.assignment.Task.ExecutionID] {
					continue
				}
				claimID := ClaimID(uuid.NewString())
				status, err := d.claim(ctx, target, req.RunnerID, req.SessionID, c.assignmentID, c.raw, claimID)
				if err != nil {
					return claimTargetWalk{}, err
				}
				switch status {
				case "claimed":
					// The cursor is deliberately NOT reset to the head here. It used to be,
					// so that an element shifted left by a concurrent LREM could not be
					// stranded — but a claim removes the entry the cursor points at, so
					// leaving it where it is already names the next entry, and the
					// wrap-to-head on an exhausted page still revisits anything skipped by a
					// shift. Resetting instead sent every poll back over the same prefix:
					// with a page of dead assignments in front of the live ones, each poll
					// re-walked that whole prefix before it could claim anything, and an
					// unadvanced cursor cannot step over a prefix the way a skipped page can.
					d.storeClaimWalk(req.RunnerID, false)
					return claimTargetWalk{claim: Claim{ClaimID: claimID, Assignment: c.assignment}, resolved: true}, nil
				case "retry":
					// 'retry' is a verdict on the entry, not a transient hold: the
					// state changed (a peer claimed it), the payload was replaced,
					// or the list no longer contains it — so re-attempting the same
					// entries would repeat the same verdicts. Attempting the page's
					// other entries is how the walk escapes it; once the attempt
					// budget is spent, the step below past the page is the other
					// half. See redisClaimAttemptsPerPoll.
					attempts++
					if attempts >= redisClaimAttemptsPerPoll {
						stopped = true
						break claimAttempts
					}
					continue
				case "none", "draining":
					d.storeClaimCursorForTarget(req.RunnerID, target, 0)
					d.storeClaimWalk(req.RunnerID, false)
					return claimTargetWalk{resolved: true}, nil
				case "not_found", "stale":
					return claimTargetWalk{}, runnerSessionStatusError(status)
				default:
					return claimTargetWalk{}, fmt.Errorf("claim redis assignment: unexpected result %q", status)
				}
			}
		}

		// Nothing on this page was claimable (or the attempt budget stopped the
		// scan part-way through it). Wrap to the head at end-of-queue or once the
		// cursor has passed the length sampled at entry; otherwise resume from
		// the next page — in this poll, while the scan budget lasts.
		scanned += len(assignmentIDs)
		cursor = nextClaimCursor(cursor, len(assignmentIDs), int(total))
		d.storeClaimCursorForTarget(req.RunnerID, target, cursor)
		if stopped || cursor == 0 {
			// A stopped walk has queue left to examine, so it resumes at the
			// short cadence; a wrap is a completed sweep and resumes at the idle
			// one.
			return claimTargetWalk{pending: stopped && cursor != 0}, nil
		}
	}

	// The scan budget ran out with queue left unexamined.
	return claimTargetWalk{pending: true}, nil
}

// nextClaimCursor is where the following poll resumes. A short (end-of-queue) page
// or a cursor that has passed the length sampled at entry wraps to the head, so a
// position skipped by a concurrent removal is revisited on the next sweep rather
// than stranded.
func nextClaimCursor(cursor, pageLen, total int) int {
	next := cursor + pageLen
	if pageLen < redisClaimQueuePage || next >= total {
		next = 0
	}
	return next
}

// claimCursorField names one runner's resume position on one queue target. The
// separator cannot occur in a runner ID or a key, so a field never collides
// with another runner's.
func claimCursorField(runnerID, target string) string {
	return runnerID + "\x00" + target
}

func (d *RedisRunnerDirectory) loadClaimCursorForTarget(runnerID, target string) int {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	return d.claimCursors[claimCursorField(runnerID, target)]
}

func (d *RedisRunnerDirectory) storeClaimCursorForTarget(runnerID, target string, cursor int) {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	field := claimCursorField(runnerID, target)
	if cursor <= 0 {
		delete(d.claimCursors, field)
		return
	}
	if d.claimCursors == nil {
		d.claimCursors = make(map[string]int)
	}
	d.claimCursors[field] = cursor
}

// loadClaimCursor is the legacy queue's resume position — the only one that
// existed before queue lanes. Production claim walks read their resume
// position per target through loadClaimCursorForTarget; this single-target
// wrapper remains for the pre-lanes liveness tests that drive it directly.
func (d *RedisRunnerDirectory) loadClaimCursor(runnerID string) int {
	return d.loadClaimCursorForTarget(runnerID, d.keys.queue)
}

// clearClaimCursors drops every resume position held for a runner, on every
// target. A runner that deregistered or was replaced has no position worth
// resuming from on any queue, and leaving them behind would leak one map entry
// per target per dead runner.
func (d *RedisRunnerDirectory) clearClaimCursors(runnerID string) {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	for field := range d.claimCursors {
		if len(field) > len(runnerID) && field[:len(runnerID)] == runnerID && field[len(runnerID)] == 0 {
			delete(d.claimCursors, field)
		}
	}
}

// storeClaimWalk records whether the last claim scan for the runner stopped at
// its scan budget with queue left unexamined. Only that exit sets it: every
// definite answer (a claim, 'none'/'draining', an empty page, a wrapped sweep)
// clears it, so the poll loop's short wait applies exactly while a walk is in
// progress.
func (d *RedisRunnerDirectory) storeClaimWalk(runnerID string, walking bool) {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	if !walking {
		delete(d.claimWalks, runnerID)
		return
	}
	if d.claimWalks == nil {
		d.claimWalks = make(map[string]bool)
	}
	d.claimWalks[runnerID] = true
}

// ClaimWalkPending implements ClaimWalkReporter.
func (d *RedisRunnerDirectory) ClaimWalkPending(runnerID string) bool {
	d.claimCursorMu.Lock()
	defer d.claimCursorMu.Unlock()
	return d.claimWalks[runnerID]
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
	status, err := d.evalStatus(ctx, redisSettleHandoffLua, d.appendLaneRequeueKeys([]string{
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
	}), string(claimID), string(disposition), assignmentID,
		d.laneRequeueCandidateCount(), d.laneRequeueModeArg())
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

// recoverableHandoff replays at most one handoff owned by this runner's session
// that a resolver may take.
//
// It is handed the candidate claim IDs from the per-runner handoff index that
// the caller already read in its registration pipeline, rather than reading the
// fleet-wide handoff ledger. That ledger is keyed by claim across every runner,
// so scanning it made every poll cost O(fleet handoff debt) to find the handful
// of entries this runner owns. The index is written by the same Lua transitions
// that create a handoff record, so it is a complete superset of this runner's
// debt; it is nevertheless not trusted on its own, and each candidate is
// re-checked against the authoritative owner below.
func (d *RedisRunnerDirectory) recoverableHandoff(ctx context.Context, runnerID, sessionID string, candidateClaimIDs []string) (Claim, bool, error) {
	claimIDs := append([]string(nil), candidateClaimIDs...)
	sort.Strings(claimIDs)
	for _, rawClaimID := range claimIDs {
		// The index entry outlives the handoff record it names until the
		// transition that clears that record runs, so ownership is re-read here.
		// An entry whose record is gone, or whose record now names another
		// runner, is dropped: that is the only cleanup the index needs, and it is
		// what keeps it bounded to live debt.
		owner, err := d.rdb.HGet(ctx, d.keys.handoffRunner, rawClaimID).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return Claim{}, false, fmt.Errorf("read redis handoff owner %q: %w", rawClaimID, err)
		}
		if owner != runnerID {
			d.pruneHandoffClaimIndex(ctx, runnerID, rawClaimID)
			continue
		}
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
			// Still this runner's record, still correctly indexed; only its state
			// is not yet (or no longer) resolver-eligible. Keep the entry -- a
			// 'reserved' claim becomes recoverable when its lease fence is
			// written, and the transition that clears the record prunes it.
			continue
		}
		status, err := d.takeHandoffRecovery(ctx, rawClaimID)
		if err != nil {
			return Claim{}, false, fmt.Errorf("take redis handoff recovery %q: %w", rawClaimID, err)
		}
		if status != "taken" {
			continue
		}
		claim, ok, err := d.buildHandoffClaim(ctx, rawClaimID, state)
		if err != nil {
			return Claim{}, false, err
		}
		if !ok {
			continue
		}
		return claim, true, nil
	}
	return Claim{}, false, nil
}

// takeHandoffRecovery attempts to take the single resolver token a recoverable
// handoff carries, returning the Lua's status: "taken" for this caller, and
// "noop" or "busy" for anyone else.
//
// It is a helper rather than an inlined call because two callers now race for
// the same token on purpose — the owning runner's poll, which resolves debt for
// a runner that is alive, and the control plane's orphaned-handoff reaper, which
// resolves debt for one that is not. Both must go through the same fence: a
// second take before the first token's deadline is a "busy", which is what keeps
// two resolvers from settling the same claim against the engine twice.
func (d *RedisRunnerDirectory) takeHandoffRecovery(ctx context.Context, claimID string) (string, error) {
	return d.evalStatus(ctx, redisTakeHandoffRecoveryLua, []string{
		d.keys.handoffState,
		d.keys.handoffClaim,
		d.keys.handoffRecoveryReady,
		d.keys.handoffRecoveryDeadline,
	}, claimID, strconv.FormatInt(time.Now().UTC().UnixMilli(), 10), strconv.FormatInt(d.claimTTLMillis(), 10))
}

// buildHandoffClaim reads the ledger record behind a taken resolver token and
// assembles the Claim a resolver needs. It is called only after the token is
// held, and it returns the token on every failure so the debt is never left
// behind a deadline its own owner cannot shorten.
//
// ok is false when the record is gone, which is not an error: a transition that
// settled the claim between the take and this read has already done the work.
func (d *RedisRunnerDirectory) buildHandoffClaim(ctx context.Context, claimID string, state HandoffDebtState) (Claim, bool, error) {
	assignmentID, err := d.rdb.HGet(ctx, d.keys.handoffClaim, claimID).Result()
	if err != nil {
		d.restoreHandoffRecovery(ctx, ClaimID(claimID))
		if errors.Is(err, redis.Nil) {
			return Claim{}, false, nil
		}
		return Claim{}, false, fmt.Errorf("read redis handoff assignment %q: %w", claimID, err)
	}
	rawAssignment, err := d.rdb.HGet(ctx, d.keys.assignmentData, assignmentID).Result()
	if err != nil {
		d.restoreHandoffRecovery(ctx, ClaimID(claimID))
		if errors.Is(err, redis.Nil) {
			return Claim{}, false, nil
		}
		return Claim{}, false, fmt.Errorf("read redis handoff assignment payload %q: %w", assignmentID, err)
	}
	assignment, err := unmarshalRedisAssignment(rawAssignment)
	if err != nil {
		d.restoreHandoffRecovery(ctx, ClaimID(claimID))
		return Claim{}, false, err
	}
	generationRaw, err := d.rdb.HGet(ctx, d.keys.handoffGeneration, claimID).Result()
	if errors.Is(err, redis.Nil) {
		generationRaw = "0"
	} else if err != nil {
		d.restoreHandoffRecovery(ctx, ClaimID(claimID))
		return Claim{}, false, fmt.Errorf("read redis handoff generation %q: %w", claimID, err)
	}
	generation, err := strconv.ParseUint(generationRaw, 10, 64)
	if err != nil {
		d.restoreHandoffRecovery(ctx, ClaimID(claimID))
		return Claim{}, false, fmt.Errorf("decode redis handoff generation %q: %w", claimID, err)
	}
	debt := &HandoffDebt{State: state, AdmissionGeneration: generation}
	if state == HandoffDebtLeaseCreated {
		rawLease, err := d.rdb.HGet(ctx, d.keys.handoffLeaseMeta, claimID).Result()
		if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(claimID))
			return Claim{}, false, fmt.Errorf("read redis handoff lease %q: %w", claimID, err)
		}
		lease, err := unmarshalRedisLeaseMeta(rawLease, assignment.Task)
		if err != nil {
			d.restoreHandoffRecovery(ctx, ClaimID(claimID))
			return Claim{}, false, fmt.Errorf("decode redis handoff lease %q: %w", claimID, err)
		}
		debt.Lease = lease
	}
	return Claim{ClaimID: ClaimID(claimID), Assignment: assignment, Handoff: debt}, true, nil
}

// pruneHandoffClaimIndex drops an index entry whose handoff record no longer
// names its runner. Like the lease index prune it is best effort on purpose: a
// stale entry costs one bounded read on the next poll, and a poll that failed
// over index hygiene would stop the runner from recovering work altogether.
func (d *RedisRunnerDirectory) pruneHandoffClaimIndex(ctx context.Context, runnerID, claimID string) {
	_, _ = d.rdb.SRem(ctx, d.keys.handoffClaimIndexKey(runnerID), claimID).Result()
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

// leasedAssignmentVerdict steers walkLeasedAssignments from its visit callback.
type leasedAssignmentVerdict int

const (
	// walkLeasedSkip: not a match; keep walking.
	walkLeasedSkip leasedAssignmentVerdict = iota
	// walkLeasedKeep: a fallback match. It becomes the walk's result only when
	// no later candidate is taken; the first kept candidate wins.
	walkLeasedKeep
	// walkLeasedTake: the best match; the walk stops here.
	walkLeasedTake
)

// walkLeasedAssignments walks the assignments runnerID currently holds as
// leased, in the same two-pass shape as every other lease scan: the per-runner
// index first, and the full assignment-state hash only when the index names
// fewer live leases than the runner's own count says it should.
//
// visit decides each live candidate's verdict; it is never called for a
// candidate whose state or owner does not match the runner. The walk returns
// the first taken candidate, else the first kept one.
func (d *RedisRunnerDirectory) walkLeasedAssignments(ctx context.Context, runnerID string, visit func(assignmentID string) (*engine.TaskLease, leasedAssignmentVerdict, error)) (*engine.TaskLease, bool, error) {
	var kept *engine.TaskLease
	visited := make(map[string]struct{})
	for pass := 0; pass < 2; pass++ {
		candidates, err := d.leasedAssignmentCandidates(ctx, runnerID, pass > 0)
		if err != nil {
			return nil, false, err
		}
		live := 0
		for _, candidate := range candidates {
			if _, done := visited[candidate]; done {
				continue
			}
			visited[candidate] = struct{}{}
			state, err := d.rdb.HGet(ctx, d.keys.assignmentState, candidate).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return nil, false, fmt.Errorf("walk leased assignment state %q: %w", candidate, err)
			}
			owner, err := d.rdb.HGet(ctx, d.keys.assignmentRunner, candidate).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return nil, false, fmt.Errorf("walk leased assignment owner %q: %w", candidate, err)
			}
			if state != redisAssignmentLeased || owner != runnerID {
				continue
			}
			live++
			lease, verdict, err := visit(candidate)
			if err != nil {
				return nil, false, err
			}
			switch verdict {
			case walkLeasedTake:
				return lease, true, nil
			case walkLeasedKeep:
				if kept == nil {
					kept = lease
				}
			}
		}
		if pass > 0 {
			break
		}
		count, err := d.rdb.HGet(ctx, d.keys.runnerLeaseCount, runnerID).Int()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, false, fmt.Errorf("read runner lease count %q: %w", runnerID, err)
		}
		if live >= count {
			break
		}
	}
	if kept != nil {
		return kept, true, nil
	}
	return nil, false, nil
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
// claim reserves one queued assignment for the runner, removing it from the
// target queue it was found on. The target is where the entry was read from,
// not necessarily the legacy queue: an assignment a lane holds must leave that
// lane, and the transition's LREM is what makes the claim exclusive.
func (d *RedisRunnerDirectory) claim(ctx context.Context, target, runnerID, sessionID, assignmentID, expectedData string, claimID ClaimID) (string, error) {
	status, err := d.evalStatus(ctx, redisClaimAssignmentLua, []string{
		target,
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
		d.keys.handoffClaimIndexKey(runnerID),
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
		d.keys.handoffClaimIndexKey(runnerID),
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
	status, err := d.evalStatus(ctx, redisReleaseClaimLua, d.appendLaneRequeueKeys([]string{
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
	}), string(claimID), string(reason), assignmentID, d.keys.handoffClaimIndexPrefix(),
		d.laneRequeueCandidateCount(), d.laneRequeueModeArg())
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
	status, err := d.evalStatus(ctx, redisReleaseLeasedLua, d.appendLaneRequeueKeys([]string{
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
	}), req.RunnerID, string(req.AssignmentID), string(req.LeaseID), string(req.LeaseToken),
		boolRedisArg(req.RemoveSeen), assignmentID,
		d.laneRequeueCandidateCount(), d.laneRequeueModeArg())
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
//
// A key that names its task (NodeName set) must resolve to that task. Map
// batches share their parent's lease identity, so when the indexes name a
// sibling, or nothing, the runner's own leased assignments are searched for the
// one holding this token under this task.
//
// A key that names no task (a renewal) falls back to any live assignment of
// this runner under the identity; see lookupRenewalLease.
func (d *RedisRunnerDirectory) LookupLease(ctx context.Context, runnerID, sessionID string, key LeaseLookupKey) (*engine.TaskLease, bool, error) {
	assignmentID, ok, err := d.resolveLeaseAssignmentID(ctx, key)
	if err != nil {
		return nil, false, err
	}
	var lease *engine.TaskLease
	if ok {
		if lease, ok, err = d.lookupLeaseAt(ctx, runnerID, sessionID, assignmentID); err != nil {
			return nil, false, err
		}
	}
	// The indexes resolve on one identity field — by-token on the token, by-id
	// on the lease id — and the raw-assignmentID fallback resolves on nothing,
	// so an index hit can hand back a lease the key's identity does not name;
	// the reachable shape is a matching lease id under the wrong token. The
	// contract above calls that a mismatch, not a match, and memory enforces it
	// on every path (matchesReleasedLease), so the resolved lease answers for
	// the key here too. A key that names no identity has nothing to answer to
	// and keeps the old behavior.
	if ok && (key.LeaseToken != "" || key.LeaseID != "") && !leaseIdentityMatches(lease.LeaseToken, lease.LeaseID, key) {
		lease = nil
		ok = false
	}
	if key.NodeName == "" {
		if ok {
			return lease, true, nil
		}
		// A renewal names no task; the index alone cannot resolve it. See
		// lookupRenewalLease.
		return d.lookupRenewalLease(ctx, runnerID, sessionID, key, assignmentID)
	}
	if ok && key.namesTask(&lease.Task) {
		return lease, true, nil
	}
	if key.LeaseToken == "" {
		return nil, false, nil
	}
	return d.walkLeasedAssignments(ctx, runnerID, func(candidate string) (*engine.TaskLease, leasedAssignmentVerdict, error) {
		if candidate == assignmentID {
			return nil, walkLeasedSkip, nil
		}
		token, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseToken, candidate).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, walkLeasedSkip, fmt.Errorf("lookup sibling lease token %q: %w", candidate, err)
		}
		if token != string(key.LeaseToken) {
			return nil, walkLeasedSkip, nil
		}
		sibling, found, err := d.lookupLeaseAt(ctx, runnerID, sessionID, candidate)
		if err != nil {
			return nil, walkLeasedSkip, err
		}
		if !found || !key.namesTask(&sibling.Task) {
			return nil, walkLeasedSkip, nil
		}
		return sibling, walkLeasedTake, nil
	})
}

// lookupRenewalLease resolves a renewal key -- one that names no task -- to any
// live assignment this runner holds under the key's lease identity.
//
// A map node and its batches share one lease identity while the by-token /
// by-id indexes hold one assignment per identity, so a batch renewal misses the
// index whenever the assignment it names belongs to another runner, was
// released, or lost its metadata: the renewal used to be refused outright and
// the runner cancelled a healthy batch. Falling back to the runner's own leased
// set keeps the renewal resolvable exactly as the report path does (see
// LookupLease). The engine renews the same target -- the parent node -- through
// any of them, so which live sibling resolves is immaterial to the lease.
//
// A batch assignment is preferred over its parent map node. The resolved lease
// feeds the renewal deadline backstop, and a batch carries no ExecutionDeadline
// while its parent does; resolving a batch's renewal to the parent would let
// the parent's absolute deadline fire a timeout commit against an expansion
// that is still making progress. The parent's deadline remains reachable
// through its own lease while the map node itself is being executed.
func (d *RedisRunnerDirectory) lookupRenewalLease(ctx context.Context, runnerID, sessionID string, key LeaseLookupKey, resolvedID string) (*engine.TaskLease, bool, error) {
	if key.LeaseToken == "" && key.LeaseID == "" {
		return nil, false, nil
	}
	return d.walkLeasedAssignments(ctx, runnerID, func(candidate string) (*engine.TaskLease, leasedAssignmentVerdict, error) {
		if candidate == resolvedID {
			// The index already named this one and LookupLease refused it —
			// wrong runner session, released, expired metadata, or a key
			// identity it does not carry; it cannot resolve now either.
			return nil, walkLeasedSkip, nil
		}
		token, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseToken, candidate).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, walkLeasedSkip, fmt.Errorf("lookup renewal lease token %q: %w", candidate, err)
		}
		leaseID, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseID, candidate).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, walkLeasedSkip, fmt.Errorf("lookup renewal lease id %q: %w", candidate, err)
		}
		if !leaseIdentityMatches(engine.LeaseToken(token), engine.LeaseID(leaseID), key) {
			return nil, walkLeasedSkip, nil
		}
		lease, found, err := d.lookupLeaseAt(ctx, runnerID, sessionID, candidate)
		if err != nil {
			return nil, walkLeasedSkip, err
		}
		if !found {
			return nil, walkLeasedSkip, nil
		}
		if lease.SubgraphPayload != nil {
			return lease, walkLeasedTake, nil
		}
		return lease, walkLeasedKeep, nil
	})
}

// lookupLeaseAt returns the finalized lease stored under assignmentID when it
// is still leased to (runnerID, sessionID).
func (d *RedisRunnerDirectory) lookupLeaseAt(ctx context.Context, runnerID, sessionID, assignmentID string) (*engine.TaskLease, bool, error) {
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

// RefreshLeaseMeta re-arms the metadata expiry of every live assignment the
// runner holds under the key's lease identity.
//
// FinalizeClaim arms each expiry once, for the lease's own TTL plus one
// claim-recovery margin — about 90s for a default 60s lease — and nothing used
// to extend it. A node that legitimately outlives that window then loses the
// metadata its own renewals and reports are resolved through: LookupLease
// reads redis.Nil, renew is refused with "lease not found", the runner cancels
// its handler, and the assignment is stranded in 'leased' where no reclaim path
// can see it. The renewal path calls this after each successful engine-side
// extension so the directory expiry tracks the lease the engine actually
// granted.
//
// A renewal key resolves to one assignment while a map node and its batches
// share one identity, so refreshing only that assignment would let every
// sibling's expiry drift to its finalized deadline while the engine lease keeps
// being extended: the sibling's own next renewal then loses the metadata it is
// resolved through and the batch is cancelled. The runner renews the same
// target through any sibling, so one successful renewal must refresh them all.
//
// Absent metadata reports "expired" rather than an error: the lease is already
// unrecoverable by then, and the renewal that led here has already succeeded,
// so failing it would only widen the damage.
func (d *RedisRunnerDirectory) RefreshLeaseMeta(ctx context.Context, runnerID, sessionID string, key LeaseLookupKey, live time.Duration) error {
	if key.LeaseToken == "" && key.LeaseID == "" {
		return nil
	}
	ttl := strconv.FormatInt(d.assignmentLeaseMetaTTLMillisFor(live), 10)
	_, _, err := d.walkLeasedAssignments(ctx, runnerID, func(candidate string) (*engine.TaskLease, leasedAssignmentVerdict, error) {
		token, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseToken, candidate).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, walkLeasedSkip, fmt.Errorf("refresh lease metadata token %q: %w", candidate, err)
		}
		leaseID, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseID, candidate).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, walkLeasedSkip, fmt.Errorf("refresh lease metadata id %q: %w", candidate, err)
		}
		if !leaseIdentityMatches(engine.LeaseToken(token), engine.LeaseID(leaseID), key) {
			return nil, walkLeasedSkip, nil
		}
		// The Lua re-checks state, runner and session, so a candidate that a
		// release or re-register overtook is a no-op rather than an error.
		status, err := d.evalStatus(ctx, redisRefreshLeaseMetaLua, []string{
			d.keys.assignmentState,
			d.keys.assignmentRunner,
			d.keys.assignmentSession,
			d.keys.assignmentLeaseMetaKey(candidate),
		}, candidate, runnerID, sessionID, ttl)
		if err != nil {
			return nil, walkLeasedSkip, fmt.Errorf("refresh redis lease metadata: %w", err)
		}
		switch status {
		case "refreshed", "noop", "expired":
			return nil, walkLeasedSkip, nil
		default:
			return nil, walkLeasedSkip, fmt.Errorf("refresh redis lease metadata: unexpected result %q", status)
		}
	})
	return err
}

// resolveLeaseAssignmentID resolves a finalized assignment ID from a lease
// identity, mirroring ReleaseLeased's token > leaseID > assignmentID precedence
// but read-only. ok=false means no index entry matches.
//
// An explicit AssignmentID whose stored identity matches wins over the
// indexes: a map node and all of its batches share one lease identity, so the
// single-value indexes name only whichever of them finalized last.
func (d *RedisRunnerDirectory) resolveLeaseAssignmentID(ctx context.Context, key LeaseLookupKey) (string, bool, error) {
	if key.AssignmentID != "" && (key.LeaseToken != "" || key.LeaseID != "") {
		field, want := d.keys.assignmentLeaseToken, string(key.LeaseToken)
		if want == "" {
			field, want = d.keys.assignmentLeaseID, string(key.LeaseID)
		}
		stored, err := d.rdb.HGet(ctx, field, string(key.AssignmentID)).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return "", false, fmt.Errorf("resolve explicit lease assignment: %w", err)
		}
		if err == nil && stored == want {
			return string(key.AssignmentID), true, nil
		}
	}
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
	status, err := d.evalStatus(ctx, redisReleaseExpiredLeaseLua, d.appendLaneRequeueKeys([]string{
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
	}), string(req.AssignmentID), string(req.LeaseID), string(req.LeaseToken),
		d.laneRequeueCandidateCount(), d.laneRequeueModeArg())
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
	status, err := d.evalStatus(ctx, redisClearAssignmentLua, d.appendLaneRequeueKeys([]string{
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
	}), string(assignmentID), d.laneRequeueCandidateCount(), d.laneRequeueModeArg())
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
//
// It reads the registration fields and the scalar control projection in one
// pipeline. The debt-bearing drain projection is not attached here: it is a
// management view with its own accessor, and folding it in made a single-runner
// read cost one full pass over the fleet-wide handoff and deactivation ledgers.
func (d *RedisRunnerDirectory) Runner(ctx context.Context, runnerID string) (RunnerSnapshot, bool) {
	pipe := d.rdb.Pipeline()
	sessionCmd := pipe.HGet(ctx, d.keys.runnerSession, runnerID)
	capacityCmd := pipe.HGet(ctx, d.keys.runnerCapacity, runnerID)
	inFlightCmd := pipe.HGet(ctx, d.keys.runnerInflight, runnerID)
	capabilitiesCmd := pipe.HGet(ctx, d.keys.runnerCapabilities, runnerID)
	namespacesCmd := pipe.HGet(ctx, d.keys.runnerNamespaces, runnerID)
	heartbeatCmd := pipe.HGet(ctx, d.keys.runnerHeartbeat, runnerID)
	labelsCmd := pipe.HGet(ctx, d.keys.runnerLabels, runnerID)
	desiredCmd := pipe.HGet(ctx, d.keys.runnerControlDesired, runnerID)
	generationCmd := pipe.HGet(ctx, d.keys.runnerControlGeneration, runnerID)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return RunnerSnapshot{}, false
	}

	snapshot, ok := decodeRunnerSnapshot(runnerID, runnerRawFields{
		session:      sessionCmd.Val(),
		capacity:     capacityCmd.Val(),
		inflight:     inFlightCmd.Val(),
		capabilities: capabilitiesCmd.Val(),
		namespaces:   namespacesCmd.Val(),
		heartbeat:    heartbeatCmd.Val(),
		labels:       labelsCmd.Val(),
	})
	if !ok {
		return RunnerSnapshot{}, false
	}
	// Best-effort, matching the previous behavior: a malformed control value
	// leaves Control unset rather than failing the whole registration read.
	if state, err := decodeRunnerControlState(desiredCmd.Val(), generationCmd.Val()); err == nil {
		snapshot.Control = runnerControlStateSnapshot(state)
	}
	return snapshot, true
}

// ListLiveRunners returns a snapshot of every registered runner using a
// pipelined bulk fetch (HKeys plus one pipeline of scalar and ledger reads) to
// avoid O(n) serial Redis calls.
//
// The control projection is built from that same pipeline. Reading it per
// runner instead made each entry a private pipeline of its own, so the list
// cost one round-trip and one full pass over the fleet-wide debt ledgers per
// runner -- O(runners) round-trips and O(runners x fleet debt) bytes for data
// the ledgers already hold once.
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
	desiredCmd := pipe.HMGet(ctx, d.keys.runnerControlDesired, runnerIDs...)
	generationCmd := pipe.HMGet(ctx, d.keys.runnerControlGeneration, runnerIDs...)
	requestedAtCmd := pipe.HMGet(ctx, d.keys.runnerControlRequestedAt, runnerIDs...)
	reasonCmd := pipe.HMGet(ctx, d.keys.runnerControlReason, runnerIDs...)
	drainDeadlineCmd := pipe.HMGet(ctx, d.keys.runnerControlDrainDeadline, runnerIDs...)
	claimsCmd := pipe.HMGet(ctx, d.keys.runnerClaimCount, runnerIDs...)
	leasesCmd := pipe.HMGet(ctx, d.keys.runnerLeaseCount, runnerIDs...)
	drainObservationCmd := pipe.HMGet(ctx, d.keys.runnerDrainObservation, runnerIDs...)
	ledgerCommands := d.queueRedisControlLedger(ctx, pipe)
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
	desireds := desiredCmd.Val()
	generations := generationCmd.Val()
	requestedAts := requestedAtCmd.Val()
	reasons := reasonCmd.Val()
	drainDeadlines := drainDeadlineCmd.Val()
	claimsList := claimsCmd.Val()
	leasesList := leasesCmd.Val()
	drainObservations := drainObservationCmd.Val()
	ledger := ledgerCommands.ledger()

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
		snap, ok := decodeRunnerSnapshot(runnerIDs[i], raw)
		if !ok {
			continue
		}
		control, _, controlErr := decodeRunnerControlProjection(
			hmgetString(desireds, i), hmgetString(generations, i), hmgetString(requestedAts, i),
			hmgetString(drainDeadlines, i), hmgetString(reasons, i), raw.session,
			unmarshalRedisRunnerDrainObservation(hmgetString(drainObservations, i)),
			ledger.handoffDebtStats(snap.RunnerID, parseRedisInt(hmgetString(claimsList, i)), parseRedisInt(hmgetString(leasesList, i))),
			ledger.pendingActivationCleanup(snap.RunnerID),
			d.clockNow(), d.runnerDrainObservationFreshness(),
		)
		if controlErr == nil {
			snap.Control = &control
		}
		out = append(out, snap)
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
	// handoffClaimIDs is this runner's slice of the per-runner handoff index,
	// read in the same pipeline and bounded by this runner's own debt. The fleet
	// ledger it used to be filtered out of is keyed by claim across every runner,
	// so reading that was O(fleet debt) on a path that only ever wants these few.
	handoffClaimIDs []string
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
	handoffIndexCmd := pipe.SMembers(ctx, d.keys.handoffClaimIndexKey(runnerID))
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
		sessionID:       sessionID,
		capabilities:    capabilities,
		policy:          policy,
		namespaces:      namespaces,
		labels:          labels,
		hasHeadroom:     headroom > 0,
		handoffClaimIDs: handoffIndexCmd.Val(),
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
//
// Its reach is exactly the claims still in the pre-claimation index — a claim
// that reached 'leased' was removed from that index when it was finalized, so
// this can never observe one. 'leased' assignments that stranded after their
// lease metadata TTL'd out are the stranded-lease reaper's job, not this scan's.
func (d *RedisRunnerDirectory) ReclaimExpiredClaims(ctx context.Context) error {
	reclaimed, err := d.rdb.Eval(ctx, redisRecoverExpiredClaimsLua, d.appendLaneRequeueKeys([]string{
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
	}), strconv.FormatInt(time.Now().UTC().UnixMilli(), 10), d.keys.handoffClaimIndexPrefix(),
		d.laneRequeueCandidateCount(), d.laneRequeueModeArg()).Int64()
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

// redisLaneRequeueLua is the shared lane-aware requeue helper spliced into the
// head of every Lua script that can return an assignment to the queue or take
// it out of one. The removal-only scripts (the dead-queued reap and the clear
// transition) use laneDrop and nothing else. It mirrors enqueueQueueKeys on the
// Go side, and relies on its callers appending, in this order, the candidate
// queue keys (every configured lane, then the legacy queue) and the lane marker
// hash to KEYS, and two trailing ARGV values: the candidate count and the lane
// write mode.
//
// The requeue contract per write mode:
//   - legacy_only: the entry goes back to the legacy queue and the marker is
//     removed — an old reader that only scans the legacy queue must see it, so
//     a marker left over from a previous dual window must not divert it.
//   - dual: the entry goes back to the lane its marker names (legacy when the
//     marker is missing or names a key this configuration no longer serves),
//     and additionally to the legacy queue, deduplicated when the two agree.
//   - lane_only: the entry goes back to the marker's lane, legacy as fallback.
//
// Every mode clears the whole candidate set first: a stale copy on any lane
// would only ever be skipped, never removed, by the claim walk, and nothing
// else on the completion or reclaim paths collects it.
const redisLaneRequeueLua = `
local laneCandidates = tonumber(ARGV[#ARGV - 1])
local laneMode = ARGV[#ARGV]
local laneBase = #KEYS - laneCandidates
local laneLegacy = KEYS[#KEYS - 1]
local laneMarker = KEYS[#KEYS]
local function laneClear(id)
  local i
  for i = laneBase, #KEYS - 1 do
    redis.call('LREM', KEYS[i], 0, id)
  end
end
local function lanePush(id)
  local target = laneLegacy
  if laneMode ~= 'legacy_only' then
    local marked = redis.call('HGET', laneMarker, id)
    if marked and marked ~= '' then
      local i
      for i = laneBase, #KEYS - 1 do
        if KEYS[i] == marked then target = marked break end
      end
    end
  end
  redis.call('LPUSH', target, id)
  if laneMode == 'dual' and target ~= laneLegacy then
    redis.call('LPUSH', laneLegacy, id)
  end
  if laneMode ~= 'legacy_only' then
    redis.call('HSET', laneMarker, id, target)
  else
    redis.call('HDEL', laneMarker, id)
  end
end
local function laneDrop(id)
  laneClear(id)
  redis.call('HDEL', laneMarker, id)
end
`

const redisRegisterRunnerLua = redisLaneRequeueLua + `
local oldRunner = ARGV[1]
local newSession = ARGV[2]
-- Instance guard: mirrors instanceConflict in memory_runner_directory.go and
-- must run before any mutation below. KEYS[47] is runnerInstanceUID.
local newUID = ARGV[10] or ''
local liveMillis = tonumber(ARGV[11] or '0') or 0
if newUID ~= '' and liveMillis > 0 then
  local currentUID = redis.call('HGET', KEYS[47], oldRunner)
  local lastBeat = tonumber(redis.call('HGET', KEYS[17], oldRunner) or '')
  if currentUID and currentUID ~= '' and currentUID ~= newUID and lastBeat and (tonumber(ARGV[7]) - lastBeat) < liveMillis then
    return 'runner_id_conflict'
  end
end
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
      redis.call('SADD', KEYS[46], claimID)
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
        laneClear(assignmentID)
        if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then lanePush(assignmentID) end
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
if newUID ~= '' then
  redis.call('HSET', KEYS[47], oldRunner, newUID)
else
  redis.call('HDEL', KEYS[47], oldRunner)
end
-- KEYS[48] is runnerDescriptors. Replaced in the same transition as the
-- session so a replaced session can never keep serving its old descriptors;
-- an empty ARGV[12] (an old runner, or nothing accepted) clears them.
local descriptors = ARGV[12] or ''
if descriptors ~= '' then
  redis.call('HSET', KEYS[48], oldRunner, descriptors)
else
  redis.call('HDEL', KEYS[48], oldRunner)
end
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
// Queue keys are dynamic once lanes are configured: KEYS[1..ARGV[3]] are the
// LREM candidate set (every configured lane queue, then the legacy queue),
// KEYS[ARGV[3]+1..ARGV[3]+ARGV[4]] are the RPUSH target set, and the fixed
// bookkeeping keys follow at KEYS[ARGV[3]+ARGV[4]+1..+10]. A re-enqueue must
// LREM every candidate — a stale copy can sit on any lane the entry was placed
// on before, and a stale copy that is merely skipped by the claim walk would
// never be removed otherwise — while RPUSH names only the queues the write
// mode selects. ARGV[5] is the lane marker value; empty means "do not write a
// marker" (legacy-only mode and deployments without lanes), which is
// equivalent to a missing marker because the requeue transitions read a
// missing marker as legacy.
const redisEnqueueAssignmentLua = `
local candidates = tonumber(ARGV[3])
local targets = tonumber(ARGV[4])
local base = candidates + targets
local seen = redis.call('SADD', KEYS[base+1], ARGV[1]) == 0
if seen then
  local state = redis.call('HGET', KEYS[base+3], ARGV[1])
  if state and state ~= 'released' then
    return 'duplicate'
  end
end
redis.call('HSET', KEYS[base+2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[base+3], ARGV[1], 'queued')
redis.call('HDEL', KEYS[base+4], ARGV[1])
redis.call('HDEL', KEYS[base+5], ARGV[1])
redis.call('HDEL', KEYS[base+6], ARGV[1])
redis.call('HDEL', KEYS[base+7], ARGV[1])
redis.call('HDEL', KEYS[base+8], ARGV[1])
redis.call('DEL', KEYS[base+9])
local i
for i = 1, candidates do
  redis.call('LREM', KEYS[i], 0, ARGV[1])
end
for i = candidates + 1, candidates + targets do
  redis.call('RPUSH', KEYS[i], ARGV[1])
end
if ARGV[5] ~= '' then
  redis.call('HSET', KEYS[base+10], ARGV[1], ARGV[5])
else
  redis.call('HDEL', KEYS[base+10], ARGV[1])
end
return 'enqueued'
`

const redisRecoverExpiredClaimsLua = redisLaneRequeueLua + `
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
    if runnerID then redis.call('SADD', ARGV[2] .. runnerID, claimID) end
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
  -- 'claimed' is the only recoverable state on purpose; do not widen this to
  -- 'leased'. It would be dead code: a claim that reaches 'leased' has already
  -- had claim:assignment, claim:runner, claim:session and claim:expiry removed
  -- by redisFinalizeClaimLua in the same atomic step that writes 'leased', and
  -- this loop is driven by ZRANGEBYSCORE claim:expiry and HKEYS claim:assignment
  -- (KEYS[11], KEYS[7]) — both empty for a leased assignment, so reclaim() is
  -- never invoked for one. A stranded 'leased' assignment is reclaimed by the
  -- control plane's stranded-lease reaper instead; see StrandedLeaseReaper.
  if assignmentID and redis.call('HGET', KEYS[3], assignmentID) == 'claimed' and redis.call('HGET', KEYS[4], assignmentID) == claimID then
    redis.call('HSET', KEYS[3], assignmentID, 'queued')
    redis.call('HDEL', KEYS[4], assignmentID)
    redis.call('HDEL', KEYS[5], assignmentID)
    redis.call('HDEL', KEYS[6], assignmentID)
    laneClear(assignmentID)
    if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then lanePush(assignmentID) end
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
-- The reservation and the per-runner index that finds it again are written in
-- the same step. Recovery reads that index rather than scanning the fleet, so a
-- lost index write here would leave debt its owner never sees.
redis.call('SADD', KEYS[27], ARGV[5])
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
redis.call('SADD', KEYS[28], claimID)
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

const redisSettleHandoffLua = redisLaneRequeueLua + `
local claimID = ARGV[1]
local assignmentID = redis.call('HGET', KEYS[8], claimID)
local runnerID = redis.call('HGET', KEYS[9], claimID)
local state = redis.call('HGET', KEYS[13], claimID)
if not assignmentID or not state then return 'noop' end
if assignmentID ~= ARGV[3] then return 'noop' end
if state ~= 'lease_may_exist' and state ~= 'lease_created' then return 'unresolved' end
local assignmentState = redis.call('HGET', KEYS[4], assignmentID)
-- Ownership is proven by either direction of the claim index. assignmentClaim is
-- the reverse index and is written and cleared in the same steps as
-- assignmentState, so a record that lost one lost the other: a claim that
-- predates handoff-ledger recovery, or whose writer died mid-transition, shows
-- up as a missing state with claimsAssignment still naming the assignment.
-- claimsAssignment is fenced against assignmentID by the caller's own read
-- above, so it is an independent witness, not a restatement of the same read.
-- A state that is present must still be 'claimed': 'leased' is written through
-- this field, so a live lease would be visible here and must keep blocking.
local owned = redis.call('HGET', KEYS[5], assignmentID) == claimID
if not owned then owned = redis.call('HGET', KEYS[8], claimID) == assignmentID end
if not owned then return 'unresolved' end
if assignmentState and assignmentState ~= 'claimed' then return 'unresolved' end
if ARGV[2] == 'requeue' then
  redis.call('HSET', KEYS[4], assignmentID, 'queued')
  redis.call('HDEL', KEYS[5], assignmentID)
  redis.call('HDEL', KEYS[6], assignmentID)
  redis.call('HDEL', KEYS[7], assignmentID)
  laneClear(assignmentID)
  if redis.call('HEXISTS', KEYS[3], assignmentID) == 1 then lanePush(assignmentID) end
elseif ARGV[2] == 'drop' then
  laneDrop(assignmentID)
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

const redisReleaseClaimLua = redisLaneRequeueLua + `
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
  -- runnerID is read from claimsRunner, so the index key cannot be declared by
  -- the caller: build it from the shared prefix, which keeps the shared Cluster
  -- hash tag and therefore the same slot.
  if runnerID then redis.call('SADD', ARGV[4] .. runnerID, claimID) end
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
    laneClear(assignmentID)
    if redis.call('HEXISTS', KEYS[3], assignmentID) == 1 then lanePush(assignmentID) end
  elseif ARGV[2] == 'drop' then
    laneDrop(assignmentID)
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

const redisReleaseLeasedLua = redisLaneRequeueLua + `
if not redis.call('HGET', KEYS[1], ARGV[1]) then return 'not_found' end
local assignmentID = nil
if ARGV[2] ~= '' and ARGV[4] ~= '' and redis.call('HGET', KEYS[7], ARGV[2]) == ARGV[4] then assignmentID = ARGV[2] end
if not assignmentID and ARGV[2] ~= '' and ARGV[4] == '' and ARGV[3] ~= '' and redis.call('HGET', KEYS[6], ARGV[2]) == ARGV[3] then assignmentID = ARGV[2] end
if not assignmentID and ARGV[4] ~= '' then assignmentID = redis.call('HGET', KEYS[10], ARGV[4]) end
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
  -- Terminal. The caller sets this flag only for the Accepted,
  -- DuplicateTerminal and ExecutionInactive outcomes (undeliverable_lease.go),
  -- which all mean the work is settled and this assignment is never re-queued.
  -- The record itself is deleted in this branch, so every copy of it — on any
  -- lane and on the legacy queue — and its lane marker go with it: a copy left
  -- behind would only ever be skipped, never removed, by the claim walk, and
  -- nothing else on the completion path would collect it.
  --
  -- The released branch below (RemoveSeen=false) must NOT clear any of these.
  -- That path is the stale-token outcome: the plane still owns the entry, and
  -- the caller re-enqueues it through EnqueueAssignment, which is what clears
  -- the stale copies and rewrites the marker under the current write mode.
  -- Adding a removal there would race that re-enqueue and could drop an entry
  -- that was just placed back on a queue.
  laneDrop(assignmentID)
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

// Like the reap transition, this one ends in laneDrop: the record is being
// erased, so every queue copy of it and its lane marker go with it. The clear
// is reached only where the caller has decided the assignment is terminally
// done (see ClearAssignment), which is the same direction the marker's other
// removal points take.
const redisClearAssignmentLua = redisLaneRequeueLua + `
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
laneDrop(assignmentID)
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

const redisReleaseExpiredLeaseLua = redisLaneRequeueLua + `
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
-- This is a terminal cleanup: the record is gone, so every lane candidate copy
-- and the lane marker go with it. A leftover copy would be skipped forever by
-- the claim walk, and a leftover marker with it.
laneDrop(assignmentID)
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

// Deregister implements RunnerDeregisterer.
func (d *RedisRunnerDirectory) Deregister(ctx context.Context, runnerID, sessionID string) error {
	if runnerID == "" {
		return ErrRunnerIDRequired
	}
	status, err := d.evalStatus(ctx, redisDeregisterRunnerLua, []string{
		d.keys.runnerSession,
		d.keys.runnerHeartbeat,
		d.keys.runnerInstanceUID,
	}, runnerID, sessionID)
	if err != nil {
		return fmt.Errorf("deregister redis runner: %w", err)
	}
	return runnerSessionStatusError(status)
}

const redisDeregisterRunnerLua = `
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then
  return 'not_found'
end
if ARGV[2] == '' or current ~= ARGV[2] then
  return 'stale'
end
redis.call('HSET', KEYS[2], ARGV[1], '0')
redis.call('HDEL', KEYS[3], ARGV[1])
return 'ok'
`

// RemoveRunner implements RunnerRemover. One Lua transition checks the same
// server-side debt that gates drain completion, then deletes every scalar
// runner field and both runner-derived indexes atomically.
func (d *RedisRunnerDirectory) RemoveRunner(ctx context.Context, runnerID string) error {
	if runnerID == "" {
		return ErrRunnerIDRequired
	}
	status, err := d.evalStatus(ctx, redisRemoveRunnerLua, []string{
		d.keys.runnerSession,
		d.keys.runnerCapacity,
		d.keys.runnerInflight,
		d.keys.runnerCapabilities,
		d.keys.runnerLabels,
		d.keys.runnerPolicy,
		d.keys.runnerNamespaces,
		d.keys.runnerHeartbeat,
		d.keys.runnerClaimCount,
		d.keys.runnerLeaseCount,
		d.keys.runnerControlDesired,
		d.keys.runnerControlGeneration,
		d.keys.runnerControlRequestedAt,
		d.keys.runnerControlActor,
		d.keys.runnerControlReason,
		d.keys.runnerControlDrainDeadline,
		d.keys.runnerActivationInventory,
		d.keys.runnerDrainObservation,
		d.keys.runnerInstanceUID,
		d.keys.claimsRunner,
		d.keys.assignmentState,
		d.keys.assignmentRunner,
		d.keys.handoffRunner,
		d.keys.deactivationObligationRunner,
		d.keys.deactivationObligationState,
		d.keys.runnerLeasedAssignmentsKey(runnerID),
		d.keys.handoffClaimIndexKey(runnerID),
		d.keys.runnerDescriptors,
	}, runnerID)
	if err != nil {
		return fmt.Errorf("remove redis runner: %w", err)
	}
	switch status {
	case "removed":
		d.clearClaimCursors(runnerID)
		return nil
	case "outstanding":
		return ErrRunnerHasOutstandingWork
	default:
		return fmt.Errorf("remove redis runner: unexpected result %q", status)
	}
}

// KEYS 1..19 are all hashes whose field is runnerID. KEYS 20..25 are
// authoritative debt ledgers; counts alone are not trusted because a damaged
// or pre-index record must fail closed. KEYS 26..27 are runner-derived sets.
// KEYS 28 is runnerDescriptors, another runnerID-field hash, appended rather
// than folded into 1..19 so the indexes above stay stable.
const redisRemoveRunnerLua = `
local runnerID = ARGV[1]
if tonumber(redis.call('HGET', KEYS[9], runnerID) or '0') ~= 0 then return 'outstanding' end
if tonumber(redis.call('HGET', KEYS[10], runnerID) or '0') ~= 0 then return 'outstanding' end

local claimOwners = redis.call('HGETALL', KEYS[20])
for index = 2, #claimOwners, 2 do
  if claimOwners[index] == runnerID then return 'outstanding' end
end

local assignmentOwners = redis.call('HGETALL', KEYS[22])
for index = 1, #assignmentOwners, 2 do
  if assignmentOwners[index + 1] == runnerID and redis.call('HGET', KEYS[21], assignmentOwners[index]) == 'leased' then
    return 'outstanding'
  end
end

local handoffOwners = redis.call('HGETALL', KEYS[23])
for index = 2, #handoffOwners, 2 do
  if handoffOwners[index] == runnerID then return 'outstanding' end
end

local activationOwners = redis.call('HGETALL', KEYS[24])
for index = 1, #activationOwners, 2 do
  if activationOwners[index + 1] == runnerID then
    local state = redis.call('HGET', KEYS[25], activationOwners[index])
    if state == 'pending_fence' or state == 'ready' then return 'outstanding' end
  end
end

for index = 1, 19 do redis.call('HDEL', KEYS[index], runnerID) end
redis.call('HDEL', KEYS[28], runnerID)
redis.call('DEL', KEYS[26])
redis.call('DEL', KEYS[27])
return 'removed'
`
