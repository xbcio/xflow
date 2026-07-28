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
	defaultRedisRunnerDirectoryClaimTTL = 30 * time.Second
	redisRunnerDirectoryKeyPrefix       = "xflow:runner-directory:{control}"

	redisAssignmentQueued   = "queued"
	redisAssignmentClaimed  = "claimed"
	redisAssignmentLeased   = "leased"
	redisAssignmentReleased = "released"
)

var errClaimNotActive = errors.New("runner claim is no longer active")

// RedisRunnerDirectoryOption configures a RedisRunnerDirectory.
type RedisRunnerDirectoryOption func(*redisRunnerDirectoryConfig)

type redisRunnerDirectoryConfig struct {
	claimTTL time.Duration
	observer RunnerClaimObserver
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
	rdb      redis.Cmdable
	claimTTL time.Duration
	observer RunnerClaimObserver
	keys     redisRunnerDirectoryKeys
}

var _ RunnerDirectory = (*RedisRunnerDirectory)(nil)
var _ ClaimReclaimer = (*RedisRunnerDirectory)(nil)
var _ ExpiredLeaseReleaser = (*RedisRunnerDirectory)(nil)

// NewRedisRunnerDirectory constructs a Redis-backed RunnerDirectory. Every
// key used by its Lua transitions includes the same Redis Cluster hash tag.
func NewRedisRunnerDirectory(rdb redis.Cmdable, opts ...RedisRunnerDirectoryOption) *RedisRunnerDirectory {
	cfg := redisRunnerDirectoryConfig{claimTTL: defaultRedisRunnerDirectoryClaimTTL}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &RedisRunnerDirectory{
		rdb:      rdb,
		claimTTL: cfg.claimTTL,
		observer: cfg.observer,
		keys:     newRedisRunnerDirectoryKeys(redisRunnerDirectoryKeyPrefix),
	}
}

// Register installs a fresh fenced session, returns active unfinalized claims
// for the prior session to the durable queue, and transfers finalized leases
// to the replacement session for reconnect replay.
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

	now := req.Now
	if now.IsZero() {
		now = time.Now()
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
	}, req.RunnerID, session.SessionID, strconv.Itoa(req.Capacity), string(capabilities), string(policy), string(namespaces), strconv.FormatInt(now.UnixMilli(), 10), string(labels))
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
	status, err := d.evalStatus(ctx, redisHeartbeatLua, []string{
		d.keys.runnerSession,
		d.keys.runnerCapacity,
		d.keys.runnerInflight,
		d.keys.runnerHeartbeat,
	}, req.RunnerID, req.SessionID, strconv.Itoa(req.Capacity), strconv.Itoa(req.InFlight), heartbeatMillis)
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
// session, then reserves the first compatible queued assignment with available
// runner headroom. Replaying the same lease is intentional at-least-once
// delivery: a control-plane crash after finalization or response loss cannot
// make the assignment unreachable.
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

	if replay, ok, err := d.replayLease(ctx, req.RunnerID, req.SessionID); err != nil {
		return Claim{}, false, err
	} else if ok {
		return replay, true, nil
	}

	capabilities := runner.capabilities
	capabilitiesChanged := req.Capabilities != nil
	if capabilitiesChanged {
		capabilities = cloneCapabilities(req.Capabilities)
	}
	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		return Claim{}, false, fmt.Errorf("marshal runner capabilities: %w", err)
	}

	labelsChanged := req.Labels != nil
	effectiveLabels := runner.labels
	if labelsChanged {
		effectiveLabels = cloneLabels(req.Labels)
		labelsJSON, err := json.Marshal(effectiveLabels)
		if err != nil {
			return Claim{}, false, fmt.Errorf("marshal runner labels: %w", err)
		}
		if err := d.rdb.HSet(ctx, d.keys.runnerLabels, req.RunnerID, string(labelsJSON)).Err(); err != nil {
			return Claim{}, false, fmt.Errorf("refresh runner labels: %w", err)
		}
	}

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
		if rs := assignment.Routing.RunnerSelector; rs != nil && !MatchLabels(effectiveLabels, rs.MatchLabels) {
			continue
		}

		claimID := ClaimID(uuid.NewString())
		status, err := d.claim(ctx, req, capabilitiesChanged, string(capabilitiesJSON), assignmentID, raw, claimID)
		if err != nil {
			return Claim{}, false, err
		}
		switch status {
		case "claimed":
			return Claim{ClaimID: claimID, Assignment: assignment}, true, nil
		case "retry":
			continue
		case "none":
			return Claim{}, false, nil
		case "not_found", "stale":
			return Claim{}, false, runnerSessionStatusError(status)
		default:
			return Claim{}, false, fmt.Errorf("claim redis assignment: unexpected result %q", status)
		}
	}

	status, err := d.claim(ctx, req, capabilitiesChanged, string(capabilitiesJSON), "", "", "")
	if err != nil {
		return Claim{}, false, err
	}
	switch status {
	case "none":
		return Claim{}, false, nil
	case "not_found", "stale":
		return Claim{}, false, runnerSessionStatusError(status)
	default:
		return Claim{}, false, fmt.Errorf("claim redis assignment: unexpected result %q", status)
	}
}

func (d *RedisRunnerDirectory) replayLease(ctx context.Context, runnerID, sessionID string) (Claim, bool, error) {
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
		if err != nil {
			return Claim{}, false, fmt.Errorf("read leased assignment %q: %w", assignmentID, err)
		}
		assignment, err := unmarshalRedisAssignment(rawAssignment)
		if err != nil {
			return Claim{}, false, err
		}
		rawLease, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseMeta, assignmentID).Result()
		if err != nil {
			return Claim{}, false, fmt.Errorf("read persisted lease %q: %w", assignmentID, err)
		}
		lease, err := unmarshalRedisLeaseMeta(rawLease, assignment.Task)
		if err != nil {
			return Claim{}, false, fmt.Errorf("decode persisted lease %q: %w", assignmentID, err)
		}
		d.observeLeaseReplay(ctx)
		return Claim{Assignment: assignment, Lease: lease}, true, nil
	}
	return Claim{}, false, nil
}

func (d *RedisRunnerDirectory) claim(ctx context.Context, req ClaimRequest, capabilitiesChanged bool, capabilitiesJSON, assignmentID, expectedData string, claimID ClaimID) (string, error) {
	changed := "0"
	if capabilitiesChanged {
		changed = "1"
	}
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
		d.keys.runnerInflight,
		d.keys.runnerCapabilities,
		d.keys.claimsExpiry,
	}, req.RunnerID, req.SessionID, strconv.Itoa(req.Capacity), changed, capabilitiesJSON, assignmentID, expectedData, string(claimID), strconv.FormatInt(d.claimTTLMillis(), 10), strconv.FormatInt(time.Now().UTC().UnixMilli(), 10))
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
// reason. Releasing an already reclaimed claim is idempotent.
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
	}, string(claimID), string(reason))
	if err != nil {
		return fmt.Errorf("release redis claim: %w", err)
	}
	if status != "released" && status != "noop" {
		return fmt.Errorf("release redis claim: unexpected result %q", status)
	}
	return nil
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
// AssignmentID, LeaseID, and LeaseToken all match the stored record. It is used
// by the LeaseSweeper before engine reclaim to prevent a stale finalized lease
// from occupying runner capacity or suppressing redelivery via the seen marker
// after the engine has revoked and re-issued the lease. Runner and session
// registration are preserved.
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

// ClearAssignment removes the assignment from durable queue, claim, lease,
// and dedupe records.
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
	}, string(assignmentID))
	if err != nil {
		return fmt.Errorf("clear redis assignment: %w", err)
	}
	if status != "cleared" {
		return fmt.Errorf("clear redis assignment: unexpected result %q", status)
	}
	return nil
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

	capacity, err := strconv.Atoi(capacityRaw)
	if err != nil {
		return RunnerSnapshot{}, false
	}
	inFlight, err := strconv.Atoi(inFlightRaw)
	if err != nil {
		return RunnerSnapshot{}, false
	}
	heartbeatMillis, err := strconv.ParseInt(heartbeatRaw, 10, 64)
	if err != nil {
		return RunnerSnapshot{}, false
	}
	var capabilities []protocol.Capability
	if err := json.Unmarshal([]byte(capabilitiesRaw), &capabilities); err != nil {
		return RunnerSnapshot{}, false
	}
	namespaces := normalizeRunnerNamespaces(nil)
	if namespacesRaw != "" {
		if err := json.Unmarshal([]byte(namespacesRaw), &namespaces); err != nil {
			return RunnerSnapshot{}, false
		}
	}
	var labels map[string]string
	if labelsRaw != "" {
		if err := json.Unmarshal([]byte(labelsRaw), &labels); err != nil {
			return RunnerSnapshot{}, false
		}
	}
	return RunnerSnapshot{
		RunnerID:      runnerID,
		Capacity:      capacity,
		InFlight:      inFlight,
		Labels:        labels,
		Capabilities:  cloneCapabilities(capabilities),
		Namespaces:    namespaces,
		LastHeartbeat: time.UnixMilli(heartbeatMillis),
	}, true
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

// ReclaimExpiredClaims returns expired unfinalized claims to the durable
// queue. The expiry index is a same-slot ZSET, so every script key is declared
// through KEYS and Redis Cluster can validate the transaction safely.
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
local claims = redis.call('HKEYS', KEYS[8])
for _, claimID in ipairs(claims) do
  if redis.call('HGET', KEYS[8], claimID) == ARGV[1] then
    local assignmentID = redis.call('HGET', KEYS[7], claimID)
    if assignmentID then
      if redis.call('HGET', KEYS[3], assignmentID) == 'claimed' and redis.call('HGET', KEYS[4], assignmentID) == claimID then
        redis.call('HSET', KEYS[3], assignmentID, 'queued')
        redis.call('HDEL', KEYS[4], assignmentID)
        redis.call('HDEL', KEYS[5], assignmentID)
        redis.call('HDEL', KEYS[6], assignmentID)
        redis.call('LREM', KEYS[1], 0, assignmentID)
        if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then
          redis.call('LPUSH', KEYS[1], assignmentID)
        end
      end
    end
    redis.call('HDEL', KEYS[7], claimID)
    redis.call('HDEL', KEYS[8], claimID)
    redis.call('HDEL', KEYS[9], claimID)
    redis.call('ZREM', KEYS[19], claimID)
  end
end
local states = redis.call('HGETALL', KEYS[3])
for index = 1, #states, 2 do
  local assignmentID = states[index]
  local state = states[index + 1]
  if state == 'leased' and redis.call('HGET', KEYS[5], assignmentID) == ARGV[1] then
    redis.call('HSET', KEYS[6], assignmentID, ARGV[2])
  end
end
redis.call('HSET', KEYS[11], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[12], ARGV[1], ARGV[3])
redis.call('HSETNX', KEYS[13], ARGV[1], '0')
redis.call('HSET', KEYS[14], ARGV[1], ARGV[4])
redis.call('HSET', KEYS[15], ARGV[1], ARGV[5])
redis.call('HSET', KEYS[16], ARGV[1], ARGV[6])
redis.call('HSET', KEYS[17], ARGV[1], ARGV[7])
redis.call('HSET', KEYS[20], ARGV[1], ARGV[8])
redis.call('HSET', KEYS[10], ARGV[1], '0')
redis.call('HSETNX', KEYS[18], ARGV[1], '0')
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
return 'ok'
`

const redisEnqueueAssignmentLua = `
if redis.call('SADD', KEYS[2], ARGV[1]) == 0 then
  return 'duplicate'
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
local function reclaim(claimID)
  local assignmentID = redis.call('HGET', KEYS[7], claimID)
  local runnerID = redis.call('HGET', KEYS[8], claimID)
  local recovered = false
  if assignmentID then
    if redis.call('HGET', KEYS[3], assignmentID) == 'claimed' and redis.call('HGET', KEYS[4], assignmentID) == claimID then
      redis.call('HSET', KEYS[3], assignmentID, 'queued')
      redis.call('HDEL', KEYS[4], assignmentID)
      redis.call('HDEL', KEYS[5], assignmentID)
      redis.call('HDEL', KEYS[6], assignmentID)
      redis.call('LREM', KEYS[1], 0, assignmentID)
      if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then
        redis.call('LPUSH', KEYS[1], assignmentID)
      end
      recovered = true
    end
  end
  redis.call('HDEL', KEYS[7], claimID)
  redis.call('HDEL', KEYS[8], claimID)
  redis.call('HDEL', KEYS[9], claimID)
  redis.call('ZREM', KEYS[11], claimID)
  if runnerID then
    local count = tonumber(redis.call('HGET', KEYS[10], runnerID) or '0')
    if count > 0 then
      redis.call('HINCRBY', KEYS[10], runnerID, -1)
    end
  end
  return recovered
end

local recovered = 0
local expired = redis.call('ZRANGEBYSCORE', KEYS[11], '-inf', ARGV[1])
for _, claimID in ipairs(expired) do
  if reclaim(claimID) then
    recovered = recovered + 1
  end
end
-- Claims created by the pre-ZSET implementation have no score. Recover them
-- on the first new directory operation instead of leaving them orphaned.
local claims = redis.call('HKEYS', KEYS[7])
for _, claimID in ipairs(claims) do
  if redis.call('ZSCORE', KEYS[11], claimID) == false and reclaim(claimID) then
    recovered = recovered + 1
  end
end
return recovered
`

const redisClaimAssignmentLua = `
local current = redis.call('HGET', KEYS[12], ARGV[1])
if not current then
  return 'not_found'
end
if ARGV[2] == '' or current ~= ARGV[2] then
  return 'stale'
end
if ARGV[4] == '1' then
  redis.call('HSET', KEYS[15], ARGV[1], ARGV[5])
end
-- Headroom is derived purely from server-side accounting: the authoritative
-- total capacity (written only by register/heartbeat) minus the tasks this
-- runner already has in flight (active claims + finalized leases). The poll
-- request's ARGV[3] Capacity is deliberately NOT written to runnerCapacity and
-- NOT used here: doing so would let a client-supplied remainder both pollute
-- RunnerSnapshot.Capacity and double-count in-flight work already reflected by
-- claims+leases. runnerInflight is a heartbeat observation only and must not
-- gate the claim.
local capacity = tonumber(redis.call('HGET', KEYS[13], ARGV[1]) or '0')
local claims = tonumber(redis.call('HGET', KEYS[10], ARGV[1]) or '0')
local leases = tonumber(redis.call('HGET', KEYS[11], ARGV[1]) or '0')
if capacity - claims - leases <= 0 then
  return 'none'
end
if ARGV[6] == '' then
  return 'none'
end
if redis.call('HGET', KEYS[3], ARGV[6]) ~= 'queued' then
  return 'retry'
end
if redis.call('HGET', KEYS[2], ARGV[6]) ~= ARGV[7] then
  return 'retry'
end
if redis.call('LREM', KEYS[1], 0, ARGV[6]) == 0 then
  return 'retry'
end
redis.call('HSET', KEYS[3], ARGV[6], 'claimed')
redis.call('HSET', KEYS[4], ARGV[6], ARGV[8])
redis.call('HSET', KEYS[5], ARGV[6], ARGV[1])
redis.call('HSET', KEYS[6], ARGV[6], ARGV[2])
redis.call('HSET', KEYS[7], ARGV[8], ARGV[6])
redis.call('HSET', KEYS[8], ARGV[8], ARGV[1])
redis.call('HSET', KEYS[9], ARGV[8], ARGV[2])
redis.call('HINCRBY', KEYS[10], ARGV[1], 1)
redis.call('ZADD', KEYS[16], tonumber(ARGV[10]) + tonumber(ARGV[9]), ARGV[8])
return 'claimed'
`

const redisFinalizeClaimLua = `
local assignmentID = redis.call('HGET', KEYS[8], ARGV[1])
local runnerID = redis.call('HGET', KEYS[9], ARGV[1])
local sessionID = redis.call('HGET', KEYS[10], ARGV[1])
if not assignmentID or not runnerID or not sessionID then
  return 'noop'
end
if redis.call('HGET', KEYS[16], runnerID) ~= sessionID then
  return 'stale'
end
if redis.call('HGET', KEYS[1], assignmentID) ~= 'claimed' or redis.call('HGET', KEYS[2], assignmentID) ~= ARGV[1] then
  return 'noop'
end
redis.call('HDEL', KEYS[8], ARGV[1])
redis.call('HDEL', KEYS[9], ARGV[1])
redis.call('HDEL', KEYS[10], ARGV[1])
redis.call('ZREM', KEYS[15], ARGV[1])
local claims = tonumber(redis.call('HGET', KEYS[11], runnerID) or '0')
if claims > 0 then
  redis.call('HINCRBY', KEYS[11], runnerID, -1)
end
redis.call('HINCRBY', KEYS[12], runnerID, 1)
redis.call('HSET', KEYS[1], assignmentID, 'leased')
redis.call('HDEL', KEYS[2], assignmentID)
redis.call('HSET', KEYS[5], assignmentID, ARGV[2])
redis.call('HSET', KEYS[6], assignmentID, ARGV[3])
redis.call('HSET', KEYS[7], assignmentID, ARGV[4])
if ARGV[2] ~= '' then
  redis.call('HSET', KEYS[13], ARGV[2], assignmentID)
end
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[14], ARGV[3], assignmentID)
end
return 'finalized'
`

const redisReleaseClaimLua = `
local assignmentID = redis.call('HGET', KEYS[11], ARGV[1])
local runnerID = redis.call('HGET', KEYS[12], ARGV[1])
if not assignmentID then
  return 'noop'
end
if redis.call('HGET', KEYS[4], assignmentID) == 'claimed' and redis.call('HGET', KEYS[5], assignmentID) == ARGV[1] then
  if ARGV[2] == 'requeue' then
    redis.call('HSET', KEYS[4], assignmentID, 'queued')
    redis.call('HDEL', KEYS[5], assignmentID)
    redis.call('HDEL', KEYS[6], assignmentID)
    redis.call('HDEL', KEYS[7], assignmentID)
    redis.call('LREM', KEYS[1], 0, assignmentID)
    if redis.call('HEXISTS', KEYS[3], assignmentID) == 1 then
      redis.call('LPUSH', KEYS[1], assignmentID)
    end
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
    redis.call('HSET', KEYS[4], assignmentID, 'released')
    redis.call('HDEL', KEYS[5], assignmentID)
    redis.call('HDEL', KEYS[6], assignmentID)
    redis.call('HDEL', KEYS[7], assignmentID)
  end
end
redis.call('HDEL', KEYS[11], ARGV[1])
redis.call('HDEL', KEYS[12], ARGV[1])
redis.call('HDEL', KEYS[13], ARGV[1])
redis.call('ZREM', KEYS[15], ARGV[1])
if runnerID then
  local claims = tonumber(redis.call('HGET', KEYS[14], runnerID) or '0')
  if claims > 0 then
    redis.call('HINCRBY', KEYS[14], runnerID, -1)
  end
end
return 'released'
`

const redisReleaseLeasedLua = `
if not redis.call('HGET', KEYS[1], ARGV[1]) then
  return 'not_found'
end
local assignmentID = nil
if ARGV[4] ~= '' then
  assignmentID = redis.call('HGET', KEYS[10], ARGV[4])
end
if not assignmentID and ARGV[3] ~= '' then
  assignmentID = redis.call('HGET', KEYS[9], ARGV[3])
end
if not assignmentID or assignmentID == '' then
  assignmentID = ARGV[2]
end
if assignmentID == '' then
  return 'noop'
end
if redis.call('HGET', KEYS[3], assignmentID) ~= 'leased' then
  return 'noop'
end
if redis.call('HGET', KEYS[4], assignmentID) ~= ARGV[1] then
  return 'noop'
end
local currentLeaseID = redis.call('HGET', KEYS[6], assignmentID) or ''
local currentLeaseToken = redis.call('HGET', KEYS[7], assignmentID) or ''
if ARGV[4] ~= '' and currentLeaseToken ~= ARGV[4] then
  return 'noop'
end
if ARGV[4] == '' and ARGV[3] ~= '' and currentLeaseID ~= ARGV[3] then
  return 'noop'
end
if currentLeaseID ~= '' and redis.call('HGET', KEYS[9], currentLeaseID) == assignmentID then
  redis.call('HDEL', KEYS[9], currentLeaseID)
end
if currentLeaseToken ~= '' and redis.call('HGET', KEYS[10], currentLeaseToken) == assignmentID then
  redis.call('HDEL', KEYS[10], currentLeaseToken)
end
local leases = tonumber(redis.call('HGET', KEYS[11], ARGV[1]) or '0')
if leases > 0 then
  redis.call('HINCRBY', KEYS[11], ARGV[1], -1)
end
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
return 'released'
`

const redisClearAssignmentLua = `
local claimID = redis.call('HGET', KEYS[5], ARGV[1])
local runnerID = redis.call('HGET', KEYS[6], ARGV[1])
local state = redis.call('HGET', KEYS[4], ARGV[1])
if claimID then
  local claimRunner = redis.call('HGET', KEYS[12], claimID)
  if claimRunner then
    runnerID = claimRunner
  end
  redis.call('HDEL', KEYS[11], claimID)
  redis.call('HDEL', KEYS[12], claimID)
  redis.call('HDEL', KEYS[13], claimID)
  redis.call('ZREM', KEYS[18], claimID)
  if runnerID then
    local claims = tonumber(redis.call('HGET', KEYS[14], runnerID) or '0')
    if claims > 0 then
      redis.call('HINCRBY', KEYS[14], runnerID, -1)
    end
  end
end
if state == 'leased' and runnerID then
  local leases = tonumber(redis.call('HGET', KEYS[15], runnerID) or '0')
  if leases > 0 then
    redis.call('HINCRBY', KEYS[15], runnerID, -1)
  end
end
local leaseID = redis.call('HGET', KEYS[8], ARGV[1])
local leaseToken = redis.call('HGET', KEYS[9], ARGV[1])
if leaseID and redis.call('HGET', KEYS[16], leaseID) == ARGV[1] then
  redis.call('HDEL', KEYS[16], leaseID)
end
if leaseToken and redis.call('HGET', KEYS[17], leaseToken) == ARGV[1] then
  redis.call('HDEL', KEYS[17], leaseToken)
end
redis.call('LREM', KEYS[1], 0, ARGV[1])
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HDEL', KEYS[8], ARGV[1])
redis.call('HDEL', KEYS[9], ARGV[1])
redis.call('HDEL', KEYS[10], ARGV[1])
return 'cleared'
`

const redisReleaseExpiredLeaseLua = `
local assignmentID = ARGV[1]
local leaseID = ARGV[2]
local leaseToken = ARGV[3]

local state = redis.call('HGET', KEYS[2], assignmentID)
if state ~= 'leased' then
  return 'already_released'
end

local currentLeaseID = redis.call('HGET', KEYS[5], assignmentID) or ''
local currentLeaseToken = redis.call('HGET', KEYS[6], assignmentID) or ''
if currentLeaseID ~= leaseID or currentLeaseToken ~= leaseToken then
  return 'token_mismatch'
end

local runnerID = redis.call('HGET', KEYS[3], assignmentID)

redis.call('HDEL', KEYS[1], assignmentID)
redis.call('HDEL', KEYS[2], assignmentID)
redis.call('HDEL', KEYS[3], assignmentID)
redis.call('HDEL', KEYS[4], assignmentID)
redis.call('HDEL', KEYS[5], assignmentID)
redis.call('HDEL', KEYS[6], assignmentID)
redis.call('HDEL', KEYS[7], assignmentID)

if leaseID ~= '' and redis.call('HGET', KEYS[8], leaseID) == assignmentID then
  redis.call('HDEL', KEYS[8], leaseID)
end
if leaseToken ~= '' and redis.call('HGET', KEYS[9], leaseToken) == assignmentID then
  redis.call('HDEL', KEYS[9], leaseToken)
end

if runnerID then
  local leases = tonumber(redis.call('HGET', KEYS[10], runnerID) or '0')
  if leases > 0 then
    redis.call('HINCRBY', KEYS[10], runnerID, -1)
  end
end

redis.call('SREM', KEYS[11], assignmentID)
redis.call('LREM', KEYS[12], 0, assignmentID)

return 'released'
`
