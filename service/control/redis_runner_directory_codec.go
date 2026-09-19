package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// redisRunnerDirectoryKeys is the fixed set of Redis keys backing a
// RedisRunnerDirectory. Every key shares the same Redis Cluster hash tag so
// the directory's Lua transitions stay single-slot.
type redisRunnerDirectoryKeys struct {
	prefix string

	queue                string
	seen                 string
	assignmentData       string
	assignmentState      string
	assignmentClaim      string
	assignmentRunner     string
	assignmentSession    string
	assignmentLeaseID    string
	assignmentLeaseToken string
	// assignmentLeaseMetaLegacy is the pre-U-7 shared HASH — field =
	// assignment ID, no TTL on the key, never deleted as a whole. It is named
	// here for exactly one reason: three places need the same literal (the
	// clear transition, the orphan reaper, and the real-Redis key inventory),
	// and a second hard-coded copy of the string is how the format drifts.
	//
	// Nothing in this version reads or writes it as live state. Lease metadata
	// is written to keys.assignmentLeaseMetaKey(assignmentID); this field only
	// ever appears as a deletion target, and never as a key the correctness of
	// a transition depends on.
	assignmentLeaseMetaLegacy                    string
	claimsAssignment                             string
	claimsRunner                                 string
	claimsSession                                string
	claimsExpiry                                 string
	runnerSession                                string
	runnerCapacity                               string
	runnerInflight                               string
	runnerCapabilities                           string
	runnerLabels                                 string
	runnerPolicy                                 string
	runnerNamespaces                             string
	runnerHeartbeat                              string
	runnerClaimCount                             string
	runnerLeaseCount                             string
	leaseByID                                    string
	leaseByToken                                 string
	runnerControlDesired                         string
	runnerControlGeneration                      string
	runnerControlRequestedAt                     string
	runnerControlActor                           string
	runnerControlReason                          string
	runnerControlDrainDeadline                   string
	runnerControlReceiptHash                     string
	runnerControlReceiptDesired                  string
	runnerControlReceiptGeneration               string
	runnerControlReceiptRequestedAt              string
	runnerControlReceiptReason                   string
	runnerControlReceiptClaims                   string
	runnerControlReceiptLeases                   string
	runnerControlReceiptUnsettledDebt            string
	runnerControlReceiptHandoffDebt              string
	runnerControlReceiptLeaseMayExistDebt        string
	runnerControlReceiptReplayableDebt           string
	runnerControlReceiptPendingActivationCleanup string
	runnerControlReceiptDrainDeadline            string
	runnerControlReceiptStoredAt                 string
	runnerControlReceiptStatus                   string
	runnerControlReceiptExpiry                   string
	runnerControlAudit                           string

	// Drain-triggered activation cleanup obligation keys are all keyed by an
	// opaque obligation ID. They share the directory hash tag so Register,
	// control transitions, and receipt acks can use single-slot Lua CAS.
	deactivationObligationState           string
	deactivationObligationRunner          string
	deactivationObligationSession         string
	deactivationObligationNamespace       string
	deactivationObligationWorkflowID      string
	deactivationObligationWorkflowVersion string
	deactivationObligationEntryUnitID     string
	deactivationObligationReplicaIndex    string
	deactivationObligationGeneration      string
	deactivationObligationDrainGeneration string
	// runnerActivationInventory retains the registration's reported activation
	// generations so an obligation created immediately after reconnect can bind
	// delivery to that session only when it proved it still hosts the old work.
	runnerActivationInventory string
	// runnerDrainObservation stores the last session- and generation-fenced
	// runner-local drain sample as one JSON value per runner. Lua never parses
	// it; heartbeat validates the scalar generation before HSET.
	runnerDrainObservation string

	// Handoff keys are all claim-scoped except handoffAssignment, which gives
	// token-fenced terminal cleanup an O(1) assignment -> current handoff path.
	handoffState            string
	handoffGeneration       string
	handoffLeaseMeta        string
	handoffClaim            string
	handoffAssignment       string
	handoffRunner           string
	handoffSession          string
	handoffLeaseID          string
	handoffLeaseToken       string
	handoffRecoveryReady    string
	handoffRecoveryDeadline string
}

func newRedisRunnerDirectoryKeys(prefix string) redisRunnerDirectoryKeys {
	return redisRunnerDirectoryKeys{
		prefix:                                prefix,
		queue:                                 prefix + ":queue",
		seen:                                  prefix + ":seen",
		assignmentData:                        prefix + ":assignment:data",
		assignmentState:                       prefix + ":assignment:state",
		assignmentClaim:                       prefix + ":assignment:claim",
		assignmentRunner:                      prefix + ":assignment:runner",
		assignmentSession:                     prefix + ":assignment:session",
		assignmentLeaseID:                     prefix + ":assignment:lease-id",
		assignmentLeaseToken:                  prefix + ":assignment:lease-token",
		assignmentLeaseMetaLegacy:             prefix + ":assignment:lease-meta",
		claimsAssignment:                      prefix + ":claim:assignment",
		claimsRunner:                          prefix + ":claim:runner",
		claimsSession:                         prefix + ":claim:session",
		claimsExpiry:                          prefix + ":claim:expiry",
		runnerSession:                         prefix + ":runner:session",
		runnerCapacity:                        prefix + ":runner:capacity",
		runnerInflight:                        prefix + ":runner:inflight",
		runnerCapabilities:                    prefix + ":runner:capabilities",
		runnerLabels:                          prefix + ":runner:labels",
		runnerPolicy:                          prefix + ":runner:policy",
		runnerNamespaces:                      prefix + ":runner:namespaces",
		runnerHeartbeat:                       prefix + ":runner:heartbeat",
		runnerClaimCount:                      prefix + ":runner:claim-count",
		runnerLeaseCount:                      prefix + ":runner:lease-count",
		leaseByID:                             prefix + ":lease:by-id",
		leaseByToken:                          prefix + ":lease:by-token",
		runnerControlDesired:                  prefix + ":runner:control:desired",
		runnerControlGeneration:               prefix + ":runner:control:generation",
		runnerControlRequestedAt:              prefix + ":runner:control:requested-at",
		runnerControlActor:                    prefix + ":runner:control:actor",
		runnerControlReason:                   prefix + ":runner:control:reason",
		runnerControlDrainDeadline:            prefix + ":runner:control:drain-deadline",
		runnerControlReceiptHash:              prefix + ":runner:control:receipt:hash",
		runnerControlReceiptDesired:           prefix + ":runner:control:receipt:desired",
		runnerControlReceiptGeneration:        prefix + ":runner:control:receipt:generation",
		runnerControlReceiptRequestedAt:       prefix + ":runner:control:receipt:requested-at",
		runnerControlReceiptReason:            prefix + ":runner:control:receipt:reason",
		runnerControlReceiptClaims:            prefix + ":runner:control:receipt:claims",
		runnerControlReceiptLeases:            prefix + ":runner:control:receipt:leases",
		runnerControlReceiptUnsettledDebt:     prefix + ":runner:control:receipt:unsettled-debt",
		runnerControlReceiptHandoffDebt:       prefix + ":runner:control:receipt:handoff-debt",
		runnerControlReceiptLeaseMayExistDebt: prefix + ":runner:control:receipt:lease-may-exist-debt",
		runnerControlReceiptReplayableDebt:    prefix + ":runner:control:receipt:replayable-debt",
		runnerControlReceiptPendingActivationCleanup: prefix + ":runner:control:receipt:pending-activation-cleanup",
		runnerControlReceiptDrainDeadline:            prefix + ":runner:control:receipt:drain-deadline",
		runnerControlReceiptStoredAt:                 prefix + ":runner:control:receipt:stored-at",
		runnerControlReceiptStatus:                   prefix + ":runner:control:receipt:status",
		runnerControlReceiptExpiry:                   prefix + ":runner:control:receipt:expiry",
		runnerControlAudit:                           prefix + ":runner:control:audit",
		deactivationObligationState:                  prefix + ":runner:deactivation:state",
		deactivationObligationRunner:                 prefix + ":runner:deactivation:runner",
		deactivationObligationSession:                prefix + ":runner:deactivation:session",
		deactivationObligationNamespace:              prefix + ":runner:deactivation:namespace",
		deactivationObligationWorkflowID:             prefix + ":runner:deactivation:workflow-id",
		deactivationObligationWorkflowVersion:        prefix + ":runner:deactivation:workflow-version",
		deactivationObligationEntryUnitID:            prefix + ":runner:deactivation:entry-unit-id",
		deactivationObligationReplicaIndex:           prefix + ":runner:deactivation:replica-index",
		deactivationObligationGeneration:             prefix + ":runner:deactivation:generation",
		deactivationObligationDrainGeneration:        prefix + ":runner:deactivation:drain-generation",
		runnerActivationInventory:                    prefix + ":runner:activation-inventory",
		runnerDrainObservation:                       prefix + ":runner:control:drain-observation",
		handoffState:                                 prefix + ":runner:handoff:state",
		handoffGeneration:                            prefix + ":runner:handoff:generation",
		handoffLeaseMeta:                             prefix + ":runner:handoff:lease-meta",
		handoffClaim:                                 prefix + ":runner:handoff:claim",
		handoffAssignment:                            prefix + ":runner:handoff:assignment",
		handoffRunner:                                prefix + ":runner:handoff:runner",
		handoffSession:                               prefix + ":runner:handoff:session",
		handoffLeaseID:                               prefix + ":runner:handoff:lease-id",
		handoffLeaseToken:                            prefix + ":runner:handoff:lease-token",
		handoffRecoveryReady:                         prefix + ":runner:handoff:recovery-ready",
		handoffRecoveryDeadline:                      prefix + ":runner:handoff:recovery-deadline",
	}
}

// assignmentLeaseMetaKey returns the assignment-scoped lease metadata key. The
// prefix supplies the shared Redis Cluster hash tag for every Lua key.
func (keys redisRunnerDirectoryKeys) assignmentLeaseMetaKey(assignmentID string) string {
	return keys.prefix + ":assignment:lease-meta:" + assignmentID
}

// runnerLeasedAssignmentsKey returns the per-runner set of assignment IDs that
// runner currently holds in 'leased' state. It is the index that keeps a poll's
// lease replay proportional to the runner's own leases instead of to every
// assignment in the directory. Like assignmentLeaseMetaKey it is derived from
// the prefix, so it carries the same Cluster hash tag as the Lua transitions
// that write it.
func (keys redisRunnerDirectoryKeys) runnerLeasedAssignmentsKey(runnerID string) string {
	return keys.prefix + ":runner:leased-assignments:" + runnerID
}

// redisHandoffClaimIndexSuffix names the per-runner handoff index. It is a bare
// suffix rather than a full key because the two Lua transitions that promote an
// unledgered claim into handoff debt only learn the owning runner inside the
// script; they build the key from handoffClaimIndexPrefix instead.
const redisHandoffClaimIndexSuffix = ":runner:handoff-index:"

// handoffClaimIndexKey returns the per-runner set of claim IDs that
// handoffRunner currently maps to runnerID. It is the index that keeps a poll's
// handoff recovery proportional to the runner's own debt instead of to every
// handoff claim in the fleet. Like assignmentLeaseMetaKey it is derived from the
// prefix, so it carries the same Cluster hash tag as the Lua transitions that
// write it.
func (keys redisRunnerDirectoryKeys) handoffClaimIndexKey(runnerID string) string {
	return keys.prefix + redisHandoffClaimIndexSuffix + runnerID
}

// handoffClaimIndexPrefix is the ARGV form handed to Lua scripts whose owner is
// only known at runtime. Concatenating a runner ID onto it is safe on Redis
// Cluster because the prefix still carries the shared hash tag, so every key a
// script can name lands in the same slot as the keys it declares.
func (keys redisRunnerDirectoryKeys) handoffClaimIndexPrefix() string {
	return keys.prefix + redisHandoffClaimIndexSuffix
}

// redisActivationInventoryItem intentionally encodes generation as a string:
// JSON/Lua number conversion would otherwise lose exact uint64 fencing values
// above 2^53 while a reconnect decides whether it may inherit cleanup work.
type redisActivationInventoryItem struct {
	WorkflowID      string `json:"workflow_id"`
	WorkflowVersion string `json:"workflow_version"`
	EntryUnitID     string `json:"entry_unit_id"`
	ReplicaIndex    uint32 `json:"replica_index"`
	Generation      string `json:"generation"`
}

type redisRunnerDrainObservation struct {
	SessionID         string    `json:"session_id"`
	Generation        uint64    `json:"generation"`
	RecoveryOnly      bool      `json:"recovery_only"`
	InFlight          int       `json:"in_flight"`
	ActiveActivations uint32    `json:"active_activations"`
	ObservedAt        time.Time `json:"observed_at"`
}

func marshalRedisRunnerDrainObservation(observation *runnerDrainObservation) (string, error) {
	if observation == nil {
		return "", nil
	}
	payload, err := json.Marshal(redisRunnerDrainObservation{
		SessionID:         observation.sessionID,
		Generation:        observation.generation,
		RecoveryOnly:      observation.recoveryOnly,
		InFlight:          observation.inFlight,
		ActiveActivations: observation.activeActivations,
		ObservedAt:        observation.observedAt,
	})
	if err != nil {
		return "", fmt.Errorf("marshal runner drain observation: %w", err)
	}
	return string(payload), nil
}

// unmarshalRedisRunnerDrainObservation fails closed: an older or damaged
// payload simply cannot contribute runner-local quiescence to the projection.
func unmarshalRedisRunnerDrainObservation(payload string) *runnerDrainObservation {
	if payload == "" {
		return nil
	}
	var record redisRunnerDrainObservation
	if err := json.Unmarshal([]byte(payload), &record); err != nil || record.SessionID == "" || record.InFlight < 0 {
		return nil
	}
	return &runnerDrainObservation{
		sessionID:         record.SessionID,
		generation:        record.Generation,
		recoveryOnly:      record.RecoveryOnly,
		inFlight:          record.InFlight,
		activeActivations: record.ActiveActivations,
		observedAt:        record.ObservedAt,
	}
}

func marshalRedisActivationInventory(items []protocol.ActivationInventoryItem) (string, error) {
	encoded := make([]redisActivationInventoryItem, 0, len(items))
	for _, item := range items {
		encoded = append(encoded, redisActivationInventoryItem{
			WorkflowID:      item.WorkflowID,
			WorkflowVersion: item.WorkflowVersion,
			EntryUnitID:     item.EntryUnitID,
			ReplicaIndex:    item.ReplicaIndex,
			Generation:      strconv.FormatUint(item.Generation, 10),
		})
	}
	payload, err := json.Marshal(encoded)
	if err != nil {
		return "", fmt.Errorf("marshal runner activation inventory: %w", err)
	}
	return string(payload), nil
}

func boolRedisArg(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

type redisAssignmentRecord struct {
	AssignmentID AssignmentID        `json:"assignment_id"`
	Task         engine.Task         `json:"task"`
	AutoDepth    int                 `json:"auto_depth"`
	ActivationID int                 `json:"activation_id"`
	UnitIdx      *int                `json:"unit_idx,omitempty"`
	Routing      engine.TaskRouting  `json:"routing"`
	Namespace    namespace.Namespace `json:"namespace,omitempty"`
}

func marshalRedisAssignment(assignment Assignment) (string, error) {
	payload, err := json.Marshal(redisAssignmentRecord{
		AssignmentID: assignment.AssignmentID,
		Task:         assignment.Task,
		AutoDepth:    assignment.Task.AutoDepth,
		ActivationID: assignment.Task.ActivationID,
		UnitIdx:      controlUnitIdxPtr(assignment.Task.UnitIdx),
		Routing:      assignment.Routing,
		Namespace:    assignment.Namespace,
	})
	if err != nil {
		return "", fmt.Errorf("marshal assignment %q: %w", assignment.AssignmentID, err)
	}
	return string(payload), nil
}

func unmarshalRedisAssignment(payload string) (Assignment, error) {
	var record redisAssignmentRecord
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return Assignment{}, fmt.Errorf("decode redis assignment: %w", err)
	}
	if record.AssignmentID == "" {
		return Assignment{}, errors.New("decode redis assignment: missing assignment id")
	}
	record.Task.AutoDepth = record.AutoDepth
	record.Task.ActivationID = record.ActivationID
	if record.UnitIdx != nil {
		record.Task.UnitIdx = *record.UnitIdx
	} else {
		record.Task.UnitIdx = engine.UnitIdxUnknown
	}
	if record.Namespace == "" {
		record.Namespace = namespace.Default
	}
	return Assignment{AssignmentID: record.AssignmentID, Task: record.Task, Routing: record.Routing, Namespace: record.Namespace}, nil
}

// controlUnitIdxPtr omits the wire field when the task's UnitIdx is the
// "unknown" sentinel, so absence on decode is distinguishable from a real
// unit index of 0. See engine.UnitIdxUnknown.
func controlUnitIdxPtr(unitIdx int) *int {
	if unitIdx == engine.UnitIdxUnknown {
		return nil
	}
	v := unitIdx
	return &v
}

// redisLeaseMeta embeds the complete runner-facing lease rather than only
// identity fields. This makes a finalized handoff replayable after the
// control-plane restarts or loses the poll response.
type redisLeaseMeta struct {
	Lease        *engine.TaskLease `json:"lease,omitempty"`
	AutoDepth    int               `json:"auto_depth,omitempty"`
	ActivationID int               `json:"activation_id,omitempty"`
}

type legacyRedisLeaseMeta struct {
	LeaseID     engine.LeaseID    `json:"lease_id,omitempty"`
	LeaseToken  engine.LeaseToken `json:"lease_token,omitempty"`
	Attempt     int               `json:"attempt,omitempty"`
	NodeType    string            `json:"node_type,omitempty"`
	NodeVersion int               `json:"node_version,omitempty"`
	IssuedAt    time.Time         `json:"issued_at,omitempty"`
	TTL         time.Duration     `json:"ttl,omitempty"`
}

func marshalRedisLeaseMeta(lease *engine.TaskLease) (string, error) {
	if lease == nil {
		return "{}", nil
	}
	copy := *lease
	payload, err := json.Marshal(redisLeaseMeta{
		Lease:        &copy,
		AutoDepth:    lease.Task.AutoDepth,
		ActivationID: lease.Task.ActivationID,
	})
	if err != nil {
		return "", fmt.Errorf("marshal lease metadata: %w", err)
	}
	return string(payload), nil
}

func unmarshalRedisLeaseMeta(payload string, task engine.Task) (*engine.TaskLease, error) {
	var record redisLeaseMeta
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return nil, err
	}
	if record.Lease != nil {
		lease := *record.Lease
		lease.Task = task
		lease.Task.AutoDepth = record.AutoDepth
		lease.Task.ActivationID = record.ActivationID
		return &lease, nil
	}

	// Accept records written by the previous directory implementation during a
	// rolling upgrade. Core recovers a fresh full input when such a lease is
	// replayed, while the persisted identity remains fenced.
	var legacy legacyRedisLeaseMeta
	if err := json.Unmarshal([]byte(payload), &legacy); err != nil {
		return nil, err
	}
	return &engine.TaskLease{
		LeaseID:     legacy.LeaseID,
		LeaseToken:  legacy.LeaseToken,
		Attempt:     legacy.Attempt,
		Task:        task,
		NodeType:    legacy.NodeType,
		NodeVersion: legacy.NodeVersion,
		IssuedAt:    legacy.IssuedAt,
		TTL:         legacy.TTL,
	}, nil
}
