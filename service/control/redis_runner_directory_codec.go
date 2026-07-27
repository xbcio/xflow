package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
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
	assignmentLeaseMeta  string
	claimsAssignment     string
	claimsRunner         string
	claimsSession        string
	claimsExpiry         string
	runnerSession        string
	runnerCapacity       string
	runnerInflight       string
	runnerCapabilities   string
	runnerPolicy         string
	runnerNamespaces     string
	runnerHeartbeat      string
	runnerClaimCount     string
	runnerLeaseCount     string
	leaseByID            string
	leaseByToken         string
}

func newRedisRunnerDirectoryKeys(prefix string) redisRunnerDirectoryKeys {
	return redisRunnerDirectoryKeys{
		prefix:               prefix,
		queue:                prefix + ":queue",
		seen:                 prefix + ":seen",
		assignmentData:       prefix + ":assignment:data",
		assignmentState:      prefix + ":assignment:state",
		assignmentClaim:      prefix + ":assignment:claim",
		assignmentRunner:     prefix + ":assignment:runner",
		assignmentSession:    prefix + ":assignment:session",
		assignmentLeaseID:    prefix + ":assignment:lease-id",
		assignmentLeaseToken: prefix + ":assignment:lease-token",
		assignmentLeaseMeta:  prefix + ":assignment:lease-meta",
		claimsAssignment:     prefix + ":claim:assignment",
		claimsRunner:         prefix + ":claim:runner",
		claimsSession:        prefix + ":claim:session",
		claimsExpiry:         prefix + ":claim:expiry",
		runnerSession:        prefix + ":runner:session",
		runnerCapacity:       prefix + ":runner:capacity",
		runnerInflight:       prefix + ":runner:inflight",
		runnerCapabilities:   prefix + ":runner:capabilities",
		runnerPolicy:         prefix + ":runner:policy",
		runnerNamespaces:     prefix + ":runner:namespaces",
		runnerHeartbeat:      prefix + ":runner:heartbeat",
		runnerClaimCount:     prefix + ":runner:claim-count",
		runnerLeaseCount:     prefix + ":runner:lease-count",
		leaseByID:            prefix + ":lease:by-id",
		leaseByToken:         prefix + ":lease:by-token",
	}
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
	Routing      engine.TaskRouting  `json:"routing"`
	Namespace    namespace.Namespace `json:"namespace,omitempty"`
}

func marshalRedisAssignment(assignment Assignment) (string, error) {
	payload, err := json.Marshal(redisAssignmentRecord{
		AssignmentID: assignment.AssignmentID,
		Task:         assignment.Task,
		AutoDepth:    assignment.Task.AutoDepth,
		ActivationID: assignment.Task.ActivationID,
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
	if record.Namespace == "" {
		record.Namespace = namespace.Default
	}
	return Assignment{AssignmentID: record.AssignmentID, Task: record.Task, Routing: record.Routing, Namespace: record.Namespace}, nil
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
