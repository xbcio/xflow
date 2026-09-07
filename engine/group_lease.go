package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// GroupLeasePayload carries the full context a remote runner needs to execute a
// group. It is the single source of truth for the group's entry input (not
// TaskLease.Input which is nil for group tasks).
type GroupLeasePayload struct {
	ProtocolVersion int                    `json:"protocol_version"`
	GroupExecID     string                 `json:"group_exec_id"`
	GroupID         string                 `json:"group_id"`
	GroupUnitIdx    int                    `json:"group_unit_idx"`
	WorkflowVersion string                 `json:"workflow_version"`
	GraphHash       string                 `json:"graph_hash"`
	PackageHash     string                 `json:"package_hash"`
	Package         *graph.SubgraphPackage `json:"package,omitempty"`
	Input           *types.Input           `json:"input,omitempty"`
	IdempotencyKey  string                 `json:"idempotency_key"`
	Deadline        time.Time              `json:"deadline,omitempty"`
	// TaskType distinguishes initial group exec from resume.
	TaskType TaskType `json:"task_type,omitempty"`
}

// ErrGroupLeaseAlreadyActive is returned when BuildGroupLease cannot acquire
// because the unit is already running under an active lease.
var ErrGroupLeaseAlreadyActive = errors.New("group lease already active")

// ErrGroupLeaseNotActive is returned by RecoverGroupLease when the unit has no
// live group lease in the backend. This is NOT an internal failure: a durable
// at-least-once replay that arrives after the group already committed hits it
// on every fast group. Callers must treat it as "this assignment is finished,
// drop it" rather than propagating it as a server error.
var ErrGroupLeaseNotActive = errors.New("group lease not active")

// BuildGroupLease assembles a group lease for a queued group task. Unlike
// BuildTaskLease, the lease payload carries the full SubgraphPackage and entry
// input, and TaskLease.Input is nil (the group payload is authoritative).
func (e *Engine) BuildGroupLease(ctx context.Context, t *Task) (*TaskLease, *GroupLeasePayload, error) {
	if t == nil {
		return nil, nil, fmt.Errorf("build group lease: nil task")
	}

	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return nil, nil, err
	}
	if !active {
		return nil, nil, ErrExecutionInactive
	}

	if t.UnitIdx < 0 || t.UnitIdx >= g.UnitCount() || g.UnitKindAt(t.UnitIdx) != graph.UnitGroup {
		return nil, nil, fmt.Errorf("task unit %d is not a group unit", t.UnitIdx)
	}

	gm := g.GroupMetaAt(t.UnitIdx)

	// Project the group package for the runner.
	pkg, pkgHash, err := graph.ProjectGroupPackage(g, t.UnitIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("project group package: %w", err)
	}

	// Build entry input (seeded input for the group's entry node).
	entryInput, err := e.buildInput(ctx, &Task{
		ExecutionID:  t.ExecutionID,
		NodeName:     g.NodeName(gm.EntryIdx),
		NodeIdx:      gm.EntryIdx,
		UnitIdx:      t.UnitIdx,
		ActivationID: t.ActivationID,
	}, g)
	if err != nil {
		return nil, nil, fmt.Errorf("build group entry input: %w", err)
	}

	leaseID, leaseToken := newLeaseCredentials()
	issuedAt := time.Now().UTC()
	ttl := e.defaultLeaseTTL

	groupID := fmt.Sprintf("%s/%s/%d", t.ExecutionID, gm.Name, t.ActivationID)
	idempotencyKey := fmt.Sprintf("normal/%s/%s/%d", t.ExecutionID, gm.Name, t.ActivationID)

	groupLease := &GroupLease{
		ExecutionID:    t.ExecutionID,
		GroupUnitIdx:   t.UnitIdx,
		GroupID:        groupID,
		IdempotencyKey: idempotencyKey,
		LeaseID:        leaseID,
		LeaseToken:     leaseToken,
		Attempt:        1,
		Input:          entryInput,
		IssuedAt:       issuedAt,
		TTL:            ttl,
		GroupName:      gm.Name,
		EntryNodeIdx:   gm.EntryIdx,
		ActivationID:   t.ActivationID,
	}

	gs, ok := e.state.(GroupStateStore)
	if !ok {
		return nil, nil, fmt.Errorf("state store does not support group leases")
	}

	acquired, err := gs.AcquireGroupLease(ctx, groupLease)
	if err != nil {
		return nil, nil, err
	}
	if !acquired {
		return nil, nil, ErrGroupLeaseAlreadyActive
	}
	e.notifyGroupLeaseAcquired(ctx)

	// Build the external TaskLease wrapper. Input is nil — the group payload
	// is the single source of truth.
	taskLease := &TaskLease{
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Task:       *t,
		Attempt:    groupLease.Attempt,
		IssuedAt:   issuedAt,
		TTL:        ttl,
		NodeType:   GroupNodeType,
	}

	var deadline time.Time
	if gm.Timeout > 0 {
		deadline = issuedAt.Add(gm.Timeout)
	}

	payload := &GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     uuid.New().String(),
		GroupID:         groupID,
		GroupUnitIdx:    t.UnitIdx,
		WorkflowVersion: g.WorkflowVersion(),
		GraphHash:       g.Hash(),
		PackageHash:     pkgHash,
		Package:         pkg,
		Input:           entryInput,
		IdempotencyKey:  idempotencyKey,
		Deadline:        deadline,
	}

	return taskLease, payload, nil
}

// RecoverGroupLease rebuilds the lease representation for a group unit after a
// crash between AcquireGroupLease and finalization. It does NOT mutate state —
// the group lease must already be live in the backend.
func (e *Engine) RecoverGroupLease(ctx context.Context, execID types.ExecutionID, unitIdx int) (*TaskLease, *GroupLeasePayload, error) {
	g, active, err := e.loadActiveGraph(ctx, execID)
	if err != nil {
		return nil, nil, err
	}
	if !active {
		return nil, nil, ErrExecutionInactive
	}
	if unitIdx < 0 || unitIdx >= g.UnitCount() || g.UnitKindAt(unitIdx) != graph.UnitGroup {
		return nil, nil, fmt.Errorf("unit %d is not a group unit", unitIdx)
	}

	gs, ok := e.state.(GroupStateStore)
	if !ok {
		return nil, nil, fmt.Errorf("state store does not support group leases")
	}

	// Read the authoritative lease state from the backend.
	lease, err := gs.(GroupLeaseReader).GetGroupLease(ctx, execID, unitIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("recover group lease: %w", err)
	}
	if lease == nil {
		return nil, nil, fmt.Errorf("%w: execution %s unit %d", ErrGroupLeaseNotActive, execID, unitIdx)
	}

	gm := g.GroupMetaAt(unitIdx)
	pkg, pkgHash, err := graph.ProjectGroupPackage(g, unitIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("project group package for recovery: %w", err)
	}

	var deadline time.Time
	if gm.Timeout > 0 {
		deadline = lease.IssuedAt.Add(gm.Timeout)
	}

	taskLease := &TaskLease{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
		Task: Task{
			ExecutionID: execID,
			NodeName:    gm.Name,
			NodeIdx:     gm.EntryIdx,
			UnitIdx:     unitIdx,
			Type:        TaskTypeGroupExec,
		},
		Attempt:  lease.Attempt,
		IssuedAt: lease.IssuedAt,
		TTL:      lease.TTL,
		NodeType: GroupNodeType,
	}

	payload := &GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     lease.GroupID,
		GroupID:         lease.GroupID,
		GroupUnitIdx:    unitIdx,
		WorkflowVersion: g.WorkflowVersion(),
		GraphHash:       g.Hash(),
		PackageHash:     pkgHash,
		Package:         pkg,
		Input:           lease.Input,
		IdempotencyKey:  lease.IdempotencyKey,
		Deadline:        deadline,
	}

	return taskLease, payload, nil
}

// GroupLeaseReader is an optional interface on GroupStateStore for reading back
// a persisted group lease. Required for recovery (RecoverGroupLease).
type GroupLeaseReader interface {
	GetGroupLease(ctx context.Context, id types.ExecutionID, unitIdx int) (*GroupLease, error)
}

// CommitGroupResult validates and commits a group execution result through
// the GroupStateStore. It maps the GroupResult to the internal commit request,
// validates exit ports against the compiled boundary outputs, and propagates
// downstream arrivals.
func (e *Engine) CommitGroupResult(ctx context.Context, lease *TaskLease, res GroupResult) (CommitOutcome, error) {
	if lease == nil {
		return "", fmt.Errorf("nil lease")
	}

	g, active, err := e.loadActiveGraph(ctx, lease.Task.ExecutionID)
	if err != nil {
		return "", err
	}
	if !active {
		return CommitOutcomeExecutionInactive, nil
	}

	unitIdx := lease.Task.UnitIdx
	if unitIdx < 0 || unitIdx >= g.UnitCount() || g.UnitKindAt(unitIdx) != graph.UnitGroup {
		return "", fmt.Errorf("invalid group unit %d", unitIdx)
	}

	gm := g.GroupMetaAt(unitIdx)

	// Validate exits against compiled boundary outputs.
	validExits := make(map[string]bool)
	for _, bo := range gm.BoundaryOutputs {
		key := fmt.Sprintf("%d:%s", bo.Src.NodeIdx, bo.Src.Port)
		validExits[key] = true
	}

	exits := make([]GroupExit, 0, len(res.Exits))
	for _, exit := range res.Exits {
		nodeIdx, ok := g.NodeIndex(exit.NodeName)
		if !ok {
			return "", fmt.Errorf("exit references unknown node %q", exit.NodeName)
		}
		key := fmt.Sprintf("%d:%s", nodeIdx, exit.Port)
		if !validExits[key] {
			return "", fmt.Errorf("exit (%s, %s) is not a valid boundary output", exit.NodeName, exit.Port)
		}
		exits = append(exits, GroupExit{
			NodeName: exit.NodeName,
			Port:     exit.Port,
			Data:     exit.Data,
		})
	}

	// Determine fatality based on outcome and OnError strategy.
	//
	// The default arm is load-bearing, not defensive boilerplate: Outcome
	// crosses the wire as a bare JSON string from a remote runner, so an
	// unrecognized value is reachable input, not an impossible state. Without
	// it such a value falls through to fatal=false and commits as a non-fatal
	// failure, releasing downstream on a result nothing understood. This used
	// to guard only "suspended" (durable group suspend, since removed); the
	// rest of the space was silently accepted.
	fatal := false
	switch res.Outcome {
	case GroupOutcomeSuccess:
	case GroupOutcomeFailed:
		fatal = groupOnErrorFatal(gm.OnError)
	case GroupOutcomeTimeout, GroupOutcomeCanceled:
		fatal = true
	default:
		return "", fmt.Errorf("unknown group outcome %q", res.Outcome)
	}

	// Delegate to the existing commitGroup path.
	//
	// IssuedAt must be carried over from the outer TaskLease (set by
	// BuildGroupLease/RecoverGroupLease from the persisted lease): commitGroup
	// derives the exec-duration observation from lease.IssuedAt, and a
	// zero-value time.Time there would report ~56 years of duration on every
	// remote commit — this is the only production path (a runner reports back
	// through CommitGroupResult), so leaving it unset would silently corrupt
	// the histogram in production while local (executeGroup) tests, which set
	// IssuedAt directly, stayed green. See commitGroup's own IssuedAt guard for
	// the belt-and-braces defense against a future third construction site
	// making the same mistake.
	groupLease := &GroupLease{
		ExecutionID:  lease.Task.ExecutionID,
		GroupUnitIdx: unitIdx,
		GroupID:      res.GroupExecID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		IssuedAt:     lease.IssuedAt,
	}

	err = e.commitGroup(ctx, g, groupLease, gm, exits, fatal, groupResultError(res), true)
	if err != nil {
		return "", err
	}
	return CommitOutcomeAccepted, nil
}

func groupResultError(res GroupResult) error {
	if res.Outcome == GroupOutcomeSuccess {
		return nil
	}
	if res.Error != "" {
		return fmt.Errorf("%s", res.Error)
	}
	return fmt.Errorf("group %s", res.Outcome)
}

// RenewGroupLease extends a group lease deadline. Returns true if renewal
// succeeded, false if the lease is no longer active (expired, committed, or
// token mismatch).
func (e *Engine) RenewGroupLease(ctx context.Context, lease *TaskLease, extend time.Duration) (bool, error) {
	if lease == nil {
		return false, fmt.Errorf("nil lease")
	}
	gs, ok := e.state.(GroupStateStore)
	if !ok {
		return false, fmt.Errorf("state store does not support group leases")
	}
	newDeadline := time.Now().UTC().Add(extend)
	start := time.Now()
	renewed, err := gs.RenewGroupLease(ctx, lease.Task.ExecutionID, lease.Task.UnitIdx, lease.LeaseToken, newDeadline)
	d := time.Since(start)

	// result is one of "ok" / "not_renewed" / "error" — never "fenced".
	// RenewGroupLease's backend contract returns a bare (bool, error): both the
	// local state store and the Redis Lua script fold three distinct causes —
	// the lease no longer exists, the unit already reached a terminal state, or
	// the caller's token lost a real fence race — into the same
	// (false, nil) return. There is no signal left at this layer to tell them
	// apart, so reporting "fenced" here would be inventing a distinction the
	// backend never gave us. "not_renewed" names the ambiguity instead of
	// hiding it.
	result := "ok"
	switch {
	case err != nil:
		result = "error"
	case !renewed:
		result = "not_renewed"
	}
	e.notifyGroupLeaseRenew(ctx, result, d)
	return renewed, err
}
