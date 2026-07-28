package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
)

// GroupLeasePayload carries the full context a remote runner needs to execute a
// group. It is the single source of truth for the group's entry input (not
// TaskLease.Input which is nil for group tasks).
type GroupLeasePayload struct {
	ProtocolVersion int                  `json:"protocol_version"`
	GroupExecID     string               `json:"group_exec_id"`
	GroupID         string               `json:"group_id"`
	GroupUnitIdx    int                   `json:"group_unit_idx"`
	WorkflowVersion string               `json:"workflow_version"`
	GraphHash       string               `json:"graph_hash"`
	PackageHash     string               `json:"package_hash"`
	Package         *graph.GroupPackage   `json:"package,omitempty"`
	Input           *types.Input          `json:"input,omitempty"`
	IdempotencyKey  string               `json:"idempotency_key"`
	Deadline        time.Time            `json:"deadline,omitempty"`
	// SignalJournal carries the full signal history for resume replay.
	SignalJournal []GroupSignal `json:"signal_journal,omitempty"`
	// TaskType distinguishes initial group exec from resume.
	TaskType     TaskType      `json:"task_type,omitempty"`
}

// ErrGroupLeaseAlreadyActive is returned when BuildGroupLease cannot acquire
// because the unit is already running under an active lease.
var ErrGroupLeaseAlreadyActive = errors.New("group lease already active")

// ErrGroupSuspendNotSupported is returned when a group result tries to
// suspend (not yet supported).
var ErrGroupSuspendNotSupported = errors.New("group suspend not supported in this milestone")

// BuildGroupLease assembles a group lease for a queued group task. Unlike
// BuildTaskLease, the lease payload carries the full GroupPackage and entry
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

	leaseID := LeaseID("lease-" + uuid.New().String())
	leaseToken := LeaseToken("token-" + uuid.New().String())
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

	// Build the external TaskLease wrapper. Input is nil — the group payload
	// is the single source of truth.
	taskLease := &TaskLease{
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Task:       *t,
		Attempt:    groupLease.Attempt,
		IssuedAt:   issuedAt,
		TTL:        ttl,
		NodeType:   "xflow.group",
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
		return nil, nil, fmt.Errorf("no active group lease for execution %s unit %d", execID, unitIdx)
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
		NodeType: "xflow.group",
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

	if res.Outcome == GroupOutcomeSuspended {
		return "", ErrGroupSuspendNotSupported
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
	fatal := false
	switch res.Outcome {
	case GroupOutcomeFailed:
		fatal = groupOnErrorFatal(gm.OnError)
	case GroupOutcomeTimeout, GroupOutcomeCanceled:
		fatal = true
	}

	// Delegate to the existing commitGroup path.
	groupLease := &GroupLease{
		ExecutionID:  lease.Task.ExecutionID,
		GroupUnitIdx: unitIdx,
		GroupID:      res.GroupExecID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
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
	return gs.RenewGroupLease(ctx, lease.Task.ExecutionID, lease.Task.UnitIdx, lease.LeaseToken, newDeadline)
}
