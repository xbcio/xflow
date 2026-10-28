package local

import (
	"context"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var _ engine.SubWorkflowLinkStore = (*memoryState)(nil)

// BeginSubWorkflowWait parks a claimed node on its child execution: the node
// moves Running (or Committing) → Waiting with its lease identity preserved,
// the link is stored, and any delivery intents are written, all under the
// single memory mutex.
//
// A second call with the same lease is refused (accepted=false) rather than
// reapplied: the predecessor status must still be pre-Waiting, exactly like the
// expansion transition this mirrors. Replay safety does not depend on it — the
// link's child id is derived from the parent execution, node, activation and
// generation, so a redelivery that re-parks after a reclaim writes the same
// child — but a silent second "success" would hide a dispatch that ran twice
// against two different leases.
func (s *memoryState) BeginSubWorkflowWait(_ context.Context, lease *engine.TaskLease, link engine.SubWorkflowLink, entries []engine.OutboxEntry) (bool, error) {
	if lease == nil || link.ChildExecutionID == "" || link.ParentNode != lease.Task.NodeName {
		return false, engine.ErrInvalidLeaseToken
	}
	for _, entry := range entries {
		if entry.ID == "" {
			return false, engine.ErrInvalidLeaseToken
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)
	node := s.nodes[key]
	if !matchesSubWorkflowLease(node, lease) {
		return false, nil
	}
	nodeCopy := *node
	nodeCopy.Status = types.NodeStatusWaiting
	// The node record carries the child too (design §6): it is the parent-side
	// half of the drill-down, and unlike the link it survives the commit that
	// concludes the node, so a finished wait is still auditable.
	nodeCopy.ChildExecutionID = link.ChildExecutionID
	s.nodes[key] = &nodeCopy
	linkCopy := link
	s.subWorkflowLinks[key] = &linkCopy
	for _, entry := range entries {
		s.putOutboxLocked(lease.Task.ExecutionID, entry.ID, entry.Task, entry.AvailableAt)
	}
	return true, nil
}

// GetSubWorkflowLink reads back the link a node parked under. Absence is not an
// error: a node that never parked, and a graph that predates the capability,
// both answer ok=false.
func (s *memoryState) GetSubWorkflowLink(_ context.Context, id types.ExecutionID, nodeName string) (engine.SubWorkflowLink, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	link := s.subWorkflowLinks[memoryNodeKey(id, nodeName)]
	if link == nil {
		return engine.SubWorkflowLink{}, false, nil
	}
	return *link, true, nil
}

// matchesSubWorkflowLease is the fence for parking a node: the node must still
// be claimed by exactly this lease. Waiting is excluded on purpose — a park of
// an already-parked node changes nothing — while Committing is accepted so a
// park racing an in-flight commit of the same lease is decided by the commit,
// whose own fence accepts both Running and Waiting.
func matchesSubWorkflowLease(node *engine.NodeSnapshot, lease *engine.TaskLease) bool {
	if node == nil {
		return false
	}
	if node.Status != types.NodeStatusRunning && node.Status != types.NodeStatusCommitting {
		return false
	}
	return node.LeaseID == lease.LeaseID &&
		node.LeaseToken == lease.LeaseToken &&
		node.Attempt == lease.Attempt &&
		node.ActivationID == lease.Task.ActivationID
}

// subWorkflowConclusionKey keys one child's conclusion under its parent node. The
// child id is part of the key so a retry's next generation can never read the
// previous generation's conclusion; the execID/ prefix is what lets execution
// deletion clear it with the parent's other records.
func subWorkflowConclusionKey(parentID types.ExecutionID, nodeName string, childID types.ExecutionID) string {
	return memoryNodeKey(parentID, nodeName) + "/" + string(childID)
}

// RecordSubWorkflowConclusion stores how a child ended under the mutex, as an
// unconditional idempotent write: a child's terminal state is unique, so a
// repeat writes the same content. It is not fenced on the link or the node, for
// the same reason the Redis backend's is not — the reclaim path must be able to
// record while the parent's lease is stale.
func (s *memoryState) RecordSubWorkflowConclusion(_ context.Context, parentID types.ExecutionID, nodeName string, conclusion engine.SubWorkflowConclusion) error {
	if conclusion.ChildExecutionID == "" || nodeName == "" {
		return engine.ErrInvalidLeaseToken
	}
	conclusion.Output = cloneData(conclusion.Output)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subWorkflowConclusions[subWorkflowConclusionKey(parentID, nodeName, conclusion.ChildExecutionID)] = &conclusion
	return nil
}

// GetSubWorkflowConclusion reads the conclusion recorded for exactly one child.
// Absence is not an error.
func (s *memoryState) GetSubWorkflowConclusion(_ context.Context, parentID types.ExecutionID, nodeName string, childID types.ExecutionID) (engine.SubWorkflowConclusion, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conclusion := s.subWorkflowConclusions[subWorkflowConclusionKey(parentID, nodeName, childID)]
	if conclusion == nil {
		return engine.SubWorkflowConclusion{}, false, nil
	}
	out := *conclusion
	out.Output = cloneData(out.Output)
	return out, true, nil
}
