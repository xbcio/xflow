package local

import (
	"context"
	"fmt"
	"sort"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var _ engine.LeaseExpander = (*memoryState)(nil)
var _ engine.DurableLeaseExpander = (*memoryState)(nil)

func (s *memoryState) BeginTaskExpansionWithOutbox(_ context.Context, lease *engine.TaskLease, children []engine.SubExecution, entries []engine.OutboxEntry) (bool, error) {
	if lease == nil || len(children) != len(entries) {
		return false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.nodes[memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesExpansionLease(node, lease, types.NodeStatusCommitting) {
		return false, nil
	}
	key := expansionSubExecutionKey(lease)
	for index, child := range children {
		if child.ChildExecID == "" || child.ParentExecID != lease.Task.ExecutionID || child.ParentNode != lease.Task.NodeName || entries[index].ID == "" {
			return false, engine.ErrInvalidLeaseToken
		}
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	s.nodes[memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)] = &copy
	for index, child := range children {
		childCopy := child
		childCopy.Result = cloneData(child.Result)
		s.subExecs[key] = append(s.subExecs[key], &childCopy)
		s.putOutboxLocked(lease.Task.ExecutionID, entries[index].ID, entries[index].Task, entries[index].AvailableAt)
	}
	return true, nil
}

func (s *memoryState) BeginTaskExpansion(_ context.Context, lease *engine.TaskLease) (bool, error) {
	if lease == nil {
		return false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.nodes[memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesExpansionLease(node, lease, types.NodeStatusCommitting) {
		return false, nil
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	s.nodes[memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)] = &copy
	return true, nil
}

func (s *memoryState) CreateExpandedSubExecution(_ context.Context, lease *engine.TaskLease, sub *engine.SubExecution) (bool, error) {
	if lease == nil || sub == nil || sub.ChildExecID == "" {
		return false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.nodes[memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesExpansionLease(node, lease, types.NodeStatusWaiting) {
		return false, nil
	}
	key := expansionSubExecutionKey(lease)
	for _, existing := range s.subExecs[key] {
		if existing.ChildExecID == sub.ChildExecID {
			return true, nil
		}
	}
	copy := *sub
	copy.Result = cloneData(sub.Result)
	s.subExecs[key] = append(s.subExecs[key], &copy)
	return true, nil
}

func (s *memoryState) CompleteExpandedSubExecution(_ context.Context, lease *engine.TaskLease, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, bool, []map[string]any, error) {
	if lease == nil || childExecID == "" {
		return false, false, nil, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.nodes[memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesExpansionLease(node, lease, types.NodeStatusWaiting) {
		return false, false, nil, nil
	}
	subs := s.subExecs[expansionSubExecutionKey(lease)]
	found := false
	for _, sub := range subs {
		if sub.ChildExecID != childExecID {
			continue
		}
		found = true
		if sub.Status == types.ExecutionStatusRunning {
			sub.Status = status
			sub.Result = cloneData(result)
		}
		break
	}
	if !found {
		return false, false, nil, nil
	}
	for _, sub := range subs {
		if sub.Status == types.ExecutionStatusRunning {
			return false, true, nil, nil
		}
	}
	return true, true, expansionResults(subs), nil
}

func matchesExpansionLease(node *engine.NodeSnapshot, lease *engine.TaskLease, status types.NodeStatus) bool {
	return node != nil && node.Status == status && node.LeaseID == lease.LeaseID && node.LeaseToken == lease.LeaseToken && node.Attempt == lease.Attempt && node.ActivationID == lease.Task.ActivationID
}

func expansionSubExecutionKey(lease *engine.TaskLease) string {
	return fmt.Sprintf("%s/%s/%s", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID)
}

func expansionResults(subs []*engine.SubExecution) []map[string]any {
	sorted := append([]*engine.SubExecution(nil), subs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].BatchIndex < sorted[j].BatchIndex })
	results := make([]map[string]any, 0, len(sorted))
	for _, sub := range sorted {
		if sub.Result != nil {
			results = append(results, cloneData(sub.Result))
		}
	}
	return results
}
