package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

var _ LeaseExpander = (*fakeState)(nil)
var _ DurableLeaseExpander = (*fakeState)(nil)

func (f *fakeState) BeginTaskExpansionWithOutbox(_ context.Context, lease *TaskLease, children []SubExecution, entries []OutboxEntry) (bool, error) {
	if lease == nil || len(children) != len(entries) {
		return false, ErrInvalidLeaseToken
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	node := f.nodes[fakeExpansionNodeKey(lease)]
	if !matchesFakeExpansionLease(node, lease, types.NodeStatusCommitting) {
		return false, nil
	}
	for index, child := range children {
		if child.ChildExecID == "" || child.ParentExecID != lease.Task.ExecutionID || child.ParentNode != lease.Task.NodeName || entries[index].ID == "" {
			return false, ErrInvalidLeaseToken
		}
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	f.nodes[fakeExpansionNodeKey(lease)] = &copy
	key := fakeExpansionSubKey(lease)
	for index, child := range children {
		childCopy := child
		childCopy.Result = cloneMap(child.Result)
		f.subExecs[key] = append(f.subExecs[key], &childCopy)
		f.putAtomicOutboxLocked(lease.Task.ExecutionID, entries[index].ID, entries[index].Task, entries[index].AvailableAt)
	}
	return true, nil
}

func (f *fakeState) BeginTaskExpansion(_ context.Context, lease *TaskLease) (bool, error) {
	if lease == nil {
		return false, ErrInvalidLeaseToken
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	node := f.nodes[fakeExpansionNodeKey(lease)]
	if !matchesFakeExpansionLease(node, lease, types.NodeStatusCommitting) {
		return false, nil
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	f.nodes[fakeExpansionNodeKey(lease)] = &copy
	return true, nil
}

func (f *fakeState) CreateExpandedSubExecution(_ context.Context, lease *TaskLease, sub *SubExecution) (bool, error) {
	if lease == nil || sub == nil || sub.ChildExecID == "" {
		return false, ErrInvalidLeaseToken
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !matchesFakeExpansionLease(f.nodes[fakeExpansionNodeKey(lease)], lease, types.NodeStatusWaiting) {
		return false, nil
	}
	key := fakeExpansionSubKey(lease)
	for _, existing := range f.subExecs[key] {
		if existing.ChildExecID == sub.ChildExecID {
			return true, nil
		}
	}
	copy := *sub
	copy.Result = cloneMap(sub.Result)
	f.subExecs[key] = append(f.subExecs[key], &copy)
	return true, nil
}

func (f *fakeState) CompleteExpandedSubExecution(_ context.Context, lease *TaskLease, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, bool, []map[string]any, error) {
	if lease == nil || childExecID == "" {
		return false, false, nil, ErrInvalidLeaseToken
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !matchesFakeExpansionLease(f.nodes[fakeExpansionNodeKey(lease)], lease, types.NodeStatusWaiting) {
		return false, false, nil, nil
	}
	subs := f.subExecs[fakeExpansionSubKey(lease)]
	found := false
	for _, sub := range subs {
		if sub.ChildExecID != childExecID {
			continue
		}
		found = true
		if sub.Status == types.ExecutionStatusRunning {
			sub.Status = status
			sub.Result = cloneMap(result)
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
	sorted := append([]*SubExecution(nil), subs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].BatchIndex < sorted[j].BatchIndex })
	results := make([]map[string]any, 0, len(sorted))
	for _, sub := range sorted {
		if sub.Result != nil {
			results = append(results, cloneMap(sub.Result))
		}
	}
	return true, true, results, nil
}

func matchesFakeExpansionLease(node *NodeSnapshot, lease *TaskLease, status types.NodeStatus) bool {
	return node != nil && node.Status == status && node.LeaseID == lease.LeaseID && node.LeaseToken == lease.LeaseToken && node.Attempt == lease.Attempt && node.ActivationID == lease.Task.ActivationID
}

func fakeExpansionNodeKey(lease *TaskLease) string {
	return string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
}

func fakeExpansionSubKey(lease *TaskLease) string {
	return fmt.Sprintf("%s/%s/%s", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID)
}
