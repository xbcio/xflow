package engine

import "testing"

// 锁定 TaskType 数值：既有值不得漂移，group 值必须追加（spec §11.2）。
func TestTaskTypeAppendOnly(t *testing.T) {
	if TaskTypeNodeExec != 0 || TaskTypeNodeResume != 1 || TaskTypeNodeAdvance != 2 ||
		TaskTypeNodeSkip != 3 || TaskTypeNodeBatch != 4 {
		t.Fatal("existing TaskType values must not shift")
	}
	if TaskTypeGroupExec != 5 || TaskTypeGroupResume != 6 {
		t.Fatalf("group task types must append at end: exec=%d resume=%d",
			TaskTypeGroupExec, TaskTypeGroupResume)
	}
}

// 编译期断言：契约类型齐备（无 backend 时不实例化）。
func TestGroupContractTypesExist(t *testing.T) {
	_ = GroupResult{Outcome: GroupOutcomeSuccess, Exits: []GroupExitResult{{Port: "main"}}}
	_ = GroupCommitRequest{Outcome: GroupOutcomeSuccess}
	_ = GroupLease{}
	var _ GroupCommitResult
}
