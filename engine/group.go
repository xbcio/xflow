package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

type GroupOutcome string

const (
	GroupOutcomeSuccess   GroupOutcome = "success"
	GroupOutcomeFailed    GroupOutcome = "failed"
	GroupOutcomeTimeout   GroupOutcome = "timeout"
	GroupOutcomeCanceled  GroupOutcome = "canceled"
	GroupOutcomeSuspended GroupOutcome = "suspended" // 里程碑 D 实现
)

// GroupExitResult 是一个实际 fire 的出口端口输出。
type GroupExitResult struct {
	NodeIdx  int
	NodeName string
	Port     string
	Data     map[string]any
}

// GroupResult 是 runner（里程碑 A 为 fake executor）对一次组执行的回报。
type GroupResult struct {
	ProtocolVersion int
	GroupExecID     string
	Attempt         int
	Outcome         GroupOutcome
	Exits           []GroupExitResult
	Error           string
}

// GroupLease 是整组的所有权租约，语义对齐既有 TaskLease。
type GroupLease struct {
	LeaseID        LeaseID
	LeaseToken     LeaseToken
	Attempt        int
	ExecutionID    types.ExecutionID
	GroupUnitIdx   int
	GroupID        string
	IdempotencyKey string
	Input          *types.Input
	IssuedAt       time.Time
	TTL            time.Duration
	Namespace      namespace.Namespace
}

// GroupCommitRequest 携带一次 group 级原子 commit 所需信息。
type GroupCommitRequest struct {
	ExecutionID  types.ExecutionID
	GroupUnitIdx int
	GroupID      string
	LeaseID      LeaseID
	LeaseToken   LeaseToken
	Attempt      int
	Outcome      GroupOutcome
	Exits        []GroupExitResult
	Error        string
	Fatal        bool
	// Downstream 是本次组执行点亮的下游 unit 到达描述（复用 DownstreamArrival，
	// 见 atomic.go）。commit 在同一原子转换里对每个下游 unit 做 DECR in-degree +
	// active 计数 + wait_all/wait_any 阈值判定，并按结果写 execute/skip outbox intent。
	Downstream []DownstreamArrival
}

// GroupCommitResult 复用既有 CommitOutcome 语义（accepted/stale/duplicate/inactive）。
type GroupCommitResult struct {
	Outcome         CommitOutcome
	Applied         bool
	ExecutionDone   bool
	ExecutionStatus types.ExecutionStatus
	OutboxIDs       []string
}

// GroupStateStore 是 group 级原子状态转换能力（可选能力，仿 LegacyNodeCommitter/
// DurableLeaseExpander）。local 与 Redis backend 必须通过同一 contract suite。
// 里程碑 A 定义 acquire/renew/commit 子集；suspend/resume/revoke/seed-triggered
// 在后续里程碑扩展为独立接口。
type GroupStateStore interface {
	// AcquireGroupLease 原子把 group unit 从 pending/retry-ready 转入 running，
	// 记录 lease/token/attempt/deadline 与入口 checkpoint。已被占用返回 false。
	AcquireGroupLease(ctx context.Context, lease *GroupLease) (acquired bool, err error)
	// RenewGroupLease token+attempt fenced 延长当前 owner 的 deadline；
	// terminal/旧 token 返回 false。
	RenewGroupLease(ctx context.Context, id types.ExecutionID, unitIdx int, token LeaseToken, deadline time.Time) (renewed bool, err error)
	// CommitGroup 一次原子完成：校验后的 boundary 输出、group unit terminal、
	// lease 清理、remaining/failed 计数（按 unit）、下游 outbox。禁止退化为
	// "先写状态再 enqueue"。
	CommitGroup(ctx context.Context, req GroupCommitRequest) (GroupCommitResult, error)
}

// GroupCommitter is a convenience alias for backends that expose only the
// commit capability without the full lease lifecycle. Concrete backends
// typically implement GroupStateStore; this narrower interface allows test
// doubles and composition.
type GroupCommitter interface {
	CommitGroup(ctx context.Context, req GroupCommitRequest) (GroupCommitResult, error)
}

// GroupLeaseExpirer is an optional capability for reclaiming expired group
// leases. When a running group lease has passed its deadline, ExpireGroupLease
// transitions it back to retry-ready and increments the attempt counter.
// The return value indicates whether the lease was actually expired (it may
// have already been committed or renewed).
type GroupLeaseExpirer interface {
	ExpireGroupLease(ctx context.Context, id types.ExecutionID, unitIdx int, token LeaseToken) (expired bool, err error)
}
