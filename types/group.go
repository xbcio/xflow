package types

import "time"

// GroupDef 声明一组连通节点作为 co-location 单元（spec §3.1）。
// 成员关系只存于 Members（单一真相源）；节点本身不重复记 group。
type GroupDef struct {
	Name    string   `json:"name,omitempty"`
	Members []string `json:"members,omitempty"`
	// RunnerSelector 决定整组放置；成员不得再单独设置 selector。
	RunnerSelector *RunnerSelector `json:"runner_selector,omitempty"`
	// OnError 是组级失败策略。空值等价于 OnErrorStop。
	//
	// 组级支持 OnErrorStop、OnErrorContinue（以及等价的空值）与
	// OnErrorOutput；OnErrorMainOutput 与任何未知取值都在 graph.Compile
	// 被拒绝（validateGroupOnError）。不存在 OnErrorFail。
	//
	// OnErrorOutput 要求 ErrorOutputs 至少声明一条目标——组没有像节点那样
	// 预先编译好的 "error" 端口可路由，ErrorOutputs 就是这条路由的声明。
	// 任一成员终止失败（重试耗尽）时，组的其余成员不再执行，组改为在其声明的
	// error 端口上 fire，错误信息（失败成员名、错误、错误详情）落在组名下的
	// 输出（与节点输出同一存取路径，下游通过 $('group_name').json 读取）。
	// 语义与机制见 NODE-GROUP-COLOCATION.md §12.2（原「未支持」记录已更新为
	// 实现决定）。
	OnError string `json:"on_error,omitempty"`
	// ErrorOutputs declares the downstream targets for the group's
	// synthesized "error" output port. Only meaningful when OnError is
	// OnErrorOutput; validateGroupOnError rejects a non-empty ErrorOutputs on
	// any other policy (a declared-but-unreachable route is a compile error,
	// not a silent no-op) and rejects OnErrorOutput with an empty
	// ErrorOutputs (nothing to route to).
	ErrorOutputs []Connection `json:"error_outputs,omitempty"`
	// Retry 是组级重试；组级 retry = 从入口整组重跑。
	Retry *RetrySettings `json:"retry,omitempty"`
	// Timeout 是组的业务 deadline（非 lease TTL）。
	Timeout time.Duration `json:"timeout,omitempty"`
	// Mode 空值=durable（Redis 权威 + outbox，可 inspect）；
	// "transient"=ExecutionModeTransient（短 TTL fence，不做 SQL projection）。
	Mode string `json:"mode,omitempty"`
	// ActivationReplicas is the desired number of distinct runners that host a
	// trigger-entry group. Zero preserves the legacy single-activation behavior;
	// one is therefore equivalent to zero. Sibling replicas are placed on
	// different runners and join the same source-level consumer group.
	ActivationReplicas uint32 `json:"activation_replicas,omitempty"`
}

// GroupMode 常量。
const (
	GroupModeDurable   = ""
	GroupModeTransient = "transient"
)
