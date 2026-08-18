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
	// 组级只支持 OnErrorStop 与 OnErrorContinue（以及等价的空值）；
	// OnErrorOutput / OnErrorMainOutput 与任何未知取值都在 graph.Compile
	// 被拒绝（validateGroupOnError）。原因是组没有错误输出端口可路由：
	// GroupMeta.BoundaryOutputs 只由真实跨界成员边推导，编译期不会为
	// OnError 合成端口，CommitGroupResult 也会拒绝不在 BoundaryOutputs 里
	// 的 exit。建这个机制的范围见 NODE-GROUP-COLOCATION.md §12.2。
	// 不存在 OnErrorFail。
	OnError string `json:"on_error,omitempty"`
	// Retry 是组级重试；组级 retry = 从入口整组重跑。
	Retry *RetrySettings `json:"retry,omitempty"`
	// Timeout 是组的业务 deadline（非 lease TTL）。
	Timeout time.Duration `json:"timeout,omitempty"`
	// Mode 空值=durable（Redis 权威 + outbox，可 inspect）；
	// "transient"=ExecutionModeTransient（短 TTL fence，不做 SQL projection）。
	Mode string `json:"mode,omitempty"`
}

// GroupMode 常量。
const (
	GroupModeDurable   = ""
	GroupModeTransient = "transient"
)
