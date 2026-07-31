package node

import (
	"context"
	"time"

	core "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/action"
	codepkg "github.com/xbcio/xflow/node/internal/code"
	scriptpkg "github.com/xbcio/xflow/node/internal/code/script"
	"github.com/xbcio/xflow/node/internal/code/script/wasm"
	"github.com/xbcio/xflow/node/internal/flow"
	"github.com/xbcio/xflow/node/internal/group"
	"github.com/xbcio/xflow/node/internal/transform"
	nodetrigger "github.com/xbcio/xflow/node/internal/trigger"
)

type ExecuteFunc = core.ExecuteFunc
type Definition = core.Definition
type TriggerActivateFunc = core.TriggerActivateFunc
type TriggerDefinition = core.TriggerDefinition

type HTTPMethod = core.HTTPMethod
type HTTPNode = action.HTTPNode
type DatabaseNode = action.DatabaseNode
type GRPCNode = action.GRPCNode

type FunctionNode = codepkg.FunctionNode
type UserFunc = codepkg.UserFunc
type ScriptNode = scriptpkg.ScriptNode
type SetNode = transform.SetNode
type PickNode = transform.PickNode
type RenameNode = transform.RenameNode
type FilterNode = transform.FilterNode
type SortField = transform.SortField
type SortNode = transform.SortNode
type LimitNode = transform.LimitNode
type RemoveDuplicatesNode = transform.RemoveDuplicatesNode
type AggregateOperation = transform.AggregateOperation
type AggregateNode = transform.AggregateNode

type MergeMode = core.MergeMode
type StartNode = flow.StartNode
type EndNode = flow.EndNode
type IfNode = flow.IfNode
type SwitchRule = flow.SwitchRule
type SwitchNode = flow.SwitchNode
type MergeNode = flow.MergeNode
type SplitNode = flow.SplitNode
type LoopNode = flow.LoopNode
type WaitMode = flow.WaitMode
type WaitNode = flow.WaitNode

type ApprovalMode = group.ApprovalMode
type ApprovalParams = group.ApprovalParams
type ApprovalNode = group.ApprovalNode
type NotificationNode = group.NotificationNode

type TimerTriggerNode = nodetrigger.TimerTriggerNode
type CronTriggerNode = nodetrigger.CronTriggerNode
type WebhookTriggerNode = nodetrigger.WebhookTriggerNode
type KafkaTriggerNode = nodetrigger.KafkaTriggerNode
type RedisHubTriggerNode = nodetrigger.RedisHubTriggerNode

// HTTPEntrySeedRuntime is the production types.EntrySeedRuntime that posts
// entry-unit seed admissions to the control plane. It is re-exported here so
// callers outside node/internal (e.g. the runner's ActivationHandler) can
// construct a per-activation, generation-stamped seed runtime.
type HTTPEntrySeedRuntime = nodetrigger.HTTPEntrySeedRuntime

const (
	HTTPGet    = core.HTTPGet
	HTTPPost   = core.HTTPPost
	HTTPPut    = core.HTTPPut
	HTTPDelete = core.HTTPDelete
	HTTPPatch  = core.HTTPPatch

	MergeWaitAll = core.MergeWaitAll
	MergeWaitAny = core.MergeWaitAny

	WaitNodeType   = flow.WaitNodeType
	WaitModeSignal = flow.WaitModeSignal
	WaitModeTimer  = flow.WaitModeTimer

	ApprovalNodeType   = group.ApprovalNodeType
	ApprovalAny        = group.ApprovalAny
	ApprovalAll        = group.ApprovalAll
	ApprovalSequential = group.ApprovalSequential
)

func Define(nodeType string, execute ExecuteFunc) *Definition {
	return core.Define(nodeType, execute)
}

func DefineTrigger(nodeType string, activate TriggerActivateFunc) *TriggerDefinition {
	return core.DefineTrigger(nodeType, activate)
}

func HTTP(method, rawURL string) *HTTPNode { return action.HTTP(method, rawURL) }
func Database(operation, table, credential string) *DatabaseNode {
	return action.Database(operation, table, credential)
}
func GRPC(service, method, host string) *GRPCNode { return action.GRPC(service, method, host) }

func Function(name string) *FunctionNode { return codepkg.Function(name) }
func Expr(code string) *FunctionNode     { return codepkg.Expr(code) }
func RegisterFunc(name string, fn UserFunc) {
	codepkg.RegisterFunc(name, fn)
}
func LookupFunc(name string) (UserFunc, bool) {
	return codepkg.LookupFunc(name)
}
func Script(code string) *ScriptNode { return scriptpkg.Script(code) }

// SetScriptObserver installs the global observer for xflow.script executions.
func SetScriptObserver(o scriptpkg.Observer) {
	scriptpkg.SetObserver(o)
}

// WarmupScriptEngines absorbs script-engine cold start before traffic arrives:
// js/qjs's ~330 ms QuickJS-wasm compile and the wasm reactor runtime open
// (which resolves the on-disk compilation cache). Hosts should call it once at
// startup. An error means one engine failed to warm — log it and continue, since
// every engine also warms lazily on its first request.
func WarmupScriptEngines(ctx context.Context) error { return scriptpkg.Warmup(ctx) }

// PrewarmWasmModule registers a base64 wasm module (as carried by a ScriptNode
// running on wasm/wazero-reactor) and its config so WarmupScriptEngines
// compiles it and builds its instance pool at startup instead of under the
// first request's deadline. Call before WarmupScriptEngines.
func PrewarmWasmModule(code string, cfg any) { scriptpkg.PrewarmWasm(code, cfg) }

// WasmConfigLoader is the external configuration source interface for a wasm
// reactor module. Re-exported from the wasm package for external registration.
type WasmConfigLoader = wasm.ConfigLoader

// RegisterWasmConfigLoader associates a ConfigLoader with a wasm module code
// string. During warmup the loader is invoked once to build the initial pool;
// if ttl > 0 a background goroutine polls for version changes and triggers pool
// swaps. Call before WarmupScriptEngines.
func RegisterWasmConfigLoader(code string, loader WasmConfigLoader, ttl time.Duration) {
	scriptpkg.RegisterWasmConfigLoader(code, loader, ttl)
}
func Set(fields map[string]any) *SetNode {
	return transform.Set(fields)
}
func Pick(fields ...string) *PickNode {
	return transform.Pick(fields...)
}
func Rename(mapping map[string]string) *RenameNode {
	return transform.Rename(mapping)
}
func Filter(itemsExpr string, condition string) *FilterNode {
	return transform.Filter(itemsExpr, condition)
}
func SortAsc(field string) SortField  { return transform.SortAsc(field) }
func SortDesc(field string) SortField { return transform.SortDesc(field) }
func Sort(itemsExpr string, fields ...SortField) *SortNode {
	return transform.SortItems(itemsExpr, fields...)
}
func Limit(itemsExpr string, max int) *LimitNode {
	return transform.Limit(itemsExpr, max)
}
func RemoveDuplicates(itemsExpr string, fields ...string) *RemoveDuplicatesNode {
	return transform.RemoveDuplicates(itemsExpr, fields...)
}
func Aggregate(itemsExpr string) *AggregateNode {
	return transform.Aggregate(itemsExpr)
}

func Start() *StartNode { return flow.Start() }
func End() *EndNode     { return flow.End() }
func IF(condition string) *IfNode {
	return flow.IF(condition)
}
func Switch(rules []SwitchRule, defaultOutput string) *SwitchNode {
	return flow.Switch(rules, defaultOutput)
}
func SwitchExpr(expression string, defaultOutput string) *SwitchNode {
	return flow.SwitchExpr(expression, defaultOutput)
}
func Merge(mode MergeMode) *MergeNode                { return flow.Merge(mode) }
func Split(itemsExpr string) *SplitNode              { return flow.Split(itemsExpr) }
func Loop(itemsExpr string, batchSize int) *LoopNode { return flow.Loop(itemsExpr, batchSize) }
func Wait(signalName string) *WaitNode               { return flow.Wait(signalName) }
func WaitDuration(duration string) *WaitNode         { return flow.WaitDuration(duration) }

func Approval(approvers []string, mode ApprovalMode) *ApprovalNode {
	return group.Approval(approvers, mode)
}
func Notification(channel string, to any) *NotificationNode {
	return group.Notification(channel, to)
}

func TimerTrigger() *TimerTriggerNode       { return nodetrigger.TimerTrigger() }
func CronTrigger() *CronTriggerNode         { return nodetrigger.CronTrigger() }
func WebhookTrigger() *WebhookTriggerNode   { return nodetrigger.WebhookTrigger() }
func KafkaTrigger() *KafkaTriggerNode       { return nodetrigger.KafkaTrigger() }
func RedisHubTrigger() *RedisHubTriggerNode { return nodetrigger.RedisHubTrigger() }
