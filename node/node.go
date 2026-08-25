package node

import (
	"context"

	core "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/action"
	codepkg "github.com/xbcio/xflow/node/internal/code"
	scriptpkg "github.com/xbcio/xflow/node/internal/code/script"
	"github.com/xbcio/xflow/node/internal/code/script/wasm"
	"github.com/xbcio/xflow/node/internal/flow"
	"github.com/xbcio/xflow/node/internal/group"
	supplypkg "github.com/xbcio/xflow/node/internal/supply"
	"github.com/xbcio/xflow/node/internal/transform"

	// Blank import: linking node/trigger runs the five trigger subpackages'
	// init() self-registration against node/registry. Without this line the
	// build still succeeds and every trigger unit test still passes (they
	// construct nodes directly), but registry.LookupTrigger("xflow.trigger.kafka")
	// returns not-found and a runner cannot activate any trigger. See
	// node/trigger_registry_test.go, which exists to fail loudly if this
	// import is ever removed.
	_ "github.com/xbcio/xflow/node/trigger"
)

type ExecuteFunc = core.ExecuteFunc
type Definition = core.Definition

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
type MapNode = flow.MapNode
type WaitMode = flow.WaitMode
type WaitNode = flow.WaitNode

type ApprovalMode = group.ApprovalMode
type ApprovalParams = group.ApprovalParams
type ApprovalNode = group.ApprovalNode
type NotificationNode = group.NotificationNode

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

// ScriptFile creates a script node whose code comes from a local file. The file
// is read and stored in the ArtifactStore at AddWorkflow time (resolveArtifacts);
// at runtime the ScriptNode resolves the content by digest from the artifact
// store. Use this for large artifacts (wasm modules) to avoid inlining multi-MB
// blobs in workflow definitions.
func ScriptFile(path string) *ScriptNode { return scriptpkg.Script("").File(path) }

// ScriptArtifact creates a script node whose code is already stored in the
// ArtifactStore under the given content-addressable digest. Use this when the
// caller manages artifact storage externally.
func ScriptArtifact(digest string) *ScriptNode { return scriptpkg.Script("").Artifact(digest) }

// SupplyExternalNode declares a consumed SupplyResource. Re-exported so callers
// outside the module can build one.
type SupplyExternalNode = supplypkg.ExternalNode

// SupplyStaticNode declares supply content carried by the definition itself.
type SupplyStaticNode = supplypkg.StaticNode

// SupplyExternal declares that this workflow consumes the named SupplyResource.
// The node never executes; it makes the dependency visible on the graph so the
// activation-time readiness gate and the server-side reverse index can see it.
// Pair it with WorkflowBuilder.DependsOn to declare which node consumes it.
func SupplyExternal(resource string) *SupplyExternalNode { return supplypkg.External(resource) }

// SupplyStatic declares supply content that travels with the definition. Use it
// when there is genuinely no remote source — NOT as a fallback for one that is
// merely unavailable (see the readiness gate).
func SupplyStatic(content []byte) *SupplyStaticNode { return supplypkg.Static(content) }

// SetScriptObserver installs the global observer for xflow.script executions.
func SetScriptObserver(o scriptpkg.Observer) {
	scriptpkg.SetObserver(o)
}

// WasmObserver receives wasm reactor pool observations (config swaps,
// instance lifecycle, module compilation, borrow wait). Re-exported from the
// internal wasm package's Observer so a host process can install one without
// importing an internal package.
type WasmObserver = wasm.Observer

// SetWasmObserver installs the global observer for wasm reactor pool activity.
// Call once at startup (or pass nil to remove it). Unlike SetScriptObserver,
// which is engine-family-wide, this only covers the wasm/wazero-reactor
// engine — script engines other than wasm have no reactor pool to observe.
func SetWasmObserver(o WasmObserver) {
	wasm.SetObserver(o)
}

// HTTPHostPolicy decides whether an xflow.http node may dispatch to a given
// host: a nil return permits the request, a non-nil error aborts it before any
// bytes are sent. Re-exported from the internal action package's HostPolicy so
// a host process can install one without importing an internal package — which
// it cannot do, the internal rule being a compile error outside node/.
type HTTPHostPolicy = action.HostPolicy

// NewHTTPHostPolicy builds a host policy from optional allow and deny lists.
// deny takes precedence over allow, matching is case-insensitive, and ports are
// ignored. Both lists empty returns nil, which is the no-filtering default.
func NewHTTPHostPolicy(allow, deny []string) HTTPHostPolicy {
	return action.NewHostPolicy(allow, deny)
}

// SetHTTPHostPolicy installs the SSRF allow/deny policy for xflow.http nodes.
// It is consulted before the initial request and again on every redirect hop,
// so a redirect cannot smuggle a request to a host the policy would reject.
// Call once at startup; pass nil to remove it.
//
// Until this forwarder existed the policy was unreachable rather than merely
// unset: the variable, its constructor and both consult sites all live in
// node/internal/action, and Go's internal rule makes that package a compile
// error for any importer outside node/ — including sdk/xflow and every
// embedder. The variable's own doc invited "embedded runtimes" to set it,
// which was a promise the language would not let them keep.
//
// There is still no configuration or CLI surface for this. A deployment that
// wants host filtering has to call this from its own startup code.
func SetHTTPHostPolicy(p HTTPHostPolicy) {
	action.HTTPHostPolicy = p
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

// RegisterWasmSupplyConsumer makes a wasm module consume the named supply node's
// content: a change rebuilds its instance pool through a last-good-preserving
// atomic swap. Call it at activation time — at process start a runner does not
// yet know which workflows it will host.
func RegisterWasmSupplyConsumer(code string, supplyNode string) error {
	return scriptpkg.RegisterWasmSupplyConsumer(code, supplyNode)
}

// RegisterWasmSupplyConsumerByDigest is RegisterWasmSupplyConsumer for modules
// identified by their artifact store digest (e.g. "sha256:<64 hex>"). Use this
// when the module is stored in the ArtifactStore and ScriptNode uses
// artifact_digest rather than an inline code string.
func RegisterWasmSupplyConsumerByDigest(digest string, supplyNode string) error {
	return scriptpkg.RegisterWasmSupplyConsumerByDigest(digest, supplyNode)
}

// UnregisterWasmSupplyConsumerByDigest undoes RegisterWasmSupplyConsumerByDigest.
// Call it when the workflow is deactivated on this runner, so a module it no
// longer hosts stops rebuilding its pool on every supply change. Unregistering a
// pair that was never registered is a no-op.
func UnregisterWasmSupplyConsumerByDigest(digest string, supplyNode string) {
	scriptpkg.UnregisterWasmSupplyConsumerByDigest(digest, supplyNode)
}

// CompileWasmModule eagerly compiles a wasm module (base64 code string) into the
// reactor engine cache. Used by resolveArtifacts so the engine exists before
// supply consumers attempt to configure its pool.
func CompileWasmModule(ctx context.Context, code string) error {
	return scriptpkg.CompileWasmModule(ctx, code)
}

// CompileWasmModuleBytes is CompileWasmModule for callers that already hold the
// module's raw bytes, as the artifact store and the runner activation path both
// do. Encoding a 6.85 MB module to base64 only for the callee to decode it back
// costs two multi-MB allocations and both transforms; this entry point skips
// them. Use CompileWasmModule only when the caller genuinely starts from base64.
func CompileWasmModuleBytes(ctx context.Context, wasmBytes []byte) error {
	return scriptpkg.CompileWasmModuleBytes(ctx, wasmBytes)
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
func Merge(mode MergeMode) *MergeNode { return flow.Merge(mode) }

// Deprecated: xflow.split was never implemented. graph.Compile rejects any
// workflow containing a split node -- before that rejection existed, such a
// workflow did not fail, it hung until its deadline (a split node expands into
// batch tasks, but batches need a projected body and split has no body
// parameter). Use Map with a body instead.
func Split(itemsExpr string) *SplitNode { return flow.Split(itemsExpr) }

func Map(itemsExpr string, batchSize int) *MapNode { return flow.Map(itemsExpr, batchSize) }
func Wait(signalName string) *WaitNode             { return flow.Wait(signalName) }
func WaitDuration(duration string) *WaitNode       { return flow.WaitDuration(duration) }

func Approval(approvers []string, mode ApprovalMode) *ApprovalNode {
	return group.Approval(approvers, mode)
}
func Notification(channel string, to any) *NotificationNode {
	return group.Notification(channel, to)
}
