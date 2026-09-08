package script

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"

	"github.com/xbcio/xflow/exprx"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/registry"

	_ "github.com/xbcio/xflow/node/internal/code/script/js"

	"github.com/xbcio/xflow/node/internal/code/script/wasm"
	"github.com/xbcio/xflow/node/supply"
)

// ScriptNode implements xflow.script — runs a sandboxed dynamic script.
type ScriptNode struct {
	nodeinternal.BaseNode
	Code        string
	Lang        string
	RuntimeName string
	Creds       []string
	RootNames   []string

	// ArtifactDigest is the content-addressable digest (e.g. "sha256:<hex>") of
	// a script artifact stored in the ArtifactStore. When set, Execute resolves
	// the code bytes via Input.ArtifactCode instead of expecting them inline.
	ArtifactDigest string

	// FilePath is a local filesystem path to the script artifact. Used by the
	// ScriptFile DSL constructor; resolveArtifacts in AddWorkflow reads the file,
	// Puts it to the ArtifactStore, and rewrites this to ArtifactDigest before
	// the workflow runs. FilePath must never reach Execute — it is resolved at
	// registration time only.
	FilePath string
}

// Script creates a script node. The caller MUST explicitly choose a language
// and runtime — there are no implicit defaults, so the choice (and its
// security/perf tradeoff) is always made consciously.
//
//	node.Script(code).Language("js").Runtime("goja")
//	node.Script(code).Language("js").Runtime("qjs")
//	node.Script(b64wasm).Language("wasm").Runtime("wazero")
//	node.Script(code).Language("js").Runtime("goja").Credentials("aes_key", "api_token")
//
// Runtime selection guide:
//   - js/goja: fastest cold start, pooled VMs, lowest per-call overhead.
//     CANNOT interrupt tight pure-computation loops (e.g. `while(true){}`).
//     Pick for short, well-bounded scripts.
//   - js/qjs: QuickJS via wasm. ~330ms first-load (cached process-wide),
//     ~3ms per call after. Genuine mid-execution termination. Pick when
//     scripts may be long-running or CPU-bound.
//   - wasm/wazero: any language compiled to wasip1. Strictest sandbox,
//     true ctx cancellation. Pick for untrusted code or non-JS guests.
func Script(code string) *ScriptNode {
	return &ScriptNode{Code: code}
}

func (n *ScriptNode) Language(lang string) *ScriptNode { n.Lang = lang; return n }
func (n *ScriptNode) Runtime(rt string) *ScriptNode    { n.RuntimeName = rt; return n }
func (n *ScriptNode) Credentials(names ...string) *ScriptNode {
	n.Creds = names
	return n
}

// Roots declares which expression roots the script actually reads, so the
// engine can ship only those instead of the whole environment.
//
// Without it every script receives every root, and BuildExprEnv publishes the
// node's input TWICE — flattened at the top level and again under $input — so a
// guest that reads one of them pays for both. Measured on a production traffic
// pipeline: the clean node's payload was 2.96x the raw Kafka record, of which
// 67.7% was two copies of a $item it never reads. The cost is not the marshal
// (~1%); it is the guest rebuilding those objects inside the sandbox, where the
// same bytes decode ~32x slower than natively (see wasm.stripItems).
//
// Declaring nothing keeps the full environment, so this cannot break a script
// that reads a root nobody thought to list. Declare only what the guest reads:
//
//	node.Script(b64wasm).Language("wasm").Runtime("wazero").Roots("$item")
//
// A declared root that is absent from the environment is not an error — it is
// simply not sent, the same as today.
func (n *ScriptNode) Roots(names ...string) *ScriptNode {
	n.RootNames = names
	return n
}

// Artifact sets the content-addressable digest of a pre-stored script artifact.
// When set, Execute resolves the code from the artifact store at runtime.
func (n *ScriptNode) Artifact(digest string) *ScriptNode {
	n.ArtifactDigest = digest
	n.Code = ""
	return n
}

// File sets a local filesystem path to the script source/binary. resolveArtifacts
// (called by AddWorkflow) reads this file, stores it in the ArtifactStore, and
// rewrites the parameter to artifact_digest. File must not reach runtime.
func (n *ScriptNode) File(path string) *ScriptNode {
	n.FilePath = path
	n.Code = ""
	return n
}

func (n *ScriptNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.script",
		DisplayName: "Script",
		Params: []types.ParamSpec{
			{Name: "language", DisplayName: "Language", Type: types.ParamString, Required: true, Description: "Language family: js | wasm (no default — choose explicitly)"},
			{Name: "runtime", DisplayName: "Runtime", Type: types.ParamString, Required: true, Description: "Engine: js->goja|qjs, wasm->wazero (no default — choose explicitly)"},
			{Name: "code", DisplayName: "Code", Type: types.ParamString, Required: false, Description: "JS source (js) or base64 wasm module (wasm); omit when artifact_digest is set"},
			{Name: "artifact_digest", DisplayName: "Artifact Digest", Type: types.ParamString, Required: false, Description: "Content-addressable digest (sha256:<hex>) of the script in the artifact store"},
			{Name: "credentials", DisplayName: "Credentials", Type: types.ParamArray, Required: false, Description: "Declared credential names injected as $credentials"},
			{Name: "roots", DisplayName: "Roots", Type: types.ParamArray, Required: false, Description: "Expression roots the script reads; omit to send the whole environment"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *ScriptNode) NodeType() string { return "xflow.script" }
func (n *ScriptNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *ScriptNode) RawParams() any {
	// Always emit language and runtime — there are no defaults. Empty values
	// flow through to Execute which surfaces them as config errors, so the
	// DSL output stays a faithful mirror of what was set on the builder.
	params := map[string]any{
		"language": n.Lang,
		"runtime":  n.RuntimeName,
	}
	switch {
	case n.FilePath != "":
		// Marker for resolveArtifacts: AddWorkflow reads the file, Puts to
		// ArtifactStore, and rewrites to artifact_digest before graph.Compile.
		params["__artifact_file_path"] = n.FilePath
	case n.ArtifactDigest != "":
		params["artifact_digest"] = n.ArtifactDigest
	default:
		params["code"] = n.Code
	}
	if len(n.Creds) > 0 {
		params["credentials"] = n.Creds
	}
	if len(n.RootNames) > 0 {
		params["roots"] = n.RootNames
	}
	return params
}

func (n *ScriptNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	start := time.Now()
	language, _ := input.Params["language"].(string)
	runtime, _ := input.Params["runtime"].(string)

	code, _ := input.Params["code"].(string)
	// The digest travels with the code into the engine. It is what lets the wasm
	// reactor find its compiled module by a 64-byte key instead of comparing the
	// ~9 MB base64 string on every message; see engine.Source. It stays empty on
	// the inline-code path, where there is no digest to carry.
	digest, _ := input.Params["artifact_digest"].(string)
	if code == "" {
		// artifact_digest path: resolve code bytes from the artifact store.
		// Resolution is memoised by (namespace, digest, language) — a digest names
		// one immutable byte sequence, so re-reading and re-encoding it on every
		// message is pure waste on the hottest path in the node.
		if digest != "" {
			// Checked before the cache, not through it. A cache hit would let an
			// execution whose dispatch path never wired a resolver run code that
			// another execution fetched, so a wiring defect would surface only on
			// a cold process — see Input.HasArtifactResolver.
			if !input.HasArtifactResolver() {
				observeExecute(ctx, language, runtime, "config", time.Since(start))
				return nil, types.NewPermanentError("script.artifact_unavailable",
					"xflow.script: artifact_digest is set but no artifact resolver is configured")
			}
			resolved, ok, err := sharedArtifactCode.get(ctx, input.Namespace(), digest, language, input.ArtifactCode)
			if err != nil {
				observeExecute(ctx, language, runtime, "config", time.Since(start))
				return nil, types.NewTransientError("script.artifact_fetch",
					fmt.Sprintf("xflow.script: failed to fetch artifact %s: %v", digest, err))
			}
			if !ok {
				observeExecute(ctx, language, runtime, "config", time.Since(start))
				return nil, types.NewPermanentError("script.artifact_unavailable",
					"xflow.script: artifact_digest is set but no artifact resolver is configured")
			}
			code = resolved
		}
	}
	if code == "" {
		observeExecute(ctx, language, runtime, "config", time.Since(start))
		return nil, types.NewPermanentError("script.code_required", "xflow.script: code parameter is required (or set artifact_digest)")
	}
	if language == "" {
		observeExecute(ctx, language, runtime, "config", time.Since(start))
		return nil, types.NewPermanentError("script.language_required", "xflow.script: language parameter is required (choose: js | wasm)")
	}
	if runtime == "" {
		observeExecute(ctx, language, runtime, "config", time.Since(start))
		return nil, types.NewPermanentError("script.runtime_required", "xflow.script: runtime parameter is required (js -> goja|qjs, wasm -> wazero)")
	}

	eng, ok := engine.Lookup(language, runtime)
	if !ok {
		observeExecute(ctx, language, runtime, "config", time.Since(start))
		return nil, types.NewPermanentError("script.unknown_engine", fmt.Sprintf("xflow.script: unknown engine (language=%q, runtime=%q)", language, runtime))
	}

	// Name the node for whatever the engine reports about its own cost. This is
	// the last layer that knows: below it the engine sees a source string and a
	// globals map, so a runner hosting several script nodes reports every eval
	// against one merged series and "which node is burning CPU" has no answer.
	//
	// Attached after the lookup because nothing above it reaches an engine, and
	// carried in the context rather than in Source so engines that do not report
	// cost neither see nor forward it.
	ctx = engine.WithNodeIdentity(ctx, engine.NodeIdentity{
		Workflow: input.WorkflowName,
		Node:     input.NodeName,
	})

	declared := readNameList(input.Params["credentials"])
	// Input.Credential has no error return; a nil value means "not found", which
	// ResolveCredentials turns into a config error for a declared-but-absent name.
	creds, first, err := engine.ResolveCredentials(declared, func(name string) (map[string]any, error) {
		return input.Credential(name), nil
	})
	if err != nil {
		observeExecute(ctx, language, runtime, "config", time.Since(start))
		return nil, types.NewPermanentError("script.credentials", err.Error())
	}

	globals := buildScriptGlobals(input, creds, first)

	// Enforce a per-execution timeout so runtimes that honour ctx cancellation
	// (js/qjs, wasm/wazero) terminate a runaway script at the deadline. Without
	// this, memory-backend dispatch (context.Background()) leaves the script
	// with no deadline, so `while(true){}` blocks the worker forever.
	timeout := readScriptTimeout(input.Params)
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Execution-time supply-consumer wiring (spec §4.3.1). The control plane can
	// no longer name the module at compile time -- artifact_digest may be an
	// expression -- so it sends a DECLARATION instead, and this is where the real
	// digest, just resolved by boundary evaluation, meets it.
	//
	// Everything before the guard is here because the ORDER is load-bearing, for
	// the same reason the activation-time version documents at
	// service/runner/trigger_activation_handler.go:271-286: RegisterConsumer
	// notifies immediately when the content is already cached, and the wasm
	// notify handler silently succeeds when the module is not compiled yet. The
	// registry then records the content as accepted and never redelivers it,
	// while registration has already marked the module source-driven -- a state
	// in which it refuses every message. Compiling first closes that window.
	//
	// The declaration lookup happens BEFORE the digest is inspected. A
	// declaration can exist only when the control plane resolved a non-empty
	// artifact_digest at activation time (collectWasmBindings never emits one for
	// an inline-code node), so a live declaration with an empty resolved digest
	// here means the boundary expression failed to resolve -- not "nothing to
	// guard". Checking `digest != ""` first would let that case run the node's
	// inline code against zero rules, which is exactly the leak this task exists
	// to close.
	if language == wasmScriptLanguage {
		if declared := supplyDeclarations.lookup(input.WorkflowName, input.NodeName); len(declared) > 0 {
			if digest == "" {
				observeExecute(ctx, language, runtime, "config", time.Since(start))
				return nil, types.NewTransientError("script.supply_consumer", fmt.Sprintf(
					"node declares supply consumers %v but artifact_digest resolved to empty; refusing to evaluate against an empty rule set", declared))
			}
			if err := ensureWasmSupplyConsumers(ctx, digest, code, declared, input.WorkflowName, input.NodeName); err != nil {
				observeExecute(ctx, language, runtime, "config", time.Since(start))
				// Transient, not Permanent: a supply that has not arrived yet is
				// self-healing on redelivery, and a Permanent verdict would make
				// Kafka drop the record outright.
				return nil, types.NewTransientError("script.supply_consumer", err.Error())
			}
		}
	}

	src := engine.Source{Code: code, Digest: digest}

	// Batch path: a Kafka batch trigger delivers {messages: [...], count: N}.
	// Detected by shape rather than a parameter so the same script node works on
	// both the batch and single-message trigger paths without reconfiguration.
	if records, ok := batchRecords(input.Data); ok {
		var results []any
		var batchErr error
		if be, hasBatch := eng.(engine.BatchEngine); hasBatch {
			results, batchErr = be.ExecuteBatch(ctx, src, records, globals)
		} else {
			results, batchErr = engine.ExecuteBatchSerial(ctx, eng, src, records, globals)
		}
		if batchErr != nil {
			observeExecute(ctx, language, runtime, "error", time.Since(start))
			if ctx.Err() != nil {
				return nil, types.NewTransientError("script.timeout", batchErr.Error())
			}
			if types.IsPermanent(batchErr) {
				// Same split as the single-record path below, minus its skip
				// branch, and the asymmetry is deliberate rather than an
				// oversight waiting to be tidied up.
				//
				// Down there, "skip" drops one record. Here it would drop the
				// whole batch, which is a different act wearing the same name --
				// and nobody downstream could tell the difference, because a
				// short results array looks exactly like a batch where no record
				// matched a rule.
				//
				// It also cannot trigger. Both batch implementations already skip
				// per-record internally and return nil: the wasm reactor's
				// ExecuteBatch consults engine.IsRecordSkippable per record, and
				// ExecuteBatchSerial `continue`s past every non-ctx error. So a
				// record-skippable error reaching this line would mean a
				// BatchEngine that does not, and treating that as batch-fatal is
				// the honest reading -- we do not know which records it stands
				// for.
				return nil, batchErr
			}
			return &types.Output{Data: map[string]any{"error": batchErr.Error()}, Port: "error"}, nil
		}
		data := map[string]any{
			"results":   results,
			"count":     len(results),
			"hit_count": countHits(results),
		}
		b, sizeErr := checkResultSize(data)
		if sizeErr != nil {
			observeExecute(ctx, language, runtime, "error", time.Since(start))
			return &types.Output{Data: map[string]any{"error": sizeErr.Error()}, Port: "error"}, nil
		}
		// Host-attested: the collector lives in the context, which no guest can
		// reach, and the digest is the one this node resolved code from — not
		// anything the guest returned.
		//
		// Recorded only on success, where success means "reaches the main port",
		// not merely "the guest returned". The size cap above is the reason this
		// sits below it rather than above: an oversized result routes to the
		// error port exactly like a trap does, and a manifest that attests one
		// but not the other would be claiming a distinction the consumer cannot
		// see. Consumers stamp the digest onto the business row this node
		// produced; both branches produce no such row.
		types.ArtifactUseCollectorFrom(ctx).Record(input.NodeName, digest)
		observeOutputBytes(ctx, language, runtime, len(b))
		observeExecute(ctx, language, runtime, "main", time.Since(start))
		return &types.Output{Data: data, Port: "main"}, nil
	}

	result, err := eng.Execute(ctx, src, globals, engine.DefaultHelpers())
	if err != nil {
		// If the per-execution context expired (deadline or cancellation), the
		// failure is a transient system condition regardless of how the runtime
		// surfaces it: goja raises an Interrupt error, while qjs/wazero wrap
		// ctx.Err(). Classifying by ctx.Err() catches all three runtimes so the
		// classification survives the wire instead of collapsing to a bare
		// transient string. Any other script error is the script's own
		// deterministic outcome and is routed via the explicit "error" port
		// (engine/outputPortRetryError), preserving existing OnError routing.
		if ctx.Err() != nil {
			observeExecute(ctx, language, runtime, "error", time.Since(start))
			return nil, types.NewTransientError("script.timeout", err.Error())
		}
		// A per-record failure condemns THIS record and nothing else: the engine
		// instance is either still healthy or was already torn down and replaced
		// before the error got here, so the next record evaluates on a clean one.
		// Drop the record and report success, which is exactly what the batch
		// path (engine.ExecuteBatchSerial and the wasm reactor's ExecuteBatch)
		// does with the identical error -- both `continue` past it.
		//
		// This branch exists because those two paths disagreed, and the
		// disagreement was invisible in tests: a map body's item always takes
		// THIS path (one record per item never has the {messages:[...]} shape
		// that selects the batch path above), so the batch path's skip never ran
		// in the deployment that mattered. The stall is not hypothetical -- it is
		// what an oversized cleaned record did to 14 of 18 Kafka partitions.
		//
		// Ordered ABOVE the permanence check, and that order is load-bearing. A
		// wasm host trap is BOTH permanent and record-skippable, because the two
		// verdicts answer different questions: permanence says "redelivering
		// these bytes cannot help", skippability adds "and the blame stops at
		// this record". When both hold, skipping strictly dominates -- it
		// advances the offsets and keeps the partition alive, whereas returning
		// the error accomplishes none of what the branch below is for:
		// entryseed.go's admission check declines to admit a failed group on BOTH
		// of its branches, so a permanently-classified trap parks the commit
		// frontier exactly as an unclassified one does. With the permanence check
		// first, this branch was dead for the very error that motivated it, and
		// every test still passed because they exercise the classifier directly
		// rather than through this ordering.
		//
		// Returning main with no data rather than routing to the error port is
		// what makes this a skip instead of a failure. The cost is real and is
		// why the metric below is not optional: a dropped record is
		// indistinguishable downstream from a record that matched no rule. Only
		// outcome="skipped" tells them apart.
		//
		// No ArtifactUseCollector.Record here, for the reason the batch path
		// states: an attestation claims this node produced a business row from
		// that digest, and a skipped record produces no such row.
		if engine.IsRecordSkippable(err) {
			observeExecute(ctx, language, runtime, "skipped", time.Since(start))
			return &types.Output{Data: map[string]any{}, Port: "main"}, nil
		}
		// ...unless the engine classified the failure as permanent itself AND did
		// not confine the blame to one record. What reaches here says "these
		// bytes will never succeed" without naming a record to drop, so it is the
		// node that failed rather than a record that was rejected.
		//
		// That classification only reaches the engine if it travels AS an error.
		// The error port flattens a failure into Output.Data["error"], and
		// engine/outputPortRetryError rebuilds it with errors.New(msg) -- a fresh
		// error with an empty unwrap chain. buildEffectiveClassification then
		// reports Classified:false and GroupExecResult.Deterministic stays false,
		// so a failure that cannot succeed on retry is reported as retryable.
		//
		// Only permanence changes lanes. An unclassified throw and a
		// self-declared transient failure both keep the error port, so workflows
		// that branch on it are unaffected.
		if types.IsPermanent(err) {
			observeExecute(ctx, language, runtime, "error", time.Since(start))
			return nil, err
		}
		observeExecute(ctx, language, runtime, "error", time.Since(start))
		return &types.Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}
	// Pre-flight check: if the context already expired (e.g. a parent cancelled
	// before the engine started, or a very tight deadline), classify as timeout
	// instead of accepting a result from a raced engine execution.
	if ctx.Err() != nil {
		observeExecute(ctx, language, runtime, "error", time.Since(start))
		return nil, types.NewTransientError("script.timeout", ctx.Err().Error())
	}
	data := engine.MapResult(result)
	b, sizeErr := checkResultSize(data)
	if sizeErr != nil {
		observeExecute(ctx, language, runtime, "error", time.Since(start))
		return &types.Output{Data: map[string]any{"error": sizeErr.Error()}, Port: "error"}, nil
	}
	// Same host attestation as the batch path above, and below the size cap for
	// the same reason: the error port is the error port whether the guest
	// trapped or merely overflowed. The record never travels through data, so
	// the guest's return value cannot influence it whatever the order.
	types.ArtifactUseCollectorFrom(ctx).Record(input.NodeName, digest)

	observeOutputBytes(ctx, language, runtime, len(b))
	observeExecute(ctx, language, runtime, "main", time.Since(start))
	return &types.Output{Data: data, Port: "main"}, nil
}

// checkResultSize enforces DefaultMaxOutputBytes on the JSON-encoded result.
// Done at the node layer so every engine inherits the same cap without
// having to thread limits through each runtime. It returns the encoded
// bytes so the caller can reuse them for telemetry instead of re-marshalling.
func checkResultSize(data map[string]any) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("xflow.script: encode result: %w", err)
	}
	if len(b) > engine.DefaultMaxOutputBytes {
		return b, &engine.OutputSizeError{Size: len(b), Limit: engine.DefaultMaxOutputBytes}
	}
	return b, nil
}

// readNameList accepts both []string (Go DSL) and []any (decoded YAML/JSON).
func readNameList(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		names := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				names = append(names, s)
			}
		}
		return names
	default:
		return nil
	}
}

// readScriptTimeout resolves the per-execution timeout from node parameters.
// Accepts a numeric value (seconds) or a duration string ("5s", "90s"). Falls
// back to engine.DefaultScriptTimeout when absent or invalid so a script never
// runs without a deadline.
func readScriptTimeout(params map[string]any) time.Duration {
	switch v := params["timeout"].(type) {
	case float64:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case int:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case int64:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case string:
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return engine.DefaultScriptTimeout
}

// buildScriptGlobals assembles the engine environment for one execution.
//
// The "code" entry is removed from the $params view. $params otherwise mirrors
// input.Params verbatim, and input.Params["code"] IS the script being executed —
// so leaving it in hands every script its own source as part of its input, on
// every message. For a base64-encoded wasm module that is several MB per call
// inside a 16 MiB linear memory (engine.DefaultWasmMemoryPages): the guest's
// alloc traps in runtime.mallocgcLarge and the failure surfaces as the opaque
// `wasm reactor: alloc: wasm error: unreachable`.
//
// The rest of $params is preserved — scripts legitimately read their own
// configuration through it, so dropping the whole root would trade a memory
// defect for a silent behaviour change.
//
// The copy is shallow but MUST NOT be skipped: input.Params belongs to the
// engine's activation record, and deleting the key in place would leave the node
// unable to run a second time ("code parameter is required" on the retry).
func buildScriptGlobals(input *types.Input, creds map[string]any, first any) map[string]any {
	env := exprx.BuildExprEnv(input, map[string]any{
		"$credentials": creds,
		"$credential":  first,
		"$params":      paramsWithoutCode(input.Params),
	})
	return projectRoots(env, readNameList(input.Params["roots"]))
}

// engineRoots are the keys BuildExprEnv publishes that a projection must never
// drop, because the engine — not the script — depends on them.
//
// $config carries the reactor's rule set and is consumed host-side by
// wasm.splitConfig before the payload is ever encoded; dropping it here would
// leave the pool unconfigured. The credential roots are what a Credentials()
// declaration exists to deliver, so a Roots() declaration must not silently
// revoke them.
var engineRoots = map[string]bool{
	"$config":      true,
	"$credentials": true,
	"$credential":  true,
}

// projectRoots narrows env to the declared roots plus engineRoots, returning env
// unchanged when nothing was declared.
//
// The two routes a value takes into the payload are mutually exclusive here, and
// that is the point. BuildExprEnv publishes input.Data BOTH flattened at the env
// top level AND whole under $input, so every value arrives twice; a projection
// that kept both copies of a declared name would save nothing. A declaration
// therefore picks one route:
//
//	Roots("$item", "tags")  →  env["$item"], env["tags"];  no $input at all
//	Roots("$input")         →  env["$input"] whole;        nothing flattened
//
// The second form is for a guest that reads input.Data wholesale (a clean-stage
// guest does). Mixing them — declaring "$input" alongside named keys — keeps
// $input whole and drops the flattened duplicates, since $input already contains
// them.
//
// A script that reads a root it did not declare sees it absent. That is the
// opt-in cost: the declaration is a claim about what the guest reads, and an
// incomplete claim is a behaviour change rather than an error. Declaring nothing
// keeps everything, so no existing script is affected.
//
// Neither env nor input.Data is mutated: both belong to the engine's activation
// record, so deleting in place would corrupt a retry of the same node and would
// strip the roots from the js and expression paths that still promise them.
func projectRoots(env map[string]any, declared []string) map[string]any {
	if len(declared) == 0 {
		return env
	}
	keep := make(map[string]bool, len(declared))
	for _, name := range declared {
		keep[name] = true
	}

	out := make(map[string]any, len(keep)+len(engineRoots))
	for k, v := range env {
		// $input is never kept by the top-level sweep: undeclared it is pure
		// duplication, and declared it is handled below so the whole-map form is
		// preserved rather than rebuilt.
		if k == "$input" {
			continue
		}
		if keep[k] || engineRoots[k] {
			out[k] = v
		}
	}
	if keep["$input"] {
		if inner, ok := env["$input"]; ok {
			out["$input"] = inner
		}
	}
	return out
}

// paramsWithoutCode returns params minus the "code" key, sharing the remaining
// values. It returns nil for nil so a caller that passed no params still sees a
// nil $params rather than an empty map.
func paramsWithoutCode(params map[string]any) map[string]any {
	if params == nil {
		return nil
	}
	if _, has := params["code"]; !has {
		return params
	}
	out := make(map[string]any, len(params)-1)
	for k, v := range params {
		if k == "code" {
			continue
		}
		out[k] = v
	}
	return out
}

// wasmScriptLanguage is the language value that selects the wasm engine. It is
// spelled once here so the supply guard and the engine lookup cannot drift —
// engine.Register("wasm", ...) in wasm/reactor.go and wasm/wazero.go both key
// off this exact literal.
const wasmScriptLanguage = "wasm"

// ensureWasmSupplyConsumers makes the module named by digest a registered
// consumer of every declared supply, and REFUSES if that did not take effect.
//
// The steady-state cost is len(supplies) map probes: once configured, the
// predicate short-circuits before any compile or registration is attempted.
//
// The verdict is deliberately not "registration returned nil". See
// wasm.SupplyConfiguredByDigest: registration succeeds against a module that has
// never been handed content, and that module then evaluates every record against
// no rules at all. That is not a crash, it is silent pass-through -- for a cleansing
// pipeline it means the credential a clean rule exists to strip is never
// stripped.
//
// workflowName and nodeName identify the node THIS guard is running for, and
// become the wasm-side registration's owner (spec Z.4 / Z.8): the registry
// stores one consumer per (digest, supplyNode), and this call site is one of
// three across the codebase that register against that same slot (the other
// two are the activation-time legacy binding and the warm-up declaration
// consumer). Without a distinct owner per call site, one site's release would
// silently erase the registrations the other two installed. This site itself
// never releases -- a script node's registration lives for the process, and
// the "node:" prefix keeps this owner namespace from colliding with the
// activation-identity owners the legacy binding uses (see
// service/runner/trigger_activation_handler.go).
//
// Errors name the digest and the supply node and nothing else. The module code
// and the node's params never appear: this error is logged.
func ensureWasmSupplyConsumers(ctx context.Context, digest, code string, supplies []string, workflowName, nodeName string) error {
	if wasm.SupplyConfiguredByDigest(digest) {
		return nil
	}
	// Compile BEFORE registering. See the caller's comment for why the reverse
	// order leaves the module permanently source-driven with no configuration.
	if err := wasm.CompileModule(ctx, code); err != nil {
		return fmt.Errorf("compile wasm module %s declared as a consumer of %v: %w",
			digest, supplies, err)
	}
	owner := "node:" + workflowName + "/" + nodeName
	for _, supplyNode := range supplies {
		if err := wasm.RegisterSupplyConsumerByDigest(digest, supplyNode, owner, supply.Default); err != nil {
			return fmt.Errorf("register wasm module %s as consumer of supply %q: %w",
				digest, supplyNode, err)
		}
	}
	if !wasm.SupplyConfiguredByDigest(digest) {
		return fmt.Errorf("wasm module %s is declared source-driven for %v but holds no "+
			"supply-borne configuration; refusing to evaluate against an empty rule set",
			digest, supplies)
	}
	return nil
}

func init() { registry.Register(&ScriptNode{}) }

// batchRecords extracts the message list from a Kafka batch trigger's data.
// Returns ok=false for single-message input so that path is untouched.
func batchRecords(data map[string]any) ([]any, bool) {
	if data == nil {
		return nil, false
	}
	raw, has := data["messages"]
	if !has {
		return nil, false
	}
	switch list := raw.(type) {
	case []any:
		return list, true
	case []map[string]any:
		out := make([]any, 0, len(list))
		for _, m := range list {
			out = append(out, m)
		}
		return out, true
	default:
		return nil, false
	}
}

// countHits counts results whose "tags" is a non-empty list. Only hits travel
// downstream to the consumer, so this is the number that matters for the hit-rate metric:
// a rule set that stops matching anything is otherwise indistinguishable from an
// idle topic.
func countHits(results []any) int {
	n := 0
	for _, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		switch tags := m["tags"].(type) {
		case []any:
			if len(tags) > 0 {
				n++
			}
		case []string:
			if len(tags) > 0 {
				n++
			}
		}
	}
	return n
}
