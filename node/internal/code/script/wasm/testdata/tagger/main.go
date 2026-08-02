// tagger is an API-traffic cleansing/tagging reactor guest.
//
// It differs from the reference `reactor` guest in what it produces: reactor
// returns the names of matching rules, tagger returns a cleansed record plus the
// tags those rules assign. That is the shape the SAS traffic pipeline needs —
// downstream consumes tagged records, not rule names.
//
// Two rule kinds, applied in order:
//
//	clean: when `expr` matches, the named field is dropped from the output record
//	       (a rule with no `when` always applies)
//	tag:   when `expr` matches, `tag` is added to the record's tag set
//
// Rule expressions evaluate against the host's whole expression environment, but
// the OUTPUT record is only the environment's non-root keys — see isEngineRoot
// for why that distinction is what keeps a cleaned field from surviving.
//
// ABI is identical to the reference guest (docs/design/WASM-ENGINE-POOLING.md §4):
//
//	abi_version() int32           negotiation
//	alloc(size int32) int32       reserve inBuf, return ptr
//	configure(n int32) int32      compile inBuf[:n] rules once; >=0 rule count | <0 err
//	eval(n int32) int32           evaluate inBuf[:n] input; >=0 out len | <0 err
//	out_ptr() int32               outBuf ptr
//	out_len() int32               last write length
//	teardown() int32              graceful stop hook
package main

import (
	"encoding/json"
	"unsafe"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

const abiVersion int32 = 1

// Error codes (docs §4.2), kept in sync with the host's constants.
const (
	errDecode       int32 = -1
	errUnconfigured int32 = -2
	errConfig       int32 = -3
	errEval         int32 = -4
	errOutput       int32 = -5
)

// maxOutput mirrors engine.DefaultMaxOutputBytes (1 MiB).
const maxOutput = 1 << 20

const (
	kindClean = "clean"
	kindTag   = "tag"
)

var (
	inBuf  []byte
	outBuf []byte
	outN   int32

	configured bool
	progs      []compiledRule
)

type compiledRule struct {
	kind  string
	field string
	tag   string
	// prog is nil for an unconditional rule (empty `when`), which always applies.
	prog *vm.Program
}

func main() {}

//go:wasmexport abi_version
func abi_version() int32 { return abiVersion }

//go:wasmexport alloc
func alloc(size int32) int32 {
	if size < 0 {
		return 0
	}
	if int(size) > cap(inBuf) {
		inBuf = make([]byte, size)
	}
	inBuf = inBuf[:size]
	if size == 0 {
		return int32(uintptr(unsafe.Pointer(unsafe.SliceData(make([]byte, 1)))))
	}
	return int32(uintptr(unsafe.Pointer(&inBuf[0])))
}

//go:wasmexport out_ptr
func out_ptr() int32 {
	if len(outBuf) == 0 {
		return 0
	}
	return int32(uintptr(unsafe.Pointer(&outBuf[0])))
}

//go:wasmexport out_len
func out_len() int32 { return outN }

// configRule is one rule in the configure payload. `when` is optional; a clean
// rule without it drops the field unconditionally, which is how PII stripping is
// expressed.
type configRule struct {
	Kind  string `json:"kind"`
	Field string `json:"field"`
	Tag   string `json:"tag"`
	When  string `json:"when"`
}

//go:wasmexport configure
func configure(n int32) int32 {
	var cfg struct {
		Rules []configRule `json:"rules"`
	}
	if err := json.Unmarshal(inBuf[:n], &cfg); err != nil {
		writeErr("decode config: " + err.Error())
		return errDecode
	}
	next := make([]compiledRule, 0, len(cfg.Rules))
	for _, r := range cfg.Rules {
		switch r.Kind {
		case kindClean:
			if r.Field == "" {
				writeErr("clean rule requires a field")
				return errConfig
			}
		case kindTag:
			if r.Tag == "" {
				writeErr("tag rule requires a tag")
				return errConfig
			}
		default:
			writeErr("unknown rule kind: " + r.Kind)
			return errConfig
		}
		cr := compiledRule{kind: r.Kind, field: r.Field, tag: r.Tag}
		if r.When != "" {
			p, err := expr.Compile(r.When,
				expr.Env(map[string]any{}),
				expr.AllowUndefinedVariables(),
				expr.AsBool(),
			)
			if err != nil {
				// Whole-config rejection: the host discards this fresh instance
				// on <0 and keeps the last-good pool (docs §6.3).
				writeErr("compile rule: " + err.Error())
				return errConfig
			}
			cr.prog = p
		}
		next = append(next, cr)
	}
	progs = next
	configured = true
	return int32(len(progs))
}

//go:wasmexport eval
func eval(n int32) (result int32) {
	if !configured {
		writeErr("eval before configure")
		return errUnconfigured
	}
	defer func() {
		if r := recover(); r != nil {
			writeErr("eval panic")
			result = errEval
		}
	}()

	var env map[string]any
	if err := json.Unmarshal(inBuf[:n], &env); err != nil {
		writeErr("decode input: " + err.Error())
		return errDecode
	}

	// The eval input is the host's whole expression environment, which carries
	// the traffic record twice: merged at the top level AND again under the
	// engine roots ($input, $inputs, $params, ...). Only the top-level non-root
	// keys are the record. Emitting the roots would ship a second, uncleansed
	// copy of every field a clean rule stripped — the credential the rules exist
	// to remove would still reach the persisted output.
	//
	// The roots stay in env so a rule expression can still reference them; they
	// are excluded from the OUTPUT record only.
	rec := make(map[string]any, len(env))
	for k, v := range env {
		if isEngineRoot(k) {
			continue
		}
		rec[k] = v
	}

	// Tag rules read the ORIGINAL record: a clean rule that drops a field must
	// not change whether a later tag rule matches, otherwise the result would
	// depend on rule ordering in a way the rule author cannot see. env is
	// already that pre-clean view, and `rec` is the copy the clean rules mutate.
	tags := make([]string, 0, len(progs))
	seen := make(map[string]bool, len(progs))
	for _, r := range progs {
		if !r.matches(env) {
			continue
		}
		switch r.kind {
		case kindClean:
			delete(rec, r.field)
		case kindTag:
			if !seen[r.tag] {
				seen[r.tag] = true
				tags = append(tags, r.tag)
			}
		}
	}

	b, err := json.Marshal(map[string]any{"record": rec, "tags": tags})
	if err != nil {
		writeErr("marshal result: " + err.Error())
		return errEval
	}
	if len(b) > maxOutput {
		return errOutput
	}
	writeOut(b)
	return outN
}

// isEngineRoot reports whether k is one of the host's expression-environment
// roots rather than a field of the traffic record. The host merges the node's
// data into the env top level and ALSO exposes it under these roots, so
// including them in the output record would duplicate — and un-cleanse — the
// data.
//
// Matched by the "$" prefix rather than an explicit list: the roots are the
// host's contract (exprx.BuildExprEnv currently publishes $input, $inputs,
// $vars, $config, $params, $runtime, $supplies) and a new one must not silently
// become a leak. A traffic record field cannot collide with this: the host
// reserves the prefix.
func isEngineRoot(k string) bool {
	return len(k) > 0 && k[0] == '$'
}

// matches reports whether the rule applies to env. A rule with no compiled
// condition is unconditional. A rule whose expression errors on this input does
// not match — a malformed field in one record must not fail the whole batch.
func (r compiledRule) matches(env map[string]any) bool {
	if r.prog == nil {
		return true
	}
	out, err := expr.Run(r.prog, env)
	if err != nil {
		return false
	}
	b, ok := out.(bool)
	return ok && b
}

//go:wasmexport teardown
func teardown() int32 {
	progs = nil
	configured = false
	return 0
}

func writeOut(b []byte) {
	if len(b) > cap(outBuf) {
		outBuf = make([]byte, len(b))
	}
	outBuf = outBuf[:len(b)]
	copy(outBuf, b)
	outN = int32(len(b))
}

// writeErr writes a structured error detail into outBuf. The host reads it for
// internal logging only — it is never echoed to callers.
func writeErr(msg string) {
	b, _ := json.Marshal(map[string]any{"error": msg})
	writeOut(b)
}
