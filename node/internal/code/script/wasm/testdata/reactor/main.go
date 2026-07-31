// reactor is the reference reactor-model guest for the wasm pooling engine.
//
// Unlike the command-model echo guest (stdin/stdout, one-shot _start), this
// guest is a resident instance: it is compiled with -buildmode=c-shared so
// GOOS=wasip1 emits `_initialize` (run once by the host) plus the
// //go:wasmexport functions below, which the host calls repeatedly on the same
// instance. Package-level state (the compiled expr programs) survives across
// calls — that persistence is the whole point of the reactor model.
//
// ABI (see docs/design/WASM-ENGINE-POOLING.md §4):
//
//	abi_version() int32           negotiation
//	alloc(size int32) int32       host-allocates-for-input: reserve inBuf, return ptr
//	configure(n int32) int32      compile inBuf[:n] rules once; >=0 rule count | <0 err
//	eval(n int32) int32           evaluate inBuf[:n] input; >=0 out len | <0 err
//	out_ptr() int32               guest-allocates-for-output: outBuf ptr
//	out_len() int32               last write length
//	teardown() int32              graceful stop hook (no external resources here)
//
// Memory convention: host calls alloc, writes input to the returned pointer,
// calls configure/eval, then reads outBuf[:out_len()] at out_ptr(). A single
// instance is only ever driven by one host goroutine at a time (the pool
// enforces this — constraint #2).
package main

import (
	"encoding/json"
	"unsafe"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// abiVersion is the ABI contract version this guest implements. The host
// verifies it on load and refuses a mismatch.
const abiVersion int32 = 1

// Error codes (docs §4.2). Kept in sync with the host's constants.
const (
	errDecode       int32 = -1 // input JSON decode failed
	errUnconfigured int32 = -2 // eval before configure
	errConfig       int32 = -3 // rule compile failed
	errEval         int32 = -4 // evaluation panicked
	errOutput       int32 = -5 // output exceeds cap (guest-side guard; host also caps)
)

// maxOutput mirrors engine.DefaultMaxOutputBytes (1 MiB). A guest-side guard so
// a runaway result is rejected as ERR_OUTPUT rather than growing linear memory.
const maxOutput = 1 << 20

var (
	inBuf  []byte // host-written input buffer
	outBuf []byte // guest-written output buffer
	outN   int32  // length of the last outBuf write

	configured bool
	progs      []compiledRule
)

type compiledRule struct {
	name string
	prog *vm.Program
}

// main is required for a main package but is never the entry point under
// -buildmode=c-shared; _initialize runs instead.
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
		// Return a stable non-nil pointer for zero-length writes.
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

// configRule is one rule in the configure payload.
type configRule struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
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
		p, err := expr.Compile(r.Expr,
			expr.Env(map[string]any{}),
			expr.AllowUndefinedVariables(),
			expr.AsBool(),
		)
		if err != nil {
			// Whole-config rejection: compile atomically, only swap on full
			// success. Host discards this fresh instance on <0 (docs §6.3).
			writeErr("compile rule " + r.Name + ": " + err.Error())
			return errConfig
		}
		next = append(next, compiledRule{name: r.Name, prog: p})
	}
	progs = next // atomic replace (single goroutine per instance)
	configured = true
	return int32(len(progs))
}

//go:wasmexport eval
func eval(n int32) (result int32) {
	if !configured {
		writeErr("eval before configure")
		return errUnconfigured
	}
	// Standard Go recover works under wasip1 — an expr runtime panic is caught
	// and reported as ERR_EVAL rather than killing the instance.
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

	matched := make([]string, 0, len(progs))
	for _, r := range progs {
		out, err := expr.Run(r.prog, env)
		if err != nil {
			continue // a rule that errors on this input simply does not match
		}
		if b, ok := out.(bool); ok && b {
			matched = append(matched, r.name)
		}
	}

	b, err := json.Marshal(map[string]any{"matched": matched})
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

//go:wasmexport teardown
func teardown() int32 {
	// Pure-compute guest: nothing external to flush. Drop references so the
	// guest GC can reclaim before mod.Close.
	progs = nil
	configured = false
	return 0
}

// writeOut copies b into outBuf and records its length.
func writeOut(b []byte) {
	if len(b) > cap(outBuf) {
		outBuf = make([]byte, len(b))
	}
	outBuf = outBuf[:len(b)]
	copy(outBuf, b)
	outN = int32(len(b))
}

// writeErr writes a structured error detail into outBuf. The host reads it via
// out_len for internal logging only (never echoed to callers, per the secure
// coding policy on sensitive-info disclosure).
func writeErr(msg string) {
	b, _ := json.Marshal(map[string]any{"error": msg})
	writeOut(b)
}
