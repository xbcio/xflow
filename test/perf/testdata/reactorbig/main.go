// reactorbig is a copy of the reference reactor-model wasm guest at
// node/internal/code/script/wasm/testdata/reactor/main.go, duplicated here
// because that directory is under node/internal and this package (test/perf)
// cannot import it. See wasm_stale_output_real_kafka_test.go for why this
// specific guest is what the real-Kafka stale-output test needs: it is the
// UNMODIFIED reference reactor guest, whose ERR_OUTPUT branch returns bare
// (no writeErr call) exactly like the production decode guest that motivated
// commit b75342f ("fix(wasm): refuse to log a guest's stale output as a
// failure reason") — so a run against this guest reproduces the same
// stale-out_len condition under a real, external Kafka broker instead of only
// via direct in-process pool manipulation
// (node/internal/code/script/wasm/stale_output_real_load_test.go).
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
package main

import (
	"encoding/json"
	"unsafe"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

const abiVersion int32 = 1

const (
	errDecode       int32 = -1
	errUnconfigured int32 = -2
	errConfig       int32 = -3
	errEval         int32 = -4
	// errOutput (ERR_OUTPUT): output exceeds cap (guest-side guard; host also
	// caps). Returned BARE below — no writeErr call — which is the exact shape
	// of the production defect this guest reproduces.
	errOutput int32 = -5
)

// maxOutput mirrors engine.DefaultMaxOutputBytes (1 MiB, see
// node/internal/code/script/engine/limits.go). A guest-side guard so a
// runaway result is rejected as ERR_OUTPUT rather than growing linear memory.
const maxOutput = 1 << 20

var (
	inBuf  []byte
	outBuf []byte
	outN   int32

	configured bool
	progs      []compiledRule
)

type compiledRule struct {
	name string
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
			writeErr("compile rule " + r.Name + ": " + err.Error())
			return errConfig
		}
		next = append(next, compiledRule{name: r.Name, prog: p})
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

	matched := make([]string, 0, len(progs))
	for _, r := range progs {
		out, err := expr.Run(r.prog, env)
		if err != nil {
			continue
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
		// Bare return: no writeErr call. out_len keeps describing the
		// PREVIOUS successful eval's output. This is the exact shape of the
		// production defect (see file header).
		return errOutput
	}
	writeOut(b)
	return outN
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

func writeErr(msg string) {
	b, _ := json.Marshal(map[string]any{"error": msg})
	writeOut(b)
}
