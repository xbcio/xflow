// reactornoop is a pure-passthrough reactor guest that copies its input bytes
// directly to output without any JSON work. Its only purpose is to isolate the
// host/sandbox transport overhead (alloc + host→guest memcpy + eval call +
// guest→host memcpy) from the guest-side JSON cost present in every other
// reactor guest. Used by BenchmarkEvalCost_D4_EvalOnce in
// eval_cost_bench_test.go.
package main

import "unsafe"

const abiVersion int32 = 1

var (
	inBuf  []byte
	outBuf []byte
	outN   int32
)

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

// configure accepts and ignores any config.
//
//go:wasmexport configure
func configure(_ int32) int32 { return 0 }

// eval copies inBuf[:n] directly to outBuf — zero JSON, zero business logic.
// The output is the raw input bytes, which decodeStdout will try to parse as
// JSON. Since the input IS valid JSON (it is the encodeStdin output), parsing
// will succeed, but the cost of decodeStdout is not what we are measuring here.
//
//go:wasmexport eval
func eval(n int32) int32 {
	if int(n) > cap(outBuf) {
		outBuf = make([]byte, n)
	}
	outBuf = outBuf[:n]
	copy(outBuf, inBuf[:n])
	outN = n
	return n
}

//go:wasmexport teardown
func teardown() int32 { return 0 }
