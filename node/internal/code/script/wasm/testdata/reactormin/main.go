// reactormin is a minimal reactor guest that implements ONLY the ABI functions
// marked "required" in docs/design/WASM-ENGINE-POOLING.md §4.1 — it deliberately
// omits the recommended `out_len` and the optional `teardown`.
//
// Its purpose is to hold the host honest about the ABI contract: a guest author
// who implements just the required set must get a working pooled instance, with
// the host degrading gracefully (no error detail on failures, no graceful-stop
// hook) rather than crashing or refusing to load.
package main

import (
	"encoding/json"
	"unsafe"
)

const abiVersion int32 = 1

const errDecode int32 = -1

var (
	inBuf  []byte
	outBuf []byte
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

// configure accepts and ignores any config: this guest has no rules to compile.
//
//go:wasmexport configure
func configure(_ int32) int32 { return 0 }

// eval echoes the decoded input back under "echo" so a test can prove the
// round-trip works without out_len.
//
//go:wasmexport eval
func eval(n int32) int32 {
	var env map[string]any
	if err := json.Unmarshal(inBuf[:n], &env); err != nil {
		return errDecode
	}
	b, err := json.Marshal(map[string]any{"echo": env})
	if err != nil {
		return errDecode
	}
	if len(b) > cap(outBuf) {
		outBuf = make([]byte, len(b))
	}
	outBuf = outBuf[:len(b)]
	copy(outBuf, b)
	return int32(len(b))
}
