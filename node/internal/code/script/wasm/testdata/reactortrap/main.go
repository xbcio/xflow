// reactortrap is a reactor guest whose eval traps deterministically on any
// input, used to test how a host-side trap is classified for retry.
//
// It stands in for the real failure this models: a malformed message that
// drives a guest into an out-of-bounds access or an explicit unreachable. The
// trap dooms the instance, so the host cannot tell it apart from an
// infrastructure fault by looking at the instance -- only by looking at whether
// re-running the same input would trap again. It would.
package main

import "unsafe"

const abiVersion int32 = 1

var (
	inBuf  []byte
	outBuf = make([]byte, 1)
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
		return int32(uintptr(unsafe.Pointer(&outBuf[0])))
	}
	return int32(uintptr(unsafe.Pointer(&inBuf[0])))
}

//go:wasmexport out_ptr
func out_ptr() int32 { return int32(uintptr(unsafe.Pointer(&outBuf[0]))) }

//go:wasmexport out_len
func out_len() int32 { return outN }

//go:wasmexport configure
func configure(n int32) int32 { return 0 }

//go:wasmexport eval
func eval(n int32) int32 {
	// Out-of-bounds slice index: compiles to a wasm trap, terminating the
	// instance the same way a real guest bug on malformed input would. Written
	// through a package-level variable so the compiler cannot fold it away.
	var empty []byte
	trapIndex = len(empty) + 1
	return int32(empty[trapIndex])
}

var trapIndex int

//go:wasmexport teardown
func teardown() int32 { return 0 }
