// reactorspin is a reactor guest whose eval loops forever, used to test the
// pool's timeout → doom → rebuild path (constraint #4). It shares the reactor
// ABI but its eval never returns, so only wazero's WithCloseOnContextDone can
// terminate it — after which the instance is permanently closed and the pool
// must discard and rebuild it.
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
func configure(n int32) int32 { return 0 } // accepts any config as 0 rules

//go:wasmexport eval
func eval(n int32) int32 {
	sink := 0
	for {
		sink++ // tight CPU loop; interrupted only by ctx-done
		_ = sink
	}
}

//go:wasmexport teardown
func teardown() int32 { return 0 }
