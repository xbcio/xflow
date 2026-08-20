// reactorstale is a guest that returns a negative code WITHOUT writing an error
// detail first, leaving out_len pointing at the previous call's successful
// output.
//
// It reproduces a defect found in production. SAS's decode guest returned
// ERR_OUTPUT bare when a message serialised past its limit; out_len is only
// assigned by writeOut, so it still held the length of the LAST SUCCESSFUL
// message. The host read that, and logged one message's full request/response
// as another message's failure reason — wrong content, and live traffic in a
// log line.
//
// The ABI (§4.2) says a guest writes a structured error detail JSON into outBuf
// alongside a negative code. This guest does not, which is exactly the point: a
// non-compliant guest must not be able to make the host attribute stale output
// to a failed call.
package main

import "unsafe"

const abiVersion int32 = 1

const errOutput int32 = -5

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

//go:wasmexport configure
func configure(_ int32) int32 { return 0 }

// eval succeeds on the first call and fails bare on every call after.
//
// The first call writes a recognisable payload so a test can tell whether the
// host attributed it to the SECOND call. The second and later calls return a
// negative code and touch neither outBuf nor outN — the shape of a guest that
// forgot its writeErr.
//
//go:wasmexport eval
func eval(n int32) int32 {
	if outN == 0 {
		payload := []byte(`{"secret":"first-call-output-must-not-be-logged"}`)
		if len(payload) > cap(outBuf) {
			outBuf = make([]byte, len(payload))
		}
		outBuf = outBuf[:len(payload)]
		copy(outBuf, payload)
		outN = int32(len(payload))
		return outN
	}
	return errOutput
}
