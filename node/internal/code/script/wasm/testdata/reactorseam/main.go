// reactorseam is a minimal reactor-model guest built for Task 18's
// ScriptNode.Execute seam regression (the $config key collision between
// exprx.go's workflow config and reactor.go's rule-engine config key).
//
// It exists as a SEPARATE fixture from testdata/reactor and testdata/reactormin
// for a historical memory reason that NO LONGER APPLIES: driving the reactor
// through the real node path used to put the executing module's own multi-MB
// base64 string into $params.code, because $params mirrored input.Params
// verbatim and input.Params["code"] IS that module. Both existing fixtures
// import encoding/json and copy their whole eval env at least once, and copying
// a several-MB string inside the guest's 16 MiB linear memory cap
// (engine.DefaultWasmMemoryPages) trapped on alloc before any assertion ran —
// measured to exhaust the roughly 5-6 MiB of headroom left after the Go runtime,
// WASI, and the module's own code pages.
//
// That was the defect, not a constraint: script.go's paramsWithoutCode now drops
// the "code" key from the $params view, so the eval input no longer carries the
// module. A future cleanup can retarget the seam test at testdata/reactor and
// delete this fixture; it is kept for now only to avoid bundling an unrelated
// change into the fix.
//
// reactorseam sidesteps the whole class of problem by never importing
// encoding/json and never copying its whole input: configure and eval both
// hand-scan the raw byte buffer for the couple of literal substrings this
// test needs, in place, with no intermediate map or re-serialization. This is
// deliberately NOT a general-purpose JSON guest — its parsing is byte-pattern
// matching valid only against the tightly-controlled request shapes Task 18
// sends it, and it must never be used as a template for a "real" rule guest.
package main

import "unsafe"

const abiVersion int32 = 1

var (
	inBuf  []byte
	outBuf []byte
	outN   int32

	ruleNames []string
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

// configure extracts every `"name":"<value>"` occurrence from the raw config
// bytes and remembers it as a configured rule name. It never validates or
// compiles an expression (there is none to compile here — this guest exists
// to prove WHICH rule set reached it, not to evaluate rules), so any content
// shaped like {"rules":[{"name":"..."}]} is accepted.
//
//go:wasmexport configure
func configure(n int32) int32 {
	ruleNames = extractNameFields(inBuf[:n])
	return int32(len(ruleNames))
}

// eval reports two things about the RAW eval input bytes, both computed by
// scanning in place (no decode, no re-encode):
//   - "matched": the rule names configure() last recorded — this is what
//     proves which content actually configured this guest, independent of
//     eval's own input;
//   - "hasConfigKey": whether the literal top-level JSON key "$config" is
//     present anywhere in the eval input at all. A false positive (matching
//     inside an unrelated string) is not a concern for this test's
//     hand-controlled inputs.
//
//go:wasmexport eval
func eval(n int32) int32 {
	hasConfig := containsSubstring(inBuf[:n], `"$config"`)

	out := make([]byte, 0, 64+16*len(ruleNames))
	out = append(out, '{', '"', 'm', 'a', 't', 'c', 'h', 'e', 'd', '"', ':', '[')
	for i, name := range ruleNames {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, '"')
		out = append(out, name...)
		out = append(out, '"')
	}
	out = append(out, ']', ',')
	out = append(out, '"', 'h', 'a', 's', 'C', 'o', 'n', 'f', 'i', 'g', 'K', 'e', 'y', '"', ':')
	if hasConfig {
		out = append(out, 't', 'r', 'u', 'e')
	} else {
		out = append(out, 'f', 'a', 'l', 's', 'e')
	}
	out = append(out, '}')

	if len(out) > cap(outBuf) {
		outBuf = make([]byte, len(out))
	}
	outBuf = outBuf[:len(out)]
	copy(outBuf, out)
	outN = int32(len(out))
	return outN
}

// extractNameFields scans b for the literal pattern `"name":"<value>"` and
// returns every <value> found, in order. It is a byte-pattern scan, not a
// JSON parser: it does not track nesting or escaping, which is fine for this
// guest's tightly-controlled test inputs (rule names never contain a raw `"`).
func extractNameFields(b []byte) []string {
	const key = `"name":"`
	var out []string
	for i := 0; i+len(key) <= len(b); i++ {
		if string(b[i:i+len(key)]) == key {
			start := i + len(key)
			end := start
			for end < len(b) && b[end] != '"' {
				end++
			}
			out = append(out, string(b[start:end]))
			i = end
		}
	}
	return out
}

// containsSubstring reports whether needle occurs anywhere in b. Equivalent to
// bytes.Contains, reimplemented to avoid importing the bytes package's
// dependency surface — keeping this guest's compiled footprint (and therefore
// its share of the 16 MiB linear memory budget) as small as possible.
func containsSubstring(b []byte, needle string) bool {
	n := len(needle)
	if n == 0 {
		return true
	}
	for i := 0; i+n <= len(b); i++ {
		if string(b[i:i+n]) == needle {
			return true
		}
	}
	return false
}
