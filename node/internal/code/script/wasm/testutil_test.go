package wasm

import (
	"crypto/rand"
	"testing"
)

// testReactorCode returns a base64-encoded reactor guest module that is
// byte-for-byte unique per call: a WASM custom section (ignored by the runtime,
// legal at any position per the core spec) carrying a random nonce is appended
// to the reference reactor module. Tests that touch the process-wide
// sharedReactorHost (RegisterSupplyConsumer always does) need this — sharing
// reactorWasm's bytes across tests would collide on the same cached
// *reactorEngine (keyed by content sha256) and codeCache entry (keyed by the
// base64 string), letting one test's registration bleed into another's.
func testReactorCode(t *testing.T) string {
	t.Helper()
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("testReactorCode: rand: %v", err)
	}
	return b64(appendCustomSection(reactorWasm, "xflow-test-nonce", nonce))
}

// decodeForTest returns the raw module bytes behind a base64 code string, for
// tests that need to drive the bytes entry point and the code entry point with
// the same module.
func decodeForTest(t *testing.T, code string) []byte {
	t.Helper()
	raw, err := decodeCode(code)
	if err != nil {
		t.Fatalf("decodeForTest: %v", err)
	}
	return raw
}

// appendCustomSection appends a WASM custom section (id 0) to mod. Custom
// sections are unconditionally skipped by any conformant loader (including
// wazero), so this changes the module's content hash without altering its
// behavior.
func appendCustomSection(mod []byte, name string, payload []byte) []byte {
	content := appendULEB128(nil, uint64(len(name)))
	content = append(content, name...)
	content = append(content, payload...)

	out := append([]byte(nil), mod...)
	out = append(out, 0x00) // custom section id
	out = appendULEB128(out, uint64(len(content)))
	out = append(out, content...)
	return out
}

// appendULEB128 appends v encoded as unsigned LEB128, the WASM binary format's
// varint encoding used for section and vector lengths.
func appendULEB128(b []byte, v uint64) []byte {
	for {
		c := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			c |= 0x80
		}
		b = append(b, c)
		if v == 0 {
			return b
		}
	}
}

// contentWithRule returns supply content carrying exactly one rule. Supply
// content is raw bytes (that is what a SupplyResource distributes), which is why
// these helpers exist alongside ruleConfig — the latter builds the map form the
// legacy globals path takes.
func contentWithRule(name, expr string) []byte {
	return []byte(`{"rules":[{"name":"` + name + `","expr":"` + expr + `"}]}`)
}

// emptyContent is a valid config carrying zero rules — "pass everything through,
// tag nothing". It is a legitimate business setting, distinct from "no content
// has arrived yet" (see WASM-ENGINE-POOLING.md §6.5).
func emptyContent() []byte { return []byte(`{"rules":[]}`) }

// badContent has an unparseable expression, so configure rejects it and the whole
// pool build fails.
func badContent() []byte {
	return []byte(`{"rules":[{"name":"broken","expr":"this is (not valid expr"}]}`)
}
