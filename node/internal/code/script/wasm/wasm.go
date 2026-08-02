// Package wasm implements the wasm language family (wazero runtime) for the
// xflow.script node. Guests are WASI modules that read a JSON object from
// stdin and write a JSON object to stdout.
package wasm

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// decodeCode turns the base64 node param into raw wasm bytes.
func decodeCode(code string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		return nil, fmt.Errorf("wasm: decode base64 module: %w", err)
	}
	return b, nil
}

// moduleKey returns the canonical identity of a module carried as a base64 code
// string: the hex sha256 of its DECODED bytes, which is the same key
// reactorHost.engines uses. Registries that record facts ABOUT a module key on
// this rather than on the code string.
//
// It hashes the decoded bytes rather than the string because base64 is not a
// canonical encoding. When a module's length is not a multiple of 3 the final
// character carries unused low bits, and Go's StdEncoding accepts any value in
// them instead of requiring the canonical zero — so one module has several valid
// encodings (3 of them at len%3==2, which every multi-MB guest in this package
// happens to be). Keying a registry on the string therefore splits one module
// into several identities, and a fact recorded under one is invisible under
// another. module_identity_test.go pins what that costs: a supply-driven module
// silently falling back to the globals path, evaluating against no rules and
// passing every record through untagged and uncleansed.
//
// Cost is ~3 ms for a 3 MB module (~1.4 ms decode + ~1.8 ms hash), so this
// belongs on registration and warm-up paths ONLY. The per-message hot path keeps
// its base64-keyed codeCache memo in front of it: re-keying that cache would put
// these 3 ms on every message and cap throughput near 310 msg/s against the
// 12508 msg/s this engine actually sustains.
func moduleKey(code string) (string, error) {
	b, err := decodeCode(code)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// encodeStdin marshals the globals object written to guest stdin.
func encodeStdin(globals map[string]any) ([]byte, error) {
	if globals == nil {
		globals = map[string]any{}
	}
	return json.Marshal(globals)
}

// decodeStdout parses guest stdout into the raw completion value.
func decodeStdout(out []byte) (any, error) {
	if len(out) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(out, &v); err != nil {
		return nil, fmt.Errorf("wasm: decode stdout json: %w", err)
	}
	return v, nil
}
