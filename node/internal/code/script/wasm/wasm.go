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

// encodeStdin marshals the globals object written to guest stdin, minus $items.
//
// It is the single funnel for every wasm payload — both reactor paths and the
// command model — so the exclusion cannot be bypassed by adding a call site.
func encodeStdin(globals map[string]any) ([]byte, error) {
	if globals == nil {
		globals = map[string]any{}
	}
	return json.Marshal(stripItems(globals))
}

// itemsGlobal is the map-body root holding the map node's ENTIRE items array
// (execution/subgraph/map_body.go's bodyItemScope).
const itemsGlobal = "$items"

// stripItems removes $items from a wasm guest's payload.
//
// $items remains a promised DSL root everywhere else — js, expression bodies,
// and node/internal/flow/map.go's per-item env all still carry it. Only wasm
// drops it, because only wasm pays for it: the array must be re-encoded, copied
// into linear memory, and re-parsed by the guest ONCE PER ITEM, so a batch of n
// items pays n times for the whole array and the per-item cost grows with the
// batch it belongs to.
//
// Measured (BenchmarkReactorEval_AllItemsScope, and end-to-end against a live
// Kafka pipeline): carrying a 40-element array cost 61x a bare eval, and made
// bigger batches SLOWER per item — 80/62/38 msg/s at batch 10/20/40. Dropping it
// gave ~3.5x and restored normal amortisation.
//
// A better transport does not exist, which is why this excludes the value rather
// than encoding it more cheaply. The cost was measured across all three ABI
// stages (docs at testdata/reactor/main.go): Go-side json.Marshal is 1%, the
// alloc+memcpy of 22 KB into linear memory is 0.39us (0.01%), and the remaining
// 99% is the guest rebuilding objects from those bytes inside the sandbox — the
// same payload decodes ~32x slower under wasm than natively. A shared buffer,
// zero-copy, or a binary format can only touch the 1%.
//
// The array is dropped from BOTH places it reaches the payload. exprx.BuildExprEnv
// flattens Input.Data's keys to the env top level AND publishes Input.Data itself
// as $input, so $items is serialised twice per item. Removing only the root would
// leave the second copy and forfeit half the saving — the same shape as the
// cleansed credentials that still reached storage through their $input copy.
//
// Neither source map is mutated: globals and Input.Data belong to the engine's
// activation record, and deleting in place would corrupt a retry of the same node
// (see script.go's paramsWithoutCode for the same constraint).
func stripItems(globals map[string]any) map[string]any {
	inner, nested := globals["$input"].(map[string]any)
	if nested {
		_, nested = inner[itemsGlobal]
	}
	if _, root := globals[itemsGlobal]; !root && !nested {
		return globals
	}

	out := make(map[string]any, len(globals))
	for k, v := range globals {
		if k == itemsGlobal {
			continue
		}
		out[k] = v
	}
	if nested {
		clone := make(map[string]any, len(inner))
		for k, v := range inner {
			if k == itemsGlobal {
				continue
			}
			clone[k] = v
		}
		out["$input"] = clone
	}
	return out
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
