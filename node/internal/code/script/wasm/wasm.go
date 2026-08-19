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
	"strings"
)

// decodeCode turns the base64 node param into raw wasm bytes.
func decodeCode(code string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		return nil, fmt.Errorf("wasm: decode base64 module: %w", err)
	}
	return b, nil
}

// moduleKeyFromDigest converts an artifact digest ("sha256:<64 hex>") into the
// moduleKey reactorHost.engines uses, reporting whether the digest was
// well-formed.
//
// The two are the same value by construction, not by coincidence: the artifact
// store digests the module's raw bytes (store.ContentHash), and moduleKey
// hashes those same bytes after base64-decoding them. That is what lets a
// caller who fetched a module BY digest name the compiled engine without
// touching the multi-MB string again.
//
// It returns ok=false rather than an error because every caller's response is
// the same — fall back to the content-keyed path — and none of them can repair
// a malformed digest.
func moduleKeyFromDigest(digest string) (string, bool) {
	key, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(key) != 64 {
		return "", false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	return key, true
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
// belongs on registration and warm-up paths ONLY. A per-message caller that
// already knows the module's digest passes it through engine.Source and reaches
// the engine via moduleKeyFromDigest, which costs a 64-byte comparison; one that
// does not falls back to reactorHost.codeCache's base64-keyed memo, which avoids
// these 3 ms at the price of a full-length key compare.
func moduleKey(code string) (string, error) {
	b, err := decodeCode(code)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// encodeStdin marshals the globals object written to guest stdin, minus $items
// and minus $supplies.
//
// It is the single funnel for every wasm payload — both reactor paths and the
// command model — so the exclusions cannot be bypassed by adding a call site.
func encodeStdin(globals map[string]any) ([]byte, error) {
	if globals == nil {
		globals = map[string]any{}
	}
	return json.Marshal(stripSupplies(stripItems(globals)))
}

// suppliesGlobal is the root holding every decoded supply's whole content
// (exprx.BuildExprEnv assigns supply.Default.Decoded() to it on every call).
const suppliesGlobal = "$supplies"

// stripSupplies removes $supplies from a wasm guest's payload.
//
// A wasm guest cannot use it. Supply content reaches a wasm module through the
// configure export at pool-build time (pool.go's configure, driven by
// supply_consumer.go), which is the whole point of the two-phase init: the rules
// are parsed ONCE per instance instead of once per message. A guest that also
// received them on stdin would be re-parsing, inside the sandbox, content it
// already holds pre-parsed.
//
// $supplies stays a live DSL root everywhere else — js, expression parameters,
// and every non-wasm node still read it through exprx.BuildExprEnv. Only wasm
// drops it, and only because only wasm has the configure channel that makes it
// redundant.
//
// Measured (BenchmarkStdinRedundancy* and BenchmarkStdinRedundancyEndToEnd, at
// the live topic's mean record size of 6845 bytes with a 60-rule supply):
// $supplies was 13.3 KB of a 27.6 KB payload, and dropping it took a full eval
// from 11.83ms to 4.68ms — 2.53x. The Go-side marshal is only ~90us of that;
// the rest is the guest re-parsing the bytes inside the sandbox, where the same
// payload decodes ~32x slower than natively (see stripItems).
//
// This is the same shape as stripItems and shares its constraint: the source map
// is not mutated, because globals belongs to the engine's activation record and
// deleting in place would corrupt a retry of the same node AND strip $supplies
// from the js and expression paths that still promise it.
//
// Unlike $items, there is no second copy to chase. $items reached the payload
// twice because BuildExprEnv flattens Input.Data both at the top level and under
// $input, and $items lives in Input.Data (it arrives via a map body's execution
// scope). $supplies is assigned directly onto the env — never into Input.Data —
// so the root is its only route. TestGuestPayloadDropsSupplies pins that by
// scanning for the VALUE, so a future change that routes it through Data fails
// rather than silently restoring the cost.
func stripSupplies(globals map[string]any) map[string]any {
	if _, has := globals[suppliesGlobal]; !has {
		return globals
	}
	out := make(map[string]any, len(globals)-1)
	for k, v := range globals {
		if k == suppliesGlobal {
			continue
		}
		out[k] = v
	}
	return out
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
