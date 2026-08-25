package wasm

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"testing"

	"github.com/xbcio/xflow/node/supply"
)

// base64Alphabet is StdEncoding's alphabet, used to enumerate the encodings of a
// given byte string that differ only in the unused low bits of the final
// character.
const base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

// alternateEncodingOf returns a base64 string that is NOT equal to s but decodes
// to exactly the same bytes. It never returns "": every byte string has at least
// one alternate encoding.
//
// Two independent mechanisms produce one, and both are properties of base64
// itself rather than contrived edge cases.
//
// The first is spare bits. When the input length is not a multiple of 3 the
// final character carries unused low bits (2 spare when len%3==2, 4 when
// len%3==1), and Go's StdEncoding accepts any value in them rather than
// requiring the canonical zero. A module of len%3==2 has 3 alternate encodings,
// one of len%3==1 has 15.
//
// The second is line breaks, which is the fallback here because it works for any
// length including a multiple of 3. StdEncoding.DecodeString ignores \r and \n
// wherever they appear, so a wrapped string decodes to the same bytes — and
// production decodes with exactly that function (decodeCode, wasm.go). Wrapped
// base64 is if anything the more likely of the two to arrive for real: a YAML
// block scalar or any MIME-style 76-column wrap produces it.
//
// The fallback is not decoration. reactorMinWasm is not a fixture in this repo;
// wasm_test.go compiles it with whatever Go toolchain is running, so its length
// mod 3 changes with the toolchain. When this helper could return "", the three
// tests below skipped on that outcome — which means roughly one toolchain in
// three would have silently switched off the regression coverage for a bug whose
// symptom is that a clean rule's credential stripping never runs, with the suite
// still reporting green.
//
// The consequence for this package: a base64 code string is NOT a module
// identity. Several distinct strings name the same module.
func alternateEncodingOf(t *testing.T, s string) string {
	t.Helper()
	want, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("alternateEncodingOf: %q is not valid base64: %v", s, err)
	}
	pad := 0
	for pad < len(s) && s[len(s)-1-pad] == '=' {
		pad++
	}
	if pad > 0 {
		i := len(s) - pad - 1
		for k := 0; k < len(base64Alphabet); k++ {
			c := base64Alphabet[k]
			if c == s[i] {
				continue
			}
			cand := s[:i] + string(c) + s[i+1:]
			got, err := base64.StdEncoding.DecodeString(cand)
			if err == nil && bytes.Equal(got, want) {
				return cand
			}
		}
	}
	// len%3==0, or a StdEncoding that stopped accepting non-canonical spare bits.
	// Wrap instead. Split mid-string rather than appending, so the result cannot
	// be mistaken for a trailing-whitespace artifact of the test itself.
	cand := s[:len(s)/2] + "\n" + s[len(s)/2:]
	got, err := base64.StdEncoding.DecodeString(cand)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("alternateEncodingOf: neither spare bits nor a line break yields an "+
			"alternate encoding (len=%d, wrap err=%v); base64's decoding tolerance has "+
			"changed and the module-identity tests below need a new premise", len(s), err)
	}
	return cand
}

// mustModuleKey is moduleKey for tests, failing rather than returning an error.
// Tests that seed a registry must derive the key exactly as production does; a
// test that keyed its own way would assert against a registry no module reaches.
func mustModuleKey(t *testing.T, code string) string {
	t.Helper()
	key, err := moduleKey(code)
	if err != nil {
		t.Fatalf("moduleKey: %v", err)
	}
	return key
}

// TestAlternateEncodingIsSameModule is the premise the two tests below rely on.
// It asserts the base64 property directly, so that if a future Go release made
// StdEncoding reject non-canonical padding those tests would fail HERE — with a
// clear explanation — rather than appearing to pass because the alternate
// encoding silently became unreachable.
func TestAlternateEncodingIsSameModule(t *testing.T) {
	code := b64(reactorMinWasm)
	alt := alternateEncodingOf(t, code)
	if alt == code {
		t.Fatal("alternateEncodingOf returned the input; it must return a DIFFERENT string")
	}
	decoded, err := base64.StdEncoding.DecodeString(alt)
	if err != nil {
		t.Fatalf("alternate encoding does not decode: %v", err)
	}
	if !bytes.Equal(decoded, reactorMinWasm) {
		t.Fatal("alternate encoding decodes to different bytes; it is not the same module")
	}
}

// TestAlternateEncodingOfHandlesEveryLengthClass pins both of alternateEncodingOf's
// branches on every toolchain.
//
// TestAlternateEncodingIsSameModule above can only exercise whichever branch
// reactorMinWasm's length happens to select, and that length is decided by the Go
// toolchain compiling it. Without this test the line-break fallback would be
// unexercised on any toolchain producing a len%3!=0 module — which is the same
// shape of hole the fallback was added to close.
func TestAlternateEncodingOfHandlesEveryLengthClass(t *testing.T) {
	for _, n := range []int{9, 10, 11} {
		payload := make([]byte, n)
		for i := range payload {
			payload[i] = byte(i * 7)
		}
		code := base64.StdEncoding.EncodeToString(payload)
		alt := alternateEncodingOf(t, code)
		if alt == code {
			t.Fatalf("len%%3==%d: alternateEncodingOf returned its input", n%3)
		}
		got, err := base64.StdEncoding.DecodeString(alt)
		if err != nil {
			t.Fatalf("len%%3==%d: alternate encoding %q does not decode: %v", n%3, alt, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("len%%3==%d: alternate encoding decodes to different bytes", n%3)
		}
	}
}

// TestSourceDrivenIsKeyedByModuleNotEncoding is the correctness case for keying
// the registries by module sha256.
//
// sourceDriven records "this module's rules come from a supply/loader, not from
// globals[$config]". Keyed by the base64 string, the SAME module registered
// under one encoding and executed under another is not recognised: the engine
// stays on the legacy globals path and evaluates against no rules at all. That
// is not a crash, it is silent pass-through — every record flows untagged and
// uncleansed, which for the SAS pipeline means the credential a clean rule
// exists to strip is never stripped.
//
// engines (host.go) is already keyed by module sha256 and would correctly treat
// these two strings as one module; sourceDriven must agree with it.
func TestSourceDrivenIsKeyedByModuleNotEncoding(t *testing.T) {
	code := b64(reactorMinWasm)
	alt := alternateEncodingOf(t, code)
	h := newReactorHost()
	h.seedSourceDrivenByKey(mustModuleKey(t, code))

	if !h.isSourceDriven(alt) {
		t.Fatal("a module registered under one base64 encoding was not recognised as " +
			"source-driven under another encoding of the SAME bytes; the registry must " +
			"key on module identity (sha256), not on the encoding")
	}

	// The consequence, at the level that actually matters: the engine created for
	// the alternate encoding must still resolve to the source-driven path.
	e, err := h.engineForCode(context.Background(), alt)
	if err != nil {
		t.Fatalf("engineForCode(alt): %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("engine for an alternate encoding of a registered module took the legacy " +
			"globals path; it would evaluate with no rules and pass every record through")
	}
}

// TestPrewarmIsKeyedByModuleNotEncoding is the same identity bug on the prewarm
// queue. Its documented contract is that re-registering the same module REPLACES
// its config (warmup_test.go TestPrewarm_ReregisterReplacesConfig). Keyed by the
// encoding, the same module under two encodings yields two queue entries, so
// warm-up compiles it twice and — worse — builds a pool from whichever config
// happens to be iterated last, making the effective config nondeterministic.
func TestPrewarmIsKeyedByModuleNotEncoding(t *testing.T) {
	code := b64(reactorMinWasm)
	alt := alternateEncodingOf(t, code)
	h := newReactorHost()
	h.addPrewarm(code, ruleConfig([2]string{"v1", "x > 1"}))
	h.addPrewarm(alt, ruleConfig([2]string{"v2", "x > 2"}))

	mods := h.prewarmModules()
	if len(mods) != 1 {
		t.Fatalf("prewarm has %d entries for ONE module registered under two encodings, "+
			"want 1: warm-up would compile it twice and the surviving config would "+
			"depend on map iteration order", len(mods))
	}
	rules := mods[0].cfg.(map[string]any)["rules"].([]any)
	if name := rules[0].(map[string]any)["name"]; name != "v2" {
		t.Fatalf("kept config %v, want the later registration (v2)", name)
	}
}

// TestUnregisterUnderAnotherEncodingRemovesTheConsumer covers the third registry
// that keyed on the encoding. Registering a consumer under one encoding and
// unregistering under another must remove it: keyed by the string, the
// unregister misses and the consumer stays installed, so every later content
// change keeps rebuilding the pool of a module nothing is executing any more.
//
// It asserts through the production entry points and observable behaviour — the
// consumer either receives a later Apply or it does not — rather than by
// comparing internal key strings. A key-comparison test would restate the
// implementation and would still pass if register and unregister BOTH derived a
// wrong-but-matching key.
func TestUnregisterUnderAnotherEncodingRemovesTheConsumer(t *testing.T) {
	code := b64(reactorMinWasm)
	alt := alternateEncodingOf(t, code)
	reg := supply.NewRegistry()
	obs := &countingConsumerObserver{}
	reg.SetObserver(obs)

	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	if got := obs.last(); got != 1 {
		t.Fatalf("consumer count after register = %d, want 1", got)
	}

	// Unregister naming the module by a DIFFERENT encoding of the same bytes.
	UnregisterSupplyConsumer(alt, "rules", reg)

	if got := obs.last(); got != 0 {
		t.Fatalf("consumer count after unregistering under another encoding of the "+
			"SAME module = %d, want 0: the registration leaked, and every later "+
			"content change would keep rebuilding a pool nothing executes", got)
	}
}

// countingConsumerObserver records the most recent consumer count the registry
// reported, which is how this file observes register/unregister without reaching
// into registry internals.
type countingConsumerObserver struct {
	mu sync.Mutex
	n  int
}

func (o *countingConsumerObserver) OnConsumerCount(_ context.Context, _ string, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.n = n
}

func (o *countingConsumerObserver) last() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.n
}

// TestSourceDrivenFlipSurvivesCodeCacheEviction covers the second reason the
// mark path must not resolve engines through codeCache: that cache is a bounded
// LRU, while engines is authoritative and never evicts.
//
// The sequence is ordinary production traffic, not a contrived one. A module
// executes (creating its engine and caching it), enough other modules then
// execute to push it out of the LRU, and only afterwards does its workflow
// activate and register a supply consumer. Resolving the flip through the LRU
// silently finds nothing, so the still-live engine stays on the legacy globals
// path and evaluates every record against no rules — and nothing later repairs
// it, because engineForCode resolves the flag only when it CREATES an engine and
// this one already exists.
func TestSourceDrivenFlipSurvivesCodeCacheEviction(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()

	code := testReactorCode(t)
	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if e.configFromSource.Load() {
		t.Fatal("a module with no registration must start on the globals path")
	}

	// Evict it from codeCache without touching engines. Adding directly is what
	// keeps this test fast: driving eviction through engineForCode would compile
	// DefaultWasmModuleCacheSize real modules.
	for i := 0; i < DefaultWasmModuleCacheSize; i++ {
		h.codeCache.Add(fmt.Sprintf("filler-%d", i), &reactorEngine{})
	}
	if _, ok := h.codeCache.Get(code); ok {
		t.Fatal("precondition failed: the module is still in codeCache, so this test " +
			"would not exercise eviction")
	}

	h.seedSourceDrivenByKey(mustModuleKey(t, code))

	if !e.configFromSource.Load() {
		t.Fatal("registration did not reach a live engine that had been evicted from " +
			"codeCache; the flip must resolve through h.engines, which is authoritative " +
			"and never evicts. As written the module keeps serving traffic on the globals " +
			"path with no rules at all")
	}
}

// keying by module identity introduces: deriving the key now requires decoding
// the code, so an undecodable string can no longer be registered silently.
//
// Failing loudly at registration is the right trade. The alternative — falling
// back to the raw string as its own key — would accept the registration and then
// never match the module, reintroducing the silent-pass-through failure this
// change exists to remove. Registration happens at activation time, where an
// error is surfaced to the operator; a silent mismatch is discovered in
// production, as untagged traffic.
func TestRegisterSupplyConsumerRejectsUndecodableCode(t *testing.T) {
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer("!!!not base64!!!", "rules", reg); err == nil {
		t.Fatal("RegisterSupplyConsumer accepted an undecodable module; it must fail at " +
			"registration rather than register under a key no module can ever match")
	}
}
