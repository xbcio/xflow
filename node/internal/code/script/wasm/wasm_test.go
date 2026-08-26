package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	_ "github.com/xbcio/xflow/node/internal/code/script/js" // registers goja for the parity test
)

var (
	echoWasm         []byte
	spinWasm         []byte
	reactorWasm      []byte
	reactorSpinWasm  []byte
	reactorMinWasm   []byte
	reactorTrapWasm  []byte
	reactorNoopWasm  []byte
	reactorStaleWasm []byte
	taggerWasm       []byte
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wasmtest")
	if err != nil {
		panic(err)
	}
	echoWasm = buildGuest(dir, "echo")
	spinWasm = buildGuest(dir, "spin")
	reactorWasm = buildReactorGuest(dir, "reactor")
	reactorSpinWasm = buildReactorGuest(dir, "reactorspin")
	reactorMinWasm = buildReactorGuest(dir, "reactormin")
	reactorTrapWasm = buildReactorGuest(dir, "reactortrap")
	reactorNoopWasm = buildReactorGuest(dir, "reactornoop")
	reactorStaleWasm = buildReactorGuest(dir, "reactorstale")
	taggerWasm = buildReactorGuest(dir, "tagger")
	exitCode := m.Run()
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	os.Exit(exitCode)
}

// buildGuest compiles testdata/<name>/main.go to a WASI command module and
// returns its bytes.
func buildGuest(dir, name string) []byte {
	out := filepath.Join(dir, name+".wasm")
	cmd := exec.Command("go", "build", "-o", out, "./testdata/"+name+"/main.go")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		panic("build " + name + " guest: " + string(b))
	}
	b, err := os.ReadFile(out)
	if err != nil {
		panic(err)
	}
	return b
}

// buildReactorGuest compiles a reactor-model guest. Unlike a command module it
// needs -buildmode=c-shared so wasip1 emits _initialize plus the //go:wasmexport
// functions instead of a one-shot _start.
func buildReactorGuest(dir, name string) []byte {
	out := filepath.Join(dir, name+".wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./testdata/"+name+"/main.go")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		panic("build " + name + " reactor guest: " + string(b))
	}
	b, err := os.ReadFile(out)
	if err != nil {
		panic(err)
	}
	return b
}

func newWasm(t *testing.T) engine.Engine {
	t.Helper()
	e, ok := engine.Lookup("wasm", "wazero")
	if !ok {
		t.Fatal("wazero engine not registered")
	}
	return e
}

func b64(b []byte) string {
	return engine.DefaultHelpers().Base64Encode(string(b))
}

func TestWasm_IORoundTrip(t *testing.T) {
	out, err := newWasm(t).Execute(context.Background(),
		engine.Code(b64(echoWasm)),
		map[string]any{"$input": map[string]any{"x": 7.0}},
		engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	echo := m["echo"].(map[string]any)
	if echo["x"] != 7.0 {
		t.Fatalf("echo.x = %v, want 7", echo["x"])
	}
}

func TestWasm_CredentialsViaStdin(t *testing.T) {
	out, err := newWasm(t).Execute(context.Background(),
		engine.Code(b64(echoWasm)),
		map[string]any{
			"$credentials": map[string]any{"aes_key": map[string]any{"key": "kk"}},
			"$credential":  map[string]any{"token": "t-1"},
		},
		engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	if m["credKey"] != "kk" {
		t.Fatalf("credKey = %v, want kk", m["credKey"])
	}
	if m["firstToken"] != "t-1" {
		t.Fatalf("firstToken = %v, want t-1", m["firstToken"])
	}
}

func TestWasm_SandboxNoFS(t *testing.T) {
	out, err := newWasm(t).Execute(context.Background(),
		engine.Code(b64(echoWasm)), nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.(map[string]any)["fsBlocked"] != true {
		t.Fatal("sandbox breach: guest opened a host file")
	}
}

func TestWasm_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond)
	_, err := newWasm(t).Execute(ctx, engine.Code(b64(echoWasm)), nil, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestWasm_InFlightTimeout(t *testing.T) {
	// Deadline fires DURING execution; the guest loops forever, so only
	// wazero's close-on-context-done can terminate it.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := newWasm(t).Execute(ctx, engine.Code(b64(spinWasm)), nil, engine.DefaultHelpers())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error from in-flight cancellation")
		}
	case <-time.After(30 * time.Second):
		// Generous guard: wazero's context-done checkpoint polling on a tight
		// CPU loop is much slower under -race (seconds, vs sub-second normally),
		// so a 5s guard flakes. With the fix the guest is still interrupted well
		// within 30s; without it (default-config runtime) the guest loops
		// forever and this guard fires.
		t.Fatal("Execute hung: in-flight ctx cancellation not wired (guest not interrupted)")
	}
}

// TestWasm_ModuleCacheHit checks that repeated Execute calls for the same guest
// reuse one compiled module instead of recompiling.
//
// It used to run the loop and assert only that err was nil, discarding the
// output. Compiling fresh every time and reusing a cached module produce the
// same result and the same (absent) error for a guest that compiles cleanly, so
// no edit to the cache could turn that red — including deleting the cache
// lookup outright. The hit/miss counters that would have made it observable
// from outside are still a TODO in wazero.go, and the one real probe
// (Observer.OnModuleCompile) is wired into the reactor host path, which this
// test does not use.
//
// So the check has to be a white-box one. Two things are asserted: the cache is
// keyed by the sha256 of the bytes actually compiled — wazero.go's own comment
// states this is a requirement, not an implementation detail — and the compiled
// module handed back is the same object across iterations.
func TestWasm_ModuleCacheHit(t *testing.T) {
	we, ok := newWasm(t).(*wazeroEngine)
	if !ok {
		t.Fatalf("engine.Lookup(\"wasm\", \"wazero\") returned %T, want *wazeroEngine; "+
			"the module cache cannot be inspected through the interface", newWasm(t))
	}
	code := b64(echoWasm)
	sum := sha256.Sum256(echoWasm)
	key := hex.EncodeToString(sum[:])

	var first wazero.CompiledModule
	for i := range 3 {
		if _, err := we.Execute(context.Background(), engine.Code(code),
			map[string]any{"$input": map[string]any{"n": float64(i)}}, engine.DefaultHelpers()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		// Peek, not Get: Get bumps LRU recency, so the probe would change the
		// state it is measuring.
		cm, cached := we.compiled.c.Peek(key)
		if !cached {
			t.Fatalf("iteration %d: no cache entry under the guest's sha256; either "+
				"nothing was cached or the key is not the digest of the compiled bytes", i)
		}
		if i == 0 {
			first = cm
			continue
		}
		if cm != first {
			t.Fatalf("iteration %d: cached module identity changed (%p -> %p); the guest "+
				"was recompiled instead of served from cache", i, first, cm)
		}
	}
}

func TestWasm_CredentialParityWithJS(t *testing.T) {
	globals := map[string]any{
		"$credentials": map[string]any{"aes_key": map[string]any{"key": "shared-kk"}},
		"$credential":  map[string]any{"token": "shared-tt"},
	}

	// wasm path: echo guest extracts credKey and firstToken
	wOut, err := newWasm(t).Execute(context.Background(), engine.Code(b64(echoWasm)), globals, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("wasm exec: %v", err)
	}
	wm := wOut.(map[string]any)
	wasmKey := wm["credKey"]
	wasmTok := wm["firstToken"]

	// js (goja) path: read the same fields
	jsEngine, ok := engine.Lookup("js", "goja")
	if !ok {
		t.Fatal("goja engine not registered")
	}
	jOut, err := jsEngine.Execute(context.Background(),
		engine.Code(`({credKey: $credentials.aes_key.key, firstToken: $credential.token})`),
		globals, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("js exec: %v", err)
	}
	jm := jOut.(map[string]any)
	jsKey := jm["credKey"]
	jsTok := jm["firstToken"]

	if wasmKey != jsKey || wasmTok != jsTok {
		t.Fatalf("family mismatch: wasm(key=%v,tok=%v) js(key=%v,tok=%v)", wasmKey, wasmTok, jsKey, jsTok)
	}
	// And confirm the actual expected values came through (not both nil).
	if wasmKey != "shared-kk" || wasmTok != "shared-tt" {
		t.Fatalf("credential values wrong: key=%v tok=%v", wasmKey, wasmTok)
	}
}

// realisticRecord builds one apisix traffic record of approximately `size`
// bytes, matching the distribution measured against the live test-env topic
// (mean 6845, p50 2594, p90 9753, p99 119111 bytes).
//
// It lives here rather than beside any one benchmark because both the eval
// observer tests and the pool-width benchmark measure against the same shape,
// and a record built two different ways would make their numbers
// incomparable.
func realisticRecord(size int) map[string]any {
	body := make([]byte, 0, size)
	for len(body) < size {
		body = append(body, "abcdefghijklmnopqrstuvwxyz0123456789"...)
	}
	return map[string]any{
		"topic":     "apisix-traffic",
		"partition": 0,
		"offset":    1,
		"value": fmt.Sprintf(`{"response":{"status":200,"headers":`+
			`{"content-type":"application/json"}},"request":{"method":"GET",`+
			`"uri":"/api/v1/resource"},"body":%q}`, body[:size]),
	}
}
