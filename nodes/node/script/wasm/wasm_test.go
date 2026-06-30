package wasm

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/nodes/node/script"
	_ "github.com/xbcio/xflow/nodes/node/script/js" // registers goja for the parity test
)

var (
	echoWasm []byte
	spinWasm []byte
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wasmtest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	echoWasm = buildGuest(dir, "echo")
	spinWasm = buildGuest(dir, "spin")
	os.Exit(m.Run())
}

// buildGuest compiles testdata/<name>/main.go to a WASI module and returns its
// bytes.
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

func newWasm(t *testing.T) script.Engine {
	t.Helper()
	e, ok := script.Lookup("wasm", "wazero")
	if !ok {
		t.Fatal("wazero engine not registered")
	}
	return e
}

func b64(b []byte) string {
	return script.DefaultHelpers().Base64Encode(string(b))
}

func TestWasm_IORoundTrip(t *testing.T) {
	out, err := newWasm(t).Execute(context.Background(),
		b64(echoWasm),
		map[string]any{"$input": map[string]any{"x": 7.0}},
		script.DefaultHelpers())
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
		b64(echoWasm),
		map[string]any{
			"$credentials": map[string]any{"aes_key": map[string]any{"key": "kk"}},
			"$credential":  map[string]any{"token": "t-1"},
		},
		script.DefaultHelpers())
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
		b64(echoWasm), nil, script.DefaultHelpers())
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
	_, err := newWasm(t).Execute(ctx, b64(echoWasm), nil, script.DefaultHelpers())
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
		_, err := newWasm(t).Execute(ctx, b64(spinWasm), nil, script.DefaultHelpers())
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

func TestWasm_ModuleCacheHit(t *testing.T) {
	e := newWasm(t)
	code := b64(echoWasm)
	for i := range 3 {
		if _, err := e.Execute(context.Background(), code, map[string]any{"$input": map[string]any{"n": float64(i)}}, script.DefaultHelpers()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

func TestWasm_CredentialParityWithJS(t *testing.T) {
	globals := map[string]any{
		"$credentials": map[string]any{"aes_key": map[string]any{"key": "shared-kk"}},
		"$credential":  map[string]any{"token": "shared-tt"},
	}

	// wasm path: echo guest extracts credKey and firstToken
	wOut, err := newWasm(t).Execute(context.Background(), b64(echoWasm), globals, script.DefaultHelpers())
	if err != nil {
		t.Fatalf("wasm exec: %v", err)
	}
	wm := wOut.(map[string]any)
	wasmKey := wm["credKey"]
	wasmTok := wm["firstToken"]

	// js (goja) path: read the same fields
	jsEngine, ok := script.Lookup("js", "goja")
	if !ok {
		t.Fatal("goja engine not registered")
	}
	jOut, err := jsEngine.Execute(context.Background(),
		`({credKey: $credentials.aes_key.key, firstToken: $credential.token})`,
		globals, script.DefaultHelpers())
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
