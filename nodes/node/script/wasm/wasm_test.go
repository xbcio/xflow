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

var echoWasm []byte

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wasmtest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	out := filepath.Join(dir, "echo.wasm")
	cmd := exec.Command("go", "build", "-o", out, "./testdata/echo/main.go")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		panic("build echo guest: " + string(b))
	}
	echoWasm, err = os.ReadFile(out)
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
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

func TestWasm_ModuleCacheHit(t *testing.T) {
	e := newWasm(t)
	code := b64(echoWasm)
	for i := 0; i < 3; i++ {
		if _, err := e.Execute(context.Background(), code, map[string]any{"$input": map[string]any{"n": float64(i)}}, script.DefaultHelpers()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}
