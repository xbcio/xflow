package wasm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/xbcio/xflow/nodes/node/script"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

func init() {
	script.Register("wasm", "wazero", func() script.Engine { return sharedWazero })
}

var sharedWazero = &wazeroEngine{compiled: map[string]wazero.CompiledModule{}}

type wazeroEngine struct {
	mu       sync.RWMutex
	rt       wazero.Runtime
	rtOnce   sync.Once
	compiled map[string]wazero.CompiledModule
}

func (e *wazeroEngine) Name() string { return "wasm/wazero" }

func (e *wazeroEngine) runtime(ctx context.Context) wazero.Runtime {
	e.rtOnce.Do(func() {
		// WithCloseOnContextDone lets wazero abort in-flight guest execution
		// when ctx expires — even a tight CPU-bound loop with no host calls is
		// interrupted (spec §7.3). The up-front ctx.Err() guard in Execute still
		// short-circuits an already-expired ctx without starting execution.
		e.rt = wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
		wasi_snapshot_preview1.MustInstantiate(ctx, e.rt)
	})
	return e.rt
}

func (e *wazeroEngine) compile(ctx context.Context, wasmBytes []byte) (wazero.CompiledModule, error) {
	sum := sha256.Sum256(wasmBytes)
	key := hex.EncodeToString(sum[:])

	e.mu.RLock()
	cm, ok := e.compiled[key]
	e.mu.RUnlock()
	if ok {
		return cm, nil
	}

	cm, err := e.runtime(ctx).CompileModule(ctx, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("wasm/wazero: compile module: %w", err)
	}
	e.mu.Lock()
	e.compiled[key] = cm
	e.mu.Unlock()
	return cm, nil
}

func (e *wazeroEngine) Execute(ctx context.Context, code string, globals map[string]any, _ script.Helpers) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Fail fast on an already-cancelled/expired context. wazero's in-flight
	// cancellation is cooperative (checked at call boundaries), so a short
	// guest can finish before a checkpoint is hit — this guarantees a
	// cancelled context is always honored.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wasm/wazero: run module: %w", err)
	}
	wasmBytes, err := decodeCode(code)
	if err != nil {
		return nil, err
	}
	cm, err := e.compile(ctx, wasmBytes)
	if err != nil {
		return nil, err
	}

	stdin, err := encodeStdin(globals)
	if err != nil {
		return nil, err
	}

	var stdout bytes.Buffer
	// Sandbox: stdin/stdout only. No WithFSConfig, clock, random, or env.
	cfg := wazero.NewModuleConfig().
		WithStdin(bytes.NewReader(stdin)).
		WithStdout(&stdout).
		WithName("")

	mod, err := e.runtime(ctx).InstantiateModule(ctx, cm, cfg)
	if mod != nil {
		_ = mod.Close(ctx)
	}
	if err != nil {
		// A normal WASI command exits via os.Exit; wazero surfaces that as a
		// *sys.ExitError. Exit code 0 is success — read stdout. Any non-zero
		// code is a genuine guest runtime error.
		var exitErr *sys.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 0 {
			return decodeStdout(stdout.Bytes())
		}
		// Context cancellation (e.g. timeout) and non-zero exits are errors.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("wasm/wazero: run module: %w", ctxErr)
		}
		return nil, fmt.Errorf("wasm/wazero: run module: %w", err)
	}

	return decodeStdout(stdout.Bytes())
}
