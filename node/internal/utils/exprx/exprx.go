// Package expr provides the expression evaluation helpers shared by builtin
// nodes (xflow.if, xflow.switch, xflow.loop, xflow.split, xflow.function,
// xflow.script). These helpers are not part of the public node API.
package exprx

import (
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/xbcio/xflow/types"
	lru "github.com/hashicorp/golang-lru/v2"
)

// DefaultExprCacheSize bounds the compiled-expression LRU cache. Expressions
// are static configuration content, so the working set is normally small; the
// bound protects high-cardinality deployments from unbounded growth.
const DefaultExprCacheSize = 256

// exprCache holds compiled expr programs keyed by source code (plus the
// compile-mode flag). expr.Env is only used for type inference at compile
// time — the resulting *vm.Program is safe for concurrent reuse across
// different env values, so caching by code avoids recompiling the same
// expression on every node execution (e.g. once per xflow.loop iteration).
// Bounded by an LRU so a deployment with high expression churn stays
// memory-bounded instead of growing without limit.
var exprCache = newExprCache(DefaultExprCacheSize)

type exprCacheKey struct {
	code   string
	asBool bool
}

type exprCacheStore struct {
	c *lru.Cache[exprCacheKey, *vm.Program]
}

func newExprCache(size int) *exprCacheStore {
	c, err := lru.New[exprCacheKey, *vm.Program](size)
	if err != nil {
		panic(err)
	}
	return &exprCacheStore{c: c}
}

// CompileExpr compiles code into a *vm.Program, reusing a cached program for
// identical (code, asBool) pairs instead of recompiling on every call.
func CompileExpr(code string, env map[string]any, asBool bool) (*vm.Program, error) {
	key := exprCacheKey{code: code, asBool: asBool}
	if cached, ok := exprCache.c.Get(key); ok {
		return cached, nil
	}

	opts := []expr.Option{expr.Env(env)}
	if asBool {
		opts = append(opts, expr.AsBool())
	}
	program, err := expr.Compile(code, opts...)
	if err != nil {
		return nil, err
	}

	// LRU.Add is atomic and thread-safe; concurrent compilers of the same key
	// may both compile but only one entry is retained.
	exprCache.c.Add(key, program)
	return program, nil
}

// EvalExpr compiles (with caching) and runs code against env, returning the
// result. Set asBool to require the expression to evaluate to a boolean
// (used by conditional nodes like xflow.if and rules-mode xflow.switch).
func EvalExpr(code string, env map[string]any, asBool bool) (any, error) {
	program, err := CompileExpr(code, env, asBool)
	if err != nil {
		return nil, fmt.Errorf("compile expression: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("evaluate expression: %w", err)
	}
	return result, nil
}

// BuildExprEnv constructs the expression evaluation environment from node input.
// Available variables: $input (Data), $inputs (multi-port), $vars, $config,
// $params, and $runtime. The extra map, when non-nil, is merged into the env
// top level (overwriting same-named keys) so callers can inject additional
// variables — e.g. xflow.function spreads its "params" and xflow.script adds
// $credentials/$credential — without re-implementing the base environment.
func BuildExprEnv(input *types.Input, extra map[string]any) map[string]any {
	env := make(map[string]any, 16)

	if input.Data != nil {
		for k, v := range input.Data {
			env[k] = v
		}
	}

	env["$input"] = input.Data
	env["$inputs"] = input.Inputs
	env["$vars"] = input.Vars
	env["$config"] = input.Config
	env["$params"] = input.Params
	env["$runtime"] = RuntimeEnv(input)

	for k, v := range extra {
		env[k] = v
	}

	return env
}

// RuntimeEnv builds the $runtime sub-environment from node input.
func RuntimeEnv(input *types.Input) map[string]any {
	env := map[string]any{
		"vars": map[string]any(nil),
	}
	if input.Runtime != nil {
		env["vars"] = input.Runtime.Vars
	}
	return env
}
