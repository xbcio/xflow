//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestScriptFunctionActionParity closes the A3 §6 parity-matrix gaps for the
// two built-in code node types: xflow.function and xflow.script. Each fixture
// exercises a classified error path across the local embedded and server-runner
// topologies, asserting that attempt counts, terminal statuses, error codes,
// and downstream routing survive the wire.
func TestScriptFunctionActionParity(t *testing.T) {
	addr := requireRedis(t)

	cases := []parityCase{
		{
			Name: "function_config_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				inner, ok := registry.Lookup("xflow.function")
				if !ok {
					t.Fatal("xflow.function handler not found in node registry")
				}
				return instrumentedBuiltinBuild(types.NodeDef{
					Name: "source",
					Type: "xflow.function",
					Parameters: map[string]any{
						"function_name": "unregistered_parity_func",
					},
				}, inner, "function_config_permanent")
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "function.not_registered",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "function_timeout_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				fnName := "parity_function_timeout"
				node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
					return nil, context.DeadlineExceeded
				})
				inner, ok := registry.Lookup("xflow.function")
				if !ok {
					t.Fatal("xflow.function handler not found in node registry")
				}
				return instrumentedBuiltinBuild(types.NodeDef{
					Name: "source",
					Type: "xflow.function",
					Parameters: map[string]any{
						"function_name": fnName,
					},
				}, inner, "function_timeout_transient_exhausted")
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "function.timeout",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "function_user_error_port",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				fnName := "parity_function_user_error"
				node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
					return nil, errors.New("user function error")
				})
				inner, ok := registry.Lookup("xflow.function")
				if !ok {
					t.Fatal("xflow.function handler not found in node registry")
				}
				return instrumentedBuiltinBuild(types.NodeDef{
					Name:    "source",
					Type:    "xflow.function",
					OnError: string(types.OnErrorOutput),
					Parameters: map[string]any{
						"function_name": fnName,
					},
				}, inner, "function_user_error_port")
			},
			MaxAttempts:            1,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusSuccess,
			// OnError=error_output routes the handler error to the error port;
			// the runtime receipt marks it as ErrorSourceErrorPort.
			WantKind:               string(types.ErrorKindErrorPort),
			WantRetryable:          true,
			WantHandlerInvocations: 1,
			OKNode: types.NodeDef{
				Name: "ok",
				Type: "xflow.function",
				Parameters: map[string]any{
					"code": "\"ok\"",
				},
			},
			ErrNode: types.NodeDef{
				Name: "err",
				Type: "xflow.function",
				Parameters: map[string]any{
					"code": "\"err\"",
				},
			},
			WantDownstream: map[string]downstreamExpectation{
				"ok": {Status: types.NodeStatusSkipped},
				"err": {
					Status: types.NodeStatusSuccess,
					Output: map[string]any{"result": "err"},
				},
			},
		},
		{
			Name: "script_config_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				inner, ok := registry.Lookup("xflow.script")
				if !ok {
					t.Fatal("xflow.script handler not found in node registry")
				}
				return instrumentedBuiltinBuild(types.NodeDef{
					Name: "source",
					Type: "xflow.script",
					Parameters: map[string]any{
						"language": "js",
						"runtime":  "goja",
					},
				}, inner, "script_config_permanent")
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "script.code_required",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "script_timeout_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				inner, ok := registry.Lookup("xflow.script")
				if !ok {
					t.Fatal("xflow.script handler not found in node registry")
				}
				return instrumentedBuiltinBuild(types.NodeDef{
					Name: "source",
					Type: "xflow.script",
					Parameters: map[string]any{
						"language": "js",
						"runtime":  "qjs",
						"code":     "while(true){}",
						"timeout":  "50ms",
					},
				}, inner, "script_timeout_transient_exhausted")
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "script.timeout",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "script_user_error_port",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				inner, ok := registry.Lookup("xflow.script")
				if !ok {
					t.Fatal("xflow.script handler not found in node registry")
				}
				return instrumentedBuiltinBuild(types.NodeDef{
					Name:    "source",
					Type:    "xflow.script",
					OnError: string(types.OnErrorOutput),
					Parameters: map[string]any{
						"language": "js",
						"runtime":  "goja",
						"code":     "throw new Error('user')",
					},
				}, inner, "script_user_error_port")
			},
			MaxAttempts:            1,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusSuccess,
			// OnError=error_output routes the handler error to the error port;
			// the runtime receipt marks it as ErrorSourceErrorPort.
			WantKind:               string(types.ErrorKindErrorPort),
			WantRetryable:          true,
			WantHandlerInvocations: 1,
			OKNode: types.NodeDef{
				Name: "ok",
				Type: "xflow.function",
				Parameters: map[string]any{
					"code": "\"ok\"",
				},
			},
			ErrNode: types.NodeDef{
				Name: "err",
				Type: "xflow.function",
				Parameters: map[string]any{
					"code": "\"err\"",
				},
			},
			WantDownstream: map[string]downstreamExpectation{
				"ok": {Status: types.NodeStatusSkipped},
				"err": {
					Status: types.NodeStatusSuccess,
					Output: map[string]any{"result": "err"},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register, inv := tc.Build()
			retry := &types.RetrySettings{
				MaxAttempts:     tc.MaxAttempts,
				InitialInterval: 50,
			}
			var def *types.WorkflowDef
			if tc.OKNode.Name != "" || tc.ErrNode.Name != "" {
				def = ParityWorkflowWithDownstream(source, retry, tc.OKNode, tc.ErrNode)
			} else {
				def = ParityWorkflow(source, retry)
			}

			// script/function cases use the real built-in handlers via counting
			// wrappers; WantKind/WantRetryable are explicit manifest literals above.

			localOut := RunParityLocal(t, def, register, nil, tc.Name, "local")
			serverOut := RunParityServerRunner(t, addr, def, register, nil, tc.Name, "server-runner")
			clusterOut := RunParityCluster(t, addr, def, register, nil, tc.Name, "cluster-durable")

			invocations := invCount(inv)
			for _, o := range []*ParityOutcome{&localOut, &serverOut, &clusterOut} {
				o.HandlerInvocations = invocations
			}

			assertParityThreeWay(t, tc, localOut, serverOut, clusterOut)
			logParityMatrixRow(t, tc, "local", localOut)
			logParityMatrixRow(t, tc, "server-runner", serverOut)
			logParityMatrixRow(t, tc, "cluster-durable", clusterOut)
		})
	}
}
