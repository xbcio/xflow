//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
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
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				return types.NodeDef{
					Name: "source",
					Type: "xflow.function",
					Parameters: map[string]any{
						"function_name": "unregistered_parity_func",
					},
				}, nil
			},
			MaxAttempts: 2,
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "function.not_registered",
		},
		{
			Name: "function_timeout_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				fnName := "parity_function_timeout"
				node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
					return nil, context.DeadlineExceeded
				})
				return types.NodeDef{
					Name: "source",
					Type: "xflow.function",
					Parameters: map[string]any{
						"function_name": fnName,
					},
				}, nil
			},
			MaxAttempts: 2,
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "function.timeout",
		},
		{
			Name: "function_user_error_port",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				fnName := "parity_function_user_error"
				node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
					return nil, errors.New("user function error")
				})
				return types.NodeDef{
					Name:    "source",
					Type:    "xflow.function",
					OnError: string(types.OnErrorOutput),
					Parameters: map[string]any{
						"function_name": fnName,
					},
				}, nil
			},
			MaxAttempts: 1,
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusSuccess,
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
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				return types.NodeDef{
					Name: "source",
					Type: "xflow.script",
					Parameters: map[string]any{
						"language": "js",
						"runtime":  "goja",
					},
				}, nil
			},
			MaxAttempts: 2,
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "script.code_required",
		},
		{
			Name: "script_timeout_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				return types.NodeDef{
					Name: "source",
					Type: "xflow.script",
					Parameters: map[string]any{
						"language": "js",
						"runtime":  "qjs",
						"code":     "while(true){}",
						"timeout":  "50ms",
					},
				}, nil
			},
			MaxAttempts: 2,
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "script.timeout",
		},
		{
			Name: "script_user_error_port",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				return types.NodeDef{
					Name:    "source",
					Type:    "xflow.script",
					OnError: string(types.OnErrorOutput),
					Parameters: map[string]any{
						"language": "js",
						"runtime":  "goja",
						"code":     "throw new Error('user')",
					},
				}, nil
			},
			MaxAttempts: 1,
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusSuccess,
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
			source, register := tc.Build()
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

			localOut := RunParityLocal(t, def, register)
			serverOut := RunParityServerRunner(t, addr, def, register)
			clusterOut := RunParityCluster(t, addr, def, register)

			assertParityThreeWay(t, tc, localOut, serverOut, clusterOut)
		})
	}
}
