//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestOnErrorActionParity closes the A3 §6 parity-matrix gap for node-level
// OnError strategies. Each fixture exercises a different strategy across the
// local embedded and server-runner topologies, asserting that the source node
// status, execution status, and downstream routing are topology-independent.
func TestOnErrorActionParity(t *testing.T) {
	addr := requireRedis(t)

	srcStop := "test.onerror.stop.source"
	dstStop := "test.onerror.stop.downstream"
	srcErrorOutput := "test.onerror.error_output.source"
	dstErrorOutput := "test.onerror.error_output.downstream"
	srcMainOutput := "test.onerror.main_output.source"
	dstMainOutput := "test.onerror.main_output.downstream"
	srcContinue := "test.onerror.continue.source"
	dstContinue := "test.onerror.continue.downstream"

	okNode := func(dstType string) types.NodeDef {
		return types.NodeDef{Name: "ok", Type: dstType}
	}
	errNode := func(dstType string) types.NodeDef {
		return types.NodeDef{Name: "err", Type: dstType}
	}

	cases := []struct {
		parityCase
		WantSourceStatus types.NodeStatus
	}{
		{
			parityCase: parityCase{
				Name: "onerror_stop",
				Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
					return types.NodeDef{
							Name:    "source",
							Type:    srcStop,
							OnError: string(types.OnErrorStop),
						}, func(reg engine.HandlerRegistrar) {
							reg.RegisterGlobal(srcStop, &onErrorSourceHandler{nodeType: srcStop, mode: "permanent"})
							reg.RegisterGlobal(dstStop, onErrorDownstreamHandler{nodeType: dstStop})
						}
				},
				MaxAttempts:    1,
				WantAttempt:    1,
				WantStatus:     types.ExecutionStatusFailed,
				ErrContains:    "permanent fixture failure",
				OKNode:         okNode(dstStop),
				ErrNode:        errNode(dstStop),
				WantDownstream: map[string]downstreamExpectation{},
			},
			WantSourceStatus: types.NodeStatusFailed,
		},
		{
			parityCase: parityCase{
				Name: "onerror_error_output",
				Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
					return types.NodeDef{
							Name:    "source",
							Type:    srcErrorOutput,
							OnError: string(types.OnErrorOutput),
						}, func(reg engine.HandlerRegistrar) {
							reg.RegisterGlobal(srcErrorOutput, &onErrorSourceHandler{nodeType: srcErrorOutput, mode: "business"})
							reg.RegisterGlobal(dstErrorOutput, onErrorDownstreamHandler{nodeType: dstErrorOutput})
						}
				},
				MaxAttempts: 1,
				WantAttempt: 1,
				WantStatus:  types.ExecutionStatusSuccess,
				OKNode:      okNode(dstErrorOutput),
				ErrNode:     errNode(dstErrorOutput),
				WantDownstream: map[string]downstreamExpectation{
					"ok": {Status: types.NodeStatusSkipped},
					"err": {
						Status: types.NodeStatusSuccess,
						Output: map[string]any{"ran": true},
					},
				},
			},
			WantSourceStatus: types.NodeStatusSuccess,
		},
		{
			parityCase: parityCase{
				Name: "onerror_main_output",
				Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
					return types.NodeDef{
							Name:    "source",
							Type:    srcMainOutput,
							OnError: string(types.OnErrorMainOutput),
						}, func(reg engine.HandlerRegistrar) {
							reg.RegisterGlobal(srcMainOutput, &onErrorSourceHandler{nodeType: srcMainOutput, mode: "business"})
							reg.RegisterGlobal(dstMainOutput, onErrorDownstreamHandler{nodeType: dstMainOutput})
						}
				},
				MaxAttempts: 1,
				WantAttempt: 1,
				WantStatus:  types.ExecutionStatusSuccess,
				OKNode:      okNode(dstMainOutput),
				ErrNode:     errNode(dstMainOutput),
				WantDownstream: map[string]downstreamExpectation{
					"ok": {
						Status: types.NodeStatusSuccess,
						Output: map[string]any{"ran": true},
					},
					"err": {Status: types.NodeStatusSkipped},
				},
			},
			WantSourceStatus: types.NodeStatusSuccess,
		},
		{
			parityCase: parityCase{
				Name: "onerror_continue",
				Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
					return types.NodeDef{
							Name:    "source",
							Type:    srcContinue,
							OnError: string(types.OnErrorContinue),
						}, func(reg engine.HandlerRegistrar) {
							reg.RegisterGlobal(srcContinue, &onErrorSourceHandler{nodeType: srcContinue, mode: "permanent"})
							reg.RegisterGlobal(dstContinue, onErrorDownstreamHandler{nodeType: dstContinue})
						}
				},
				MaxAttempts: 1,
				WantAttempt: 1,
				WantStatus:  types.ExecutionStatusSuccess,
				OKNode:      okNode(dstContinue),
				ErrNode:     errNode(dstContinue),
				WantDownstream: map[string]downstreamExpectation{
					"ok": {
						Status: types.NodeStatusSuccess,
						Output: map[string]any{"ran": true},
					},
					"err": {Status: types.NodeStatusSkipped},
				},
			},
			WantSourceStatus: types.NodeStatusContinued,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register := tc.Build()
			retry := &types.RetrySettings{
				MaxAttempts:     tc.MaxAttempts,
				InitialInterval: 50,
			}
			def := ParityWorkflowWithDownstream(source, retry, tc.OKNode, tc.ErrNode)

			localOut := RunParityLocal(t, def, register)
			serverOut := RunParityServerRunner(t, addr, def, register)

			assertParity(t, tc.parityCase, localOut, serverOut)

			if localOut.SourceStatus != serverOut.SourceStatus {
				t.Errorf("source status parity: local=%s server-runner=%s", localOut.SourceStatus, serverOut.SourceStatus)
			}
			if localOut.SourceStatus != tc.WantSourceStatus {
				t.Errorf("local source status=%s, want %s", localOut.SourceStatus, tc.WantSourceStatus)
			}
			if serverOut.SourceStatus != tc.WantSourceStatus {
				t.Errorf("server-runner source status=%s, want %s", serverOut.SourceStatus, tc.WantSourceStatus)
			}

			if tc.Name == "onerror_stop" {
				assertNoActivatedDownstream(t, localOut)
				assertNoActivatedDownstream(t, serverOut)
			}
		})
	}
}

// onErrorSourceHandler returns either a permanent ClassifiedError or a structured
// business error (Output.Error) depending on its mode.
type onErrorSourceHandler struct {
	nodeType string
	mode     string // "permanent" or "business"
}

func (h *onErrorSourceHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType}
}

func (h *onErrorSourceHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	if h.mode == "permanent" {
		return nil, types.NewPermanentError("onerror."+h.nodeType, "permanent fixture failure")
	}
	return &types.Output{Error: &types.Error{Message: "business error"}}, nil
}

// onErrorDownstreamHandler records that it ran and identifies itself by node name.
type onErrorDownstreamHandler struct {
	nodeType string
}

func (h onErrorDownstreamHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType}
}

func (h onErrorDownstreamHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{
		Data: map[string]any{"ran": true, "node": input.NodeName},
		Port: "main",
	}, nil
}

// assertNoActivatedDownstream ensures that no downstream node reached an active
// terminal state. This is used for the "stop" fixture where the execution aborts
// before downstream nodes are scheduled.
func assertNoActivatedDownstream(t *testing.T, out ParityOutcome) {
	t.Helper()
	for name, status := range out.DownstreamStatuses {
		switch status {
		case types.NodeStatusSuccess, types.NodeStatusFailed, types.NodeStatusContinued:
			t.Errorf("downstream %q has active terminal status %s, want pending/skipped/absent", name, status)
		}
	}
}
