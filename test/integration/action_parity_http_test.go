//go:build integration

package integration

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestHTTPActionErrorParity verifies that the built-in xflow.http action node
// classifies HTTP errors consistently across the local embedded and
// server-runner topologies. Each fixture exercises one classification bucket
// from node/internal/action/http.go.
func TestHTTPActionErrorParity(t *testing.T) {
	addr := requireRedis(t)

	cases := []parityCase{
		{
			Name: "http_4xx_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "bad request", http.StatusBadRequest)
				}))
				t.Cleanup(srv.Close)
				inner, ok := registry.Lookup("xflow.http")
				if !ok {
					t.Fatal("xflow.http handler not found in node registry")
				}
				return instrumentedBuiltinBuild(httpNodeDef(srv.URL), inner, "http_4xx_permanent")
			},
			MaxAttempts:            3,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "http.4xx",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "http_408_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "request timeout", http.StatusRequestTimeout)
				}))
				t.Cleanup(srv.Close)
				inner, ok := registry.Lookup("xflow.http")
				if !ok {
					t.Fatal("xflow.http handler not found in node registry")
				}
				return instrumentedBuiltinBuild(httpNodeDef(srv.URL), inner, "http_408_transient_exhausted")
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "http.408",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "http_429_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "too many requests", http.StatusTooManyRequests)
				}))
				t.Cleanup(srv.Close)
				inner, ok := registry.Lookup("xflow.http")
				if !ok {
					t.Fatal("xflow.http handler not found in node registry")
				}
				return instrumentedBuiltinBuild(httpNodeDef(srv.URL), inner, "http_429_transient_exhausted")
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "http.429",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "http_5xx_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				}))
				t.Cleanup(srv.Close)
				inner, ok := registry.Lookup("xflow.http")
				if !ok {
					t.Fatal("xflow.http handler not found in node registry")
				}
				return instrumentedBuiltinBuild(httpNodeDef(srv.URL), inner, "http_5xx_transient_exhausted")
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "http.5xx",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "http_connection_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				inner, ok := registry.Lookup("xflow.http")
				if !ok {
					t.Fatal("xflow.http handler not found in node registry")
				}
				return instrumentedBuiltinBuild(httpNodeDef("http://127.0.0.1:1/"), inner, "http_connection_transient_exhausted")
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "http.connection",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register, inv := tc.Build()
			retry := &types.RetrySettings{
				MaxAttempts:     tc.MaxAttempts,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			// HTTP cases use the real xflow.http handler via a counting wrapper;
			// WantKind/WantRetryable are explicit manifest literals above.

			localOut := RunParityLocal(t, def, register)
			serverOut := RunParityServerRunner(t, addr, def, register)
			clusterOut := RunParityCluster(t, addr, def, register)

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

// httpNodeDef builds a single xflow.http source node definition using the
// supplied URL. A short timeout keeps failing fixtures fast.
func httpNodeDef(rawURL string) types.NodeDef {
	return types.NodeDef{
		Name: "http",
		Type: "xflow.http",
		Parameters: map[string]any{
			"method":  "GET",
			"url":     rawURL,
			"options": map[string]any{"timeout": "5s"},
		},
	}
}
