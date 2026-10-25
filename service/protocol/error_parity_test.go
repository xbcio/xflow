package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
)

// TestTaskResultErrorParityPreservesPermanent verifies that a permanent error
// (runner-side config/4xx failure) retains its classification across the wire
// so the server applies retry policy via types.IsPermanent rather than error
// text. This is the core A3 parity fix: pre-DTO the error collapsed to a
// string and was always retried as transient.
func TestTaskResultErrorParityPreservesPermanent(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"classified permanent", types.NewPermanentError("http.4xx", "bad request")},
		{"joined permanent", errors.Join(types.ErrPermanent, errors.New("config invalid"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalTaskResult(engine.TaskResult{Error: tc.err})
			if err != nil {
				t.Fatalf("MarshalTaskResult: %v", err)
			}
			got, err := UnmarshalTaskResult(data)
			if err != nil {
				t.Fatalf("UnmarshalTaskResult: %v", err)
			}
			if got.Error == nil {
				t.Fatal("recovered error is nil")
			}
			if !types.IsPermanent(got.Error) {
				t.Fatalf("IsPermanent = false, want true (classification lost across wire); err=%v", got.Error)
			}
		})
	}
}

// TestTaskResultErrorParityTransientStaysTransient verifies a non-permanent
// error remains non-permanent and its message survives the round-trip.
func TestTaskResultErrorParityTransientStaysTransient(t *testing.T) {
	for _, err := range []error{
		errors.New("connection refused"),
		types.NewTransientError("http.5xx", "server error"),
	} {
		data, marshalErr := MarshalTaskResult(engine.TaskResult{Error: err})
		if marshalErr != nil {
			t.Fatalf("MarshalTaskResult: %v", marshalErr)
		}
		got, unmarshalErr := UnmarshalTaskResult(data)
		if unmarshalErr != nil {
			t.Fatalf("UnmarshalTaskResult: %v", unmarshalErr)
		}
		if types.IsPermanent(got.Error) {
			t.Fatalf("IsPermanent = true, want false for transient err %v", err)
		}
		if got.Error.Error() != err.Error() {
			t.Fatalf("message = %q, want %q", got.Error.Error(), err.Error())
		}
	}
}

// TestTaskResultErrorLegacyStringPayloadDecodesTransient verifies backward
// compatibility: a payload from an older runner that only sets the error
// string (no error_detail) decodes to a plain transient error — equivalent to
// the pre-DTO behavior — rather than failing.
func TestTaskResultErrorLegacyStringPayloadDecodesTransient(t *testing.T) {
	legacy := `{"error":"handler failed"}`
	got, err := UnmarshalTaskResult([]byte(legacy))
	if err != nil {
		t.Fatalf("UnmarshalTaskResult legacy: %v", err)
	}
	if got.Error == nil || got.Error.Error() != "handler failed" {
		t.Fatalf("err = %v, want \"handler failed\"", got.Error)
	}
	if types.IsPermanent(got.Error) {
		t.Fatal("legacy string error must decode as transient, not permanent")
	}
}

// TestTaskResultErrorMarshalEmitsLegacyStringForOldPeers verifies new runners
// still emit the legacy error string so older servers that ignore error_detail
// receive a usable message.
func TestTaskResultErrorMarshalEmitsLegacyStringForOldPeers(t *testing.T) {
	data, err := MarshalTaskResult(engine.TaskResult{Error: types.NewPermanentError("http.4xx", "bad request")})
	if err != nil {
		t.Fatalf("MarshalTaskResult: %v", err)
	}
	var probe struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if probe.Error == "" {
		t.Fatal("legacy error string not emitted; old peers would see no error")
	}
}

// TestTaskResultErrorParityPreservesCodeThroughWrapping covers the cluster
// report path: the runner reports the executor's error, which is %w-wrapped
// several times (node context, executor failure, errors.Join) before it reaches
// MarshalTaskResult. Before using errors.As, the DTO was synthesized from the
// bare permanent bit and the classified error's Code was lost, so an Admin
// failure summary could only say "permanent error". The exact codes/messages
// below are the ones the non-finite-expression guards emit.
func TestTaskResultErrorParityPreservesCodeThroughWrapping(t *testing.T) {
	nonFinite := types.NewPermanentError("map.expr_non_finite", exprx.ErrNonFinite.Error())
	cases := []struct {
		name string
		code string
		err  error
	}{
		{
			name: "map non-finite item",
			code: "map.expr_non_finite",
			err: fmt.Errorf("executor node failure: %w",
				fmt.Errorf("xflow.map: item 1: %w", nonFinite)),
		},
		{
			name: "transform set non-finite field",
			code: "set.expr_non_finite",
			err: fmt.Errorf("node %q: %w", "set-status",
				types.NewPermanentError("set.expr_non_finite",
					fmt.Sprintf("xflow.transform.set: field %q: %v", "total", exprx.ErrNonFinite))),
		},
		{
			name: "function non-finite expression",
			code: "function.expr_eval",
			err: fmt.Errorf("node %q: %w", "calc",
				types.NewPermanentError("function.expr_eval", exprx.ErrNonFinite.Error())),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalTaskResult(engine.TaskResult{Error: tc.err})
			if err != nil {
				t.Fatalf("MarshalTaskResult: %v", err)
			}
			got, err := UnmarshalTaskResult(data)
			if err != nil {
				t.Fatalf("UnmarshalTaskResult: %v", err)
			}
			var ce *types.ClassifiedError
			if !errors.As(got.Error, &ce) {
				t.Fatalf("recovered error is not classified: %T %v", got.Error, got.Error)
			}
			if ce.Code != tc.code {
				t.Fatalf("code = %q, want %q (registry/failure summaries key off it)", ce.Code, tc.code)
			}
			if !types.IsPermanent(got.Error) {
				t.Fatal("permanent classification lost across the wire")
			}
			// The DTO message is the outward-safe one the handler wrote; wrapper
			// context (node names, executor internals) stays in the legacy string.
			if strings.Contains(ce.Message, "executor node failure") || strings.Contains(ce.Message, "node \"") {
				t.Fatalf("DTO message leaked wrapper context: %q", ce.Message)
			}
			if !strings.Contains(tc.err.Error(), ce.Message) {
				t.Fatalf("legacy error string %q no longer carries the DTO message %q", tc.err.Error(), ce.Message)
			}
		})
	}
}

// A wrapped transient ClassifiedError must round-trip with its code too: the
// errors.As path is about the chain, not about permanence.
func TestTaskResultErrorParityPreservesCodeThroughWrappingTransient(t *testing.T) {
	wrapped := fmt.Errorf("node %q: %w", "fetch",
		types.NewTransientError("http.5xx", "upstream unavailable"))
	data, err := MarshalTaskResult(engine.TaskResult{Error: wrapped})
	if err != nil {
		t.Fatalf("MarshalTaskResult: %v", err)
	}
	got, err := UnmarshalTaskResult(data)
	if err != nil {
		t.Fatalf("UnmarshalTaskResult: %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(got.Error, &ce) || ce.Code != "http.5xx" {
		t.Fatalf("transient code lost across the wire: %T %v", got.Error, got.Error)
	}
	if types.IsPermanent(got.Error) {
		t.Fatal("wrapped transient error decoded as permanent")
	}
}
