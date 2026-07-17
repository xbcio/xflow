package protocol

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
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
