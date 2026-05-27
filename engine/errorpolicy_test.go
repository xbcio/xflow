package engine

import (
	"errors"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestApplyOnError_Stop(t *testing.T) {
	outcome := ApplyOnError("stop", errors.New("boom"), nil, nil)
	if !outcome.ExecFatal {
		t.Error("stop strategy should be fatal")
	}
	if outcome.NodeStatus != "failed" {
		t.Errorf("expected failed, got %s", outcome.NodeStatus)
	}
	if outcome.ErrorMessage != "boom" {
		t.Errorf("expected error message 'boom', got %s", outcome.ErrorMessage)
	}
}

func TestApplyOnError_ErrorOutput(t *testing.T) {
	bizErr := &node.Error{Message: "policy violation", StatusCode: 403}
	outcome := ApplyOnError("error_output", nil, bizErr, &node.Output{Data: map[string]any{"x": 1}})
	if outcome.ExecFatal {
		t.Error("error_output should not be fatal")
	}
	if outcome.RoutePort != "error" {
		t.Errorf("expected error port, got %s", outcome.RoutePort)
	}
	if outcome.NodeStatus != "success" {
		t.Errorf("expected success, got %s", outcome.NodeStatus)
	}
	if outcome.Output["error"] == nil {
		t.Error("output should contain error data")
	}
	// original data should be preserved
	if outcome.Output["x"] != 1 {
		t.Error("original output data should be preserved")
	}
	// error data should include status_code
	errData, ok := outcome.Output["error"].(map[string]any)
	if !ok {
		t.Fatal("error field should be a map")
	}
	if errData["status_code"] != 403 {
		t.Errorf("expected status_code 403, got %v", errData["status_code"])
	}
}

func TestApplyOnError_MainOutput(t *testing.T) {
	sysErr := errors.New("timeout")
	outcome := ApplyOnError("main_output", sysErr, nil, nil)
	if outcome.ExecFatal {
		t.Error("main_output should not be fatal")
	}
	if outcome.RoutePort != "main" {
		t.Errorf("expected main port, got %s", outcome.RoutePort)
	}
	if outcome.NodeStatus != "success" {
		t.Errorf("expected success, got %s", outcome.NodeStatus)
	}
	if outcome.Output["error"] == nil {
		t.Error("output should contain error info on main port")
	}
}

func TestApplyOnError_Continue(t *testing.T) {
	sysErr := errors.New("non-critical")
	outcome := ApplyOnError("continue", sysErr, nil, &node.Output{Data: map[string]any{"partial": true}})
	if outcome.ExecFatal {
		t.Error("continue should not be fatal")
	}
	if outcome.RoutePort != "main" {
		t.Errorf("expected main port, got %s", outcome.RoutePort)
	}
	if outcome.NodeStatus != "continued" {
		t.Errorf("expected continued, got %s", outcome.NodeStatus)
	}
	// original data preserved, no error key injected
	if outcome.Output["partial"] != true {
		t.Error("original output data should be preserved")
	}
	if outcome.Output["error"] != nil {
		t.Error("continue strategy should not inject error key")
	}
}

func TestApplyOnError_DefaultIsStop(t *testing.T) {
	outcome := ApplyOnError("", errors.New("x"), nil, nil)
	if !outcome.ExecFatal {
		t.Error("empty strategy should default to stop (fatal)")
	}
}

func TestApplyOnError_NilOutput_ErrorOutput(t *testing.T) {
	outcome := ApplyOnError("error_output", errors.New("oops"), nil, nil)
	if outcome.ExecFatal {
		t.Error("error_output should not be fatal even with nil output")
	}
	if outcome.Output["error"] == nil {
		t.Error("should still have error key with nil output")
	}
}
