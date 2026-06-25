package engine

import (
	"github.com/xbcio/xflow/types"
)

// OnErrorOutcome is the result of applying an error-handling strategy to a
// node failure. The engine uses it to decide routing, status, and whether the
// entire execution should be aborted.
type OnErrorOutcome struct {
	Output       map[string]any
	RoutePort    string
	NodeStatus   types.NodeStatus
	ExecFatal    bool
	ErrorMessage string
}

// ApplyOnError maps a strategy string + error values to an OnErrorOutcome.
//
// Strategies:
//   - "stop"         (default) — fatal; execution is aborted.
//   - "error_output" — non-fatal; routes to the "error" port with error info merged into output.
//   - "main_output"  — non-fatal; routes to the "main" port with error info merged into output.
//   - "continue"     — non-fatal; routes to the "main" port, node status is "continued".
func ApplyOnError(strategy string, sysErr error, bizErr *types.Error, output *types.Output) OnErrorOutcome {
	errMsg := ""
	if sysErr != nil {
		errMsg = sysErr.Error()
	} else if bizErr != nil {
		errMsg = bizErr.Message
	}

	switch strategy {
	case "error_output":
		// The node handled the error gracefully and routes to the "error" port.
		// Node status is "success" so the execution is not marked as failed.
		out := copyOutputData(output)
		out["error"] = buildErrData(errMsg, bizErr)
		return OnErrorOutcome{
			Output:       out,
			RoutePort:    "error",
			NodeStatus:   types.NodeStatusSuccess,
			ExecFatal:    false,
			ErrorMessage: errMsg,
		}

	case "main_output":
		// The node handled the error gracefully and routes to the "main" port.
		// Node status is "success" so the execution is not marked as failed.
		out := copyOutputData(output)
		out["error"] = buildErrData(errMsg, nil)
		return OnErrorOutcome{
			Output:       out,
			RoutePort:    "main",
			NodeStatus:   types.NodeStatusSuccess,
			ExecFatal:    false,
			ErrorMessage: errMsg,
		}

	case "continue":
		out := copyOutputData(output)
		return OnErrorOutcome{
			Output:       out,
			RoutePort:    "main",
			NodeStatus:   types.NodeStatusContinued,
			ExecFatal:    false,
			ErrorMessage: errMsg,
		}

	default: // "stop" or empty
		return OnErrorOutcome{
			Output:       nil,
			RoutePort:    "",
			NodeStatus:   types.NodeStatusFailed,
			ExecFatal:    true,
			ErrorMessage: errMsg,
		}
	}
}

// copyOutputData returns a shallow copy of output.Data, or an empty map if nil.
func copyOutputData(output *types.Output) map[string]any {
	out := make(map[string]any)
	if output != nil && output.Data != nil {
		for k, v := range output.Data {
			out[k] = v
		}
	}
	return out
}

// buildErrData constructs the error metadata map placed under the "error" key.
func buildErrData(msg string, bizErr *types.Error) map[string]any {
	d := map[string]any{"message": msg}
	if bizErr != nil {
		if bizErr.StatusCode != 0 {
			d["status_code"] = bizErr.StatusCode
		}
		if bizErr.NodeName != "" {
			d["node"] = bizErr.NodeName
		}
	}
	return d
}
