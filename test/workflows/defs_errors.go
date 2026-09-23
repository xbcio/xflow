package workflows

import (
	"fmt"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// MissingFunctionName is never registered. A function node naming it fails
// permanently, which is what makes it useful as a deterministic error source: a
// test does not have to arrange a transient condition to exercise an error port.
// A missing function is a permanent error, so retry does not change its outcome
// — pair it with a node that has no retry to assert routing, and use a
// caller-supplied node to assert retry.
const MissingFunctionName = "workflows.missing_function"

// MissingFunction returns a function node naming MissingFunctionName.
func MissingFunction() *node.FunctionNode { return node.Function(MissingFunctionName) }

// ErrorPortWorkflow routes a node's error port to a recovery node while keeping
// its success port wired, so the workflow has both outcomes bound.
//
//	start → subject ─main→  done
//	               └error→ recover → done
//
// subject must expose both a "main" and an "error" port. The error strategy is
// set explicitly: an unset strategy is OnErrorStop, and a node that stops on
// failure never reaches the error port however the edges are drawn. Binding the
// port and selecting the strategy are two separate decisions.
//
// Node types: xflow.start, the subject's own type, xflow.transform.set,
// xflow.end.
func ErrorPortWorkflow(subject types.Builder) *xflow.WorkflowBuilder {
	wf := xflow.Workflow("error-port")
	start := wf.Node("start", node.Start())
	subj := wf.Node("subject", subject).OnError(types.OnErrorOutput)
	recover := wf.Node("recover", node.Set(map[string]any{"recovered": true}))
	done := wf.Node("done", node.End())

	wf.Connect(start, subj).
		Connect(subj.Output("error"), recover).
		Connect(subj, done).
		Connect(recover, done)
	return wf
}

// WithRetry enables retry on one node of a built definition. The builder has no
// retry setter — retry is a property of the deployed node, not of the node type,
// so it is applied to the definition rather than to the builder.
func WithRetry(def *types.WorkflowDef, nodeName string, attempts int, initialInterval time.Duration) error {
	if attempts < 1 {
		return fmt.Errorf("WithRetry: attempts = %d, want >= 1", attempts)
	}
	for i := range def.Nodes {
		if def.Nodes[i].Name != nodeName {
			continue
		}
		def.Nodes[i].Retry = &types.RetrySettings{
			Enabled:         true,
			MaxAttempts:     attempts,
			InitialInterval: int(initialInterval.Milliseconds()),
		}
		return nil
	}
	return fmt.Errorf("WithRetry: node %q not found", nodeName)
}
