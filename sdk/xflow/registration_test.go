package xflow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/types"
)

// TestIsRetryableRegistrationErrorClassifiesByCause pins the classes the
// contract names. A host's retry loop is only as good as this predicate: a
// transient failure misread as permanent reproduces the half-dead process R4
// describes, and a permanent one misread as transient is a retry loop that
// never ends.
func TestIsRetryableRegistrationErrorClassifiesByCause(t *testing.T) {
	staleRevision := &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictStaleRevision}
	if !errors.Is(staleRevision, backend.ErrWorkflowConflict) {
		t.Fatal("a typed replace conflict must unwrap to ErrWorkflowConflict; this " +
			"test's ordering case does not exist otherwise")
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not a failure to retry", nil, false},
		{"key conflict is deterministic", backend.ErrWorkflowConflict, false},
		{"wrapped key conflict is deterministic", fmt.Errorf("register: %w", backend.ErrWorkflowConflict), false},
		{"registry cannot replace atomically", backend.ErrWorkflowReplaceUnsupported, false},
		{"deadline exceeded is transport", context.DeadlineExceeded, true},
		{"wrapped deadline exceeded is transport", fmt.Errorf("ReplaceWorkflow: %w", context.DeadlineExceeded), true},
		{"connection refused is transport", errors.New("dial tcp 10.0.0.1:6379: connect: connection refused"), true},
		{"indeterminate mutation is retryable", &backend.WorkflowMutationIndeterminateError{Err: errors.New("redis: connection pool timeout")}, true},
		{"a racing writer's conflict is retryable", staleRevision, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRetryableRegistrationError(tc.err); got != tc.want {
				t.Fatalf("IsRetryableRegistrationError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// retryRegistrationHandler is a LocalNode handler, which a Server refuses to
// register: it dispatches every node to a remote runner and has no executor for
// one bound in-process.
type retryRegistrationHandler struct{}

func (retryRegistrationHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.retry_registration"}
}

func (retryRegistrationHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// retryRegistrationWorkflow builds a minimal registrable workflow; with tail it
// differs from the version without one, which is what a conflict needs.
func retryRegistrationWorkflow(name string, tail bool) *WorkflowBuilder {
	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	work := wf.Node("work", node.Function("return input"))
	wf.Connect(start, work)
	if tail {
		end := wf.Node("tail", node.Function("return input"))
		wf.Connect(work, end)
	}
	return wf
}

// TestServerRegistrationErrorsClassifyEndToEnd runs the failures a host
// actually meets through the SDK's own entry points, rather than through
// hand-wrapped errors: the SDK's pre-flight refusals, a definition the compiler
// rejects, and a key conflict. Each must read as permanent — a host that
// retried them would loop on an answer that cannot change — while the retry-safe
// ReplaceWorkflow call that fixes the conflict succeeds.
func TestServerRegistrationErrorsClassifyEndToEnd(t *testing.T) {
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()
	ctx := context.Background()

	_, err = srv.AddWorkflow(ctx, nil)
	if err == nil {
		t.Fatal("AddWorkflow accepted a nil workflow")
	}
	if IsRetryableRegistrationError(err) {
		t.Errorf("a nil workflow read as retryable: %v", err)
	}

	withLocal := Workflow("local")
	withLocal.Node("start", node.Start())
	withLocal.LocalNode("work", &retryRegistrationHandler{})
	_, err = srv.AddWorkflow(ctx, withLocal)
	if err == nil {
		t.Fatal("AddWorkflow accepted a workflow with local node handlers")
	}
	if IsRetryableRegistrationError(err) {
		t.Errorf("a definition with local handlers read as retryable: %v", err)
	}

	// The builder emits a definition with no nodes, which the compiler rejects:
	// the same class a malformed host definition produces.
	_, err = srv.AddWorkflow(ctx, Workflow("no-nodes"))
	if err == nil {
		t.Fatal("AddWorkflow accepted a definition with no nodes")
	}
	var compileErr *apiserver.WorkflowCompileError
	if !errors.As(err, &compileErr) {
		t.Fatalf("error = %v, want *apiserver.WorkflowCompileError", err)
	}
	if IsRetryableRegistrationError(err) {
		t.Errorf("a compiler rejection read as retryable: %v", err)
	}

	if _, err := srv.AddWorkflow(ctx, retryRegistrationWorkflow("retry-classes", false)); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	_, err = srv.AddWorkflow(ctx, retryRegistrationWorkflow("retry-classes", true))
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("error = %v, want backend.ErrWorkflowConflict", err)
	}
	if IsRetryableRegistrationError(err) {
		t.Errorf("an AddWorkflow key conflict read as retryable: %v", err)
	}
	if _, err := srv.ReplaceWorkflow(ctx, retryRegistrationWorkflow("retry-classes", true)); err != nil {
		t.Fatalf("ReplaceWorkflow did not clear the conflict it is the answer to: %v", err)
	}
}
