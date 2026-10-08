package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// countingBuildEngine counts BuildTaskLease calls so a test can tell a settled
// assignment (never built again) from a requeued one (built on every claim).
type countingBuildEngine struct {
	*fakeControlEngine
	builds int
}

func (e *countingBuildEngine) BuildTaskLease(ctx context.Context, task *engine.Task) (*engine.TaskLease, error) {
	e.builds++
	return e.fakeControlEngine.BuildTaskLease(ctx, task)
}

// TestCorePollSettlesSystemHandledTask pins the cluster half of a task the
// engine resolves itself inside BuildTaskLease (a pinned node committing its
// pin_data mock). The dispatcher can route such a task to the runner directory
// when its own handling pass declines on a transient graph-load error, and the
// poll path then gets ErrSystemTaskHandled. That is a resolved task, not a
// dispatch failure: the claim must be settled as dropped and the runner must
// not see an error. Requeueing it would replay the same already-terminal
// commit on every claim and never settle -- the defect this guards. Both
// directory implementations are covered because they settle through different
// ledgers.
func TestCorePollSettlesSystemHandledTask(t *testing.T) {
	directories := map[string]func(t *testing.T) RunnerDirectory{
		"memory": func(*testing.T) RunnerDirectory { return NewMemoryRunnerDirectory() },
		"redis": func(t *testing.T) RunnerDirectory {
			_, rdb := newRedisRunnerDirectoryTestClient(t)
			return NewRedisRunnerDirectory(rdb)
		},
	}
	for name, newDirectory := range directories {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			directory := newDirectory(t)
			session, err := directory.Register(ctx, RegisterRunnerRequest{
				RunnerID:     "runner-pinned",
				Capacity:     1,
				Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
				Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
			})
			if err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			if ok, err := directory.EnqueueAssignment(ctx, stableTestAssignment("pinned")); err != nil || !ok {
				t.Fatalf("EnqueueAssignment() = %v, %v", ok, err)
			}
			eng := &countingBuildEngine{fakeControlEngine: &fakeControlEngine{buildErr: engine.ErrSystemTaskHandled}}
			core := &Core{engine: eng, runners: directory, pollWait: time.Second}
			req := protocol.PollTaskRequest{
				RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1,
				Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
			}

			for i := 0; i < 2; i++ {
				resp, err := core.pollTask(ctx, req, TransportInfo{})
				if err != nil {
					t.Fatalf("poll %d error = %v, want nil: a system-handled task is resolved, "+
						"not a dispatch failure", i+1, err)
				}
				if resp.Lease != nil {
					t.Fatalf("poll %d lease = %+v, want none", i+1, resp.Lease)
				}
			}
			if eng.builds != 1 {
				t.Fatalf("BuildTaskLease called %d times, want 1: the claim was requeued "+
					"instead of settled", eng.builds)
			}
		})
	}
}
