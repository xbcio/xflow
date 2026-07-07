package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

type fakeStream struct {
	mu     sync.Mutex
	recvCh chan protocol.RunnerFrame
	sendCh chan protocol.ServerFrame
	ctx    context.Context
}

func newFakeStream(ctx context.Context, buf int) *fakeStream {
	return &fakeStream{recvCh: make(chan protocol.RunnerFrame, buf), sendCh: make(chan protocol.ServerFrame, buf), ctx: ctx}
}
func (f *fakeStream) Recv() (protocol.RunnerFrame, error) {
	select {
	case fr := <-f.recvCh:
		return fr, nil
	case <-f.ctx.Done():
		return protocol.RunnerFrame{}, f.ctx.Err()
	}
}
func (f *fakeStream) Send(fr protocol.ServerFrame) error {
	select {
	case f.sendCh <- fr:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}
func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) push(fr protocol.RunnerFrame) { f.recvCh <- fr }

type fakeEngine struct{ committed int }

func (f *fakeEngine) CommitTaskResult(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) error {
	f.committed++
	return nil
}

func (f *fakeEngine) Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	return "", nil
}

func (f *fakeEngine) Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error) {
	return engine.ExecutionDetail{}, nil
}

func (f *fakeEngine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	return nil
}

func (f *fakeEngine) Cancel(ctx context.Context, id types.ExecutionID) error {
	return nil
}

func newCoreForConnectTest() *Core {
	eng := &fakeEngine{}
	return &Core{engine: eng, runners: NewRunnerPool(), pollWait: time.Second}
}

func TestCoreConnectRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	core := newCoreForConnectTest()
	stream := newFakeStream(ctx, 8)
	done := make(chan error, 1)
	go func() { done <- core.Connect(stream) }()

	stream.push(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "r1", Concurrency: 2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}})
	if fr := mustRecvServer(t, stream.sendCh); fr.Welcome == nil || fr.Welcome.RunnerID != "r1" {
		t.Fatalf("expected WELCOME, got %+v", fr)
	}
	lease := engine.TaskLease{LeaseID: "L1", LeaseToken: "T1", NodeType: "xflow.function"}
	if err := core.runners.Assign(lease); err != nil {
		t.Fatalf("assign: %v", err)
	}
	if fr := mustRecvServer(t, stream.sendCh); fr.Task == nil || fr.Task.Lease == nil || fr.Task.Lease.LeaseID != "L1" {
		t.Fatalf("expected TASK L1, got %+v", fr)
	}
	stream.push(protocol.RunnerFrame{Result: &protocol.ResultFrame{
		LeaseID: "L1", Lease: &lease,
		Result: engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}})
	if fr := mustRecvServer(t, stream.sendCh); fr.Ack == nil || !fr.Ack.Accepted || fr.Ack.LeaseID != "L1" {
		t.Fatalf("expected ACK accepted L1, got %+v", fr)
	}
	stream.push(protocol.RunnerFrame{Bye: &protocol.ByeFrame{}})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Connect returned err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Connect did not return after BYE")
	}
}

func mustRecvServer(t *testing.T, ch <-chan protocol.ServerFrame) protocol.ServerFrame {
	t.Helper()
	select {
	case fr := <-ch:
		return fr
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server frame")
	}
	return protocol.ServerFrame{}
}

var _ = errors.New
