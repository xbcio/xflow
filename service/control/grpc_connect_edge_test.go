package control

import (
	"context"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/service/protocol"
)

func TestGRPCConnectRejectsNonHelloFirstFrame(t *testing.T) {
	client := startGRPCTestServer(t, &fakeControlEngine{}, NewMemoryRunnerDirectory())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer func() { _ = stream.Close() }()

	if err := stream.Send(protocol.RunnerFrame{Bye: &protocol.ByeFrame{}}); err != nil {
		t.Fatalf("send non-HELLO first frame: %v", err)
	}
	_, err = stream.Recv()
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("first non-HELLO frame status = %v (%v), want InvalidArgument", got, err)
	}
}

func TestGRPCConnectAuthenticatesHelloFromBearerMetadata(t *testing.T) {
	store := newGRPCRegisterPolicyStore(t, []string{"xflow.function"}, nil)
	directory := NewMemoryRunnerDirectory()
	client := startGRPCTestServer(t, &fakeControlEngine{}, directory, WithGRPCAuthenticator(store))

	t.Run("missing metadata cannot register", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		stream, err := client.Connect(ctx)
		if err != nil {
			t.Fatalf("Connect() error = %v", err)
		}
		defer func() { _ = stream.Close() }()

		// HelloFrame has no token-bearing payload field. A frame alone must not
		// bypass the configured authenticator.
		if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
			RunnerID:     "runner-connect-no-metadata",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		}}); err != nil {
			t.Fatalf("send HELLO without metadata: %v", err)
		}
		_, err = stream.Recv()
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Fatalf("HELLO without bearer metadata status = %v (%v), want Unauthenticated", got, err)
		}
		if _, found := directory.Runner(ctx, "runner-connect-no-metadata"); found {
			t.Fatal("HELLO without bearer metadata registered a runner")
		}
	})

	t.Run("bearer metadata registers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		stream, err := client.WithToken("secret").Connect(ctx)
		if err != nil {
			t.Fatalf("Connect() error = %v", err)
		}
		defer func() { _ = stream.Close() }()

		if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
			RunnerID:     "runner-connect-metadata",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		}}); err != nil {
			t.Fatalf("send HELLO with bearer metadata: %v", err)
		}
		welcome, err := stream.Recv()
		if err != nil {
			t.Fatalf("receive WELCOME after metadata-authenticated HELLO: %v", err)
		}
		if welcome.Welcome == nil || welcome.Welcome.RunnerID != "runner-connect-metadata" {
			t.Fatalf("first server frame = %#v, want WELCOME for metadata-authenticated runner", welcome)
		}
		if _, found := directory.Runner(ctx, "runner-connect-metadata"); !found {
			t.Fatal("metadata-authenticated HELLO did not register the runner")
		}
	})
}

func TestGRPCConnectControlObservationActiveWorkersDoesNotOverflowInt(t *testing.T) {
	const maxActiveWorkers uint32 = 1<<32 - 1

	directory := NewMemoryRunnerDirectory()
	client := startGRPCTestServer(t, &fakeControlEngine{}, directory)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer func() { _ = stream.Close() }()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID:    "runner-connect-active-workers-overflow",
		Concurrency: 1,
	}}); err != nil {
		t.Fatalf("send HELLO: %v", err)
	}
	if welcome, err := stream.Recv(); err != nil || welcome.Welcome == nil {
		t.Fatalf("receive WELCOME: frame=%#v err=%v", welcome, err)
	}

	if err := stream.Send(protocol.RunnerFrame{ControlObservation: &protocol.ControlObservationFrame{
		ActiveWorkers: maxActiveWorkers,
	}}); err != nil {
		t.Fatalf("send maximum ActiveWorkers observation: %v", err)
	}

	// The server may choose to reject an unrepresentable count. Otherwise it
	// must preserve the count exactly; in particular, a uint32-to-int wrap must
	// never make a runner look idle or report a negative in-flight count.
	terminal := make(chan error, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				terminal <- err
				return
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if runner, found := directory.Runner(ctx, "runner-connect-active-workers-overflow"); found && runner.InFlight != 0 {
			if strconv.IntSize >= 64 && int64(runner.InFlight) == int64(maxActiveWorkers) {
				return
			}
			t.Fatalf("ActiveWorkers=%d was not safely represented: InFlight=%d (int size=%d)", maxActiveWorkers, runner.InFlight, strconv.IntSize)
		}

		select {
		case err := <-terminal:
			if got := status.Code(err); got == codes.InvalidArgument {
				return
			}
			t.Fatalf("maximum ActiveWorkers terminal status = %v (%v), want InvalidArgument or an exact safe value", status.Code(err), err)
		case <-ctx.Done():
			t.Fatalf("maximum ActiveWorkers was neither rejected nor safely recorded before deadline: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
