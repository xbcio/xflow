package apiserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type runDrainModule struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (*runDrainModule) Name() string { return "run-drain-test" }

func (m *runDrainModule) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("GET /run-drain", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case m.entered <- struct{}{}:
		default:
		}
		<-m.release
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestRunMarksUnreadyBeforeHTTPDrainAndStopsControlPlaneAfterward(t *testing.T) {
	addr := reserveRunTestAddress(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDrain := func() { releaseOnce.Do(func() { close(release) }) }

	srv, err := New(
		Config{HTTPAddr: addr, Concurrency: 1},
		WithManagement(),
		WithModule(&runDrainModule{entered: entered, release: release}),
		WithHTTPTimeouts(HTTPTimeouts{
			ReadHeader: time.Second,
			Read:       5 * time.Second,
			Write:      5 * time.Second,
			Idle:       time.Second,
			Shutdown:   5 * time.Second,
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var runErr error
	runDone := make(chan struct{})
	go func() {
		runErr = srv.Run(ctx)
		close(runDone)
	}()
	t.Cleanup(func() {
		cancel()
		releaseDrain()
		select {
		case <-runDone:
		case <-time.After(6 * time.Second):
			t.Error("Run did not return during cleanup")
		}
	})

	baseURL := "http://" + addr
	waitForRunHTTPStatus(t, baseURL+PathReadyz, http.StatusOK, runDone, &runErr)

	requestDone := make(chan struct{})
	var requestErr error
	go func() {
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(baseURL + "/run-drain")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				err = errors.New("drain request returned unexpected status: " + resp.Status)
			}
		}
		requestErr = err
		close(requestDone)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking request did not reach the HTTP handler")
	}

	cancel()

	// The request keeps http.Server.Shutdown in its drain phase. Readiness must
	// already be down during that phase; the old order stayed ready here until
	// the request was released and APIServer.Shutdown finally ran.
	waitForHandlerStatus(t, srv.Handler(), PathReadyz, http.StatusServiceUnavailable)

	select {
	case <-runDone:
		t.Fatalf("Run returned before the in-flight request drained: %v", runErr)
	default:
	}
	if err := srv.Start(context.Background()); !errors.Is(err, control.ErrControlPlaneStarted) {
		t.Fatalf("control plane state during HTTP drain: Start error = %v, want %v", err, control.ErrControlPlaneStarted)
	}

	releaseDrain()
	select {
	case <-requestDone:
		if requestErr != nil {
			t.Fatalf("drain request: %v", requestErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight request did not finish after release")
	}

	select {
	case <-runDone:
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the in-flight request drained")
	}
	if err := srv.Start(context.Background()); !errors.Is(err, control.ErrControlPlaneStopped) {
		t.Fatalf("control plane state after Run: Start error = %v, want %v", err, control.ErrControlPlaneStopped)
	}
}

// TestStopGRPCForcesStopWhenDrainExceedsGrace pins the bound on the gRPC drain
// phase: GracefulStop must not wait forever on a long-lived stream. The test
// uses an unknown-service handler that blocks until its stream context is
// cancelled, which only happens when stopGRPC falls back to Stop.
func TestStopGRPCForcesStopWhenDrainExceedsGrace(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	entered := make(chan struct{})
	gs := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		close(entered)
		<-stream.Context().Done()
		return stream.Context().Err()
	}))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, "/test.Blocked/Stream"); err != nil {
		t.Fatalf("new stream: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not start")
	}

	start := time.Now()
	stopGRPC(gs, 100*time.Millisecond)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stopGRPC took %s, want the forced stop to return promptly", elapsed)
	}
}

func reserveRunTestAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve HTTP address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release HTTP address: %v", err)
	}
	return addr
}

func waitForRunHTTPStatus(t *testing.T, url string, want int, runDone <-chan struct{}, runErr *error) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
		}
		select {
		case <-runDone:
			t.Fatalf("Run returned before %s reached status %d: %v", url, want, *runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not reach status %d", url, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForHandlerStatus(t *testing.T, handler http.Handler, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("handler %s status = %d, want %d", path, recorder.Code, want)
		}
		time.Sleep(time.Millisecond)
	}
}
