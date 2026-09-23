package xflow

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/control"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

// Transport "inproc" names a control plane that lives in this very process, so
// there is nothing to dial and no way to infer one from the config: the caller
// must hand it over. Without this gate the runner would fall through to the HTTP
// branch, dial ServerURL (empty, so a parse error at best), and fail far from the
// cause — or, worse, succeed against a loopback listener and reintroduce exactly
// the socket this transport exists to remove.
func TestNewRunnerInProcRequiresAControlPlane(t *testing.T) {
	_, err := NewRunner(RunnerConfig{
		Transport:    RunnerTransportInProc,
		Capabilities: []string{"xflow.function"},
	})
	if err == nil {
		t.Fatal("NewRunner succeeded with RunnerTransportInProc and no control plane")
	}
	msg := err.Error()
	if !strings.Contains(msg, "WithRunnerControlPlane") {
		t.Errorf("error %q does not name the option that fixes it", msg)
	}
	if !strings.Contains(msg, "ControlServer") {
		t.Errorf("error %q does not point at Server.ControlServer()", msg)
	}
}

// newRunnerProtocolClient is the wiring point: under inproc it must return the
// in-process client bound to the supplied control plane, not an HTTP client
// pointed at a URL. Asserting the concrete type is the only way to see which of
// the three transports was actually chosen — every one of them satisfies
// runnersvc.ProtocolClient.
func TestNewRunnerProtocolClientSelectsTheInProcessClient(t *testing.T) {
	cp := newInProcTestControlServer(t)

	client, closeFn, err := newRunnerProtocolClient(
		RunnerConfig{Transport: RunnerTransportInProc},
		&runnerOptions{controlPlane: cp},
	)
	if err != nil {
		t.Fatalf("newRunnerProtocolClient: %v", err)
	}
	if closeFn == nil {
		t.Fatal("closeFn = nil; NewRunner's rollback path calls it unconditionally")
	}
	defer closeFn()

	if _, ok := client.(*control.InProcessRunnerClient); !ok {
		t.Fatalf("client = %T, want *control.InProcessRunnerClient", client)
	}
	// The runner builds its metrics reporter by asserting this seam. The
	// in-process client implements it, so an embedded runner that asks to
	// report metrics gets the reporter rather than the silent "transport
	// cannot report" skip the gRPC branch produces.
	if _, ok := client.(runnersvc.MetricsReportClient); !ok {
		t.Fatal("in-process client does not satisfy runnersvc.MetricsReportClient")
	}
}

// An in-process runner has no origin to dial, so ServerURL is not a
// configuration error for it. This is the whole point of the transport: a host
// embedding both halves must not be forced to invent a URL for itself.
func TestNewRunnerInProcAcceptsAnEmptyServerURL(t *testing.T) {
	cp := newInProcTestControlServer(t)

	r, err := NewRunner(RunnerConfig{
		Transport:    RunnerTransportInProc,
		Capabilities: []string{"xflow.function"},
	}, WithRunnerControlPlane(cp))
	if err != nil {
		t.Fatalf("NewRunner with an empty ServerURL under inproc: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
}

// The relaxation is scoped to inproc. Under HTTP and gRPC an empty ServerURL is
// still the unreachable-origin configuration error it always was, or the gate
// above would have quietly disabled a check for the two transports that need it.
func TestNewRunnerStillRequiresServerURLForTheWireTransports(t *testing.T) {
	tests := []struct {
		name string
		cfg  RunnerConfig
	}{
		{name: "default transport", cfg: RunnerConfig{Capabilities: []string{"xflow.function"}}},
		{name: "explicit http", cfg: RunnerConfig{Transport: RunnerTransportHTTP, Capabilities: []string{"xflow.function"}}},
		{name: "explicit grpc", cfg: RunnerConfig{Transport: RunnerTransportGRPC, Capabilities: []string{"xflow.function"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRunner(tt.cfg); err == nil {
				t.Fatal("NewRunner accepted an empty ServerURL")
			}
		})
	}
}

// WithRunnerControlPlane is documented as inproc-only, so supplying it to a
// wire transport must leave that transport alone. The risk is the reverse of
// the gate above: a refactor that keys on "a control plane is present" rather
// than on cfg.Transport would silently move a gRPC or HTTP runner in-process,
// and nothing about the config would say so.
func TestNewRunnerControlPlaneDoesNotDivertTheWireTransports(t *testing.T) {
	cp := newInProcTestControlServer(t)

	client, closeFn, err := newRunnerProtocolClient(
		RunnerConfig{Transport: RunnerTransportGRPC, GRPCTarget: "passthrough:///127.0.0.1:1"},
		&runnerOptions{controlPlane: cp},
	)
	if err != nil {
		t.Fatalf("newRunnerProtocolClient(gRPC): %v", err)
	}
	defer closeFn()
	if _, ok := client.(*control.InProcessRunnerClient); ok {
		t.Fatal("a control plane supplied to the gRPC transport diverted it in-process")
	}
}

// The accessor chain the error message tells the caller to use. Server, the API
// server, and the control plane each forward to the same *control.Server — a
// copy or a second instance here would bind the runner to a control plane that
// is not the one serving requests.
func TestServerControlServerForwardsToTheSameControlPlane(t *testing.T) {
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}

	cp := srv.ControlServer()
	if cp == nil {
		t.Fatal("ControlServer() = nil")
	}
	if cp != srv.api.ControlServer() {
		t.Fatal("ControlServer() did not forward to the API server's control plane")
	}
	if cp != srv.ControlServer() {
		t.Fatal("ControlServer() returned a different value on the second call")
	}
	if client := cp.InProcessRunnerClient(""); client == nil {
		t.Fatal("InProcessRunnerClient() = nil")
	}
}

func newInProcTestControlServer(t *testing.T) *control.Server {
	t.Helper()
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	return srv.ControlServer()
}
