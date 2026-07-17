package apiserver

import (
	"net/http"

	"google.golang.org/grpc"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// runnerProtocolModule mounts the Runner Protocol (HTTP + gRPC) onto the
// apiserver. It delegates to the ControlPlane's already-assembled runner
// adapter and gRPC server rather than re-wiring them, so runner registration,
// polling, and result reporting behave identically whether the apiserver or a
// raw control.ControlPlane serves them.
type runnerProtocolModule struct {
	cp *control.ControlPlane
}

func newRunnerProtocolModule(cp *control.ControlPlane) *runnerProtocolModule {
	return &runnerProtocolModule{cp: cp}
}

func (m *runnerProtocolModule) Name() string { return "runner-protocol" }

func (m *runnerProtocolModule) RegisterHTTP(mux *http.ServeMux) {
	protocol.RegisterRunnerRoutes(mux, m.cp.RunnerHTTPHandler())
}

func (m *runnerProtocolModule) RegisterGRPC(reg grpc.ServiceRegistrar) {
	runnerpb.RegisterRunnerProtocolServer(reg, m.cp.GRPCServer())
}
