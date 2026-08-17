package protocol

import "net/http"

const (
	RegisterRunnerPath = "/v1/runners/register"
	HeartbeatPath      = "/v1/runners/heartbeat"
	PollTaskPath       = "/v1/runners/poll"
	ReportResultPath   = "/v1/runners/result"
)

type RunnerHTTPHandler interface {
	HandleRegisterRunner(http.ResponseWriter, *http.Request)
	HandleHeartbeat(http.ResponseWriter, *http.Request)
	HandlePollTask(http.ResponseWriter, *http.Request)
	HandleReportResult(http.ResponseWriter, *http.Request)
	HandleRenewLease(http.ResponseWriter, *http.Request)
	HandleActivationAck(http.ResponseWriter, *http.Request)
	// HandleReportMetrics receives a runner's Prometheus snapshot. HTTP only:
	// gRPC is not a target deployment shape (cross-cloud goes through the Relay
	// Gateway), so the gRPC transport never carries this call.
	HandleReportMetrics(http.ResponseWriter, *http.Request)
}

func RegisterRunnerRoutes(mux *http.ServeMux, handler RunnerHTTPHandler) {
	mux.HandleFunc(RegisterRunnerPath, handler.HandleRegisterRunner)
	mux.HandleFunc(HeartbeatPath, handler.HandleHeartbeat)
	mux.HandleFunc(PollTaskPath, handler.HandlePollTask)
	mux.HandleFunc(ReportResultPath, handler.HandleReportResult)
	mux.HandleFunc(RenewLeasePath, handler.HandleRenewLease)
	mux.HandleFunc(ActivationAckPath, handler.HandleActivationAck)
	mux.HandleFunc(ReportMetricsPath, handler.HandleReportMetrics)
}

// RunnerFacingPaths enumerates every runner-facing HTTP path constant this
// package exposes. The constants are declared across server.go, group.go,
// activation.go and metrics.go; Go cannot reflect over constants, so this
// enumerable form is what the dead-constant guard in paths_test.go reads. The
// guard asserts the hand-written sample table covers every entry here, so
// adding a path constant anywhere above without adding it to this list is the
// same drift defect the guard exists to catch — keep the constants and this
// list in lockstep.
//
// These are runner-protocol-face paths (spec §0.1), distinct from the
// user-facing paths enumerated by service/apiserver.UserFacingPaths.
var RunnerFacingPaths = []string{
	RegisterRunnerPath,
	HeartbeatPath,
	PollTaskPath,
	ReportResultPath,
	RenewLeasePath,
	ActivationAckPath,
	ReportMetricsPath,
}
