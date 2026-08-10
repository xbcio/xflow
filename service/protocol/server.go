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
