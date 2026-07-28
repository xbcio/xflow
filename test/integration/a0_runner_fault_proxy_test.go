//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

// ackLossProtocolClient is a one-shot lossy ProtocolClient that simulates the
// "server accepted ReportResult but the ACK never reached the runner" fault.
//
// The proxy wraps a real runner ProtocolClient (typically *protocol.Client
// pointing at the test control plane). The first ReportResult is forwarded to
// the real control plane; if the control plane accepts it, the proxy drops the
// response and returns a transport error to the runner. All subsequent
// ReportResult calls pass through so a retry loop cannot spin forever.
//
// Synchronization uses a sync.Once barrier (no sleeps). Captured request and
// the transport-error flag are exposed for scenario assertions.
type ackLossProtocolClient struct {
	inner runnersvc.ProtocolClient

	once              sync.Once
	captured          atomic.Pointer[protocol.ReportResultRequest]
	sawTransportError atomic.Bool
}

func newAckLossProtocolClient(inner runnersvc.ProtocolClient) *ackLossProtocolClient {
	return &ackLossProtocolClient{inner: inner}
}

func (p *ackLossProtocolClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return p.inner.Register(ctx, req)
}

func (p *ackLossProtocolClient) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return p.inner.Heartbeat(ctx, req)
}

func (p *ackLossProtocolClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return p.inner.Poll(ctx, req)
}

func (p *ackLossProtocolClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	var first bool
	p.once.Do(func() { first = true })
	if !first {
		return p.inner.ReportResult(ctx, req)
	}

	resp, err := p.inner.ReportResult(ctx, req)
	if err != nil {
		return resp, err
	}
	if !resp.Accepted {
		return resp, nil
	}

	// Server has committed and ReleaseLeased before returning. Capture the
	// original request and drop the ACK so the runner observes a transport
	// failure, matching the spec §3.2 report-ack-loss semantics.
	reqCopy := req
	p.captured.Store(&reqCopy)
	p.sawTransportError.Store(true)
	return protocol.ReportResultResponse{}, errors.New("simulated ack loss: response dropped")
}

// CapturedRequest returns the first accepted ReportResultRequest, or nil if
// no accepted report was observed.
func (p *ackLossProtocolClient) CapturedRequest() *protocol.ReportResultRequest {
	return p.captured.Load()
}

// SawTransportError reports whether the proxy dropped an accepted response and
// returned a transport error to the runner.
func (p *ackLossProtocolClient) SawTransportError() bool {
	return p.sawTransportError.Load()
}

// requestLossProtocolClient is a one-shot lossy ProtocolClient that simulates
// the "handler completed, but the first ReportResult request never reached the
// server" fault.
//
// The proxy wraps a real runner ProtocolClient. The first ReportResult from a
// runner session is captured and dropped; that runner observes a transport
// error. Additional ReportResult calls from the same runner are also dropped so
// a phantom retry cannot succeed before the production LeaseSweeper reclaims
// the lost lease. Reports from a different runner session pass through, which
// lets the second runner (runner B) deliver the authoritative result.
//
// Synchronization uses a sync.Once barrier (no sleeps). Captured request and
// the transport-error flag are exposed for scenario assertions.
type requestLossProtocolClient struct {
	inner runnersvc.ProtocolClient

	once              sync.Once
	captured          atomic.Pointer[protocol.ReportResultRequest]
	sawTransportError atomic.Bool
	capturedRunnerID  atomic.Pointer[string]
}

func newRequestLossProtocolClient(inner runnersvc.ProtocolClient) *requestLossProtocolClient {
	return &requestLossProtocolClient{inner: inner}
}

func (p *requestLossProtocolClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return p.inner.Register(ctx, req)
}

func (p *requestLossProtocolClient) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return p.inner.Heartbeat(ctx, req)
}

func (p *requestLossProtocolClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return p.inner.Poll(ctx, req)
}

func (p *requestLossProtocolClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	var first bool
	p.once.Do(func() {
		first = true
		reqCopy := req
		p.captured.Store(&reqCopy)
		p.capturedRunnerID.Store(&reqCopy.RunnerID)
		p.sawTransportError.Store(true)
	})
	if first {
		return protocol.ReportResultResponse{}, errors.New("simulated request loss: report not forwarded")
	}
	if p.isDroppedRunner(req.RunnerID) {
		return protocol.ReportResultResponse{}, errors.New("simulated request loss: report not forwarded")
	}
	return p.inner.ReportResult(ctx, req)
}

func (p *requestLossProtocolClient) isDroppedRunner(runnerID string) bool {
	dropped := p.capturedRunnerID.Load()
	return dropped != nil && runnerID == *dropped
}

// CapturedRequest returns the first ReportResultRequest the proxy captured and
// dropped, or nil if no report was observed.
func (p *requestLossProtocolClient) CapturedRequest() *protocol.ReportResultRequest {
	return p.captured.Load()
}

// SawTransportError reports whether the proxy dropped the first report and
// returned a transport error to the runner.
func (p *requestLossProtocolClient) SawTransportError() bool {
	return p.sawTransportError.Load()
}
