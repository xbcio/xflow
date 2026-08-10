package runner

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// reportStartClient answers Register with a fixed sessionID so the test can
// assert that the reporter receives exactly that sessionID (not an empty string,
// which Run silently returns on). It blocks in Poll so the runner stays alive.
type reportStartClient struct {
	sessionID string
}

func (c *reportStartClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{RunnerID: "runner-a", SessionID: c.sessionID}, nil
}
func (c *reportStartClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}
func (c *reportStartClient) Poll(ctx context.Context, _ protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	<-ctx.Done()
	return protocol.PollTaskResponse{}, ctx.Err()
}
func (c *reportStartClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{}, nil
}

// TestRunnerStartsMetricsReporterWithSessionID pins the line:
//
//	go r.metricsReporter.Run(heartbeatCtx, sessionID)
//
// Deleting that line means the fakeMetricsClient never receives a report, and
// the test fails. Changing sessionID to "" makes Run return immediately (its
// first guard), so the client never receives a report either.
//
// This is the single load-bearing hop of the whole proxy feature: the reporter
// must be started, and started with the session that registration returned.
func TestRunnerStartsMetricsReporterWithSessionID(t *testing.T) {
	const wantSessionID = "session-from-register"

	metricsClient := &fakeMetricsClient{}
	ticks := newManualTicks()
	reporter := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: testGatherer(t),
		Client:   metricsClient,
		RunnerID: "runner-a",
		Interval: DefaultMetricsReportInterval,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	reporter.after = ticks.after

	protoClient := &reportStartClient{sessionID: wantSessionID}
	r := New(protoClient, execution.NewRegistry(), Config{
		RunnerID:        "runner-a",
		Concurrency:     1,
		PollWait:        1, // minimal; Poll blocks on ctx anyway
		MetricsReporter: reporter,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.Run(ctx) }()

	// Wait for the reporter to request its first timer — this proves Run was
	// called (it enters the select on r.after before any report fires).
	waitForTimerRequests(t, ticks, 1)

	// Fire the timer so reportOnce executes.
	ticks.fire(t)

	// Wait for at least one report to arrive.
	reports := waitForReports(t, metricsClient, 1)

	// Assert the sessionID is the one registration returned, NOT empty.
	if got := reports[0].sessionID; got != wantSessionID {
		t.Fatalf("ReportMetrics sessionID = %q, want %q (the reporter was started "+
			"before registration or with a wrong sessionID)", got, wantSessionID)
	}
}
