package runner

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// intervalClient answers Register once and then returns a fixed heartbeat
// response forever, so the test can assert what the runner does with the
// interval field.
type intervalClient struct {
	mu     sync.Mutex
	secs   int
	beats  int
	waitCh chan struct{}
}

func (c *intervalClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{RunnerID: "runner-a", SessionID: "session-1"}, nil
}

func (c *intervalClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	c.beats++
	secs := c.secs
	first := c.beats == 1
	c.mu.Unlock()
	if first && c.waitCh != nil {
		close(c.waitCh)
	}
	return protocol.HeartbeatResponse{
		ServerTime:                   time.Now().Unix(),
		MetricsReportIntervalSeconds: secs,
	}, nil
}

func (c *intervalClient) Poll(ctx context.Context, _ protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	<-ctx.Done()
	return protocol.PollTaskResponse{}, ctx.Err()
}

func (c *intervalClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{}, nil
}

func runWithInterval(t *testing.T, secs int) *MetricsReporter {
	t.Helper()
	reporter := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: testGatherer(t),
		Client:   &fakeMetricsClient{},
		RunnerID: "runner-a",
		Interval: DefaultMetricsReportInterval,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	client := &intervalClient{secs: secs, waitCh: make(chan struct{})}
	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-a",
		Concurrency:       1,
		HeartbeatInterval: 5 * time.Millisecond,
		PollWait:          time.Millisecond,
		MetricsReporter:   reporter,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.Run(ctx) }()
	select {
	case <-client.waitCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat within 2s")
	}
	return reporter
}

func TestRunnerAdoptsPositiveMetricsInterval(t *testing.T) {
	reporter := runWithInterval(t, 30)
	waitForInterval(t, reporter, 30*time.Second)
}

func TestRunnerSuspendsOnNegativeMetricsInterval(t *testing.T) {
	reporter := runWithInterval(t, -1)
	waitForInterval(t, reporter, -time.Second)
}

// Zero must leave the local default untouched — this is what makes an old
// server (which never sets the field) a no-op rather than a suspension.
func TestRunnerKeepsLocalDefaultWhenServerHasNoOpinion(t *testing.T) {
	reporter := runWithInterval(t, 0)
	// Give the runner several heartbeats' worth of room to wrongly change it.
	time.Sleep(50 * time.Millisecond)
	if got := reporter.currentInterval(); got != DefaultMetricsReportInterval {
		t.Fatalf("interval = %v, want the untouched local default %v", got, DefaultMetricsReportInterval)
	}
}

func waitForInterval(t *testing.T, r *MetricsReporter, want time.Duration) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.currentInterval() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("interval = %v after 2s, want %v", r.currentInterval(), want)
}
