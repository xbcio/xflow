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

// phasedIntervalClient returns different interval values depending on which
// beat is answering, and signals channels so the test can assert each site
// independently. Beat 1 returns secsA; beat 2+ returns secsB. The heartbeat
// ticker is set long enough (1s) that the test can assert the first-beat
// result before any ticker beat fires.
type phasedIntervalClient struct {
	mu             sync.Mutex
	secsA          int
	secsB          int
	beats          int
	firstBeatDone  chan struct{}
	secondBeatDone chan struct{}
}

func (c *phasedIntervalClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{RunnerID: "runner-a", SessionID: "session-1"}, nil
}

func (c *phasedIntervalClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	c.beats++
	beat := c.beats
	c.mu.Unlock()
	if beat == 1 {
		defer close(c.firstBeatDone)
		return protocol.HeartbeatResponse{
			ServerTime:                   time.Now().Unix(),
			MetricsReportIntervalSeconds: c.secsA,
		}, nil
	}
	defer func() {
		select {
		case c.secondBeatDone <- struct{}{}:
		default:
		}
	}()
	return protocol.HeartbeatResponse{
		ServerTime:                   time.Now().Unix(),
		MetricsReportIntervalSeconds: c.secsB,
	}, nil
}

func (c *phasedIntervalClient) Poll(ctx context.Context, _ protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	<-ctx.Done()
	return protocol.PollTaskResponse{}, ctx.Err()
}

func (c *phasedIntervalClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{}, nil
}

// TestRunnerFirstBeatAppliesInterval pins the first-beat call site: beat 1
// returns 20s; the heartbeat ticker is 1s. We assert the interval is 20s
// immediately after beat 1 — well before the ticker fires. Deleting the
// first-beat processMetricsInterval call makes this fail because the reporter
// keeps DefaultMetricsReportInterval until the ticker fires (which is 1s away).
func TestRunnerFirstBeatAppliesInterval(t *testing.T) {
	client := &phasedIntervalClient{
		secsA:          20,
		secsB:          60,
		firstBeatDone:  make(chan struct{}),
		secondBeatDone: make(chan struct{}, 1),
	}
	reporter := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: testGatherer(t),
		Client:   &fakeMetricsClient{},
		RunnerID: "runner-a",
		Interval: DefaultMetricsReportInterval,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-a",
		Concurrency:       1,
		HeartbeatInterval: time.Second, // long enough that ticker cannot race beat 1
		PollWait:          time.Millisecond,
		MetricsReporter:   reporter,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.Run(ctx) }()

	// Wait for beat 1 to complete.
	select {
	case <-client.firstBeatDone:
	case <-time.After(2 * time.Second):
		t.Fatal("beat 1 did not fire within 2s")
	}
	// Same apply-after-return race as the ticker test, so this waits too — but
	// only for 800ms: beat 2 fires at 1s and would apply 60s, and a poll that
	// outlived it could not tell "the first-beat site works" from "the ticker
	// site cleaned up after it".
	awaitInterval(t, reporter, 20*time.Second, 800*time.Millisecond, "after beat 1 (first-beat site not working)")
}

// awaitInterval waits for the reporter to reach want, up to within.
//
// The wait is not slack in the assertion. phasedIntervalClient signals its
// beat-done channels from a defer inside Heartbeat — that is, when the fake
// client RETURNS — while runner.applyMetricsInterval runs in the caller, after
// that return. Reading currentInterval() the instant the channel fires
// therefore races the apply, and on a loaded machine it loses: this pair of
// tests passed 10/10 in isolation and 3/3 as a package while failing inside a
// full-suite `go test ./...` run, which is the only context where the runner
// goroutine is descheduled long enough for the window to open.
//
// A deleted call site still fails: the interval never becomes want, so the
// deadline expires and the stale value is what gets reported. within must stay
// below the heartbeat interval of any test whose NEXT beat would apply a
// different value, or the poll could observe that one instead.
func awaitInterval(t *testing.T, r *MetricsReporter, want, within time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got := r.currentInterval()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: interval = %v, want %v", what, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRunnerTickerBeatAppliesInterval pins the ticker-branch call site: beat 1
// returns 20s, beat 2 returns 60s. We wait for beat 2 to complete and then
// assert 60s. Deleting the ticker-branch processMetricsInterval call makes
// this fail because the interval stays at 20s from beat 1 and never becomes 60s.
func TestRunnerTickerBeatAppliesInterval(t *testing.T) {
	client := &phasedIntervalClient{
		secsA:          20,
		secsB:          60,
		firstBeatDone:  make(chan struct{}),
		secondBeatDone: make(chan struct{}, 1),
	}
	reporter := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: testGatherer(t),
		Client:   &fakeMetricsClient{},
		RunnerID: "runner-a",
		Interval: DefaultMetricsReportInterval,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-a",
		Concurrency:       1,
		HeartbeatInterval: 10 * time.Millisecond, // fast ticker so beat 2 fires quickly
		PollWait:          time.Millisecond,
		MetricsReporter:   reporter,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.Run(ctx) }()

	// Wait for beat 1.
	select {
	case <-client.firstBeatDone:
	case <-time.After(2 * time.Second):
		t.Fatal("beat 1 did not fire within 2s")
	}
	// Wait for beat 2 (ticker branch).
	select {
	case <-client.secondBeatDone:
	case <-time.After(2 * time.Second):
		t.Fatal("beat 2 did not fire within 2s")
	}
	// The ticker branch must have applied 60s. Every later beat returns 60s
	// too, so there is no competing value and the window can be generous.
	awaitInterval(t, reporter, 60*time.Second, 2*time.Second, "after beat 2 (ticker-branch site not working)")
}
