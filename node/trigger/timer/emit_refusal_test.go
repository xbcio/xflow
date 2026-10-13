package timer

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// captureTimerLog redirects the default slog logger into a synchronized buffer
// for the duration of the test. The trigger logs from its own goroutine, so a
// plain strings.Builder would be a race the -race build would report.
func captureTimerLog(t *testing.T) *timerLogBuffer {
	t.Helper()
	buf := &timerLogBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

type timerLogBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *timerLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *timerLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestTimerTriggerLogsARefusedFiring is cron's case for the interval trigger:
// a refused tick is dropped and leaves no execution behind, so the log line is
// the only evidence an operator has that the workflow is holding its own
// trigger traffic off at its concurrency limit.
func TestTimerTriggerLogsARefusedFiring(t *testing.T) {
	buf := captureTimerLog(t)
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", engine.ErrConcurrencyLimit
	})

	tr := New().Every(5 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitEmit(time.Second) {
		t.Fatal("timer did not attempt an emit")
	}
	// The emit callback returns before the caller logs, so the line is polled
	// rather than read once.
	deadline := time.After(3 * time.Second)
	for !strings.Contains(buf.String(), "timer trigger firing was not admitted") {
		select {
		case <-deadline:
			t.Fatalf("a refused firing left no log line; captured:\n%s", buf.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := buf.String(); !strings.Contains(got, "workflow concurrency limit reached") {
		t.Fatalf("the refusal cause is missing from the log line:\n%s", got)
	}
}
