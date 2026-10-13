package cron

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

// captureCronLog redirects the default slog logger into a synchronized buffer
// for the duration of the test. The trigger logs from its own goroutine, so a
// plain strings.Builder would be a race the -race build would report.
func captureCronLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestCronTriggerLogsARefusedFiring pins the only trace a dropped tick leaves.
//
// A refused firing is dropped on purpose — the next firing is a fresh attempt —
// but it leaves nothing durable behind: no execution, no offset, no state. If
// the drop is also silent, a schedule that is being refused at every firing
// reads exactly like a schedule that never fired, and the difference matters:
// the first is a workflow rejecting its own trigger traffic at its
// concurrency limit, the second is a broken trigger.
func TestCronTriggerLogsARefusedFiring(t *testing.T) {
	buf := captureCronLog(t)
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", engine.ErrConcurrencyLimit
	})

	tr := New().Cron("@every 1s")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "cron",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitEmit(3 * time.Second) {
		t.Fatal("cron did not attempt an emit")
	}
	// The emit callback returns before the caller logs, so the line is polled
	// rather than read once.
	deadline := time.After(3 * time.Second)
	for !strings.Contains(buf.String(), "cron trigger firing was not admitted") {
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
