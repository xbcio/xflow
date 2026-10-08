package asynq

import (
	"context"
	"fmt"
	"sync"
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/namespace"
)

// StartConsumer starts an Asynq server that decodes each task and dispatches it
// to handler, translating the handler error into the Asynq retry policy. The
// returned stop function shuts the server down gracefully.
//
// Server.Start is the non-blocking entry point: it launches asynq's internal
// sub-goroutines (tracked by the server's own wait group) and returns
// immediately, unlike Server.Run which blocks on OS signals. The returned stop
// calls Shutdown, which drains those sub-goroutines. The server uses its own
// broker connection, independent of any state-store Redis client.
//
// When cfg.StatsObserver is set, the consumer also samples the queues it reads
// (depth and oldest-pending age) on cfg.StatsInterval until stopped. Sampling
// runs on its own goroutine and connection (asynq's Inspector); a failed
// sample is skipped rather than retried, so a Redis blip degrades the signal
// instead of the consumer.
//
// The returned stop function is idempotent: calling it more than once is safe
// and has no further effect. StartConsumer used to return Server.Shutdown
// directly, which is idempotent, and the stats sampler added a done-channel
// close that was not; the Once below restores the original contract for both
// shapes.
func (t *Transport) StartConsumer(cfg queue.ConsumerConfig, handler queue.TaskHandler) (func(), error) {
	srv := asynqlib.NewServer(
		t.connOpt,
		asynqlib.Config{
			Concurrency: cfg.Concurrency,
			// Without Queues, asynq falls back to its own config, which polls
			// only asynq's bare "default" queue — so a task enqueued to any
			// queue this transport actually uses sits unprocessed forever.
			Queues: queueWeights(),
		},
	)
	mux := asynqlib.NewServeMux()
	mux.HandleFunc(taskType, func(ctx context.Context, at *asynqlib.Task) error {
		task, namespaceID, err := queue.Unmarshal(at.Payload())
		if err != nil {
			// Deserialization failure is permanent; skip retry to avoid 25 futile attempts.
			return fmt.Errorf("%w: unmarshal payload: %w", asynqlib.SkipRetry, err)
		}
		ctx = namespace.WithNamespace(ctx, namespaceID)
		return handlerError(handler(ctx, task), cfg.Transient)
	})

	if err := srv.Start(mux); err != nil {
		return nil, err
	}
	// One Once gates the whole teardown, so stop() keeps Server.Shutdown's
	// idempotence even with the sampler's done-channel close inside it.
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { srv.Shutdown() }) }

	if cfg.StatsObserver != nil {
		interval := cfg.StatsInterval
		if interval <= 0 {
			interval = queue.DefaultStatsInterval
		}
		inspector := asynqlib.NewInspector(t.connOpt)
		done := make(chan struct{})
		go t.reportQueueStats(done, interval, inspector, cfg.StatsObserver)
		stop = func() {
			stopOnce.Do(func() {
				close(done)
				srv.Shutdown()
				_ = inspector.Close()
			})
		}
	}
	return stop, nil
}

// reportQueueStats samples both queues immediately and then every interval
// until done is closed. Sampling on an interval (rather than a tight loop) is
// the point: this is an alerting signal read in minutes, and CurrentStats costs
// several Redis commands per queue.
func (t *Transport) reportQueueStats(done <-chan struct{}, interval time.Duration, inspector *asynqlib.Inspector, obs queue.StatsObserver) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		t.sampleQueueStats(inspector, obs)
		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

// sampleQueueStats reads and reports one sample per queue this consumer reads.
// Errors are swallowed by design: the sample is observability, and a consumer
// must not die (or stall) because the metric could not be read.
//
// A queue that has never held a task does not exist in asynq yet and is
// reported as an explicit zero. The distinction matters: "not registered" is a
// deterministic empty queue, while a failed read of a registered queue
// produces no sample at all — reporting a failed read as zero would paint a
// backlog-free dashboard during a Redis outage. asynq does not export a
// sentinel that separates the two on GetQueueInfo, so the registered-queue
// list is the discriminator.
func (t *Transport) sampleQueueStats(inspector *asynqlib.Inspector, obs queue.StatsObserver) {
	existing, err := inspector.Queues()
	if err != nil {
		return
	}
	known := make(map[string]struct{}, len(existing))
	for _, q := range existing {
		known[q] = struct{}{}
	}
	for _, name := range []string{defaultQueueName, batchQueueName} {
		if _, ok := known[name]; !ok {
			reportQueueStatsSafely(obs, name, 0, 0, 0, 0)
			continue
		}
		info, err := inspector.GetQueueInfo(name)
		if err != nil {
			continue
		}
		reportQueueStatsSafely(obs, name, info.Pending, info.Active, info.Retry, info.Latency)
	}
}

// reportQueueStatsSafely isolates the observer call: it runs on a background
// goroutine, where a panicking observer would take the process down.
func reportQueueStatsSafely(obs queue.StatsObserver, name string, pending, active, retry int, oldestPendingAge time.Duration) {
	defer func() { _ = recover() }()
	obs.OnQueueStats(name, pending, active, retry, oldestPendingAge)
}
