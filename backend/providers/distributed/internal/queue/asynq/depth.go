package asynq

import (
	"time"

	asynqlib "github.com/hibiken/asynq"
)

// defaultDepthSampleInterval is how often the producer-side depth cache is
// refreshed when WithDepthSampleInterval is unset. It is deliberately fast
// compared with the consumer's 30s stats cadence: that signal is read in
// minutes by a dashboard, while this one gates admission on a millisecond
// time scale, and a cache older than the burst it is meant to bound would let
// every producer overshoot the bound between samples.
const defaultDepthSampleInterval = time.Second

// depthFunc reads the current depth of every queue this transport writes to,
// keyed by queue name. A queue missing from the result is UNMEASURED (not
// zero), and TryEnqueue allows it: a failed read says nothing about how full
// the queue is, and a sampling outage must degrade into "no backpressure"
// rather than into a producer that refuses to enqueue.
//
// It is a field rather than a free function so tests can substitute a
// deterministic sampler instead of racing the real one (see withDepthFunc).
type depthFunc func() (map[string]int, error)

// readDepths is the production sampler: one depth per queue this deployment
// reads, resolved from the broker through the same Inspector the consumer's
// stats sampler uses.
//
// Depth is Pending + Active only. Retry and Scheduled tasks are excluded by
// contract: they are waiting on a timer, not on a consumer, so counting them
// would make the bound reject producers for work the queue is not about to
// hand to a worker. A retry returns to Pending when its backoff elapses, and
// is counted from then on.
func (t *Transport) readDepths() (map[string]int, error) {
	existing, err := t.depthInspector.Queues()
	if err != nil {
		// The registered-queue list is the discriminator between "queue exists
		// and is empty" and "could not read", the same distinction the
		// consumer's stats sampler makes. Without it no queue's reading can be
		// trusted, so the whole sample is unusable.
		return nil, err
	}
	known := make(map[string]struct{}, len(existing))
	for _, q := range existing {
		known[q] = struct{}{}
	}
	names := t.queues.All()
	depths := make(map[string]int, len(names))
	for _, name := range names {
		if _, ok := known[name]; !ok {
			// A queue that has never held a task does not exist in asynq yet.
			// That is a deterministic empty queue, not a failed read.
			depths[name] = 0
			continue
		}
		info, err := t.depthInspector.GetQueueInfo(name)
		if err != nil {
			// Leave this queue out of the snapshot: unmeasured reads as
			// allowed on the next TryEnqueue (fail open).
			continue
		}
		depths[name] = info.Pending + info.Active
	}
	return depths, nil
}

// startDepthSampler launches the producer-side sampler. It is a no-op unless a
// depth bound is configured: an unbounded transport pays neither the inspector
// connection nor the goroutine.
func (t *Transport) startDepthSampler() {
	if t.maxDepth <= 0 {
		return
	}
	t.depthInspector = asynqlib.NewInspector(t.connOpt)
	t.depthDone = make(chan struct{})
	t.depthExited = make(chan struct{})
	go t.sampleDepthsLoop()
}

// sampleDepthsLoop samples immediately, then on the configured interval until
// depthDone is closed. Sampling first is what keeps the bound meaningful right
// after construction; the alternative is a one-interval window in which every
// producer is allowed regardless of the real depth.
func (t *Transport) sampleDepthsLoop() {
	defer close(t.depthExited)
	ticker := time.NewTicker(t.depthInterval)
	defer ticker.Stop()
	for {
		t.refreshDepths()
		select {
		case <-t.depthDone:
			return
		case <-ticker.C:
		}
	}
}

// refreshDepths replaces the cached snapshot with one fresh sample.
//
// A failed sample clears the cache rather than keeping the previous values.
// Stale values are not fail-open: a cache that still says "full" after Redis
// stopped answering would refuse every enqueue during exactly the outage in
// which producers most need to keep trying, and the queue would drain only if
// delivery were already working. Clearing turns every queue UNMEASURED, which
// TryEnqueue reads as allowed, until a later sample succeeds.
func (t *Transport) refreshDepths() {
	depths, err := t.depthFunc()
	t.depthMu.Lock()
	defer t.depthMu.Unlock()
	if err != nil {
		t.depths = nil
		return
	}
	t.depths = depths
}

// queueFull reports whether the task's own queue is at or above the configured
// depth bound. The bound is per queue, not global: the batch lane saturating
// must not stop ordinary work from being admitted.
//
// A queue absent from the cache is unmeasured and therefore allowed — from a
// failed sample, or from no sample having completed since construction. The
// bound is advisory for the same reason: it is a periodically refreshed
// estimate, so a burst can overshoot it between two samples. That is the
// intended trade: backpressure exists to keep a wide outbox flush from
// deadlocking on a full broker, not to be an exact admission counter.
func (t *Transport) queueFull(queueName string) bool {
	if t.maxDepth <= 0 {
		return false
	}
	t.depthMu.RLock()
	depth, measured := t.depths[queueName]
	t.depthMu.RUnlock()
	return measured && depth >= t.maxDepth
}

// stopDepthSampler signals the sampler goroutine to exit and releases its
// inspector. It is idempotent: Close may be called more than once (and the
// backend's shutdown path does), so the channel close and the inspector close
// share one sync.Once.
//
// It does not wait for the goroutine. Waiting would make shutdown latency
// depend on the broker answering: an in-flight sample on an unresponsive Redis
// can sit there for the dialect's read timeout, and a producer's Close must not
// hang on the very dependency that is already failing. The in-flight sample
// runs against a closed inspector and fails, which clears the cache — fail
// open, and harmless while shutting down. depthExited records the exit for
// tests that need to assert the goroutine really ended.
func (t *Transport) stopDepthSampler() {
	if t.depthDone == nil {
		return
	}
	t.depthStopOnce.Do(func() {
		close(t.depthDone)
		if t.depthInspector != nil {
			_ = t.depthInspector.Close()
		}
	})
}
