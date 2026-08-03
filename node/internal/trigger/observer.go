package trigger

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Observer receives trigger observations. Implementations must be non-blocking
// and must never use message content, keys, or offsets as labels: content is
// not an operator's to read from a metric and offsets are unbounded
// cardinality. Topic is the only message-derived label, and it is bounded by
// the number of configured triggers.
type Observer interface {
	// OnMessageDiscarded reports a message that was consumed (its offset
	// committed) but never emitted. reason is a fixed enum — currently only
	// "schema". This is the counter that makes an otherwise invisible drop
	// visible.
	OnMessageDiscarded(ctx context.Context, topic, reason string)
	// OnMessageDeadLettered reports a dead-letter publish attempt. result is
	// "ok" or "error".
	OnMessageDeadLettered(ctx context.Context, topic, result string)
	// OnBatchFlushed reports one batch leaving the aggregator. trigger is a fixed
	// enum: "size", "timeout", "idle", "close". size is the message count.
	OnBatchFlushed(ctx context.Context, topic, trigger string, size int)
	// OnBatchAdmission reports the control-plane response to a batch admission.
	// state is "accepted", "duplicate", "conflict" or "error".
	//
	// The conflict rate is the ONLY signal that shows how often a redelivered
	// batch actually re-executes. The design accepts that duplicate (delivery is
	// at-least-once), but accepting it is not the same as not looking at it.
	OnBatchAdmission(ctx context.Context, topic, state string)
}

type noopObserver struct{}

func (noopObserver) OnMessageDiscarded(context.Context, string, string)    {}
func (noopObserver) OnMessageDeadLettered(context.Context, string, string) {}
func (noopObserver) OnBatchFlushed(context.Context, string, string, int)   {}
func (noopObserver) OnBatchAdmission(context.Context, string, string)      {}

// observer holds the installed Observer as an atomic pointer rather than a
// mutex-guarded variable because obs() sits on the per-message path, which runs
// at Kafka ingest rates. The pointer indirection exists because Observer is an
// interface: storing a two-word interface value atomically requires boxing it
// behind one pointer.
var observer atomic.Pointer[Observer]

func init() {
	var o Observer = noopObserver{}
	observer.Store(&o)
}

// SetObserver installs the global trigger observer. Pass nil to restore the
// no-op default. Call once during process initialization.
func SetObserver(o Observer) {
	if o == nil {
		o = noopObserver{}
	}
	observer.Store(&o)
}

func obs() Observer { return *observer.Load() }

// discardLogInterval bounds how often a discard is logged per topic+reason. A
// malformed producer can invalidate every message in a partition, so an
// unthrottled log line would be the loudest thing in the process and would
// itself become the outage. The metric carries the exact count; the log exists
// so a deployment without --metrics-addr is not blind.
const discardLogInterval = 30 * time.Second

// discardLogger throttles discard logs per topic+reason and reports how many
// were suppressed since the last emission, so a throttled log never
// understates the volume.
type discardLogger struct {
	mu   sync.Mutex
	last map[string]time.Time
	// suppressed counts drops since the last emitted line, keyed the same way.
	suppressed map[string]int
}

var discardLog = &discardLogger{
	last:       make(map[string]time.Time),
	suppressed: make(map[string]int),
}

// allow reports whether a log line should be emitted now, and how many
// occurrences it stands for (1 plus everything suppressed since last time).
func (l *discardLogger) allow(now time.Time, key string) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	last, seen := l.last[key]
	if seen && now.Sub(last) < discardLogInterval {
		l.suppressed[key]++
		return false, 0
	}
	count := 1 + l.suppressed[key]
	l.suppressed[key] = 0
	l.last[key] = now
	return true, count
}
