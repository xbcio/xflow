package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Observer receives trigger observations. Implementations must be non-blocking
// and must never use message content or keys as labels: content is not an
// operator's to read from a metric. Offsets are unbounded cardinality and are
// never labels either — OnConsumerLag passes one as a VALUE, which is what a
// gauge is for.
//
// Topic and partition are the only message-derived labels. Partition was added
// with OnConsumerLag and is admitted only there: lag is a per-partition
// quantity, so a gauge Set from every partition under one label set reports
// whichever partition wrote last, and one caught-up partition would hide a
// stalled one. It stays bounded by the assignment, which is tens of series per
// topic, not by anything a producer controls.
type Observer interface {
	// OnMessageDiscarded reports a message that was consumed (its offset
	// committed) but never emitted. reason is a fixed enum: "schema" and
	// "schema_fail" come from validation (schema.go), "buffer_overflow" from the
	// aggregator shedding a message it had already read (aggregate.go). This is
	// the counter that makes an otherwise invisible drop visible.
	//
	// The three are not equally recoverable, and the metric's help text says so:
	// a schema drop is a decision about a message that could not be used,
	// whereas buffer_overflow discards a usable message AND lets the commit
	// frontier advance past its offset, so nothing ever redelivers it.
	OnMessageDiscarded(ctx context.Context, topic, reason string)
	// OnMessageDeadLettered reports a dead-letter publish attempt. result is
	// "ok" or "error".
	OnMessageDeadLettered(ctx context.Context, topic, result string)
	// OnConsumerLag reports how far this partition's fetch position sits behind
	// the broker's high-water mark, sampled from a message that has just been
	// fetched. fetchedAt is when that sample was taken.
	//
	// It exists because the consumer disables kafka-go's own lag reporting
	// (ReadLagInterval = -1) on the grounds that the trigger reports lag itself,
	// and for a long time the trigger did not. Four other comments in this tree
	// reason about "consumer-group lag" as an available signal; until this
	// method existed, none of them had one.
	//
	// fetchedAt is not decoration. This sample only advances when a message is
	// fetched, so a consumer that has STOPPED fetching leaves lag frozen at its
	// last value — the one failure this metric exists to catch would read as
	// healthy. Exporting the sample time makes the freeze detectable: staleness
	// is now-minus-fetchedAt, and a lag figure is only worth reading if that is
	// small. Reporting lag alone would have been worse than reporting nothing.
	OnConsumerLag(ctx context.Context, topic string, partition int, lag int64, fetchedAt time.Time)
	// OnConsumptionBlocked reports a partition halting or resuming consumption
	// under on_overflow=block. blocked is the new state, reported on transitions
	// only.
	//
	// Partition is a label here for the same reason it is on OnConsumerLag, and
	// with more force: this is a per-partition state, and one blocked partition
	// among seventeen healthy ones is precisely the case an operator needs to
	// tell apart from a whole assignment stopping.
	//
	// It exists because OnConsumerLag cannot report this. That sample only
	// advances when a message is fetched, so a partition applying backpressure —
	// which means it has stopped fetching — holds its lag at the last value it
	// saw. Without this signal the healthiest-looking consumer in the fleet is
	// the one that has stopped.
	OnConsumptionBlocked(ctx context.Context, topic string, partition int, blocked bool)
	// OnBatchFlushed reports one batch ATTEMPTING to leave the aggregator.
	// trigger is a fixed enum: "size", "timeout", "idle", "close". size is the
	// message count.
	//
	// Reported before the downstream call, not after it succeeds. Conditioning
	// the sample set on success made the size histogram describe only surviving
	// batches, so a partition whose buffer was stuck at its cap reported a mean
	// of 3.64 — see the comment at the call site.
	OnBatchFlushed(ctx context.Context, topic, trigger string, size int)
	// OnBatchFlushOutcome reports whether that attempt succeeded. result is "ok"
	// or "error". Paired with OnBatchFlushed, the ratio is the retry rate: the
	// signal that distinguishes a slow pipeline from one re-running the same
	// messages without ever committing them.
	OnBatchFlushOutcome(ctx context.Context, topic, trigger, result string)
	// OnBatchAdmission reports the control-plane response to a batch admission.
	// state is "accepted", "duplicate", "conflict", "deterministic_error" or
	// "error"; reason narrows the two non-success states to WHICH failure this
	// was, and is admissionReasonNone otherwise.
	//
	// The conflict rate is the ONLY signal that shows how often a redelivered
	// batch actually re-executes. The design accepts that duplicate (delivery is
	// at-least-once), but accepting it is not the same as not looking at it.
	//
	// reason exists because "error" alone conflates four failures that call for
	// four different responses — see the admissionReason constants. Both labels
	// are closed enums built in this package; neither is ever derived from a
	// message or from a runtime's free-text error.
	OnBatchAdmission(ctx context.Context, topic, state, reason string)
	// OnOffsetCommit reports one broker round trip that advanced the committed
	// offset. result is "ok" or "error"; messages is how many offsets that single
	// round trip carried; d is the wall time the caller spent blocked in it.
	//
	// It exists because this hop had no instrumentation at all, and it turned out
	// to be the pacer. Every other trigger metric measures work — batch size,
	// admission latency, node duration — so a profile could account for ~111 ms
	// of real work per batch while the pipeline released only ~1.03 batches per
	// second per partition. The missing 9-35x was here, and the only way to see
	// it was a stack dump. A commit cycle inferred from the batch release rate is
	// a division, not a measurement; this is the measurement.
	//
	// messages is reported alongside the duration because the two together
	// separate the two hypotheses a duration alone cannot: a commit whose cost is
	// per-round-trip (raise max_size and throughput rises with it) from one whose
	// cost scales with the offsets it carries (raise max_size and nothing moves).
	//
	// No partition label, unlike OnConsumerLag and OnConsumptionBlocked. Those
	// are gauges, where one Set per partition under a shared label set would let
	// the last writer erase the others. This is a histogram: samples from every
	// partition merge instead of overwriting, and the merged distribution answers
	// the question the metric was added for. A per-partition breakdown would
	// multiply the bucket series by the assignment to isolate a single slow
	// partition — worth adding when that becomes the question, not before.
	OnOffsetCommit(ctx context.Context, topic, result string, messages int, d time.Duration)
}

type noopObserver struct{}

func (noopObserver) OnMessageDiscarded(context.Context, string, string)           {}
func (noopObserver) OnMessageDeadLettered(context.Context, string, string)        {}
func (noopObserver) OnConsumerLag(context.Context, string, int, int64, time.Time) {}
func (noopObserver) OnConsumptionBlocked(context.Context, string, int, bool)      {}
func (noopObserver) OnBatchFlushed(context.Context, string, string, int)          {}
func (noopObserver) OnBatchFlushOutcome(context.Context, string, string, string)  {}
func (noopObserver) OnBatchAdmission(context.Context, string, string, string)     {}
func (noopObserver) OnOffsetCommit(context.Context, string, string, int, time.Duration) {
}

// observer holds the installed Observer as an atomic pointer rather than a
// mutex-guarded variable because obs() sits on the per-message path, which runs
// at Kafka ingest rates. The pointer indirection exists because Observer is an
// interface: storing a two-word interface value atomically requires boxing it
// behind one pointer.
var observer atomic.Pointer[Observer]

// observerMu guards the observerInstalled flag. It protects only the write
// path; the hot-path read (obs()) goes straight to the atomic pointer.
var observerMu sync.Mutex

// observerInstalled tracks whether a non-noop observer is currently installed,
// so a second non-nil SetObserver call can be caught and panicked. It is
// guarded by observerMu.
var observerInstalled bool

func init() {
	var o Observer = noopObserver{}
	observer.Store(&o)
}

// SetObserver installs the global trigger observer. Pass nil to restore the
// no-op default.
//
// Call once during process initialization. A second non-nil call panics:
// two callers racing to set a process-wide observer means one of them will
// silently lose all its observations, which is worse than failing loudly.
// Pass nil explicitly to remove the observer before installing a new one
// (tests use this as their teardown path).
func SetObserver(o Observer) {
	observerMu.Lock()
	defer observerMu.Unlock()
	if o == nil {
		var noop Observer = noopObserver{}
		observer.Store(&noop)
		observerInstalled = false
		return
	}
	if observerInstalled {
		panic("kafka.SetObserver: observer already installed; call SetObserver(nil) first")
	}
	observer.Store(&o)
	observerInstalled = true
}

func obs() Observer { return *observer.Load() }

// discardLogInterval bounds how often a discard is logged per topic+reason. A
// malformed producer can invalidate every message in a partition, so an
// unthrottled log line would be the loudest thing in the process and would
// itself become the outage. The metric carries the exact count; the log exists
// so a deployment without --metrics-addr is not blind.
const discardLogInterval = 30 * time.Second

// discardLogger throttles discard logs per key and reports how many were
// suppressed since the last emission, so a throttled log never understates
// the volume.
//
// Every caller's key ends in the partition, because the goroutines that log
// here run one per partition: a key without it lets one partition hold the
// gate for the whole interval while the partition/offset printed belong to
// whichever call won the race. The suppressed count is scoped the same way,
// so it counts this partition's suppressions rather than a cross-partition
// mix.
//
// The key space is partitioned by CONVENTION, not by construction: callers
// build "topic\x00<discriminator>\x00<partition>" and today's discriminators
// happen not to overlap. A new discriminator that collides with an existing
// one would silently reintroduce the cross-caller suppression this scoping
// exists to remove, and nothing here would report it.
//
// That makes the inventory below load-bearing: it is the whole of the
// no-collision argument. Re-derive it rather than trusting it — the criterion
// is every non-test call site of discardLog.allow, and the discriminator is
// the second \x00-separated segment each one builds. Today that is four sites
// yielding seven discriminators:
//
//	entryseed.go  "admission_" + state   admission_error,
//	                                     admission_deterministic_error
//	aggregate.go  "overflow"             (shed under on_overflow=drop)
//	aggregate.go  "stuck_batch"
//	schema.go     reason                 schema, schema_fail, dead_letter
//
// Note that entryseed.go's state is NOT OnBatchAdmission's five-value enum:
// the log fires only on the failure paths, so accepted/duplicate/conflict
// reach the metric and never reach this map. Reading the metric's enum as the
// key inventory overstates it by three.
//
// Only two segments are variables (entryseed.go's state, schema.go's reason)
// and both are closed enums built in this package — every call site passes a
// literal except entryseed.go:293-295, which picks between those same two
// values. schema.go passes err.Error() as the log's action, not into the key,
// so no free-text error ever reaches this map.
//
// The same enumeration bounds the maps: every key component is bounded (topic
// x seven discriminators x the partition assignment), so last/suppressed grow
// with the assignment and not with traffic. An earlier note in this tree
// claimed they were unbounded insert-only maps; that was wrong, and the
// enumeration above is why.
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
