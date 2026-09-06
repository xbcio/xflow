package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

// commitCoalescer merges concurrent commit calls into one broker round trip.
//
// It exists because the commit hop was the pipeline's pacer, and the reason is
// structural rather than a slow broker. Under CommitInterval: 0 kafka-go runs
// commitLoopImmediate: ONE goroutine draining ONE channel (Reader.commits),
// blocking on a full CommitOffsets RPC before it takes the next request. Every
// partition assigned to the Reader queues there, so the per-partition flush
// workers serialize against each other. Measured over 411 s on an 18-partition
// topic: 2927 commits, mean 2.24 s each, 88.8% of all available partition-time,
// against 325.2 s of wasm evaluation in the same window.
//
// What makes merging work is that ONE CommitMessages call carrying N partitions
// is exactly ONE RPC. makeCommits builds an offsetStash keyed topic->partition
// (reader.go:171-184, taking the max offset per partition), and
// Generation.CommitOffsets turns the whole stash into a single
// offsetCommitRequestV2 (consumergroup.go:418-459). So K callers that cost K
// serialized RPCs today cost one when merged.
//
// Batching is self-clocking, not timed. There is deliberately no linger window:
// the loop blocks for the first request, then takes only what is ALREADY
// queued. A batch is therefore exactly what backpressure accumulated while the
// previous RPC was in flight, and a caller arriving at an idle coalescer pays
// nothing beyond its own round trip. A linger timer would tax the idle case to
// help the busy one, and the busy case does not need the help.
//
// Measured against the real cluster, one Reader on both sides and the only
// difference being this type: mean commit 2.049 s -> 179 ms (11.4x), max
// 2.367 s -> 277 ms, and the arm moved out of the commit-queue-bound category
// entirely (queue depth 17.3 -> 1.5).
//
// One counterintuitive property, recorded because it will otherwise be
// misread: the measured merge factor was only 4.1 callers per RPC, and the
// latency was BETTER than a local model that merged 8.8. A low factor means
// callers are spread out, so a caller tends to meet an idle coalescer and pay
// about one round trip instead of waiting out an in-flight one. The factor's
// meaningful floor is 1.0 — which is the serial behaviour this replaces — not
// the partition count.
type commitCoalescer struct {
	// commit performs the merged call. A field rather than a direct
	// reader.CommitMessages call so the loop can be tested without a broker:
	// every property worth asserting here is about queueing and fan-out, and
	// none of them need a real Reader.
	commit func(context.Context, ...kafkago.Message) error

	// ctx bounds the merged call and stops the loop. It is the consumer's ctx,
	// NOT a caller's: a merged RPC belongs to no single caller, so no caller's
	// deadline may cancel it. Callers still honour their own ctx in the outer
	// select below, which means an impatient caller returns while its offsets
	// stay in flight — the same shape kafka-go already has, since
	// Reader.CommitMessages enqueues onto Reader.commits BEFORE it selects on
	// ctx.Done (reader.go:878-912).
	//
	// It must be a cancellable context and Close must cancel it. When a Reader
	// closes, commitLoopImmediate stops replying to requests still in its
	// channel, so a merged call left on context.Background() would block here
	// forever and take Close with it.
	ctx context.Context

	reqs chan *commitBatchRequest
	done chan struct{}
}

type commitBatchRequest struct {
	messages []kafkago.Message
	// result carries the single outcome this request will ever see. It is
	// BUFFERED, and that is load-bearing rather than stylistic: a caller whose
	// own ctx fires returns without reading it, and an unbuffered channel would
	// wedge the loop on that abandoned caller — converting a pre-existing
	// tolerable condition into a stall of every other partition.
	result chan error
}

func newCommitCoalescer(ctx context.Context, commit func(context.Context, ...kafkago.Message) error) *commitCoalescer {
	c := &commitCoalescer{
		commit: commit,
		ctx:    ctx,
		reqs:   make(chan *commitBatchRequest),
		done:   make(chan struct{}),
	}
	go c.run()
	return c
}

func (c *commitCoalescer) run() {
	defer close(c.done)
	for {
		var first *commitBatchRequest
		select {
		case first = <-c.reqs:
		case <-c.ctx.Done():
			return
		}

		waiters := []*commitBatchRequest{first}
		merged := append([]kafkago.Message(nil), first.messages...)
		// Non-blocking drain. Whatever queued while the last RPC was in flight
		// joins this one; nothing is waited for.
		for draining := true; draining; {
			select {
			case req := <-c.reqs:
				waiters = append(waiters, req)
				merged = append(merged, req.messages...)
			default:
				draining = false
			}
		}

		err := c.commit(c.ctx, merged...)

		// One outcome, fanned to every waiter, because nothing finer is
		// available: kafka-go collapses the per-partition reply before this code
		// can see it. Conn.offsetCommit walks the response's partition results
		// and returns on the FIRST nonzero error code, discarding both the rest
		// and which partition it came from (conn.go:470-476).
		//
		// That collapse is worth stating plainly, because it is the one place
		// merging is genuinely worse than committing separately. The broker
		// applies each partition independently, so when a single partition is
		// rejected the others may well have been committed — yet every caller in
		// the batch is told the commit failed and every one of them retries.
		// Committing separately, that partition would have failed alone.
		//
		// It is not a correctness problem: offsets are idempotent, so a redundant
		// re-commit of an already-committed offset is a no-op, and at-least-once
		// holds either way. What it costs is throughput while a bad partition
		// persists, and an error rate on xflow_trigger_offset_commit_duration
		// inflated by the batch width. The genuinely per-partition rejections
		// are rare (UNKNOWN_TOPIC_OR_PARTITION, INVALID_COMMIT_OFFSET_SIZE);
		// generation, membership and authorization failures are request-wide and
		// would have failed every partition anyway.
		//
		// The available mitigation, if that ever shows up in production, is to
		// re-run a failed batch as individual commits to isolate the offender.
		// Deliberately not built now: it would add a retry path that no observed
		// failure exercises, and the metric above is what would justify it.
		for _, req := range waiters {
			req.result <- err
		}
	}
}

// commitMessages submits one caller's offsets and blocks until the round trip
// that carried them completes.
//
// ctx is the CALLER's, and it governs only this caller: it can abandon the
// wait, but it never cancels the merged RPC, which may be carrying seventeen
// other partitions' offsets.
func (c *commitCoalescer) commitMessages(ctx context.Context, messages ...kafkago.Message) error {
	req := &commitBatchRequest{messages: messages, result: make(chan error, 1)}
	select {
	case c.reqs <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return context.Canceled
	}
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return context.Canceled
	}
}

// wait blocks until the loop has exited. Close must call it, and must have
// cancelled ctx first.
//
// Without this the consumer leaks one goroutine per activation, and it leaks in
// the quiet case rather than the busy one: Close usually happens with nothing
// queued, so the loop is parked on its outer select and nothing else would ever
// wake it.
func (c *commitCoalescer) wait() { <-c.done }
