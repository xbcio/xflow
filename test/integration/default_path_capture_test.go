//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// dpCapture holds the raw bytes of one run, before normalization: every
// outbox:body value the queue was about to deliver, and every task payload the
// broker handed to the consumer. A value seen twice is kept once.
type dpCapture struct {
	rdb *redis.Client
	ns  namespace.Namespace

	mu     sync.Mutex
	bodies map[string][]byte // outbox:body field (entry id) -> JSON value
	queued map[string][]byte // asynq task id -> task payload
	errs   []error           // capture or delivery failures, reported by the test
}

func newDPCapture(rdb *redis.Client, ns namespace.Namespace) *dpCapture {
	return &dpCapture{rdb: rdb, ns: ns, bodies: map[string][]byte{}, queued: map[string][]byte{}}
}

func (c *dpCapture) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

// outboxBodies stores every entry the execution's outbox holds right now. It is
// called just before a task is handed to the queue: FlushOutbox acknowledges
// (and so deletes) an entry only after its enqueue returned, so the entry being
// delivered is still there to be read.
func (c *dpCapture) outboxBodies(id types.ExecutionID) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("xflow:ns:%s:exec:{%s}:outbox:body", c.ns, id)
	all, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		c.fail(fmt.Errorf("read %s: %w", key, err))
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for field, body := range all {
		if _, seen := c.bodies[field]; !seen {
			c.bodies[field] = []byte(body)
		}
	}
}

// dpRecordingQueue is the task queue the engine of the run delivers through. It
// reads the outbox before each delivery and then hands the task to the real
// broker transport untouched, so the bytes that reach asynq are the ones the
// engine produced.
type dpRecordingQueue struct {
	next engine.TaskQueue
	cap  *dpCapture
}

func (q *dpRecordingQueue) Enqueue(ctx context.Context, t *engine.Task) error {
	q.cap.outboxBodies(t.ExecutionID)
	return q.next.Enqueue(ctx, t)
}

func (q *dpRecordingQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, d time.Duration) error {
	q.cap.outboxBodies(t.ExecutionID)
	return q.next.EnqueueDelayed(ctx, t, d)
}

// payload stores one asynq task body under its task id. A redelivery of the
// same asynq task carries the same id and is kept once.
func (c *dpCapture) payload(taskID string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, seen := c.queued[taskID]; !seen {
		c.queued[taskID] = append([]byte(nil), body...)
	}
}

// problems returns the capture failures recorded so far.
func (c *dpCapture) problems() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

// snapshot returns the captured documents, each list in key order, together
// with the failures recorded so far. Key order only makes the call repeatable;
// the baseline files are sorted by normalized content.
func (c *dpCapture) snapshot() (bodies, queued [][]byte, errs []error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	collect := func(m map[string][]byte) [][]byte {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]byte, 0, len(keys))
		for _, k := range keys {
			out = append(out, m[k])
		}
		return out
	}
	return collect(c.bodies), collect(c.queued), append([]error(nil), c.errs...)
}
