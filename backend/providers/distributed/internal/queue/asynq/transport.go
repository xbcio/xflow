// Package asynq implements queue.Transport backed by Hibiken Asynq (Redis).
// It is the default transport for the distributed backend and the only place
// that imports github.com/hibiken/asynq — swapping to another broker means
// adding a sibling package (e.g. queue/nats) rather than editing the backend.
package asynq

import (
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// taskType is the Asynq task type used for all xflow node tasks.
const taskType = "xflow:node"

// Asynq queue names and their relative weights.
//
// Every queue name carries the "xflow:" prefix, and that prefix is the whole of
// the isolation this application gets from another asynq application sharing the
// Redis database. asynq has no per-application key prefix: it addresses a queue
// as asynq:{<queue>}:* and keeps its bookkeeping under unprefixed asynq:* keys
// (internal/base/base.go), so the queue name *is* the namespace. A bare name
// like "default" is therefore not this application's private queue — it is every
// asynq application's queue in that database.
//
// The collision this prefix prevents is not hypothetical, and the case that
// motivated it is embedding: a host application already using asynq for its own
// work mounts an xflow server in the same process, sharing one Redis. Two asynq
// servers then poll the same "default" queue, so which one takes a given task is
// a race. The one that takes a task it has no handler for returns asynq's
// "handler not found" error, which is RETRYABLE rather than skipped, so the task
// goes back to asynq:{default}:retry with a backoff. The usual outcome is
// therefore not loss but churn: retry storms, latency spikes and tasks cycling
// through retry until the rightful consumer happens to win the race. Only a task
// that loses the race the default of 25 times ends up archived, which is where
// it is genuinely lost. Either way neither consumer's owner sees an error. Only
// a payload that fails to unmarshal is skipped without retry; see consumer.go.
//
// The whole server is scoped by this list, not just the pending take. asynq's
// recoverer (ListLeaseExpired, ReclaimStaleAggregationSets) and janitor
// (DeleteExpiredCompletedTasks) each loop over Config.Queues, so before the
// prefix an embedded xflow server would also recover a host's lease-expired
// tasks and reap its completed sets. UniqueTask and group keys are per-queue
// too (asynq:{<queue>}:unique:*, asynq:{<queue>}:g:*).
//
// What stays shared is asynq's five hardcoded global keys — asynq:servers,
// asynq:workers, asynq:schedulers, asynq:queues, asynq:cancel (base.go:36-40).
// They cannot be prefixed and are not configurable. That costs tooling
// visibility only (asynqmon lists both applications' servers and queues);
// asynq:cancel is a shared pubsub channel addressed by task UUID, so a
// cross-application hit is not credible. Execution correctness does not depend
// on them.
//
// defaultQueueName deliberately does not reuse taskType ("xflow:node"): asynq
// prints queue names and task types side by side in the same diagnostics, and
// making them identical there costs more than the symmetry is worth.
//
// Batch continuations (an expanded map's items) ride their own queue so a wide
// map cannot occupy every slot ahead of an unrelated execution's first node —
// the same head-of-line block measured on the local backend, where a small
// execution submitted into a queue saturated by a 200-batch map waited 854ms
// for its first node.
//
// Weighted, not strict: asynq drains a strict-priority high queue completely
// before touching the low one, which would let a steady stream of ordinary
// tasks park an in-flight map indefinitely with nothing reporting the stall.
// At 8:1 the batch queue keeps roughly 1/9 of consumer throughput.
const (
	defaultQueueName = "xflow:default"
	batchQueueName   = "xflow:batch"

	defaultQueueWeight = 8
	batchQueueWeight   = 1
)

// queueWeights is the consumer's queue configuration, and the complete set of
// queues this application reads.
//
// These names are constants rather than configuration on purpose. An operator-
// supplied queue name is a name that can be set back to asynq's shared "default"
// — silently reintroducing exactly the collision the prefix exists to prevent,
// with no compile error and nothing in the logs to notice. Isolation that
// depends on every deployment's config staying right is not isolation. A
// deployment that needs to separate two xflow applications must do it at a
// level that actually separates all of them: separate Redis instances, since a
// Redis Cluster has a single database and no DB index to move one of them to.
func queueWeights() map[string]int {
	return map[string]int{
		defaultQueueName: defaultQueueWeight,
		batchQueueName:   batchQueueWeight,
	}
}

// queueFor routes a task to its asynq queue.
func queueFor(t *engine.Task) string {
	if t != nil && t.Type == engine.TaskTypeNodeBatch {
		return batchQueueName
	}
	return defaultQueueName
}

// Option configures an asynq Transport at construction time.
type Option func(*Transport)

// WithObserver installs a producer-side enqueue observer. It composes with the
// distributed backend's WithQueueObserver option.
func WithObserver(obs queue.Observer) Option {
	return func(t *Transport) {
		if obs != nil {
			t.observer = obs
		}
	}
}

// Transport is the Asynq-backed queue.Transport. The producer client is created
// eagerly; the consumer server is created lazily in StartConsumer so API-only
// instances pay no server cost.
type Transport struct {
	connOpt   asynqlib.RedisConnOpt
	redisAddr string
	client    *asynqlib.Client
	observer  queue.Observer
}

// New creates an Asynq transport connected to the given Redis address.
// It is the legacy constructor kept for backward compatibility.
func New(redisAddr string, opts ...Option) *Transport {
	return NewWithConnOpt(asynqlib.RedisClientOpt{Addr: redisAddr}, opts...)
}

// NewWithConnOpt creates an Asynq transport from an explicit asynq connection
// option. It supports single-node, sentinel, and cluster Redis deployments.
func NewWithConnOpt(connOpt asynqlib.RedisConnOpt, opts ...Option) *Transport {
	if connOpt == nil {
		connOpt = asynqlib.RedisClientOpt{}
	}
	t := &Transport{
		connOpt:  connOpt,
		client:   asynqlib.NewClient(connOpt),
		observer: noopObserver{},
	}
	if opt, ok := connOpt.(asynqlib.RedisClientOpt); ok {
		t.redisAddr = opt.Addr
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Close closes the producer client. The consumer server (if any) is stopped via
// the stop function returned by StartConsumer.
func (t *Transport) Close() error { return t.client.Close() }

// noopObserver is the default enqueue observer; it performs no work.
type noopObserver struct{}

func (noopObserver) OnEnqueue(string, time.Duration, error) {}

var _ queue.Transport = (*Transport)(nil)
