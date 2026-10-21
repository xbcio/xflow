// Package asynq implements queue.Transport backed by Hibiken Asynq (Redis).
// It is the default transport for the distributed backend and the only place
// that imports github.com/hibiken/asynq — swapping to another broker means
// adding a sibling package (e.g. queue/nats) rather than editing the backend.
package asynq

import (
	"sync"
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

// queueNames resolves the queue names one deployment uses. suffix is the
// deployment identity derived from the owned namespace set
// (namespace.DeploymentSuffix): empty for the default deployment and for
// processes that configure no owned set, which keeps every legacy queue name
// byte-identical so a v0.0.47 and a v0.0.48 pod drain the same queues during a
// rolling upgrade. A dedicated deployment (e.g. a local dev process sharing the
// Redis) gets "<legacy>:ns:<suffix>" queues no legacy pod ever polls, so it
// neither consumes the shared deployment's tasks nor has its own consumed.
//
// The suffix is appended to the constant prefix, never substituted for it, so
// the isolation from other asynq applications (see above) is preserved. It is
// not operator-supplied free text: it comes from a validated namespace
// (namespace.Validate rejects ':', '{', '}' and the glob metacharacters), so it
// cannot reintroduce the bare "default" queue or corrupt asynq's
// asynq:{<queue>}:* hash tag.
type queueNames struct {
	suffix string
}

// name appends the deployment suffix to a legacy queue name.
func (q queueNames) name(legacy string) string {
	if q.suffix == "" {
		return legacy
	}
	return legacy + ":ns:" + q.suffix
}

// For routes a task to its asynq queue.
func (q queueNames) For(t *engine.Task) string {
	if t != nil && t.Type == engine.TaskTypeNodeBatch {
		return q.name(batchQueueName)
	}
	return q.name(defaultQueueName)
}

// Weights is the consumer's queue configuration, and the complete set of queues
// this deployment reads.
//
// The base names are constants rather than configuration on purpose. An
// operator-supplied queue name is a name that can be set back to asynq's shared
// "default" — silently reintroducing exactly the collision the prefix exists to
// prevent, with no compile error and nothing in the logs to notice. Isolation
// that depends on every deployment's config staying right is not isolation. The
// only variable part is the namespace-derived suffix above, which can only
// narrow the queue set, never widen it onto a name another application owns.
// Separating two xflow applications that have no namespace distinction still
// needs separate Redis instances, since a Redis Cluster has a single database
// and no DB index to move one of them to.
func (q queueNames) Weights() map[string]int {
	return map[string]int{
		q.name(defaultQueueName): defaultQueueWeight,
		q.name(batchQueueName):   batchQueueWeight,
	}
}

// All lists every queue this deployment reads, in a stable order.
func (q queueNames) All() []string {
	return []string{q.name(defaultQueueName), q.name(batchQueueName)}
}

// queueWeights and queueFor are the unsuffixed (legacy) deployment's names.
func queueWeights() map[string]int { return queueNames{}.Weights() }

func queueFor(t *engine.Task) string { return queueNames{}.For(t) }

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

// WithQueueSuffix routes this transport's producer, consumer and stats sampler
// onto the deployment's own queues (see queueNames). Pass the value from
// namespace.DeploymentSuffix; empty keeps the legacy queue names. The same
// suffix must reach all three halves — a producer on a suffixed queue with an
// unsuffixed consumer strands every task — which is why it is one transport
// option rather than three.
func WithQueueSuffix(suffix string) Option {
	return func(t *Transport) { t.queues = queueNames{suffix: suffix} }
}

// WithMaxQueueDepth bounds producer-side admission per queue: TryEnqueue
// reports engine.ErrQueueFull once the task's own queue holds n or more
// pending+active tasks. Zero (the default) disables the bound and starts no
// sampler; negative values are treated as zero.
//
// The bound exists for FlushOutbox, the one caller that must not block (see
// engine.NonBlockingTaskQueue). It is sampled, not exact — see queueFull — and
// it does NOT apply to Enqueue, which keeps blocking on purpose: Submit and
// lease-revoke redelivery have no durable intent behind them, so failing them
// on a transient full queue would strand work no sweeper can recover.
//
// Enabling this starts one sampling goroutine and one asynq Inspector
// connection; Close releases both.
func WithMaxQueueDepth(n int) Option {
	return func(t *Transport) {
		if n < 0 {
			n = 0
		}
		t.maxDepth = n
	}
}

// WithDepthSampleInterval overrides how often the producer-side depth cache is
// refreshed. Values <= 0 keep defaultDepthSampleInterval (1s). It has no
// effect unless WithMaxQueueDepth enables the cache.
func WithDepthSampleInterval(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.depthInterval = d
		}
	}
}

// withDepthFunc replaces the depth sampler. It exists only so tests can drive
// the cache deterministically instead of waiting on a live sample; it is
// applied at construction time, before the sampling goroutine starts, so the
// function field is never written while that goroutine reads it.
func withDepthFunc(fn depthFunc) Option {
	return func(t *Transport) {
		if fn != nil {
			t.depthFunc = fn
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
	queues    queueNames

	// Producer-side depth cache, active only when maxDepth > 0. depths maps a
	// queue name to its last measured pending+active count; a missing entry is
	// unmeasured and admits (see queueFull).
	maxDepth       int
	depthInterval  time.Duration
	depthInspector *asynqlib.Inspector
	depthFunc      depthFunc
	depthMu        sync.RWMutex
	depths         map[string]int
	depthDone      chan struct{}
	depthExited    chan struct{}
	depthStopOnce  sync.Once

	// closeOnce makes Close idempotent. Shutdown paths reach it more than once
	// in practice (backend teardown, then a caller's deferred Close), and the
	// underlying client reports "client is closed" on the second call, which is
	// not a condition any caller can act on.
	closeOnce sync.Once
	closeErr  error
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
		connOpt:       connOpt,
		client:        asynqlib.NewClient(connOpt),
		observer:      noopObserver{},
		depthInterval: defaultDepthSampleInterval,
	}
	if opt, ok := connOpt.(asynqlib.RedisClientOpt); ok {
		t.redisAddr = opt.Addr
	}
	t.depthFunc = t.readDepths
	for _, o := range opts {
		o(t)
	}
	// The sampler reads t.maxDepth, t.connOpt and t.depthFunc, so it must start
	// after every option has been applied.
	t.startDepthSampler()
	return t
}

// Close closes the producer client and stops the depth sampler (if one was
// started). It is idempotent: the first call's result is returned by every
// later call. The consumer server (if any) is stopped via the stop function
// returned by StartConsumer.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.stopDepthSampler()
		t.closeErr = t.client.Close()
	})
	return t.closeErr
}

// noopObserver is the default enqueue observer; it performs no work.
type noopObserver struct{}

func (noopObserver) OnEnqueue(string, time.Duration, error) {}

var _ queue.Transport = (*Transport)(nil)
