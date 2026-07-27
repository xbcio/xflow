// Package asynq implements queue.Transport backed by Hibiken Asynq (Redis).
// It is the default transport for the distributed backend and the only place
// that imports github.com/hibiken/asynq — swapping to another broker means
// adding a sibling package (e.g. queue/nats) rather than editing the backend.
package asynq

import (
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
)

// taskType is the Asynq task type used for all xflow node tasks.
const taskType = "xflow:node"

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
