package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/store"
)

// Option configures the Asynq backend.
type Option func(*config)

type config struct {
	concurrency int
	execTTL     time.Duration
	consumer    bool
}

// WithConcurrency sets the number of Asynq queue consumer goroutines. Default is 10.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithExecTTL sets the TTL for all Redis keys belonging to an execution.
// Default is 24 hours.
func WithExecTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.execTTL = d
		}
	}
}

// WithConsumer controls whether Bind starts an Asynq consumer and timeout
// monitor in this process. Disable it for API-only embedded SDK instances.
func WithConsumer(enabled bool) Option {
	return func(c *config) {
		c.consumer = enabled
	}
}

// Backend wires the Engine Core to Redis state and an Asynq task queue.
// Call Bind() after creating the engine to wire the Asynq server.
type Backend struct {
	state          *redisState
	queue          *asynqQueue
	registry       *execution.Registry
	rdb            *redis.Client
	timeoutMonitor *TimeoutMonitor
	redisAddr      string
	concurrency    int
	consumer       bool
}

// State returns the StateStore implementation.
func (b *Backend) State() engine.StateStore { return b.state }

// Queue returns the TaskQueue implementation.
func (b *Backend) Queue() engine.TaskQueue { return b.queue }

// Registry returns the HandlerRegistry implementation.
func (b *Backend) Registry() engine.HandlerRegistry { return b.registry }

// New creates an Asynq backend connected to the given Redis address.
// db may be nil for pure-Redis mode (no MySQL persistence).
// Call Bind(eng) after creating the engine to start queue consumers.
func New(redisAddr string, db store.Store, opts ...Option) (*Backend, error) {
	cfg := &config{concurrency: 10, execTTL: defaultExecTTL, consumer: true}
	for _, o := range opts {
		o(cfg)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	state := newRedisState(rdb, db, cfg.execTTL)
	queue := newAsynqQueue(redisAddr)
	registry := execution.NewRegistry()

	return &Backend{
		state:       state,
		queue:       queue,
		registry:    registry,
		rdb:         rdb,
		redisAddr:   redisAddr,
		concurrency: cfg.concurrency,
		consumer:    cfg.consumer,
	}, nil
}

// Bind wires the embedded execution dispatcher into the Asynq server and
// starts the timeout monitor. Returns a stop function for graceful shutdown.
func (b *Backend) Bind(eng *engine.Engine) func() {
	if !b.consumer {
		return func() {
			_ = b.queue.Close()
			_ = b.rdb.Close()
		}
	}

	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry)
	srv := asynqlib.NewServer(
		asynqlib.RedisClientOpt{Addr: b.redisAddr},
		asynqlib.Config{Concurrency: b.concurrency},
	)
	mux := asynqlib.NewServeMux()
	mux.HandleFunc(asynqTaskType, func(ctx context.Context, t *asynqlib.Task) error {
		var task engine.Task
		if err := json.Unmarshal(t.Payload(), &task); err != nil {
			return err
		}
		return dispatcher.HandleTask(ctx, &task)
	})

	tm := NewTimeoutMonitor(b.rdb, eng, nil, nil, 5*time.Second)
	b.timeoutMonitor = tm

	go tm.Run()
	go func() {
		if err := srv.Run(mux); err != nil {
			log.Printf("xflow: asynq server error: %v", err)
		}
	}()

	return func() {
		tm.Stop()
		srv.Shutdown()
		_ = b.queue.Close()
		_ = b.rdb.Close()
	}
}
