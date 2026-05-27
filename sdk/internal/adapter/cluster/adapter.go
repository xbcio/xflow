package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
)

// Option configures the cluster adapter.
type Option func(*config)

type config struct {
	concurrency int
	execTTL     time.Duration
}

// WithConcurrency sets the number of Asynq worker goroutines. Default is 10.
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

// Adapter wires the Engine Core to Redis (via redisState + asynqQueue).
// Call Bind() after creating the engine to wire the Asynq server.
type Adapter struct {
	state          *redisState
	queue          *asynqQueue
	registry       *clusterRegistry
	rdb            *redis.Client
	timeoutMonitor *TimeoutMonitor
	redisAddr      string
	concurrency    int
}

// State returns the StateBackend implementation.
func (a *Adapter) State() engine.StateBackend { return a.state }

// Queue returns the TaskQueue implementation.
func (a *Adapter) Queue() engine.TaskQueue { return a.queue }

// Registry returns the HandlerRegistry implementation.
func (a *Adapter) Registry() engine.HandlerRegistry { return a.registry }

// New creates a cluster adapter connected to the given Redis address.
// db may be nil for pure-Redis mode (no MySQL persistence).
// Call Bind(eng) after creating the engine to start workers.
func New(redisAddr string, db store.ClusterStore, opts ...Option) (*Adapter, error) {
	cfg := &config{concurrency: 10, execTTL: defaultExecTTL}
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
	registry := newClusterRegistry()

	return &Adapter{
		state:       state,
		queue:       queue,
		registry:    registry,
		rdb:         rdb,
		redisAddr:   redisAddr,
		concurrency: cfg.concurrency,
	}, nil
}

// Bind wires the engine's ExecuteNode into the Asynq server and starts
// the timeout monitor. Returns a stop function for graceful shutdown.
func (a *Adapter) Bind(eng *engine.Engine) func() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: a.redisAddr},
		asynq.Config{Concurrency: a.concurrency},
	)
	mux := asynq.NewServeMux()
	mux.HandleFunc(asynqTaskType, func(ctx context.Context, t *asynq.Task) error {
		var task engine.Task
		if err := json.Unmarshal(t.Payload(), &task); err != nil {
			return err
		}
		return eng.ExecuteNode(ctx, &task)
	})

	tm := NewTimeoutMonitor(a.rdb, eng, nil, nil, 5*time.Second)
	a.timeoutMonitor = tm

	go tm.Run()
	go func() {
		if err := srv.Run(mux); err != nil {
			log.Printf("xflow: asynq server error: %v", err)
		}
	}()

	return func() {
		tm.Stop()
		srv.Shutdown()
		_ = a.queue.Close()
		_ = a.rdb.Close()
	}
}
