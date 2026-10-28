//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/namespace"

	_ "github.com/xbcio/xflow/node" // registers the builtin nodes (start, end, if, map, wait, function)
)

// kafkaE2EOpts selects how one engine process of a deployment is assembled.
type kafkaE2EOpts struct {
	// producerOnly builds WithConsumer(false): the process submits and writes the
	// outbox but never consumes a task.
	producerOnly bool
	// asynq keeps the default Asynq transport instead of Kafka (baseline run).
	asynq bool
	// concurrency is the consumer worker count; 0 means 4.
	concurrency int
	// bodies wires the in-process map body executor the map node needs.
	bodies bool
	// register installs custom handlers before the consumer starts.
	register func(engine.HandlerRegistrar)
}

// kafkaE2ENode is one assembled process: a distributed backend on real Redis and
// real Kafka, its engine, and the bound consumer / outbox dispatcher.
type kafkaE2ENode struct {
	b   *distributed.Backend
	eng *engine.Engine
}

// startKafkaE2ENode assembles the cluster-durable stack the way sdk/xflow's
// NewCluster does (the SDK itself is out of scope here): distributed.New with
// db=nil (pure Redis), engine.New, then StartBinding, which starts the task
// consumer, the 1s outbox dispatcher and the lease timeout monitor.
func startKafkaE2ENode(t *testing.T, env *kafkaE2EEnv, o kafkaE2EOpts) *kafkaE2ENode {
	t.Helper()
	if o.concurrency == 0 {
		o.concurrency = 4
	}
	opts := []distributed.Option{
		distributed.WithConcurrency(o.concurrency),
		distributed.WithConsumer(!o.producerOnly),
		distributed.WithOwnedNamespaces([]namespace.Namespace{env.ns}),
	}
	if !o.asynq {
		env.kafkaUsed = true
		opts = append(opts, distributed.WithKafkaQueue(distributed.KafkaConfig{
			Brokers: env.brokers, CreateTopics: true, Partitions: 3, ReplicationFactor: 1,
		}))
	}
	b, err := distributed.New(env.addr, nil, opts...)
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	reg, ok := b.Registry().(engine.HandlerRegistrar)
	if !ok {
		t.Fatalf("registry %T is not a HandlerRegistrar", b.Registry())
	}
	if o.register != nil {
		o.register(reg)
	}
	engOpts := []engine.Option{engine.WithDefaultLeaseTTL(time.Minute), engine.WithNamespace(env.ns)}
	if o.bodies {
		engOpts = append(engOpts, engine.WithBatchBodyExecutor(newE2EBodyExecutor(t, b)))
	}
	eng := engine.New(b.State(), b.Queue(), engOpts...)
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding: %v", err)
	}
	t.Cleanup(stop)
	return &kafkaE2ENode{b: b, eng: eng}
}

// newE2EBodyExecutor builds the executor that runs a map body per item inside
// the batch task, the same wiring local_map_batch_roundtrip_test.go uses.
func newE2EBodyExecutor(t *testing.T, b *distributed.Backend) *subgraph.MapBodyExecutor {
	t.Helper()
	reg, ok := b.Registry().(*execution.Registry)
	if !ok {
		t.Fatalf("registry %T is not *execution.Registry", b.Registry())
	}
	exec := subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
		func() subgraph.Backend {
			return backendlocal.New(backendlocal.WithRegistry(reg), backendlocal.WithConcurrency(1))
		})
	return subgraph.NewMapBodyExecutor(exec, true, time.Time{})
}
