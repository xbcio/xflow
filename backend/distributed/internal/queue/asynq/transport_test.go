package asynq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynqlib "github.com/hibiken/asynq"
	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/distributed/internal/queue"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestNewUsesRedisClientOpt(t *testing.T) {
	tr := New("127.0.0.1:6379")
	if tr == nil {
		t.Fatal("New() returned nil")
	}
	if tr.client == nil {
		t.Fatal("transport client is nil")
	}
	opt, ok := tr.connOpt.(asynqlib.RedisClientOpt)
	if !ok {
		t.Fatalf("connOpt type = %T, want RedisClientOpt", tr.connOpt)
	}
	if opt.Addr != "127.0.0.1:6379" {
		t.Fatalf("RedisClientOpt.Addr = %q, want 127.0.0.1:6379", opt.Addr)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewWithConnOptRedisClusterClientOpt(t *testing.T) {
	opt := asynqlib.RedisClusterClientOpt{
		Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"},
	}
	tr := NewWithConnOpt(opt)
	if tr == nil {
		t.Fatal("NewWithConnOpt() returned nil")
	}
	if tr.client == nil {
		t.Fatal("transport client is nil")
	}
	got, ok := tr.connOpt.(asynqlib.RedisClusterClientOpt)
	if !ok {
		t.Fatalf("connOpt type = %T, want RedisClusterClientOpt", tr.connOpt)
	}
	if len(got.Addrs) != 2 || got.Addrs[0] != "127.0.0.1:6379" || got.Addrs[1] != "127.0.0.1:6380" {
		t.Fatalf("cluster Addrs = %v, want [127.0.0.1:6379 127.0.0.1:6380]", got.Addrs)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewWithConnOptRedisFailoverClientOpt(t *testing.T) {
	opt := asynqlib.RedisFailoverClientOpt{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379", "127.0.0.1:26380"},
	}
	tr := NewWithConnOpt(opt)
	if tr == nil {
		t.Fatal("NewWithConnOpt() returned nil")
	}
	got, ok := tr.connOpt.(asynqlib.RedisFailoverClientOpt)
	if !ok {
		t.Fatalf("connOpt type = %T, want RedisFailoverClientOpt", tr.connOpt)
	}
	if got.MasterName != "mymaster" {
		t.Fatalf("MasterName = %q, want mymaster", got.MasterName)
	}
	if len(got.SentinelAddrs) != 2 {
		t.Fatalf("SentinelAddrs = %v, want 2 entries", got.SentinelAddrs)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewWithNilConnOptFallsBack(t *testing.T) {
	tr := NewWithConnOpt(nil)
	if tr == nil {
		t.Fatal("NewWithConnOpt(nil) returned nil")
	}
	if tr.client == nil {
		t.Fatal("transport client is nil")
	}
	if tr.connOpt == nil {
		t.Fatal("connOpt is nil after fallback")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTransportEnqueueCarriesTenant(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := New(server.Addr())
	defer func() { _ = transport.Close() }()

	ctx := tenant.WithTenant(context.Background(), "tenant-acme")
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "node-a",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	if err := transport.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	var gotTenant tenant.TenantID
	var gotTask *engine.Task
	var wg sync.WaitGroup
	wg.Add(1)

	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, func(ctx context.Context, t *engine.Task) error {
		gotTask = t
		gotTenant = tenant.FromContext(ctx)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}
	defer stop()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not receive task")
	}

	if gotTenant != "tenant-acme" {
		t.Fatalf("consumer tenant = %q, want tenant-acme", gotTenant)
	}
	if gotTask == nil || string(gotTask.ExecutionID) != "exec-1" {
		t.Fatalf("consumer task = %+v, want exec-1", gotTask)
	}
}

func TestTransportConsumerDefaultsToDefaultTenant(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := New(server.Addr())
	defer func() { _ = transport.Close() }()

	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-default"),
		NodeName:    "node-a",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	if err := transport.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	var gotTenant tenant.TenantID
	var received atomic.Bool

	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, func(ctx context.Context, t *engine.Task) error {
		gotTenant = tenant.FromContext(ctx)
		received.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}
	defer stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !received.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !received.Load() {
		t.Fatal("consumer did not receive task")
	}
	if gotTenant != tenant.DefaultTenant {
		t.Fatalf("tenant = %q, want default", gotTenant)
	}
}
