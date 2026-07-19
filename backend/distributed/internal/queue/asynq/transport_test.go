package asynq

import (
	"testing"

	asynqlib "github.com/hibiken/asynq"
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
