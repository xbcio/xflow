package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/service/control"
)

func TestParseServerConfigSupportsMemoryMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":9090", "-memory", "-concurrency", "3"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.addr != ":9090" || !cfg.memory || cfg.concurrency != 3 {
		t.Fatalf("config = %+v, want addr :9090 memory concurrency 3", cfg)
	}
}

func TestParseServerConfigSupportsRedisMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":8080", "-redis", "localhost:6379"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redis != "localhost:6379" || cfg.memory {
		t.Fatalf("config = %+v, want redis localhost:6379", cfg)
	}
}

func TestParseServerConfigSupportsObservabilityFlags(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-memory",
		"-log-format", "json",
		"-metrics-addr", "127.0.0.1:0",
		"-metrics-path", "/custom-metrics",
		"-trace", "disabled",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.logFormat != "json" || cfg.metricsAddr != "127.0.0.1:0" || cfg.metricsPath != "/custom-metrics" || cfg.traceMode != "disabled" {
		t.Fatalf("config = %+v, want observability flags parsed", cfg)
	}
}

func TestParseServerConfigRejectsUnsupportedTraceMode(t *testing.T) {
	if _, err := parseServerConfig([]string{"-trace", "otlp"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for unsupported trace mode")
	}
}

func TestBuildLoggerRejectsUnknownFormat(t *testing.T) {
	if _, err := buildLogger(serverConfig{logFormat: "xml"}); err == nil {
		t.Fatal("buildLogger() error = nil, want error for unknown format")
	}
}

func TestBuildLoggerUsesZap(t *testing.T) {
	log, err := buildLogger(serverConfig{logFormat: "json"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := log.(obslogger.ZapLogger); !ok {
		t.Fatalf("buildLogger() = %T, want observability/logger.ZapLogger", log)
	}
}

func TestStartLeaseSweeperReclaimsExpiredLeases(t *testing.T) {
	state := &fakeLeaseLister{
		leases: []engine.ExpiredLease{{
			ExecutionID: "exec-1",
			NodeName:    "n",
			IssuedAt:    time.Now().Add(-time.Minute),
			TTL:         time.Second,
			LeaseToken:  "token-1",
		}},
	}
	reclaimer := &fakeLeaseReclaimer{}
	stop := startLeaseSweeper(state, reclaimer, control.LeaseSweeperConfig{Period: time.Millisecond})
	defer stop()

	deadline := time.After(time.Second)
	for {
		if reclaimer.count() > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("lease sweeper did not reclaim expired lease")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

type fakeLeaseLister struct {
	leases []engine.ExpiredLease
}

func (f *fakeLeaseLister) ListExpiredLeases(context.Context, time.Time) ([]engine.ExpiredLease, error) {
	return f.leases, nil
}

type fakeLeaseReclaimer struct {
	calls atomic.Int64
}

func (f *fakeLeaseReclaimer) ReclaimLease(context.Context, engine.ExpiredLease) (bool, error) {
	f.calls.Add(1)
	return true, nil
}

func (f *fakeLeaseReclaimer) count() int {
	return int(f.calls.Load())
}
