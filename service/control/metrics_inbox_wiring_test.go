package control

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/observability/metrics"
)

func TestControlPlaneWiresMetricsInboxWhenEnabled(t *testing.T) {
	cp := newTestControlPlaneForMetrics(t, true, false)
	if cp.MetricsInbox() == nil {
		t.Fatal("MetricsInbox() = nil with EnableMetricsProxy set")
	}
	// The endpoint must reach the SAME inbox the accessor exposes, or /metrics
	// merges an empty inbox while reports land somewhere unreachable — exactly
	// the failure mode the supply encryptor shipped with.
	if cp.httpServer.core.metricsInbox != cp.MetricsInbox() {
		t.Error("the report endpoint's inbox differs from the one exposed for scraping")
	}
	if cp.grpcServer.core.metricsInbox != nil {
		t.Error("gRPC core must not carry an inbox: the proxy is HTTP-only by design")
	}
}

func TestControlPlaneLeavesMetricsInboxNilWhenDisabled(t *testing.T) {
	cp := newTestControlPlaneForMetrics(t, false, false)
	if cp.MetricsInbox() != nil {
		t.Fatal("MetricsInbox() must stay nil unless the proxy is enabled")
	}
	if cp.httpServer.core.metricsInbox != nil {
		t.Error("core.metricsInbox must stay nil so the endpoint reports 503")
	}
}

func TestControlPlaneUsesRedisMetricsStoreWhenBackendHasRedis(t *testing.T) {
	// Guards the multi-replica requirement structurally: without this, a
	// regression to a process-local map would pass every other test in the plan
	// except the real-Redis integration test.
	cp := newTestControlPlaneForMetrics(t, true, true)
	inbox := cp.MetricsInbox()
	if inbox == nil {
		t.Fatal("MetricsInbox() = nil")
	}
	if _, ok := inbox.cfg.Store.(*RedisMetricsStore); !ok {
		t.Fatalf("inbox store = %T, want *RedisMetricsStore when the backend exposes Redis", inbox.cfg.Store)
	}
	if inbox.cfg.Live == nil {
		t.Error("inbox must carry a liveness source, or dead runners' series never expire")
	}
	if inbox.cfg.Self == nil {
		t.Error("inbox must carry the server's own gatherer, or conflicts 500 the endpoint")
	}
}

func TestMetricsInboxAcceptSurvivesBackendWithoutRedis(t *testing.T) {
	cp := newTestControlPlaneForMetrics(t, true, false)
	inbox := cp.MetricsInbox()
	if _, ok := inbox.cfg.Store.(*MemoryMetricsStore); !ok {
		t.Fatalf("inbox store = %T, want *MemoryMetricsStore without Redis", inbox.cfg.Store)
	}
	if err := inbox.Accept(context.Background(), "runner-a", []byte("payload")); err != nil {
		t.Errorf("Accept on the memory fallback: %v", err)
	}
}

// newTestControlPlaneForMetrics builds a real ControlPlane through
// NewControlPlane, so the assertions exercise production wiring rather than
// field assignment. withRedis selects the Redis-backed path: *distributed.Backend
// implements RedisClient() (backend.go:272), which is exactly the optional
// capability the inbox assembly probes; backendlocal does not, so it exercises
// the memory fallback.
func newTestControlPlaneForMetrics(t *testing.T, enable, withRedis bool) *ControlPlane {
	t.Helper()
	var cfg Config
	cfg.EnableMetricsProxy = enable
	cfg.Metrics = metrics.New()
	if withRedis {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(mr.Close)
		rb, err := distributed.New(mr.Addr(), nil)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Backend = rb
	} else {
		cfg.Backend = backendlocal.New(backendlocal.WithConcurrency(1))
	}
	cp, err := NewControlPlane(cfg)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	return cp
}
