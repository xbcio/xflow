package control

import (
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/store/memstore"
)

func TestControlPlaneUsesRedisSupplyObservedWhenBackendHasRedis(t *testing.T) {
	// Guards the multi-replica requirement structurally: without this, a
	// regression to a process-local map would pass every other test except the
	// real-Redis integration test.
	cp := newTestControlPlaneForSupplyObserved(t, true, nil)
	sink := cp.SupplyObserved()
	if _, ok := sink.(*RedisSupplyObserved); !ok {
		t.Fatalf("SupplyObserved() = %T, want *RedisSupplyObserved when the backend exposes Redis", sink)
	}
	// The heartbeat path must write into the same sink the accessor exposes, or
	// reports land somewhere no reader can see — exactly the failure mode the
	// supply encryptor shipped with.
	if cp.httpServer.core.supplyObserved != sink || cp.grpcServer.core.supplyObserved != sink {
		t.Error("a Core's sink differs from the one exposed by SupplyObserved()")
	}
}

func TestControlPlaneUsesMemorySupplyObservedWithoutRedis(t *testing.T) {
	cp := newTestControlPlaneForSupplyObserved(t, false, nil)
	sink := cp.SupplyObserved()
	if _, ok := sink.(*MemorySupplyObserved); !ok {
		t.Fatalf("SupplyObserved() = %T, want *MemorySupplyObserved without Redis", sink)
	}
}

func TestControlPlaneSupplyObservedOverrideWins(t *testing.T) {
	// An embedder whose Redis abstraction the backend does not expose can still
	// inject its own sink; the override must beat the backend-capability probe.
	custom := NewMemorySupplyObserved()
	cp := newTestControlPlaneForSupplyObserved(t, true, custom)
	if cp.SupplyObserved() != SupplyObservedSink(custom) {
		t.Fatalf("SupplyObserved() = %T, want the Config-supplied sink", cp.SupplyObserved())
	}
	if cp.httpServer.core.supplyObserved != SupplyObservedSink(custom) {
		t.Error("the HTTP core must carry the override, not a Redis sink")
	}
}

// newTestControlPlaneForSupplyObserved mirrors newTestControlPlaneForMetrics: a
// real ControlPlane through NewControlPlane, so the assertions exercise
// production wiring rather than field assignment. Supplies and an
// EntryActivationStore are both set because that pair is what switches the
// supply hint/observed wiring on at all; override is handed to
// Config.SupplyObservedSink (nil = no override). withRedis selects the
// Redis-backed path: *distributed.Backend implements RedisClient(), which is
// exactly the optional capability the sink selection probes; backendlocal does
// not, so it exercises the memory fallback.
func newTestControlPlaneForSupplyObserved(t *testing.T, withRedis bool, override SupplyObservedSink) *ControlPlane {
	t.Helper()
	var cfg Config
	cfg.Supplies = memstore.New()
	cfg.EntryActivationStore = NewMemoryEntryActivationStore()
	cfg.SupplyObservedSink = override
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
