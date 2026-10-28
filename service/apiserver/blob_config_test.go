package apiserver

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/store/blob"
)

// Compile-time contract: the metrics adapter blobObserver installs satisfies the
// observer distributed.BlobConfig accepts.
var _ distributed.BlobObserver = metrics.BlobMetrics{}

// blobTestConfig is a structurally complete blob selection. The store and index
// are in-memory so nothing is contacted.
func blobTestConfig() *distributed.BlobConfig {
	return &distributed.BlobConfig{
		Store: blob.NewMemory(),
		Index: blob.NewMemoryIndex(nil),
		Grace: time.Hour,
		Batch: 10,
	}
}

// TestBlobRequiresRedis pins the loud failure: the in-memory backend has no
// durable mode for the blob layer to extend, so a blob selection without Redis
// must stop startup instead of being ignored. The error names the field, never
// a configured value.
func TestBlobRequiresRedis(t *testing.T) {
	bc := blobTestConfig()
	bc.Grace = 1234567 * time.Nanosecond
	_, err := New(Config{Blob: bc})
	if !errors.Is(err, errBlobNoRedis) {
		t.Fatalf("New with Blob and no Redis: err = %v, want errBlobNoRedis", err)
	}
	if strings.Contains(err.Error(), "1234567") || strings.Contains(err.Error(), "1.234567ms") {
		t.Fatalf("err = %q leaks a configured value", err)
	}
}

// TestBlobReachesBackendWithRedis proves the setting reaches distributed.New
// once Redis is configured: with no durable store the backend refuses a blob
// selection, and that refusal is returned rather than swallowed.
func TestBlobReachesBackendWithRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	_, err := New(Config{RedisAddr: mr.Addr(), Blob: blobTestConfig()})
	if err == nil {
		t.Fatal("New accepted a blob selection with no durable store")
	}
	if errors.Is(err, errBlobNoRedis) {
		t.Fatalf("err = %v: Redis is configured, so the failure must come from the backend", err)
	}
	if !strings.Contains(err.Error(), "WithBlobStore") {
		t.Fatalf("err = %q, want it to come from WithBlobStore validation", err)
	}
}

// TestBlobObserverOnlyWhenMetricsAndNoObserver pins when the metrics adapter is
// installed: only for a blob selection with metrics on and no observer of the
// caller's own. Mutation: dropping the Metrics nil check panics the no-metrics
// row; dropping the Observer check replaces the caller's observer.
func TestBlobObserverOnlyWhenMetricsAndNoObserver(t *testing.T) {
	own := metrics.NewBlobMetrics(metrics.New())
	cases := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"no blob with metrics", Config{Metrics: metrics.New()}, false},
		{"blob without metrics", Config{Blob: blobTestConfig()}, false},
		{"blob with metrics", Config{Blob: blobTestConfig(), Metrics: metrics.New()}, true},
		{"blob with own observer", Config{Blob: func() *distributed.BlobConfig {
			bc := blobTestConfig()
			bc.Observer = own
			return bc
		}(), Metrics: metrics.New()}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if obs := blobObserver(tc.cfg); (obs != nil) != tc.want {
				t.Fatalf("blobObserver() = %v, want non-nil=%v", obs, tc.want)
			}
		})
	}
}

// TestBlobNilChangesNothing is the negative twin: with the field nil a
// Redis-backed server given a Metrics registry builds as before and registers no
// blob series.
func TestBlobNilChangesNothing(t *testing.T) {
	mr := miniredis.RunT(t)
	m := metrics.New()
	if obs := blobObserver(Config{Metrics: m}); obs != nil {
		t.Fatalf("blobObserver() = %T for a nil Blob, want nil", obs)
	}
	if _, err := New(Config{RedisAddr: mr.Addr(), Metrics: m}); err != nil {
		t.Fatalf("New without Blob: %v", err)
	}
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if strings.Contains(f.GetName(), "blob") {
			t.Errorf("default deployment registered blob metric %q", f.GetName())
		}
	}
}

// TestBlobConfigNotMutated: the observer is filled into a copy, so the struct
// the caller holds keeps its nil Observer even though the build fails later.
func TestBlobConfigNotMutated(t *testing.T) {
	mr := miniredis.RunT(t)
	bc := blobTestConfig()
	_, _ = New(Config{RedisAddr: mr.Addr(), Blob: bc, Metrics: metrics.New()})
	if bc.Observer != nil {
		t.Fatalf("caller's BlobConfig.Observer = %T, want it left nil", bc.Observer)
	}
}
