package xflow

import (
	"testing"
	"time"
)

// TestWithServerQueueBackpressureReachesTheAPIConfig pins both SDK-side hops of
// the producer-backpressure option: the option into serverConfig, and
// serverConfig into apiserver.Config through buildServerAPIConfig. An option
// that stops at either boundary is indistinguishable from no option at all —
// the server would keep flushing into an unbounded broker queue while the host
// believes it configured a bound.
func TestWithServerQueueBackpressureReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerQueueBackpressure(4096, 250*time.Millisecond)(sc)

	if sc.queueMaxDepth != 4096 {
		t.Fatalf("queueMaxDepth = %d, want 4096", sc.queueMaxDepth)
	}
	if sc.queueDepthInterval != 250*time.Millisecond {
		t.Fatalf("queueDepthInterval = %v, want 250ms", sc.queueDepthInterval)
	}

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.QueueMaxDepth != 4096 {
		t.Fatalf("apiserver QueueMaxDepth = %d, want 4096", cfg.QueueMaxDepth)
	}
	if cfg.QueueDepthSampleInterval != 250*time.Millisecond {
		t.Fatalf("apiserver QueueDepthSampleInterval = %v, want 250ms", cfg.QueueDepthSampleInterval)
	}
}

// TestServerQueueBackpressureZeroDisables pins the default: zero must mean
// "no bound" (the transport starts no sampler), not "a bound of zero", which
// would refuse every enqueue.
func TestServerQueueBackpressureZeroDisables(t *testing.T) {
	sc := &serverConfig{}
	WithServerQueueBackpressure(0, 0)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.QueueMaxDepth != 0 || cfg.QueueDepthSampleInterval != 0 {
		t.Fatalf("apiserver backpressure config = (%d, %v), want (0, 0) disabled",
			cfg.QueueMaxDepth, cfg.QueueDepthSampleInterval)
	}
}
