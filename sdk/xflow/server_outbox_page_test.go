package xflow

import "testing"

// TestWithServerOutboxDiscoveryPageReachesTheAPIConfig pins that the host can
// actually raise the outbox dispatcher's discovery page.
//
// On the Redis-backed server the page is the dispatcher's discovery ceiling:
// discovery walks the keyspace with SCAN, whose COUNT counts keys EXAMINED, so
// one drain reaches roughly page-over-total-keys of the ready backlog. A
// keyspace that has outgrown the default discovers ready work more slowly than
// ingress creates it, and the outbox backlog then grows without bound even
// though delivery is healthy — the measured defect this option exists to fix.
// An option that stops halfway to apiserver.Config is indistinguishable from no
// option at all.
func TestWithServerOutboxDiscoveryPageReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerOutboxDiscoveryPage(8192)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.OutboxDiscoveryPage != 8192 {
		t.Fatalf("OutboxDiscoveryPage = %d, want 8192 -- the configured page never "+
			"reaches the backend, so a deployment with a keyspace beyond the default "+
			"cannot raise its own discovery ceiling", cfg.OutboxDiscoveryPage)
	}
}

// Zero must keep meaning "engine default", and must not mean "discover nothing"
// or "take the smallest possible page": both would silently floor the
// dispatcher for a host that set the option unconditionally.
func TestServerOutboxDiscoveryPageZeroIsNotAPageSize(t *testing.T) {
	sc := &serverConfig{}
	WithServerOutboxDiscoveryPage(0)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.OutboxDiscoveryPage != 0 {
		t.Fatalf("OutboxDiscoveryPage = %d, want 0 (the engine default)",
			cfg.OutboxDiscoveryPage)
	}
}
