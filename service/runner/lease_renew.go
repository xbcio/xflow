package runner

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// LeaseRenewer is the interface the renewal goroutine uses to extend a lease.
// In production this calls the control plane's RenewLease endpoint.
type LeaseRenewer interface {
	RenewLease(ctx context.Context, leaseID string, leaseToken string, extend time.Duration) (renewed bool, err error)
}

// leaseRenewClient is the optional protocol capability for extending a lease.
// The HTTP client implements it; the gRPC client does not, so a gRPC runner
// never renews and its long handlers stay subject to the raw TTL — the same
// explicit gap as MetricsReportClient and activationAckClient.
type leaseRenewClient interface {
	RenewLease(ctx context.Context, req protocol.RenewLeaseRequest) (protocol.RenewLeaseResponse, error)
}

// protocolLeaseRenewer adapts the wire client to LeaseRenewer by supplying the
// identity the endpoint fences on. The server re-authenticates every call and
// resolves the lease from its own directory, so runner and session travel with
// each request rather than being remembered by a connection.
type protocolLeaseRenewer struct {
	client    leaseRenewClient
	runnerID  string
	sessionID string
}

func (p protocolLeaseRenewer) RenewLease(ctx context.Context, leaseID, leaseToken string, extend time.Duration) (bool, error) {
	resp, err := p.client.RenewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   p.runnerID,
		SessionID:  p.sessionID,
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Extend:     extend.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	// resp.Error is the server's reason for a refusal. It is deliberately not
	// returned or logged here: the caller only needs renewed=false to stop
	// working, and this string travels beside a lease token.
	return resp.Renewed, nil
}

// renewalExtendFor picks the extension each renewal asks for. The lease's own
// TTL is the right unit — it is what the sweeper judges against — and a lease
// that arrived without one falls back to the engine's default rather than
// requesting an unbounded extension. Buying back exactly one TTL (not more)
// means an abandoned runner still loses the node within one TTL of going
// silent.
func renewalExtendFor(lease *engine.TaskLease) time.Duration {
	if lease != nil && lease.TTL > 0 {
		return lease.TTL
	}
	return engine.DefaultLeaseTTL
}

// RenewalConfig controls the lease renewal background loop.
type RenewalConfig struct {
	Interval   time.Duration // renewal interval; default min(TTL/3, 10s)
	MaxRetries int           // max consecutive transport failures before cancel; default 3
}

// defaultRenewalInterval computes the renewal interval from the lease TTL.
func defaultRenewalInterval(ttl time.Duration) time.Duration {
	interval := ttl / 3
	if interval > 10*time.Second {
		interval = 10 * time.Second
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return interval
}

// renewLeaseLoop runs a background renewal loop. It renews the lease at the
// configured interval and cancels the provided cancel func when:
//   - The server responds with Renewed=false (lease lost / fenced)
//   - MaxRetries consecutive transport errors
//
// The goroutine exits when ctx is done.
//
// Cancelling on a refusal is the point, not a side effect: the server has told
// this runner another attempt owns the node now, so continuing to execute would
// produce a second, unfenced result for work someone else is already doing.
func renewLeaseLoop(ctx context.Context, renewer LeaseRenewer, lease *engine.TaskLease, ttl time.Duration, cfg RenewalConfig, cancel context.CancelFunc) {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRenewalInterval(ttl)
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	consecutiveErrors := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := renewer.RenewLease(ctx, string(lease.LeaseID), string(lease.LeaseToken), ttl)
			if err != nil {
				consecutiveErrors++
				if consecutiveErrors >= cfg.MaxRetries {
					cancel()
					return
				}
				continue
			}
			consecutiveErrors = 0
			if !renewed {
				// Lease was fenced — another attempt owns it now.
				cancel()
				return
			}
		}
	}
}
