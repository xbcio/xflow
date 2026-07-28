package runner

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
)

// LeaseRenewer is the interface the renewal goroutine uses to extend a lease.
// In production this calls the control plane's RenewLease endpoint.
type LeaseRenewer interface {
	RenewLease(ctx context.Context, leaseID string, leaseToken string, extend time.Duration) (renewed bool, err error)
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

// renewGroupLease runs a background renewal loop. It renews the lease at the
// configured interval and cancels the provided cancel func when:
//   - The server responds with Renewed=false (lease lost / fenced)
//   - MaxRetries consecutive transport errors
//
// The goroutine exits when ctx is done.
func renewGroupLease(ctx context.Context, renewer LeaseRenewer, lease *engine.TaskLease, ttl time.Duration, cfg RenewalConfig, cancel context.CancelFunc) {
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
