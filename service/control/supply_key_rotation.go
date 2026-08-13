package control

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultSupplyKeyRotationPeriod is how often the supply transport key rotates
// when Config.SupplyKeyRotationPeriod is left unset.
//
// The transport key protects supply content in flight between the control
// plane and runners, over a channel that is already TLS-protected in any real
// deployment. Rotation here limits the blast radius of a leaked key, not a
// live eavesdropper — so a daily cadence is the right order of magnitude, and
// rotating far more aggressively would only add heartbeat churn.
const DefaultSupplyKeyRotationPeriod = 24 * time.Hour

// minSupplyKeyRotationPeriod floors a misconfigured period. Below this, key
// churn outruns the heartbeat interval and runners spend their time adopting
// keys instead of converging on one.
const minSupplyKeyRotationPeriod = time.Minute

// supplyKeyRotationSlotKey is the fleet-wide rotation lease. Whoever creates it
// owns the next rotation; its TTL is the rotation period, so it reappears as
// claimable exactly one period later.
const supplyKeyRotationSlotKey = "xflow:supply:transport-key:rotation-slot"

// supplyKeyRefreshPeriod is how often a replica re-reads the shared key so it
// notices a peer's rotation. It is deliberately much shorter than the rotation
// period: the window between "a peer rotated" and "this replica noticed" is a
// window in which this replica encrypts with a superseded key, and a runner
// that already adopted the new one cannot decrypt that content.
const supplyKeyRefreshPeriod = 30 * time.Second

// claimRotationSlot attempts to take the fleet-wide rotation slot for one
// period. It returns true only for the replica that took it.
//
// SET NX with a TTL rather than leader-gating: leadership answers "who acts",
// but this needs "act once per period across the fleet, regardless of leader
// churn or restarts". A lease whose TTL is the period gives that directly — a
// leader change mid-period cannot cause a second rotation, and no replica has
// to persist a "last rotated at" timestamp.
//
// The process-local encryptor has no peers and no Redis, so it always owns the
// slot.
func (e *SupplyEncryptor) claimRotationSlot(ctx context.Context, period time.Duration) (bool, error) {
	if e.rdb == nil {
		return true, nil
	}
	ok, err := e.rdb.SetNX(ctx, supplyKeyRotationSlotKey, "1", period).Result()
	if err != nil {
		return false, fmt.Errorf("supply key rotation slot: %w", err)
	}
	return ok, nil
}

// clampSupplyKeyRotationPeriod resolves the configured period into the one
// actually used. Zero adopts the default; negative disables rotation entirely
// (reported as 0); anything positive is floored.
func clampSupplyKeyRotationPeriod(d time.Duration) time.Duration {
	switch {
	case d < 0:
		return 0
	case d == 0:
		return DefaultSupplyKeyRotationPeriod
	case d < minSupplyKeyRotationPeriod:
		return minSupplyKeyRotationPeriod
	default:
		return d
	}
}

// runSupplyKeyRotation rotates the supply transport key on a schedule and keeps
// this replica's copy current when a peer rotates instead.
//
// Both halves matter. Without rotation the key lives as long as the deployment;
// without the refresh, a peer's rotation leaves this replica encrypting with a
// key that runners have already replaced — the supply gate then declines and
// the runner hosts no triggers while heartbeating healthily.
func (cp *ControlPlane) runSupplyKeyRotation(ctx context.Context, period time.Duration) {
	enc := cp.supplyEncryptor

	// Seed the slot so the first rotation lands one full period from startup
	// rather than immediately. A fleet whose replicas all restart inside a
	// single period can postpone one rotation this way; that is preferable to
	// every rolling deploy forcing a key change.
	if _, err := enc.claimRotationSlot(ctx, period); err != nil && ctx.Err() == nil && cp.logger != nil {
		cp.logger.Warn("supply key rotation slot unavailable at startup", "err", err)
	}

	ticker := time.NewTicker(supplyKeyRefreshPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cp.supplyKeyRotationPass(ctx, period)
		}
	}
}

// supplyKeyRotationPass is one tick: take the rotation slot and rotate if it is
// ours, otherwise adopt whatever the fleet currently agrees on.
func (cp *ControlPlane) supplyKeyRotationPass(ctx context.Context, period time.Duration) {
	enc := cp.supplyEncryptor
	claimed, err := enc.claimRotationSlot(ctx, period)
	if err != nil {
		if ctx.Err() == nil && cp.logger != nil {
			cp.logger.Warn("supply key rotation slot check failed", "err", err)
		}
		return
	}
	if claimed {
		if err := enc.Rotate(ctx); err != nil {
			if ctx.Err() == nil && cp.logger != nil {
				// Never log the key itself; Rotate's errors carry only the
				// Redis failure.
				cp.logger.Error("supply key rotation failed", "err", err)
			}
			// The slot is spent either way. Releasing it on failure would let
			// every replica retry in a tight loop against a Redis that is
			// already failing; the next period retries naturally.
			return
		}
		if cp.logger != nil {
			cp.logger.Info("supply transport key rotated", "key_id", enc.CurrentKeyID())
		}
		return
	}
	changed, err := enc.Refresh(ctx)
	if err != nil {
		if ctx.Err() == nil && !isRedisCanceled(err) && cp.logger != nil {
			cp.logger.Warn("supply key refresh failed", "err", err)
		}
		return
	}
	if changed && cp.logger != nil {
		cp.logger.Info("adopted rotated supply transport key", "key_id", enc.CurrentKeyID())
	}
}

// isRedisCanceled reports whether err is redis' own shutdown-path error, which
// surfaces during Shutdown and is not worth a warning.
func isRedisCanceled(err error) bool {
	return err != nil && err == redis.ErrClosed
}
