package engine

import (
	"hash/fnv"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// retryBackoffCap caps a single delay between retries. Exponential growth
// stops climbing once it hits this ceiling.
const retryBackoffCap = 5 * time.Minute

// retryBackoff returns the delay to wait before the next retry attempt.
// Exponential with deterministic jitter derived from the (execution, node,
// attempt) tuple — we cannot use math/rand because workflow scripts run in a
// determinism-locked sandbox and we want callers to be able to reason about
// timing. Jitter is bounded to ±20%.
func retryBackoff(attempt int, settings *types.RetrySettings, execID types.ExecutionID, nodeName string) time.Duration {
	base := time.Second
	multiplier := 2.0
	if settings != nil {
		if settings.InitialInterval > 0 {
			base = time.Duration(settings.InitialInterval) * time.Millisecond
		}
		if settings.Multiplier > 0 {
			multiplier = settings.Multiplier
		}
	}
	if attempt < 0 {
		attempt = 0
	}
	// Compute base * multiplier^attempt as float64, then clamp.
	d := float64(base)
	for i := 0; i < attempt && i < 32; i++ {
		d *= multiplier
		if time.Duration(d) >= retryBackoffCap {
			d = float64(retryBackoffCap)
			break
		}
	}
	out := time.Duration(d)
	if settings != nil && settings.MaxInterval > 0 {
		maxD := time.Duration(settings.MaxInterval) * time.Millisecond
		if out > maxD {
			out = maxD
		}
	}
	if out > retryBackoffCap {
		out = retryBackoffCap
	}
	// ±20% deterministic jitter so concurrent retries don't all line up.
	jitter := jitterFor(execID, nodeName, attempt, out)
	return out + jitter
}

// jitterFor returns a ±20% deterministic jitter derived from a stable hash of
// (execID, nodeName, attempt). No global RNG, no Math.random.
func jitterFor(execID types.ExecutionID, nodeName string, attempt int, d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(execID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(nodeName))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte{byte(attempt), byte(attempt >> 8), byte(attempt >> 16), byte(attempt >> 24)})
	span := int64(d / 5) // 20% on either side
	if span <= 0 {
		return 0
	}
	// Map the hash into [-span, +span).
	v := int64(h.Sum64() % uint64(2*span))
	return time.Duration(v - span)
}

// retryFor returns the effective RetrySettings for a node, or nil when retries
// are disabled. The compiler already resolved per-node vs workflow defaults
// into NodeMeta.Retry.
func retryFor(meta graph.NodeMeta) *types.RetrySettings {
	if meta.Retry != nil && meta.Retry.MaxAttempts > 0 {
		return meta.Retry
	}
	return nil
}
