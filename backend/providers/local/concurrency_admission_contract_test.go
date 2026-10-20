package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMemoryConcurrencyAdmissionContract runs the shared
// ConcurrencyLimitedStateStore contract against the in-memory backend, so the
// slot semantics the engine relies on cannot drift from the Redis backend's.
func TestMemoryConcurrencyAdmissionContract(t *testing.T) {
	statestoretest.RunConcurrencyAdmissionContract(t, memoryCapabilityManifest(), func(t *testing.T) engine.StateStore {
		return New().State()
	})
}

// TestMemoryConcurrencyQueueContract runs the queue half of the same contract:
// parking, FIFO promotion, cancellation and the queue timeout. It is a separate
// suite because a store can enforce a limit without being able to queue, and
// this backend can.
func TestMemoryConcurrencyQueueContract(t *testing.T) {
	statestoretest.RunConcurrencyQueueContract(t, memoryCapabilityManifest(), func(t *testing.T) engine.StateStore {
		return New().State()
	})
}

// TestMemoryConcurrencySeedContract runs the entry-seed half of the same
// contract: a trigger-group seed contends for a slot, and queues, exactly as a
// submission does.
func TestMemoryConcurrencySeedContract(t *testing.T) {
	statestoretest.RunConcurrencySeedContract(t, memoryCapabilityManifest(), func(t *testing.T) engine.StateStore {
		return New().State()
	})
}
