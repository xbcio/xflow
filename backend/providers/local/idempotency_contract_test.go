package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// TestMemoryIdempotencyContract runs the shared engine.IdempotentStateStore
// contract against the in-memory backend, so keyed submissions mean the same
// thing here as on Redis.
func TestMemoryIdempotencyContract(t *testing.T) {
	statestoretest.RunIdempotencyContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.IdempotencyHarness {
		return statestoretest.IdempotencyHarness{State: New().State()}
	})
}
