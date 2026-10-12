package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMemoryExecutionSeedContract runs the shared ExecutionSeeder contract
// against the in-memory backend, so what a seed records and what it schedules
// cannot drift from the Redis backend's. Both are required implementations:
// rerun-from-node refuses a store without the capability, so a backend that
// skipped it would fail the feature rather than this suite.
func TestMemoryExecutionSeedContract(t *testing.T) {
	statestoretest.RunExecutionSeedContract(t, func(t *testing.T) engine.StateStore {
		return New().State()
	})
}
