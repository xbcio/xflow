package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMemoryExecutionDeadlineContract runs the shared ExecutionDeadlineExpirer
// contract against the in-memory backend, so what it lists, withdraws and keeps
// until an acknowledgement cannot drift from the Redis backend's.
func TestMemoryExecutionDeadlineContract(t *testing.T) {
	statestoretest.RunExecutionDeadlineContract(t, memoryCapabilityManifest(), func(t *testing.T) engine.StateStore {
		return New().State()
	})
}
