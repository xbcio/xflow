package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMemoryExecutionPauseContract runs the shared ExecutionPauser contract
// against the in-memory backend, so the dispatch hold a pause installs cannot
// drift from the Redis backend's. Unlike the concurrency suites this one has no
// optional half: pause is a capability the engine offers, and a backend that
// implemented only the status write would pass a test of its own and fail here.
func TestMemoryExecutionPauseContract(t *testing.T) {
	statestoretest.RunExecutionPauseContract(t, func(t *testing.T) engine.StateStore {
		return New().State()
	})
}
