package memory

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// TestMemoryStateStoreContract runs the shared StateStore contract suite
// against the in-memory backend, keeping it semantically aligned with the
// Redis/Lua backend (terminal protection, lease claim fencing, signal consume,
// resume lock, pub/sub).
func TestMemoryStateStoreContract(t *testing.T) {
	state := New().State()
	statestoretest.RunStateStoreContract(t, state)
}
