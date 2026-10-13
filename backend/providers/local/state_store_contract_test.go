package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/types"
)

// TestMemoryStateStoreContract runs the shared StateStore contract suite
// against the in-memory backend, keeping it semantically aligned with the
// Redis/Lua backend (terminal protection, lease claim fencing, signal consume,
// resume lock, pub/sub).
func TestMemoryStateStoreContract(t *testing.T) {
	state := New().State()
	statestoretest.RunStateStoreContract(t, state)
}

func TestMemoryNodeLeaseRenewContract(t *testing.T) {
	statestoretest.RunNodeLeaseRenewContract(t, func(t *testing.T) statestoretest.NodeLeaseRenewStore {
		return New().State().(statestoretest.NodeLeaseRenewStore)
	})
}

func TestMemoryDeadLetterReplayContract(t *testing.T) {
	statestoretest.RunDeadLetterReplayContract(t, func(t *testing.T) statestoretest.DeadLetterReplayBackend {
		state := newMemoryState()
		return statestoretest.DeadLetterReplayBackend{
			Store: state,
			ExpireExecution: func(t *testing.T, id types.ExecutionID) {
				state.mu.Lock()
				delete(state.executions, id)
				state.mu.Unlock()
			},
		}
	})
}
