package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// TestMemoryStateStoreContract runs the shared StateStore contract suite
// against the in-memory backend, keeping it semantically aligned with the
// Redis/Lua backend (terminal protection, lease claim fencing, signal consume,
// resume lock, pub/sub).
func TestMemoryStateStoreContract(t *testing.T) {
	state := New().State()
	statestoretest.RunStateStoreContract(t, memoryCapabilityManifest(), state)
}

// TestMemoryStateStoreContractWithStore is the same contract with a durable
// store attached. The mirror must not change a single scheduling answer: the
// in-memory maps stay authoritative, and the store only observes committed
// transitions. Running the suite both ways is what proves that.
func TestMemoryStateStoreContractWithStore(t *testing.T) {
	state := New(WithStore(memstore.New())).State()
	statestoretest.RunStateStoreContract(t, memoryCapabilityManifest(), state)
}

// TestMemoryArrivalFoldingContract runs the shared arrival-folding contract
// against the in-memory backend. Folding is a semantic capability no interface
// assertion can prove, so this suite — not the manifest — is what shows the two
// backends apply CommitNodeRequest.Arrivals the same way.
func TestMemoryArrivalFoldingContract(t *testing.T) {
	statestoretest.RunArrivalFoldingContract(t, memoryCapabilityManifest(), New().State())
}

func TestMemoryNodeLeaseRenewContract(t *testing.T) {
	statestoretest.RunNodeLeaseRenewContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.NodeLeaseRenewStore {
		return New().State().(statestoretest.NodeLeaseRenewStore)
	})
}

// TestMemoryRouteCheckedLeaseContract runs the shared route-checked acquire
// contract against the in-memory backend, so its verdicts stay identical to the
// Redis/Lua script's.
func TestMemoryRouteCheckedLeaseContract(t *testing.T) {
	statestoretest.RunRouteCheckedLeaseContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.RouteCheckedLeaseStore {
		return New().State().(statestoretest.RouteCheckedLeaseStore)
	})
}

// TestMemoryBatchOutputContract runs the shared batch-output contract against
// the in-memory backend. The backend has no round trips to save, but the
// engine's fan-in and $nodes paths pick the batch form by capability, so the
// reference implementation must agree with Redis name by name.
func TestMemoryBatchOutputContract(t *testing.T) {
	statestoretest.RunBatchOutputContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.BatchOutputBackend {
		return statestoretest.BatchOutputBackend{Store: New().State(), Namespaced: false}
	})
}

// TestMemoryExecutionInputContextContract runs the shared narrow-input contract
// against the in-memory backend. The narrow read saves a snapshot copy rather
// than round trips here, but buildInput chooses it by capability, so it must
// answer exactly as GetExecution does.
func TestMemoryExecutionInputContextContract(t *testing.T) {
	statestoretest.RunExecutionInputContextContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.ExecutionInputBackend {
		return statestoretest.ExecutionInputBackend{Store: New().State(), Namespaced: false}
	})
}

// TestMemoryExecutionTraceCarrierContract runs the shared carrier contract
// against the in-memory backend. The backend has no namespace dimension, so the
// shared isolation subtest is skipped explicitly rather than silently.
func TestMemoryExecutionTraceCarrierContract(t *testing.T) {
	statestoretest.RunExecutionTraceCarrierContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.ExecutionInputBackend {
		return statestoretest.ExecutionInputBackend{Store: New().State(), Namespaced: false}
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
