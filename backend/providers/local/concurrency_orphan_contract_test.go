package local

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// TestMemoryConcurrencyOrphanContract runs the shared orphan contract against
// the in-memory backend. The graph-loss hook nils the snapshot's graph under
// the state mutex, which is the exact torn shape the distributed backend
// produces by letting the graph key expire while the status key survives.
func TestMemoryConcurrencyOrphanContract(t *testing.T) {
	statestoretest.RunConcurrencyOrphanContract(t, memoryCapabilityManifest(), func(t *testing.T) statestoretest.ConcurrencyOrphanBackend {
		state := newMemoryState()
		mirror := memstore.New()
		state.db = mirror
		return statestoretest.ConcurrencyOrphanBackend{
			Store:  state,
			Mirror: mirror,
			LoseGraph: func(t *testing.T, id types.ExecutionID) {
				t.Helper()
				state.mu.Lock()
				defer state.mu.Unlock()
				if entry := state.executions[id]; entry != nil {
					entry.snap.Graph = nil
				}
			},
			AgeStatus: func(t *testing.T, id types.ExecutionID, age time.Duration) {
				t.Helper()
				state.mu.Lock()
				defer state.mu.Unlock()
				if entry := state.executions[id]; entry != nil {
					entry.snap.UpdatedAt = time.Now().UTC().Add(-age)
				}
			},
			RecordActivity: func(t *testing.T, id types.ExecutionID) {
				t.Helper()
				state.mu.Lock()
				defer state.mu.Unlock()
				state.activityAt[id] = time.Now().UTC()
			},
			AgeActivity: func(t *testing.T, id types.ExecutionID, age time.Duration) {
				t.Helper()
				state.mu.Lock()
				defer state.mu.Unlock()
				state.activityAt[id] = time.Now().UTC().Add(-age)
			},
			ForgetActivity: func(t *testing.T, id types.ExecutionID) {
				t.Helper()
				state.mu.Lock()
				defer state.mu.Unlock()
				delete(state.activityAt, id)
			},
			ForgetExecution: func(t *testing.T, id types.ExecutionID) {
				t.Helper()
				state.mu.Lock()
				defer state.mu.Unlock()
				delete(state.executions, id)
			},
		}
	})
}
