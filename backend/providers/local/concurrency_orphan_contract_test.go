package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
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
			TerminalStatusWithoutRelease: func(t *testing.T, id types.ExecutionID) {
				t.Helper()
				// The terminal transition lands in memory without its release
				// or projection, which is the crash residue the sweep repairs.
				state.mu.Lock()
				defer state.mu.Unlock()
				if entry := state.executions[id]; entry != nil {
					entry.snap.Status = types.ExecutionStatusCanceled
				}
			},
			ReconcileSlots: func(t *testing.T) {
				t.Helper()
				// The in-memory backend has no separate slot reconciler: the
				// orphan sweep performs the same terminal-member repair under
				// the state mutex.
				if _, err := state.ReclaimConcurrencyOrphans(context.Background(), engine.OrphanReclaimRequest{
					Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 64, Grace: time.Minute,
				}); err != nil {
					t.Fatalf("ReclaimConcurrencyOrphans() error = %v", err)
				}
			},
		}
	})
}
