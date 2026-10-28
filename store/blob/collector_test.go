package blob_test

import (
	"testing"

	"github.com/xbcio/xflow/store/blob/blobtest"
)

// TestCollectorScenariosOnMemoryIndex runs the collector scenarios, including
// the deterministic fence races, on the in-memory Index over a real ObjectBacked
// and FSStore.
func TestCollectorScenariosOnMemoryIndex(t *testing.T) {
	blobtest.RunGCScenarios(t, memoryIndexFactory)
}

// TestCollectorStressOnMemoryIndex is the randomized concurrency run. It is
// only meaningful under -race, but the Ref-readable assertion holds without it.
func TestCollectorStressOnMemoryIndex(t *testing.T) {
	blobtest.RunGCStress(t, memoryIndexFactory, 40)
}
