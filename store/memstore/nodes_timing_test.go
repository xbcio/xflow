package memstore

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/store/storetest"
	"github.com/xbcio/xflow/types"
)

// TestNodesTimingContract runs the shared started_at/finished_at contract
// against the in-memory backend. The SQL half runs from test/integration.
func TestNodesTimingContract(t *testing.T) {
	storetest.NodesTimingContract(t, New(),
		types.ExecutionID("mem-node-timing-"+time.Now().Format("150405.000000000")))
}
