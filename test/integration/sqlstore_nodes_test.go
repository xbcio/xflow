//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/store/storetest"
	"github.com/xbcio/xflow/types"
)

// TestSQLStoreNodesTimingContract runs the shared node timing contract against
// a real MySQL store. The timestamped execution id keeps a rerun independent of
// rows a previous run left behind.
func TestSQLStoreNodesTimingContract(t *testing.T) {
	p := newSQLStoreProvider(t)
	storetest.NodesTimingContract(t, p,
		types.ExecutionID("sql-node-timing-"+time.Now().Format("150405.000000000")))
}
