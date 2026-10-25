//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/storetest"
)

// TestSQLStoreExecutionsContract runs the shared namespace-scoped execution
// listing contract against a real MySQL store. The timestamped prefix keeps a
// rerun independent of any rows a prior integration run left behind.
func TestSQLStoreExecutionsContract(t *testing.T) {
	p := newSQLStoreProvider(t)
	storetest.ExecutionsContract(t, p, fmt.Sprintf("sqlexec%d", time.Now().UnixNano()))
}

// TestSQLStoreExecutionStatsContract runs the shared statistics contract
// against a real MySQL store. This is the only place the MySQL bucket
// expression and the windowed percentile sample run against the dialect they
// are written for; SQLite exercises the same statements but not the SQL.
func TestSQLStoreExecutionStatsContract(t *testing.T) {
	p := newSQLStoreProvider(t)
	storetest.ExecutionStatsContract(t, p, fmt.Sprintf("sqlstats%d", time.Now().UnixNano()))
}
