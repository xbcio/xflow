//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/storetest"
)

// TestSQLStoreSupplyContract runs the shared store.Supplies contract against a
// real MySQL. The namespace prefix is timestamped so a rerun never collides
// with rows left by a previous run (the contract asserts exact revisions).
func TestSQLStoreSupplyContract(t *testing.T) {
	p := newSQLStoreProvider(t)
	storetest.SupplyContract(t, p, fmt.Sprintf("sql-%d", time.Now().UnixNano()))
}
