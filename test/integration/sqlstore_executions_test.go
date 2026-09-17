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
