//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/storetest"
)

// TestSQLStoreScheduleOverrideContract runs the shared
// store.ScheduleOverrideStore contract against a real MySQL. The namespace
// prefix is timestamped so a rerun never collides with rows left by a previous
// run — the contract asserts exact revisions and exact cascade counts.
//
// It also requires db/xflow_schema.sql to have been applied: the table is part
// of the tracked schema, not created by the test, so a missing
// xflow_schedule_overrides fails here with a "table doesn't exist" rather than
// being papered over by an auto-migrate.
func TestSQLStoreScheduleOverrideContract(t *testing.T) {
	p := newSQLStoreProvider(t)
	storetest.ScheduleOverrideContract(t, p, fmt.Sprintf("sqlsched-%d", time.Now().UnixNano()))
}
