package sqlstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storetest"
)

func TestSQLSignalRepoSatisfiesSignalsContract(t *testing.T) {
	db := newEnrollTestDB(t)
	// The signal contract reuses fixed execution ids across subtests and asserts
	// counts, so rows from an earlier run must not survive.
	if err := db.Exec("DELETE FROM xflow_signals").Error; err != nil {
		t.Fatalf("clear xflow_signals: %v", err)
	}
	storetest.SignalsContract(t, &signalRepo{db: db})
}
