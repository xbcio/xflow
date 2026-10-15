package sqlstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storecontract"
)

func TestSQLAuditRepoSatisfiesScopedAuditReconcilerContract(t *testing.T) {
	storecontract.RunScopedAuditReconcilerContract(t, func(t *testing.T) storecontract.ScopedAuditStore {
		db := newEnrollTestDB(t)
		// newEnrollTestDB only clears the enroll tables; the contract needs an
		// empty audit table or earlier rows would join the fixture.
		if err := db.Exec("DELETE FROM xflow_audit_events").Error; err != nil {
			t.Fatalf("clear xflow_audit_events: %v", err)
		}
		return &auditRepo{db: db}
	})
}
