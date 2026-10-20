package sqlstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storetest"
)

func TestSQLAuditRepoSatisfiesAuditContract(t *testing.T) {
	db := newEnrollTestDB(t)
	// The audit contract counts unreconciled rows in its own namespaces, but an
	// exact count over an owned scope is only meaningful on a known table state;
	// clear it so a rerun cannot inherit pending admissions.
	if err := db.Exec("DELETE FROM xflow_audit_events").Error; err != nil {
		t.Fatalf("clear xflow_audit_events: %v", err)
	}
	storetest.AuditContract(t, &auditRepo{db: db}, "audit-contract-sql")
}
