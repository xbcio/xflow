package memstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storetest"
)

func TestMemstoreAuditContract(t *testing.T) {
	storetest.AuditContract(t, New(), "audit-contract-mem")
}
