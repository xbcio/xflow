package memstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storecontract"
)

func TestMemstoreSatisfiesScopedAuditReconcilerContract(t *testing.T) {
	storecontract.RunScopedAuditReconcilerContract(t, func(t *testing.T) storecontract.ScopedAuditStore {
		return New()
	})
}
