package memstore_test

import (
	"testing"

	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/storetest"
)

func TestMemstoreSupplyContract(t *testing.T) {
	storetest.SupplyContract(t, memstore.New(), "mem")
}
