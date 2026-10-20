package memstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storetest"
)

func TestMemstoreSignalsContract(t *testing.T) {
	storetest.SignalsContract(t, New())
}
