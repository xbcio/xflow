package memstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storetest"
)

func TestMemstoreNodesContract(t *testing.T) {
	storetest.NodesContract(t, New())
}
