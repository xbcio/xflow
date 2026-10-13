package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// TestMemoryReplayReceiptReaderContract runs the shared receipt-reader contract
// against the in-memory backend: every receipt it stored must scan back once,
// with the empty namespace its absent namespace dimension produces, in
// execution-then-request order.
func TestMemoryReplayReceiptReaderContract(t *testing.T) {
	statestoretest.RunReplayReceiptReaderContract(t, func(t *testing.T) statestoretest.ReplayReceiptReaderBackend {
		return statestoretest.ReplayReceiptReaderBackend{
			Store:         New().State().(statestoretest.ReplayReceiptReaderStore),
			WantNamespace: "", // local has no namespace dimension; see the reader's doc comment
		}
	})
}
