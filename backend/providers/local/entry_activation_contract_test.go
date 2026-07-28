package local_test

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
)

// TestEntryActivationContract_Memory runs the shared EntryActivationStore
// contract against control.MemoryEntryActivationStore. It lives in the
// backend/providers/local test package (an external _test package) because the
// shared contract lives in backend/internal/statestoretest, which is only
// importable from within backend/. service/control does not import
// providers/local, so importing control here introduces no cycle.
func TestEntryActivationContract_Memory(t *testing.T) {
	statestoretest.RunEntryActivationContract(t, func(t *testing.T) engine.EntryActivationStore {
		return control.NewMemoryEntryActivationStore()
	})
}
