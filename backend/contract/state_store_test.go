package contract

import (
	"testing"

	"github.com/xbcio/xflow/backend/memory"
)

func TestMemoryStateStoreContract(t *testing.T) {
	state := memory.New().State()
	RunStateStoreContract(t, state)
}
