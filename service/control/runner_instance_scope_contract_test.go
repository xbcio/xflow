package control

import (
	"testing"

	"github.com/xbcio/xflow/store/storecontract"
)

func TestMemoryRunnerPoolStoreSatisfiesScopedRunnerInstanceListerContract(t *testing.T) {
	storecontract.RunScopedRunnerInstanceListerContract(t, func(t *testing.T) storecontract.ScopedRunnerPoolStore {
		return NewMemoryRunnerPoolStore()
	})
}
