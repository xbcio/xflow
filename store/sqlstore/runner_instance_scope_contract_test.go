package sqlstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storecontract"
)

func TestSQLRunnerPoolRepoSatisfiesScopedRunnerInstanceListerContract(t *testing.T) {
	storecontract.RunScopedRunnerInstanceListerContract(t, func(t *testing.T) storecontract.ScopedRunnerPoolStore {
		// newEnrollTestDB clears xflow_runner_pools and xflow_runner_instances.
		return &runnerPoolRepo{db: newEnrollTestDB(t)}
	})
}
