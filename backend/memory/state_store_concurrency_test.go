//go:build concurrency

// Spec: .claude/docs/specs/lua-concurrency-tests.md
package memory

import (
	"testing"

	"github.com/xbcio/xflow/backend/contract"
	"github.com/xbcio/xflow/engine"
)

func TestMemoryStateStore_Concurrency(t *testing.T) {
	contract.RunStateStoreConcurrencySuite(t, func(t *testing.T) engine.StateStore {
		return New().State()
	})
}
