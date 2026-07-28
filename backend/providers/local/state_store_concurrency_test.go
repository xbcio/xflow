//go:build concurrency

// Spec: .claude/specs/lua-concurrency-tests.md
package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

func TestMemoryStateStore_Concurrency(t *testing.T) {
	statestoretest.RunStateStoreConcurrencySuite(t, func(t *testing.T) engine.StateStore {
		return New().State()
	})
}
