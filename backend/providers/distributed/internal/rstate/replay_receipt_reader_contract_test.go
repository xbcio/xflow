package rstate

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/namespace"
)

// TestRedisReplayReceiptReaderContract runs the shared receipt-reader contract
// against the Redis/Lua backend on miniredis: every authoritative receipt the
// replay script wrote — including a decidable rejection's — must scan back
// once, in namespace-then-execution (sorted-key) order, with the execution's
// namespace.
func TestRedisReplayReceiptReaderContract(t *testing.T) {
	statestoretest.RunReplayReceiptReaderContract(t, func(t *testing.T) statestoretest.ReplayReceiptReaderBackend {
		state, _, _ := newTestRedisState(t)
		return statestoretest.ReplayReceiptReaderBackend{
			Store:         state,
			WantNamespace: string(namespace.Default),
		}
	})
}
