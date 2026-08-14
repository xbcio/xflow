package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

func TestLocalGroupStateContract(t *testing.T) {
	statestoretest.RunGroupStateContract(t, func(t *testing.T) statestoretest.GroupStore {
		return New().State().(statestoretest.GroupStore)
	})
}

func TestLocalEntryAdmissionContract(t *testing.T) {
	statestoretest.RunEntryAdmissionContract(t, func(t *testing.T) statestoretest.EntryAdmissionTestStore {
		return New().State().(statestoretest.EntryAdmissionTestStore)
	})
}
