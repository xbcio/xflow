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

func TestLocalTriggerAdmissionContract(t *testing.T) {
	statestoretest.RunTriggerAdmissionContract(t, func(t *testing.T) statestoretest.TriggerAdmissionTestStore {
		return New().State().(statestoretest.TriggerAdmissionTestStore)
	})
}

func TestLocalGroupSuspendContract(t *testing.T) {
	statestoretest.RunGroupSuspendContract(t, func(t *testing.T) statestoretest.GroupSuspendTestStore {
		return New().State().(statestoretest.GroupSuspendTestStore)
	})
}
