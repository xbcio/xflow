package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

func TestLocalSubWorkflowLinkContract(t *testing.T) {
	statestoretest.RunSubWorkflowLinkContract(t, func(t *testing.T) statestoretest.SubWorkflowLinkStore {
		return New().State().(statestoretest.SubWorkflowLinkStore)
	})
}
