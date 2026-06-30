package node

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestCronTriggerDescriptor(t *testing.T) {
	n := CronTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.cron" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}
