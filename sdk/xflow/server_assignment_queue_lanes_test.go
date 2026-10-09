package xflow

import (
	"testing"

	"github.com/xbcio/xflow/service/control"
)

// TestWithServerAssignmentQueueLanesSetsTheConfig pins the two SDK-side hops of
// the queue-lane config path: the option into serverConfig, and serverConfig
// into apiserver.Config through buildServerAPIConfig — the one place the SDK
// translates its config. Everything past that boundary is asserted where it is
// observable: TestNewAPIServerPropagatesAssignmentQueueLanes (service/apiserver)
// covers apiserver.Config into control.Config and the runner directory, and
// TestSelectRunnerDirectoryWiresAssignmentQueueLanes (service/control) covers
// the directory construction itself.
func TestWithServerAssignmentQueueLanesSetsTheConfig(t *testing.T) {
	var c serverConfig
	WithServerAssignmentQueueLanes([]string{"xflow.sas.webscan-sink"})(&c)
	WithServerAssignmentQueueLaneWriteMode(control.LaneWriteDual)(&c)

	if len(c.assignmentQueueLanes) != 1 || c.assignmentQueueLanes[0] != "xflow.sas.webscan-sink" {
		t.Fatalf("assignmentQueueLanes = %v, want the lane", c.assignmentQueueLanes)
	}
	if c.assignmentQueueLaneWriteMode != control.LaneWriteDual {
		t.Fatalf("assignmentQueueLaneWriteMode = %q, want dual", c.assignmentQueueLaneWriteMode)
	}

	apiCfg := buildServerAPIConfig(ServerConfig{}, &c)
	if len(apiCfg.AssignmentQueueLanes) != 1 || apiCfg.AssignmentQueueLanes[0] != "xflow.sas.webscan-sink" {
		t.Fatalf("apiserver config lanes = %v, want the lane", apiCfg.AssignmentQueueLanes)
	}
	if apiCfg.AssignmentQueueLaneWriteMode != control.LaneWriteDual {
		t.Fatalf("apiserver config write mode = %q, want dual", apiCfg.AssignmentQueueLaneWriteMode)
	}

	// The option copies its slice: a caller mutating it afterwards cannot
	// silently change what the server was configured with.
	lanes := []string{"xflow.sas.webscan-sink"}
	var copied serverConfig
	WithServerAssignmentQueueLanes(lanes)(&copied)
	lanes[0] = "xflow.sas.ulp-result"
	if copied.assignmentQueueLanes[0] != "xflow.sas.webscan-sink" {
		t.Fatalf("assignmentQueueLanes = %v, want the original copy", copied.assignmentQueueLanes)
	}
}
