package rstate

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// rstateCapabilities is the Redis/Lua backend's declared capability
// inventory. It mirrors the local backend's list on purpose: the two must
// satisfy the same contracts, and the explicit declaration is what lets each
// contract subtest distinguish "this backend does not support it" from "the
// support was dropped".
//
// The manifest is independent of the Redis server: New(nil, nil, ...) only
// constructs the value, and Verify performs type assertions, so this test runs
// without miniredis or a real server.
var rstateCapabilities = []statestoretest.Capability{
	statestoretest.CapabilityAtomicStateStore,
	statestoretest.CapabilityOutboxLeaser,
	statestoretest.CapabilityOutboxReleaser,
	statestoretest.CapabilityExecutionStatusReader,
	statestoretest.CapabilitySuspendedNodeCanceler,
	statestoretest.CapabilityDurableSignalDeliverer,
	statestoretest.CapabilityNodeLeaseRenewer,
	statestoretest.CapabilityCancelingExecutionLister,
	statestoretest.CapabilityGroupStateStore,
	statestoretest.CapabilityGroupLeaseExpirer,
	statestoretest.CapabilityGroupLeaseReader,
	statestoretest.CapabilityGroupUnitPhaseReader,
	statestoretest.CapabilityGroupLeaseReclaimer,
	statestoretest.CapabilityEntryAdmissionStore,
	statestoretest.CapabilityExecutionSeeder,
	statestoretest.CapabilityExecutionPauser,
	statestoretest.CapabilityIdempotentStateStore,
	statestoretest.CapabilityConcurrencyLimitedStateStore,
	statestoretest.CapabilityConcurrencyQueueMode,
	statestoretest.CapabilityConcurrencyQueueExpirer,
	statestoretest.CapabilityConcurrencyQueueInspector,
	statestoretest.CapabilityConcurrencyLimitedEntryAdmissionStore,
	statestoretest.CapabilityDeadLetterStore,
	statestoretest.CapabilityOutboxFailureRecorder,
	statestoretest.CapabilityReplayReceiptReader,
	statestoretest.CapabilitySubWorkflowLinkStore,
}

// rstateCapabilityManifest returns a fresh manifest for one contract run.
func rstateCapabilityManifest() *statestoretest.Manifest {
	return statestoretest.NewManifest("distributed/rstate", rstateCapabilities...)
}

// TestRstateCapabilityManifest is the Redis backend's declaration gate. It
// fails if the manifest claims an interface the concrete Store does not
// implement, or if the declaration set drifts from the contract package's
// inventory.
func TestRstateCapabilityManifest(t *testing.T) {
	manifest := rstateCapabilityManifest()
	manifest.Verify(t, New(nil, nil, time.Minute))
	manifest.CheckSet(t, statestoretest.AllCapabilities()...)
	t.Logf("%s", manifest.Summary())
}
