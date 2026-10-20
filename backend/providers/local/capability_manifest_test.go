package local

import (
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// memoryCapabilities is the in-memory backend's declared capability
// inventory. It is explicit rather than inferred from the concrete type on
// purpose: the shared contract suites consult the declaration, and a
// capability dropped here converts its subtests into visible CAPABILITY-SKIP
// entries instead of silently shrinking coverage. TestMemoryCapabilityManifest
// pins the set against statestoretest.AllCapabilities(), so dropping one fails
// this package rather than merely skipping elsewhere.
//
// CapabilityConcurrencyQueueMode is behavioural: the queue contract is what
// proves parking/promotion/FIFO, and memoryState passes it.
var memoryCapabilities = []statestoretest.Capability{
	statestoretest.CapabilityAtomicStateStore,
	statestoretest.CapabilityOutboxLeaser,
	statestoretest.CapabilityOutboxReleaser,
	statestoretest.CapabilityExecutionStatusReader,
	statestoretest.CapabilitySuspendedNodeCanceler,
	statestoretest.CapabilityDurableSignalDeliverer,
	statestoretest.CapabilityNodeLeaseRenewer,
	statestoretest.CapabilityCancelingExecutionLister,
	statestoretest.CapabilityBatchOutputReader,
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

// memoryCapabilityManifest returns a fresh manifest for one contract run.
// Fresh per call because the manifest also records which capabilities that run
// skipped, and sharing one across suites would make that record ambiguous.
func memoryCapabilityManifest() *statestoretest.Manifest {
	return statestoretest.NewManifest("local/memory", memoryCapabilities...)
}

// TestMemoryCapabilityManifest is the backend's declaration gate: every
// declared capability must be implemented by the concrete StateStore, and the
// declared set must cover the complete inventory the contract package knows
// about. A new capability added to statestoretest therefore fails here until
// this backend either implements and declares it or documents an explicit
// expected set that leaves it out.
func TestMemoryCapabilityManifest(t *testing.T) {
	manifest := memoryCapabilityManifest()
	manifest.Verify(t, New().State())
	manifest.CheckSet(t, statestoretest.AllCapabilities()...)
	t.Logf("%s", manifest.Summary())
}
