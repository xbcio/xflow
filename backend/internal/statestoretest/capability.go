package statestoretest

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// Capability names one optional engine interface a StateStore backend may
// implement, or one behavioural capability whose presence is not encoded in a
// single interface (CapabilityConcurrencyQueueMode).
//
// The shared contract suites are written against the engine interfaces, not
// against one backend, so a suite can only cover a capability the backend
// actually has. The historical answer was an inline type assertion followed by
// `t.Skip` when it failed: the subtest vanished, the package reported ok, and
// nothing recorded that coverage had shrunk. A Manifest replaces that with an
// explicit declaration:
//
//   - declared and implemented: the suite runs the capability's contract;
//   - declared and NOT implemented: a test failure, because the manifest is a
//     claim about the backend that the concrete store contradicts;
//   - not declared: an explicit CAPABILITY-SKIP naming the capability and the
//     backend, plus an entry in the manifest's skipped set that the suite
//     reports as not covered.
//
// The identifiers are part of test output and of every backend manifest in the
// tree, so rename one only together with every manifest that names it.
type Capability string

const (
	CapabilityAtomicStateStore Capability = "AtomicStateStore"
	// CapabilityAtomicArrivalFolding is the semantic capability behind
	// engine.ArrivalFoldingCommitter: CommitNode applies CommitNodeRequest.Arrivals
	// and reports the resolved skips in CommitNodeResult.Skipped. The interface
	// carries an explicit FoldsArrivalsIntoCommit marker, so the assertion below
	// rejects a backend that merely implements CommitNode; the declaration also
	// promises the shared arrival-folding contract, which proves the mark true.
	CapabilityAtomicArrivalFolding Capability = "AtomicArrivalFolding"
	CapabilityOutboxLeaser         Capability = "OutboxLeaser"
	CapabilityOutboxReleaser       Capability = "OutboxReleaser"

	CapabilityExecutionStatusReader       Capability = "ExecutionStatusReader"
	CapabilityRouteCheckedLeaseAcquirer   Capability = "RouteCheckedLeaseAcquirer"
	CapabilitySuspendedNodeCanceler       Capability = "SuspendedNodeCanceler"
	CapabilityDurableSignalDeliverer      Capability = "DurableSignalDeliverer"
	CapabilityNodeLeaseRenewer            Capability = "NodeLeaseRenewer"
	CapabilityCancelingExecutionLister    Capability = "CancelingExecutionLister"
	CapabilityBatchOutputReader           Capability = "BatchOutputReader"
	CapabilityExecutionInputContextReader Capability = "ExecutionInputContextReader"
	CapabilityExecutionTraceCarrierReader Capability = "ExecutionTraceCarrierReader"

	CapabilityGroupStateStore      Capability = "GroupStateStore"
	CapabilityGroupLeaseExpirer    Capability = "GroupLeaseExpirer"
	CapabilityGroupLeaseReader     Capability = "GroupLeaseReader"
	CapabilityGroupUnitPhaseReader Capability = "GroupUnitPhaseReader"
	CapabilityGroupLeaseReclaimer  Capability = "GroupLeaseReclaimer"

	CapabilityEntryAdmissionStore  Capability = "EntryAdmissionStore"
	CapabilityExecutionSeeder      Capability = "ExecutionSeeder"
	CapabilityExecutionPauser      Capability = "ExecutionPauser"
	CapabilityIdempotentStateStore Capability = "IdempotentStateStore"

	CapabilityConcurrencyLimitedStateStore          Capability = "ConcurrencyLimitedStateStore"
	CapabilityConcurrencyQueueMode                  Capability = "ConcurrencyQueueMode"
	CapabilityConcurrencyQueueExpirer               Capability = "ConcurrencyQueueExpirer"
	CapabilityConcurrencyQueueInspector             Capability = "ConcurrencyQueueInspector"
	CapabilityConcurrencyLimitedEntryAdmissionStore Capability = "ConcurrencyLimitedEntryAdmissionStore"

	CapabilityDeadLetterStore       Capability = "DeadLetterStore"
	CapabilityOutboxFailureRecorder Capability = "OutboxFailureRecorder"
	CapabilityReplayReceiptReader   Capability = "ReplayReceiptReader"
	CapabilitySubWorkflowLinkStore  Capability = "SubWorkflowLinkStore"
)

// capabilitySpec is the machine-checkable half of a capability: the engine
// interface it stands for and the assertion that proves a concrete store
// implements it. A manifest declaring a capability is verified against the
// backend before any contract relies on the declaration, so a stale manifest
// fails in one place instead of producing a misleading skip somewhere else.
type capabilitySpec struct {
	iface      string
	summary    string
	implements func(any) bool
}

var capabilitySpecs = map[Capability]capabilitySpec{
	CapabilityAtomicStateStore: {
		iface:   "engine.AtomicStateStore",
		summary: "atomic execution/outbox transitions (CreateExecutionWithOutbox, CommitNode, ListOutbox, ...)",
		implements: func(v any) bool {
			_, ok := v.(engine.AtomicStateStore)
			return ok
		},
	},
	CapabilityAtomicArrivalFolding: {
		iface:   "engine.ArrivalFoldingCommitter",
		summary: "downstream arrivals folded into CommitNode with skip reporting (CommitNodeResult.Skipped)",
		implements: func(v any) bool {
			committer, ok := v.(engine.ArrivalFoldingCommitter)
			return ok && committer.FoldsArrivalsIntoCommit()
		},
	},
	CapabilityOutboxLeaser: {
		iface:   "engine.OutboxLeaser",
		summary: "delivery leases for outbox entries (LeaseOutbox/RenewOutbox)",
		implements: func(v any) bool {
			_, ok := v.(engine.OutboxLeaser)
			return ok
		},
	},
	CapabilityOutboxReleaser: {
		iface:   "engine.OutboxReleaser",
		summary: "early release of an outbox delivery lease (ReleaseOutbox)",
		implements: func(v any) bool {
			_, ok := v.(engine.OutboxReleaser)
			return ok
		},
	},
	CapabilityExecutionStatusReader: {
		iface:   "engine.ExecutionStatusReader",
		summary: "narrow execution-status read without loading the snapshot (GetExecutionStatus)",
		implements: func(v any) bool {
			_, ok := v.(engine.ExecutionStatusReader)
			return ok
		},
	},
	CapabilityRouteCheckedLeaseAcquirer: {
		iface:   "engine.RouteCheckedLeaseAcquirer",
		summary: "lease acquire that re-decides execution liveness and node routing in the same transition (AcquireTaskLeaseChecked)",
		implements: func(v any) bool {
			_, ok := v.(engine.RouteCheckedLeaseAcquirer)
			return ok
		},
	},
	CapabilitySuspendedNodeCanceler: {
		iface:   "engine.SuspendedNodeCanceler",
		summary: "fenced cancellation of a node suspended on a signal (CancelSuspendedNode)",
		implements: func(v any) bool {
			_, ok := v.(engine.SuspendedNodeCanceler)
			return ok
		},
	},
	CapabilityDurableSignalDeliverer: {
		iface:   "engine.DurableSignalDeliverer",
		summary: "atomic signal consume plus resume intent (DeliverSignalWithOutbox)",
		implements: func(v any) bool {
			_, ok := v.(engine.DurableSignalDeliverer)
			return ok
		},
	},
	CapabilityNodeLeaseRenewer: {
		iface:   "engine.NodeLeaseRenewer",
		summary: "node lease renewal past the engine's default TTL (RenewTaskLease)",
		implements: func(v any) bool {
			_, ok := v.(engine.NodeLeaseRenewer)
			return ok
		},
	},
	CapabilityCancelingExecutionLister: {
		iface:   "engine.CancelingExecutionLister",
		summary: "listing executions stuck in the canceling status (ListCancelingExecutions)",
		implements: func(v any) bool {
			_, ok := v.(engine.CancelingExecutionLister)
			return ok
		},
	},
	CapabilityBatchOutputReader: {
		iface:   "engine.BatchOutputReader",
		summary: "reading many node outputs in one round trip (GetOutputs)",
		implements: func(v any) bool {
			_, ok := v.(engine.BatchOutputReader)
			return ok
		},
	},
	CapabilityExecutionInputContextReader: {
		iface:   "engine.ExecutionInputContextReader",
		summary: "narrow input read without loading the snapshot (GetExecutionInputContext)",
		implements: func(v any) bool {
			_, ok := v.(engine.ExecutionInputContextReader)
			return ok
		},
	},
	CapabilityExecutionTraceCarrierReader: {
		iface:   "engine.ExecutionTraceCarrierReader",
		summary: "single-key trace carrier read for dispatch (GetExecutionTraceCarrier)",
		implements: func(v any) bool {
			_, ok := v.(engine.ExecutionTraceCarrierReader)
			return ok
		},
	},
	CapabilityGroupStateStore: {
		iface:   "engine.GroupStateStore",
		summary: "group lease lifecycle and atomic group commit (Acquire/Renew/CommitGroup)",
		implements: func(v any) bool {
			_, ok := v.(engine.GroupStateStore)
			return ok
		},
	},
	CapabilityGroupLeaseExpirer: {
		iface:   "engine.GroupLeaseExpirer",
		summary: "reclaiming an expired group lease (ExpireGroupLease)",
		implements: func(v any) bool {
			_, ok := v.(engine.GroupLeaseExpirer)
			return ok
		},
	},
	CapabilityGroupLeaseReader: {
		iface:   "engine.GroupLeaseReader",
		summary: "reading a group unit's live lease (GetGroupLease)",
		implements: func(v any) bool {
			_, ok := v.(engine.GroupLeaseReader)
			return ok
		},
	},
	CapabilityGroupUnitPhaseReader: {
		iface:   "engine.GroupUnitPhaseReader",
		summary: "reading a group unit's durable lifecycle phase (GetGroupUnitPhase)",
		implements: func(v any) bool {
			_, ok := v.(engine.GroupUnitPhaseReader)
			return ok
		},
	},
	CapabilityGroupLeaseReclaimer: {
		iface:   "engine.GroupLeaseReclaimer",
		summary: "token-fenced group lease revocation with redelivery (RevokeGroupLeaseWithOutbox)",
		implements: func(v any) bool {
			_, ok := v.(engine.GroupLeaseReclaimer)
			return ok
		},
	},
	CapabilityEntryAdmissionStore: {
		iface:   "engine.EntryAdmissionStore",
		summary: "atomic entry-unit seed admission (SeedExecutionFromEntry)",
		implements: func(v any) bool {
			_, ok := v.(engine.EntryAdmissionStore)
			return ok
		},
	},
	CapabilityExecutionSeeder: {
		iface:   "engine.ExecutionSeeder",
		summary: "seeding a rerun from already-resolved units (SeedExecution)",
		implements: func(v any) bool {
			_, ok := v.(engine.ExecutionSeeder)
			return ok
		},
	},
	CapabilityExecutionPauser: {
		iface:   "engine.ExecutionPauser",
		summary: "dispatch hold and resume (PauseExecution/ResumeExecution)",
		implements: func(v any) bool {
			_, ok := v.(engine.ExecutionPauser)
			return ok
		},
	},
	CapabilityIdempotentStateStore: {
		iface:   "engine.IdempotentStateStore",
		summary: "keyed request claims (ClaimIdempotency/ConfirmIdempotency/ReleaseIdempotency)",
		implements: func(v any) bool {
			_, ok := v.(engine.IdempotentStateStore)
			return ok
		},
	},
	CapabilityConcurrencyLimitedStateStore: {
		iface:   "engine.ConcurrencyLimitedStateStore",
		summary: "workflow concurrency slots on the submit path (CreateExecutionWithConcurrency)",
		implements: func(v any) bool {
			_, ok := v.(engine.ConcurrencyLimitedStateStore)
			return ok
		},
	},
	// ConcurrencyQueueMode is behavioural: no Go interface says "queue mode is
	// supported", only that the store enforces concurrency limits. The
	// implementation check is therefore necessary but not sufficient; the
	// queue contract (RunConcurrencyQueueContract) is what proves the parking,
	// promotion and timeout behaviour. A manifest that declares this capability
	// promises both.
	CapabilityConcurrencyQueueMode: {
		iface:   "engine.ConcurrencyLimitedStateStore (queue mode behaviour)",
		summary: "over-limit submissions park in a queue and are promoted in FIFO order",
		implements: func(v any) bool {
			_, ok := v.(engine.ConcurrencyLimitedStateStore)
			return ok
		},
	},
	CapabilityConcurrencyQueueExpirer: {
		iface:   "engine.ConcurrencyQueueExpirer",
		summary: "queue-timeout expiry of a waiting execution (ExpireQueuedExecutions)",
		implements: func(v any) bool {
			_, ok := v.(engine.ConcurrencyQueueExpirer)
			return ok
		},
	},
	CapabilityConcurrencyQueueInspector: {
		iface:   "engine.ConcurrencyQueueInspector",
		summary: "reading a queued execution's position in line (ConcurrencyQueuePosition)",
		implements: func(v any) bool {
			_, ok := v.(engine.ConcurrencyQueueInspector)
			return ok
		},
	},
	CapabilityConcurrencyLimitedEntryAdmissionStore: {
		iface:   "engine.ConcurrencyLimitedEntryAdmissionStore",
		summary: "concurrency slots on the entry-seed path (SeedExecutionFromEntryWithConcurrency)",
		implements: func(v any) bool {
			_, ok := v.(engine.ConcurrencyLimitedEntryAdmissionStore)
			return ok
		},
	},
	CapabilityDeadLetterStore: {
		iface:   "engine.DeadLetterStore",
		summary: "dead-letter listing and replay (ListDeadLetters/ReplayDeadLetter)",
		implements: func(v any) bool {
			_, ok := v.(engine.DeadLetterStore)
			return ok
		},
	},
	CapabilityOutboxFailureRecorder: {
		iface:   "engine.OutboxFailureRecorder",
		summary: "recording delivery failures on the path to the dead-letter queue (RecordOutboxFailure)",
		implements: func(v any) bool {
			_, ok := v.(engine.OutboxFailureRecorder)
			return ok
		},
	},
	CapabilityReplayReceiptReader: {
		iface:   "engine.ReplayReceiptReader",
		summary: "scanning durable dead-letter replay receipts (ScanReplayReceipts)",
		implements: func(v any) bool {
			_, ok := v.(engine.ReplayReceiptReader)
			return ok
		},
	},
	CapabilitySubWorkflowLinkStore: {
		iface:   "engine.SubWorkflowLinkStore",
		summary: "sub-workflow wait links (BeginSubWorkflowWait/GetSubWorkflowLink)",
		implements: func(v any) bool {
			_, ok := v.(engine.SubWorkflowLinkStore)
			return ok
		},
	},
}

// AllCapabilities returns every capability this package knows how to contract
// test, sorted. A first-party backend that implements all of them pins its
// manifest against this list in its own Test...CapabilityManifest, so adding a
// capability here forces a deliberate decision in every such manifest rather
// than letting the new contract silently skip.
func AllCapabilities() []Capability {
	all := make([]Capability, 0, len(capabilitySpecs))
	for c := range capabilitySpecs {
		all = append(all, c)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all
}

// Manifest is one backend's declaration of the optional capabilities it
// supports. It is deliberately passed explicitly to every contract suite: a
// suite that cannot see a manifest cannot distinguish "this backend lacks the
// capability" from "this backend's capability was silently dropped", which is
// the ambiguity the manifest exists to remove.
//
// A Manifest is not safe for concurrent use. The suites that use one run their
// subtests sequentially; a future parallel suite must give each goroutine its
// own manifest.
type Manifest struct {
	backend  string
	declared map[Capability]struct{}
	skipped  map[Capability]struct{}
}

// NewManifest declares capabilities for one backend. backend names the
// concrete implementation in test output (e.g. "local/memory",
// "distributed/rstate"). Unknown capability identifiers panic: they are a
// programming error in the test's own source, not a runtime condition.
func NewManifest(backend string, capabilities ...Capability) *Manifest {
	if backend == "" {
		panic("statestoretest.NewManifest: backend name must not be empty")
	}
	m := &Manifest{
		backend:  backend,
		declared: make(map[Capability]struct{}, len(capabilities)),
		skipped:  make(map[Capability]struct{}),
	}
	for _, c := range capabilities {
		if _, ok := capabilitySpecs[c]; !ok {
			panic(fmt.Sprintf(
				"statestoretest.NewManifest(%q): unknown capability %q; add it to capabilitySpecs and to AllCapabilities first",
				backend, c))
		}
		m.declared[c] = struct{}{}
	}
	return m
}

// Backend returns the manifest's backend name.
func (m *Manifest) Backend() string {
	if m == nil {
		return ""
	}
	return m.backend
}

// Declares reports whether the backend claims the capability.
func (m *Manifest) Declares(c Capability) bool {
	if m == nil {
		return false
	}
	_, ok := m.declared[c]
	return ok
}

// DeclaredCapabilities returns the declared set, sorted.
func (m *Manifest) DeclaredCapabilities() []Capability {
	if m == nil {
		return nil
	}
	return sortedCapabilities(m.declared)
}

// UndeclaredCapabilities returns the known capabilities this manifest does not
// declare, sorted. They are the ones whose contract subtests will explicitly
// skip for this backend.
func (m *Manifest) UndeclaredCapabilities() []Capability {
	undeclared := make(map[Capability]struct{}, len(capabilitySpecs))
	for c := range capabilitySpecs {
		if !m.Declares(c) {
			undeclared[c] = struct{}{}
		}
	}
	return sortedCapabilities(undeclared)
}

// SkippedCapabilities returns the capabilities a suite explicitly skipped
// because the manifest did not declare them, sorted.
func (m *Manifest) SkippedCapabilities() []Capability {
	if m == nil {
		return nil
	}
	return sortedCapabilities(m.skipped)
}

// Summary is the single-line inventory used in test output. It names both the
// declared set and the known-but-undeclared set, so a run with -v shows
// exactly what the backend claims and what the suites will therefore not
// cover.
func (m *Manifest) Summary() string {
	if m == nil {
		return "<nil capability manifest>"
	}
	declared := m.DeclaredCapabilities()
	undeclared := m.UndeclaredCapabilities()
	return fmt.Sprintf(
		"backend %q declares %d/%d capabilities [%s]; undeclared (their subtests will CAPABILITY-SKIP): [%s]",
		m.backend, len(declared), len(capabilitySpecs),
		joinCapabilities(declared), joinCapabilities(undeclared))
}

// Report logs the manifest at suite start and, at cleanup, the capabilities
// the run explicitly skipped. The per-subtest skip messages are the primary
// record; this is the aggregate view that `go test -v` prints even when the
// skipped subtests are buried in a long run.
func (m *Manifest) Report(t *testing.T) {
	t.Helper()
	if m == nil {
		t.Fatalf("statestoretest: nil capability manifest — every backend must declare the capabilities it supports")
	}
	t.Logf("capability manifest: %s", m.Summary())
	t.Cleanup(func() {
		if skipped := m.SkippedCapabilities(); len(skipped) > 0 {
			t.Logf("capability coverage: %d SKIPPED, not covered for backend %q: [%s]",
				len(skipped), m.backend, joinCapabilities(skipped))
			return
		}
		t.Logf("capability coverage: no declared capability was skipped for backend %q", m.backend)
	})
}

// Verify asserts that every capability the manifest declares is actually
// implemented by backend, and that the backend value is usable. Backends call
// it from their manifest test, before any contract relies on the declaration.
func (m *Manifest) Verify(t *testing.T, backend any) {
	t.Helper()
	if m == nil {
		t.Fatalf("statestoretest: nil capability manifest")
	}
	if backend == nil {
		t.Fatalf("backend %q: nil value passed to Verify", m.backend)
	}
	for _, c := range m.DeclaredCapabilities() {
		spec := capabilitySpecs[c]
		if !spec.implements(backend) {
			t.Errorf("backend %q DECLARES capability %s (%s) but %T does not implement it; "+
				"implement the interface or remove the capability from the manifest — a stale "+
				"declaration turns contract coverage into a false claim",
				m.backend, c, spec.iface, backend)
		}
	}
}

// CheckSet asserts the manifest declares exactly the expected capability set.
// First-party backends use it to pin their inventory against
// AllCapabilities(): dropping a capability from a manifest then fails here,
// instead of quietly converting that capability's contract subtests into
// explicit skips.
func (m *Manifest) CheckSet(t *testing.T, expected ...Capability) {
	t.Helper()
	if m == nil {
		t.Fatalf("statestoretest: nil capability manifest")
	}
	want := make(map[Capability]struct{}, len(expected))
	for _, c := range expected {
		if _, ok := capabilitySpecs[c]; !ok {
			t.Errorf("expected capability set contains unknown identifier %q", c)
			continue
		}
		want[c] = struct{}{}
	}
	for _, c := range m.DeclaredCapabilities() {
		if _, ok := want[c]; !ok {
			t.Errorf("backend %q declares capability %s, which its expected set does not list; "+
				"add it to the expected set if the backend really implements it, or remove it "+
				"from the manifest", m.backend, c)
		}
	}
	for _, c := range sortedCapabilities(want) {
		if !m.Declares(c) {
			t.Errorf("backend %q does not declare capability %s (%s), which its expected set "+
				"requires; without the declaration every contract subtest for that capability "+
				"reports CAPABILITY-SKIP instead of coverage", m.backend, c, capabilitySpecs[c].iface)
		}
	}
}

// RequireDeclared fails when any of the capabilities is not declared. Suites
// call it for the capabilities they cannot run without: for those, a missing
// declaration is a gap in the backend's manifest, not a reason to skip.
func (m *Manifest) RequireDeclared(t *testing.T, capabilities ...Capability) {
	t.Helper()
	if m == nil {
		t.Fatalf("statestoretest: nil capability manifest")
	}
	for _, c := range capabilities {
		spec, ok := capabilitySpecs[c]
		if !ok {
			t.Fatalf("statestoretest: unknown capability %q", c)
		}
		if !m.Declares(c) {
			t.Fatalf("backend %q does not declare required capability %s (%s); this suite "+
				"cannot run for that backend. Declare the capability in the backend's manifest "+
				"if it implements the interface; otherwise do not run this suite for it",
				m.backend, c, spec.iface)
		}
	}
}

// SkipUnlessDeclared is the manifest gate for a single subtest: it records the
// capability as uncovered and skips with an explicit CAPABILITY-SKIP message
// when the backend does not declare it, and returns true (running the caller's
// case) when it does.
func (m *Manifest) SkipUnlessDeclared(t *testing.T, c Capability) bool {
	t.Helper()
	if m == nil {
		t.Fatalf("statestoretest: nil capability manifest")
	}
	spec, ok := capabilitySpecs[c]
	if !ok {
		t.Fatalf("statestoretest: unknown capability %q", c)
	}
	if m.Declares(c) {
		return true
	}
	if m.skipped == nil {
		m.skipped = make(map[Capability]struct{})
	}
	m.skipped[c] = struct{}{}
	t.Skipf("CAPABILITY-SKIP %s: backend %q does not declare %s (%s); "+
		"this subtest is skipped and NOT covered — declare the capability in the backend's "+
		"manifest if the interface is implemented", c, m.backend, spec.iface, spec.summary)
	return false
}

// OptionalCapability resolves a capability a suite can live without. When the
// manifest does not declare c, the subtest skips explicitly (see
// SkipUnlessDeclared). When it does declare c, backend must implement T, and a
// failed assertion is a test failure: the manifest is a claim, not a hint.
func OptionalCapability[T any](t *testing.T, m *Manifest, c Capability, backend any) (T, bool) {
	t.Helper()
	var zero T
	if !m.SkipUnlessDeclared(t, c) {
		return zero, false
	}
	got, ok := backend.(T)
	if !ok {
		t.Fatalf("backend %q DECLARES capability %s (%s) but %T does not implement it; "+
			"a declared capability must pass its contract rather than be skipped",
			m.Backend(), c, capabilitySpecs[c].iface, backend)
	}
	return got, true
}

// RequiredCapability resolves a capability the suite cannot run without. An
// undeclared capability fails here rather than skipping, so a first-party
// backend that loses an interface never reports a green suite with a smaller
// contract.
func RequiredCapability[T any](t *testing.T, m *Manifest, c Capability, backend any) T {
	t.Helper()
	m.RequireDeclared(t, c)
	got, ok := backend.(T)
	if !ok {
		t.Fatalf("backend %q declares capability %s (%s) but %T does not implement it",
			m.Backend(), c, capabilitySpecs[c].iface, backend)
	}
	return got
}

func sortedCapabilities(set map[Capability]struct{}) []Capability {
	out := make([]Capability, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func joinCapabilities(caps []Capability) string {
	if len(caps) == 0 {
		return ""
	}
	parts := make([]string, len(caps))
	for i, c := range caps {
		parts[i] = string(c)
	}
	return strings.Join(parts, ", ")
}
