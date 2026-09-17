package control

import (
	"context"
	"errors"

	"github.com/xbcio/xflow/engine"
)

// HandoffDebtState records the durable state of a runner handoff after queue
// admission. It is intentionally control-plane-local: the engine remains the
// authority for whether a task lease is live, while this ledger makes the
// non-atomic handoff to a runner visible and recoverable.
type HandoffDebtState string

var (
	// ErrHandoffResolutionRequired prevents an uncertain handoff from being
	// returned to the queue before Core has checked the engine authority.
	ErrHandoffResolutionRequired = errors.New("runner handoff requires resolution")
)

const (
	// HandoffDebtReserved means ClaimForRunner atomically reserved an assignment,
	// but Core has not started a lease-building call yet.
	HandoffDebtReserved HandoffDebtState = "reserved"
	// HandoffDebtLeaseMayExist is persisted before Core calls an engine method
	// that can create a durable lease. It is the crash fence for the
	// Build*Lease -> FinalizeClaim gap.
	HandoffDebtLeaseMayExist HandoffDebtState = "lease_may_exist"
	// HandoffDebtLeaseCreated means Core received a lease and recorded its
	// identity before directory finalization. It still requires recovery against
	// the engine before it can be replayed safely.
	HandoffDebtLeaseCreated HandoffDebtState = "lease_created"
	// HandoffDebtFinalized means the directory converted the claim into a
	// finalized runner lease. It remains debt until report/reclaim releases it.
	HandoffDebtFinalized HandoffDebtState = "finalized"
)

// HandoffDebt is the recovery metadata attached to an unfinalized Claim. A
// non-nil Lease is evidence that Build*Lease returned successfully, not an
// authorization to replay it without re-validating it against the engine.
type HandoffDebt struct {
	State               HandoffDebtState
	AdmissionGeneration uint64
	Lease               *engine.TaskLease
}

// HandoffDisposition is an engine-informed terminal outcome for an
// unfinalized handoff. Only Core's resolver may use it after it has established
// that no live lease needs to be preserved.
type HandoffDisposition string

const (
	HandoffDispositionRequeue HandoffDisposition = "requeue"
	HandoffDispositionDrop    HandoffDisposition = "drop"
)

// HandoffDebtDirectory is an optional RunnerDirectory capability. A directory
// implementing it creates a durable handoff record with each queue reservation,
// fences the engine call before it starts, and refuses to reclaim an uncertain
// handoff as an ordinary expired claim. Keeping it optional preserves source
// compatibility for custom RunnerDirectory implementations; production
// MemoryRunnerDirectory and RedisRunnerDirectory both implement it.
type HandoffDebtDirectory interface {
	MarkClaimLeaseMayExist(ctx context.Context, claimID ClaimID) error
	RecordClaimLeaseCreated(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error
	// MakeClaimHandoffRecoverable releases the resolver token after a known
	// dispatch failure. A crashed resolver becomes recoverable on claim expiry
	// or session replacement instead.
	MakeClaimHandoffRecoverable(ctx context.Context, claimID ClaimID) error
	SettleClaimHandoff(ctx context.Context, claimID ClaimID, disposition HandoffDisposition) error
}

// FinalizedHandoffSettler is implemented by directories that retain a debt
// record after capacity is removed for an expired lease. LeaseSweeper calls it
// only after the engine reports that reclaim/commit won the lease fence.
type FinalizedHandoffSettler interface {
	SettleFinalizedHandoff(ctx context.Context, assignmentID AssignmentID, leaseID engine.LeaseID, leaseToken engine.LeaseToken) error
}

type handoffDebtStats struct {
	activeClaims      int
	leasedTasks       int
	unsettledDebt     int
	handoffDebt       int
	leaseMayExistDebt int
	replayableDebt    int
}

func cloneHandoffDebt(src HandoffDebt) HandoffDebt {
	out := src
	if src.Lease != nil {
		lease := *src.Lease
		out.Lease = &lease
	}
	return out
}
