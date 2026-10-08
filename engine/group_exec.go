package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// GroupExit is one boundary output produced by a member node during group
// execution. Milestone A supplies a fake in test; Milestone B supplies the real
// runner-embedded engine.
type GroupExit struct {
	NodeName string
	Port     string
	Data     map[string]any
}

// GroupExecutor runs one pinned group unit locally and returns its boundary
// outputs. Milestone A supplies a fake in test; Milestone B supplies the real
// runner-embedded engine.
type GroupExecutor interface {
	ExecuteGroup(ctx context.Context, task *Task, meta graph.GroupMeta) (exits []GroupExit, fatal bool, err error)
}

// WithGroupExecutor sets the optional group executor capability.
func WithGroupExecutor(ge GroupExecutor) Option {
	return func(e *Engine) { e.groupExecutor = ge }
}

// executeGroup handles a TaskTypeGroupExec task: acquires a group lease, calls
// the GroupExecutor, and commits the result. commitGroup (Task 12) will
// implement the actual downstream propagation; for now it is a stub.
func (e *Engine) executeGroup(ctx context.Context, task *Task, flush bool) error {
	if e.groupExecutor == nil {
		return fmt.Errorf("no group executor configured for group %q", task.NodeName)
	}
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if task.UnitIdx < 0 || task.UnitIdx >= g.UnitCount() || g.UnitKindAt(task.UnitIdx) != graph.UnitGroup {
		return fmt.Errorf("group exec unit %d out of range or not a group", task.UnitIdx)
	}
	meta := g.GroupMetaAt(task.UnitIdx)

	// Build group lease. Same ID/TTL generation as BuildTaskLease (lease.go).
	// Attempt is seeded as 1; AcquireGroupLease writes the real attempt count
	// back into lease.Attempt (both local and Redis backends do this: they
	// track the persisted attempt and bump it on every expiry cycle). After
	// AcquireGroupLease returns, lease.Attempt is the authoritative value used
	// to fence CommitGroup. What belongs to a future milestone is enforcement
	// of GroupMeta.Retry.MaxAttempts — scheduling a retry vs. failing the
	// execution when the group exhausts its budget.
	leaseID, leaseToken := newLeaseCredentials()
	lease := &GroupLease{
		ExecutionID:  task.ExecutionID,
		GroupUnitIdx: task.UnitIdx,
		LeaseID:      leaseID,
		LeaseToken:   leaseToken,
		Attempt:      1, // seed; overwritten by AcquireGroupLease with the real attempt
		IssuedAt:     time.Now().UTC(),
		TTL:          e.defaultLeaseTTL,
	}

	gs, ok := e.state.(GroupStateStore)
	if !ok {
		return fmt.Errorf("state store does not support group leases")
	}
	acquired, err := gs.AcquireGroupLease(ctx, lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil // already owned by another executor; safe to discard
	}
	e.notifyGroupLeaseAcquired(ctx)

	exits, fatal, execErr := e.groupExecutor.ExecuteGroup(ctx, task, meta)
	// This is the local/embedded executor path: ExecuteGroup only ever
	// reports an ordinary handler failure (or none) through execErr/fatal —
	// unlike CommitGroupResult's remote wire path, it has no distinct
	// canceled/timeout OUTCOME classification (GroupResult.Outcome) layered
	// on top. So group-level OnError governs this failure unconditionally:
	// recompute fatal from meta.OnError exactly as before this feature, and
	// decide error_output routing from the result.
	if execErr != nil {
		fatal = groupOnErrorFatal(meta.OnError)
	}
	routeToErrorOutput := execErr != nil && !fatal && meta.OnError == string(types.OnErrorOutput)
	// The GroupExecutor interface (test-only fake; see its doc comment) has no
	// notion of a failed member's identity, so this path never has one to
	// offer -- "" here matches the pre-existing omission semantics.
	return e.commitGroup(ctx, g, lease, meta, exits, fatal, execErr, "", flush, routeToErrorOutput)
}

// commitGroup commits one group unit's terminal result, propagates downstream
// unit arrivals, and finalizes the execution when all units are done.
//
// fatal and routeToErrorOutput are both caller-decided, not re-derived here.
// This matters because the two production callers disagree in one case:
// executeGroup's ExecuteGroup only ever reports an ordinary handler failure,
// so group-level OnError governs its fatality unconditionally; but
// CommitGroupResult's remote wire path carries a distinct canceled/timeout
// OUTCOME classification (GroupResult.Outcome) that must stay fatal
// regardless of on_error=continue/error_output — a cancellation is not the
// failure OnError exists to tolerate or route. Earlier revisions had
// commitGroup unconditionally overwrite the caller's fatal with
// groupOnErrorFatal(meta.OnError) whenever execErr != nil (true for every
// non-success outcome, including canceled/timeout), silently downgrading a
// cancellation to a routed/tolerated non-fatal commit on any group configured
// with on_error=continue or error_output. Trusting the caller's verdict here
// closes that.
func (e *Engine) commitGroup(ctx context.Context, g *graph.Graph, lease *GroupLease, meta graph.GroupMeta, exits []GroupExit, fatal bool, execErr error, failedMember string, flush bool, routeToErrorOutput bool) error {
	gs := e.state.(GroupStateStore) // executeGroup already asserted
	outcome := GroupOutcomeSuccess
	errMsg := ""
	if execErr != nil {
		outcome = GroupOutcomeFailed
		errMsg = execErr.Error()
		if !fatal {
			// Every backend's CommitGroup/SeedExecutionFromEntry increments its
			// failed-unit counter whenever Outcome==GroupOutcomeFailed,
			// regardless of Fatal (backend/providers/local/group_state.go,
			// backend/providers/distributed/internal/rstate/group_state.go,
			// backend/providers/local/entry_admission.go) — and a non-zero
			// failed count finalizes the WHOLE EXECUTION as Failed once the
			// remaining-unit counter reaches zero, even for a unit this engine
			// itself decided was non-fatal. This used to be gated on
			// routeToErrorOutput alone, which fixed error_output but left
			// on_error=continue's member failure still finalizing the execution
			// Failed through this exact counter (the latent bug
			// NODE-GROUP-COLOCATION.md §12.2 documented as out of scope for the
			// error_output feature). Both policies share the same contract: the
			// node/group "handled" the error and the execution is not failed by
			// it (ApplyOnError's error_output AND continue both set NodeStatus
			// to a non-failed value for exactly this reason). Reporting
			// GroupOutcomeSuccess for every non-fatal failure — not just a
			// routed one — is what keeps a tolerated group from silently
			// finalizing its execution as Failed through that counter — the
			// backends have no OTHER signal that would tell them not to count
			// it. errMsg/reqExits below still carry the real failure (routed
			// under the group's own name/"error" port for error_output, or
			// simply absent from reqExits for continue), so the information is
			// not lost, only not counted as a terminal execution failure.
			outcome = GroupOutcomeSuccess
		}
	}

	// Downstream unit arrival descriptions. Arrival counting (in-degree DECR /
	// active / wait_all|wait_any threshold) is handled by CommitGroup in the
	// SAME atomic transition — not by a subsequent AdvanceNode call — because a
	// group has no per-node state, and AdvanceNode's source guard would
	// fail-closed.
	//
	// A failed group that routes to error_output reports no real member exits
	// (the failure means the member subgraph did not reach its normal boundary
	// outputs) — only the synthesized error edges light up. A failed group that
	// is non-fatal under "continue" reports whatever exits the executor DID
	// manage to produce before the failure, same as before this feature.
	var downstream []DownstreamArrival
	if !fatal {
		downstream = e.downstreamUnitArrivals(g, meta.UnitIdx, exits, routeToErrorOutput)
	}

	// GroupExit (executor report) → GroupExitResult (commit request, Task 8).
	reqExits := make([]GroupExitResult, 0, len(exits)+1)
	for _, ex := range exits {
		reqExits = append(reqExits, GroupExitResult{
			NodeIdx:       nodeIdxOf(g, ex.NodeName),
			NodeName:      ex.NodeName,
			Port:          ex.Port,
			Data:          ex.Data,
			PrivateOutput: groupExitOutputIsPrivate(g, ex.NodeName),
		})
	}
	if routeToErrorOutput {
		// The error payload is stored under the GROUP's own name, not any
		// member's — GetOutput/$('name') is keyed purely by name string
		// (memoryNodeKey/outputKey take a name, never a node index), so a
		// downstream node's $('group_name').json reads this exactly like a
		// node's own output. NodeIdx has no real node to point at (-1, matching
		// the synthetic edge's Src.NodeIdx); nothing dereferences it for this
		// exit because the destination side of the arrival drives scheduling,
		// not this record.
		reqExits = append(reqExits, GroupExitResult{
			NodeIdx:       -1,
			NodeName:      meta.Name,
			Port:          "error",
			Data:          groupErrorOutputData(meta, execErr, failedMember),
			PrivateOutput: false,
		})
	}

	res, err := gs.CommitGroup(ctx, GroupCommitRequest{
		ExecutionID:  lease.ExecutionID,
		GroupUnitIdx: lease.GroupUnitIdx,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Outcome:      outcome,
		Fatal:        fatal,
		Error:        errMsg,
		Exits:        reqExits,
		Downstream:   downstream,
	})
	if err != nil {
		return fmt.Errorf("commit group %q: %w", meta.Name, err)
	}
	// A group commit is also where the group's downstream fan-in is counted, so
	// this is the third and last place a skip is decided. A stale-token commit
	// applied nothing and reports no skips.
	e.notifySkip(ctx, flowGroup, res.Skipped)

	// commitObserver's outcome label space is deliberately narrower than
	// CommitOutcome. CommitGroupResult short-circuits to
	// CommitOutcomeExecutionInactive — a stale/late commit discarded before
	// ever reaching this function — so that case never appears here. Folding
	// "the group's business result" and "this commit was discarded as stale"
	// into one outcome label would corrupt the failure-rate ratio this metric
	// exists to expose, the same reason the package-cache metric (the
	// previous wiring pass) excludes its error branches. Observing discarded
	// commits is a new series, not a fourth value here.
	commitOutcome := "success"
	if execErr != nil {
		if fatal {
			commitOutcome = "failed_fatal"
		} else {
			commitOutcome = "failed_tolerated"
		}
	}
	// lease.IssuedAt is zero only if some construction site built a GroupLease
	// literal without setting it. Both existing sites (executeGroup above and
	// CommitGroupResult) do set it; this guard is structural defense against a
	// future third site making the same mistake, not a known live path. A
	// silent zero-value would otherwise report a duration of decades
	// (time.Since of the zero time), quietly wrecking the histogram rather
	// than failing loudly.
	if !lease.IssuedAt.IsZero() {
		e.notifyGroupCommit(ctx, commitOutcome, time.Since(lease.IssuedAt))
	}

	if res.ExecutionDone {
		e.notifyExecutionComplete(ctx, lease.ExecutionID, res.ExecutionStatus)
		e.EvictExecution(lease.ExecutionID)
		return nil
	}
	if flush {
		if err := e.FlushOutbox(ctx, lease.ExecutionID); err != nil {
			return fmt.Errorf("%w: %w", errGroupCommitFlushPending, err)
		}
	}
	return nil
}

// groupErrorOutputData builds the payload committed under the group's own
// name/"error" port when a failed group routes to error_output. It carries
// the group name and the error message — the "error" and "error details"
// NODE-GROUP-COLOCATION.md §12.2 calls for — structured the same way a node's
// own error_output payload is (engine/errorpolicy.go buildErrData), so a
// downstream consumer can read $('group').json.error the same way it would
// read a node's.
//
// failedMember is the identity of the member node whose fatal failure
// produced execErr, when the caller has one to offer (see
// subgraph.Result.FailedMember and its propagation through
// GroupRuntime.ExecuteRequest to GroupResult.FailedMember). It is added to
// the payload under "failed_members" ([]any, currently always zero-or-one
// name) only when non-empty: unlike a node-level failure
// (types.Error.NodeName, set by the handler boundary that calls a single
// node), identity used to be unrecoverable for a group because execErr was
// subgraph.Result.Error — a plain string copied from the inner execution's
// terminal error (service/runner/group_runtime.go's ExecuteRequest) — with
// nothing a group-exec caller could attach a member identity to. That
// collapse point is now closed for the production CommitGroupResult path
// (engine/group_lease.go); the local GroupExecutor test fake still has no
// such identity to offer and passes "", which keeps the key omitted exactly
// as before this field existed.
//
// []any, not []string: this map is stored as output Data and round-trips
// through JSON on the Redis backend (rstate), which decodes a JSON array back
// into []any — the memory backend keeps whatever Go value was stored, with no
// such round trip. A []string literal here would therefore read back as
// []string on memory and []any on Redis, and an expr/function node doing a
// type assertion or a DeepEqual-style comparison against this field would see
// a different shape depending on which backend committed it. []any already
// is that shape, consistently, in both backends.
func groupErrorOutputData(meta graph.GroupMeta, execErr error, failedMember string) map[string]any {
	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
	}
	data := map[string]any{
		"group": meta.Name,
		"error": map[string]any{"message": errMsg},
	}
	if failedMember != "" {
		data["failed_members"] = []any{failedMember}
	}
	return data
}

// nodeIdxOf resolves a member name to a node index. The name is always from
// the current graph's members/exits so it is guaranteed to exist.
func nodeIdxOf(g *graph.Graph, name string) int {
	idx, _ := g.NodeIndex(name)
	return idx
}

// groupExitOutputIsPrivate derives a group exit's public-output policy solely
// from the current compiled graph. Group results can originate on a remote
// runner, so their claimed policy is never authoritative. An absent or invalid
// graph node is private by default: a bad exit must not turn into a disclosure.
func groupExitOutputIsPrivate(g *graph.Graph, name string) bool {
	if g == nil {
		return true
	}
	idx, ok := g.NodeIndex(name)
	if !ok || idx < 0 || idx >= g.NodeCount() {
		return true
	}
	node := g.NodeAt(idx)
	if node.Name != name {
		return true
	}
	return node.Output != nil && node.Output.Private
}

// groupOnErrorFatal maps the group's OnError strategy to whether a group
// failure fails the whole execution. Uses the node OnError constants
// (types/node.go): OnErrorContinue and OnErrorOutput => non-fatal (execution
// continues, the latter additionally routing to the group's declared
// error_outputs — see commitGroup's routeToErrorOutput); OnErrorStop or
// empty/default => fatal.
//
// main_output never reaches here: validateGroupOnError
// (engine/graph/group_compile.go) rejects it at compile time, because unlike a
// node, a group that failed before committing has no single member output to
// stand in for "the group's main result" — there is no principled payload
// main_output could carry. See NODE-GROUP-COLOCATION.md §12.2.
//
// The catch-all is deliberate rather than a switch: an unknown value cannot
// arrive (compile rejects it), and fatal is the safe reading if one somehow did
// — a graph snapshot decoded from an older writer that predates the validation
// would fail the execution rather than silently continue past a failed group.
func groupOnErrorFatal(onErr string) bool {
	return onErr != string(types.OnErrorContinue) && onErr != string(types.OnErrorOutput)
}

// downstreamUnitArrivals maps a source unit's fired boundary exits to per-
// downstream-unit arrivals — the unit-graph analogue of downstreamArrivals
// (atomic.go). The active boundary ports select which downstream edges carry
// an active arrival (execute) vs an inactive one (skip propagation). Consumed
// by CommitGroup, which does the atomic in-degree/active/threshold counting.
//
// g.UnitOutEdges(srcUnit) mixes two edge families for a group unit: ordinary
// member boundary edges (graph.UnitEdge.ErrorEdge == false) and the
// synthetic error edges from GroupMeta.ErrorOutputs (ErrorEdge == true). Only
// one family is ever active for a given commit: a successful group lights up
// real exits and never routeToErrorOutput; a failed group routed to
// error_output reports no real exits (see commitGroup) and routeToErrorOutput
// activates every error edge unconditionally — the group has exactly one
// error port and it fired. Both families still contribute their
// ArrivalCount so a downstream unit's static in-degree is correct regardless
// of which branch actually becomes active.
func (e *Engine) downstreamUnitArrivals(g *graph.Graph, srcUnit int, exits []GroupExit, routeToErrorOutput bool) []DownstreamArrival {
	active := make(map[string]bool, len(exits))
	for _, ex := range exits {
		active[boundaryKey(nodeIdxOf(g, ex.NodeName), ex.Port)] = true
	}
	byDst := make(map[int]DownstreamArrival)
	for _, ue := range g.UnitOutEdges(srcUnit) {
		a, ok := byDst[ue.DstUnit]
		if !ok {
			execType := TaskTypeNodeExec
			target := g.UnitNodeIndex(ue.DstUnit)
			name := g.NodeAt(target).Name
			if g.UnitKindAt(ue.DstUnit) == graph.UnitGroup {
				gm := g.GroupMetaAt(ue.DstUnit)
				execType, target, name = TaskTypeGroupExec, gm.EntryIdx, gm.Name
			}
			a = DownstreamArrival{
				NodeName:     name,
				NodeIdx:      target,
				UnitIdx:      ue.DstUnit,
				MergeMode:    g.UnitMergeMode(ue.DstUnit),
				ExecTaskType: execType,
			}
		}
		a.ArrivalCount++
		if ue.ErrorEdge {
			if routeToErrorOutput {
				a.ActiveCount++
			}
		} else if active[boundaryKey(ue.Src.NodeIdx, ue.Src.Port)] {
			a.ActiveCount++
		}
		byDst[ue.DstUnit] = a
	}
	out := make([]DownstreamArrival, 0, len(byDst))
	for _, a := range byDst {
		out = append(out, a)
	}
	return out
}

// boundaryKey builds a lookup key from a node index and port name for matching
// active boundary exits against unit out-edges.
func boundaryKey(nodeIdx int, port string) string {
	return fmt.Sprintf("%d\x00%s", nodeIdx, port)
}
