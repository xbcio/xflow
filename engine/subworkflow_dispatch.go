package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// subWorkflowWaitTTL is the ceiling on the lease one parked sub-workflow node
// holds. It is deliberately not DefaultLeaseTTL: the sweeper that reclaims an
// expired wait is also the only backstop for a lost completion notification, so
// the TTL is the notification's worst-case latency — and a 60-second clock would
// make a parent waiting on a 30-minute child pay ~30 write-amplifying reclaims
// to learn a result it could not have learned sooner.
//
// A node whose own timeout is shorter parks for the shorter value: the node
// deadline, not the notification budget, is what that author asked to bound.
const subWorkflowWaitTTL = 5 * time.Minute

// subWorkflowChildID derives the child execution id for one generation of one
// node. Deterministic and collision-free across replicas, in the same family as
// the other derived ids (IdempotentExecutionID, DeterministicExecutionID), and
// it deliberately includes the activation so that a cyclic re-entry of the same
// node starts its own child rather than resuming the previous activation's.
//
// Sibling of the derived-id rule in the design: the id used to say "attempt",
// but the node's Attempt is re-bumped by a reclaim, so a redelivery would have
// minted a second child for the same logical wait. The generation carried on
// the task is the stable value; see Task.Generation.
func subWorkflowChildID(parent types.ExecutionID, nodeName string, activationID, generation int) types.ExecutionID {
	h := sha256.New()
	h.Write([]byte(parent))
	h.Write([]byte{0})
	h.Write([]byte(nodeName))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(activationID)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(generation)))
	sum := h.Sum(nil)
	return types.ExecutionID("exec-sw-" + hex.EncodeToString(sum[:16]))
}

// subWorkflowGeneration normalizes the absent-generation encoding: a task that
// predates Task.Generation, or a first dispatch, wants child 1.
func subWorkflowGeneration(generation int) int {
	if generation <= 0 {
		return 1
	}
	return generation
}

// subWorkflowWaitLeaseTTL is min(node timeout, subWorkflowWaitTTL), with an
// unbounded or unset node timeout reading as the ceiling.
func subWorkflowWaitLeaseTTL(nodeTimeout time.Duration) time.Duration {
	if nodeTimeout > 0 && nodeTimeout < subWorkflowWaitTTL {
		return nodeTimeout
	}
	return subWorkflowWaitTTL
}

// subWorkflowChild is one dispatch's decision: the child execution it owns and
// the depth that child runs at.
type subWorkflowChild struct {
	ID    types.ExecutionID
	Depth int
}

// handleSubWorkflowNode consumes one TaskTypeSubWorkflow task: it claims the
// node, resolves the compiled binding, starts (or resumes) the child execution,
// and parks the node at waiting until the child concludes.
//
// Like the other engine-owned task types the task never had a lease when it
// arrived, so this handler acquires its own — the park is fenced on that lease
// identity, and so is the completion that later commits under it.
//
// The handled return is always true: the type is engine-owned, and every
// refusal (no resolver, unresolved binding, depth exceeded, resolution
// mismatched, store failure) is a node failure through the ordinary retry and
// on_error pipeline, never a task routed elsewhere and never a silent success.
//
// flush is accepted for the dispatcher's uniform signature and is deliberately
// not threaded further: every commit on this path goes through the ordinary
// node committers, which deliver their own outbox (finishAtomicCommit) — and
// must, because these intents are written by a task type FlushOutbox's loop
// does not handle, so no outer re-poll would pick them up.
func (e *Engine) handleSubWorkflowNode(ctx context.Context, task *Task, flush bool) (bool, error) {
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return true, err
	}
	if !active {
		// Same classification rule as the advance and skip branches: an intent
		// the engine owns but cannot apply is a classified drop, not a handled
		// task.
		return true, e.inactiveExecutionError(ctx, task.ExecutionID, task.provabilityAnchor())
	}
	if task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
		return true, fmt.Errorf("sub-workflow node index %d is out of range", task.NodeIdx)
	}
	meta := g.NodeAt(task.NodeIdx)
	if meta.Type != types.SubWorkflowNodeType || meta.Name != task.NodeName {
		return true, fmt.Errorf("task %q/%q is not a sub-workflow node", task.ExecutionID, task.NodeName)
	}
	return true, e.executeSubWorkflow(ctx, task, g, meta)
}

// executeSubWorkflow is the claim → start-child → park sequence. The three
// steps are deliberately not atomic with each other (they cannot be: the child
// lives under another execution's Redis hash tag), so each is idempotent and
// the lease sweeper's redispatch replays the whole sequence: the derived child
// id is found rather than re-created, and the park is re-written under the new
// lease generation.
func (e *Engine) executeSubWorkflow(ctx context.Context, task *Task, g *graph.Graph, meta graph.NodeMeta) error {
	// Read before the claim: a read failure here leaves no lease behind.
	execDeadline, err := e.executionDeadline(ctx, task.ExecutionID, g)
	if err != nil {
		return err
	}
	lease, err := e.acquireSubWorkflowLease(ctx, task, meta)
	if err != nil {
		return err
	}
	if lease == nil {
		// Another dispatch owns the node — a live park, a commit in flight, or
		// a terminal outcome. All are benign for this delivery.
		return nil
	}
	// The generation this dispatch serves, and the wait it may already own. The
	// task's Generation is wired through every path a dispatch arrives by: a
	// first delivery carries no generation (read as generation 1), a retry
	// intent carries the next generation in its encoded task, and a reclaim's
	// redelivery carries what the lease persisted — AcquireTaskLease stores the
	// generation with the lease (lease_generation in rstate, LeaseGeneration in
	// the local store) and ListExpiredLeases reads it back, so ReclaimLease
	// rebuilds the task with the generation that actually claimed the node.
	// The link is NOT a source for this value: inside the "child minted, park
	// not yet written" window it still names the PREVIOUS generation's child,
	// and inferring from it would re-derive that child's id, resume it, and let
	// its terminal verdict decide the node — exactly what the generation fence
	// exists to prevent.
	prev, linked, err := e.subWorkflowLink(ctx, task.ExecutionID, task.NodeName)
	if err != nil {
		return err
	}
	generation := subWorkflowGeneration(task.Generation)
	child, cause := e.startSubWorkflowChild(ctx, task, g, meta, generation)
	if cause == nil {
		// The status read inside startSubWorkflowChild happened before the child
		// existed, and the inline cancel cascade walks recorded links — which this
		// dispatch has not written yet. A cancel landing between the two is
		// therefore visible to neither, so the dispatch re-reads the parent here,
		// before it commits to the park. Without this read the node would park and
		// the child would run until the wait's reclaim (up to subWorkflowWaitTTL)
		// rediscovered the parent, even though the answer is already decided.
		canceling, err := e.subWorkflowParentCanceling(ctx, task.ExecutionID)
		if err != nil {
			// Fail the dispatch instead of parking on an unreadable status: a park
			// commits the wait for a parent that may already be canceled, and undoing
			// that commit is exactly the reclaim delay this check exists to avoid.
			// The failed delivery is retried by the sweeper, and the retry re-reads.
			return fmt.Errorf("re-read parent execution %q after starting child %q: %w", task.ExecutionID, child.ID, err)
		}
		if canceling {
			cause = errSubWorkflowParentCanceling
		}
	}
	if errors.Is(cause, errSubWorkflowParentCanceling) {
		// The parent is on its way down: this delivery raced the inline cancel
		// cascade, or the cancel landed before the child existed. Either way the
		// node is not going to run, so this dispatch stops the child this
		// generation started (if any) and releases its claim — leaving the node
		// Running would hand the sweeper a lease it can only reclaim into the
		// same abandoned path, once per TTL, until the execution expires.
		_ = e.cancelSubWorkflowChild(ctx, child.ID)
		if _, err := e.state.RevokeLease(ctx, lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseToken); err != nil && e.logger != nil {
			e.logger.Error("release sub-workflow claim for a canceling parent failed",
				"execution_id", string(lease.Task.ExecutionID), "node_name", lease.Task.NodeName, "err", err)
		}
		return nil
	}
	if cause != nil {
		return e.failSubWorkflowStart(ctx, lease, g, meta, cause)
	}
	// A wait that this dispatch resumes keeps the deadline it was parked with.
	// The reclaim renews the lease, and with it the notification budget, but it
	// must not renew the wait's own end: otherwise a bounded wait could never
	// reach its bound and the node timeout would be decorative. A node that
	// declares no timeout runs under the resolved engine default, exactly as it
	// does for every other node type — see resolveNodeTimeout.
	nodeTimeout := e.resolveNodeTimeout(meta.Timeout)
	resumed := linked && prev.ChildExecutionID == child.ID
	waitEnd := lease.Deadline()
	if nodeTimeout > 0 {
		waitEnd = lease.IssuedAt.Add(nodeTimeout)
		if resumed {
			waitEnd = prev.WaitDeadline
		}
	}
	// The wait also ends with the execution: a parent cannot wait past its own
	// deadline, whatever the node timeout says, and an unbounded node still
	// ends with its execution. Re-clamping a resumed wait is idempotent.
	waitEnd = clampToExecutionDeadline(execDeadline, waitEnd)
	// Park first, conclude second — the order is the invariant every
	// conclusion below depends on: the park is what writes the link and the
	// node's ChildExecutionID, and a commit preserves the claimed node's id, so
	// a conclusion may only run once the child is on the record. A FIRST
	// dispatch can reach the conclusion (a fast child can finish between the
	// start and the check, and under load that window is wide) and it is
	// exactly that dispatch which has nothing recorded yet: concluding first
	// would commit the node with an empty ChildExecutionID and no link, and a
	// finished node keeps its child id for the drill-down — losing it there
	// loses it for good. Parking first also re-points the link at this claim's
	// lease identity on a redelivery, so the conclusion here, a notification
	// racing it, and a later redelivery all fence on one identity.
	if accepted, err := e.parkSubWorkflowNode(ctx, lease, child, generation, waitEnd); err != nil {
		return err
	} else if !accepted {
		// Refused by the fence: the node moved on (revoked, committed under a
		// newer lease) between the claim and the park. Whoever owns the node
		// now owns the link and the child; do not gamble a conclusion commit on
		// a fence that has already refused this claim.
		return nil
	}
	// The child can already be concluded here: a redelivery after a reclaim
	// that finds the completion notification was lost. Concluding now is the
	// backstop's whole purpose; parking without concluding would wait on a
	// notification that has already been delivered and dropped. It is checked
	// before the wait's deadline because a finished child is a result and not a
	// timeout, however late this dispatch is.
	concluded, err := e.concludeTerminalChild(ctx, lease, g, meta, child)
	if err != nil {
		return err
	}
	if concluded {
		return nil
	}
	if nodeTimeout > 0 && resumed && !time.Now().UTC().Before(prev.WaitDeadline) {
		// The parked wait end may be the execution's own deadline (waitEnd is
		// clamped to it above), so an over wait does not say which budget ran
		// out. The execution's deadline wins a tie: it is settled as the
		// execution's timeout, which cancels the child itself, and on_error never
		// sees it. A graph without settings.timeout is not read at all.
		if handled, err := e.expireIfExecutionDeadlineReached(ctx, task.ExecutionID, g, time.Now()); err != nil {
			return err
		} else if handled {
			return nil
		}
		// The wait has used up the node timeout while the child is still
		// running: fail the node through the ordinary pipeline — the conclusion
		// a runner-executed node reaches when its handler outlives the same
		// deadline. A retry then opens the next generation's child, as every
		// retry of this node does. The node is parked by now, and every
		// backend's commit fence accepts a commit from waiting — the completion
		// path commits from there as a matter of course.
		//
		// The node's verdict is committed BEFORE the child is stopped, which is
		// the one order that keeps the reason deterministic: the cancel
		// finalizes the child and fires the completion notification, and with
		// the wait parked under a live link that notification could otherwise
		// get its commit in first and report this node's own timeout as "the
		// child was canceled". Committing first fences the notification out —
		// the node is terminal, so its commit is dropped — and the abandoned
		// child is stopped either way.
		err := e.failSubWorkflowStart(ctx, lease, g, meta, fmt.Errorf(
			"node %q: the wait on child execution %q exceeded the node timeout %s",
			task.NodeName, child.ID, nodeTimeout))
		_ = e.cancelSubWorkflowChild(ctx, child.ID)
		return err
	}
	return nil
}

// acquireSubWorkflowLease claims the node for this dispatch, mirroring
// BuildTaskLease's identity and attempt accounting: the attempt counts retries
// within one activation and restarts on a cyclic re-entry.
//
// A nil lease with a nil error means the store refused the claim, which is
// benign here: the node is running under a live lease, already parked, or
// terminal, and every one of those is another dispatch's business.
//
// A store with the RouteCheckedLeaseAcquirer capability gets the same treatment
// BuildTaskLease gives it: the execution-liveness and node-route decisions are
// folded into the lease write instead of being made by separate reads, so an
// execution or route that changes underneath the dispatch can no longer still
// receive a lease. The graph is normally a cache hit on this path
// (executeSubWorkflow already loaded it), and its AllowCycles is what the
// acquire's route rules need — never a backend guess.
func (e *Engine) acquireSubWorkflowLease(ctx context.Context, task *Task, meta graph.NodeMeta) (*TaskLease, error) {
	leaseID, leaseToken := newLeaseCredentials()
	lease := &TaskLease{
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Task:       *task,
		IssuedAt:   time.Now().UTC(),
		TTL:        subWorkflowWaitLeaseTTL(e.resolveNodeTimeout(meta.Timeout)),
	}
	acquirer, routeChecked := e.state.(RouteCheckedLeaseAcquirer)
	var prev *NodeSnapshot
	if routeChecked {
		g, err := e.loadGraph(ctx, task.ExecutionID)
		if err != nil {
			return nil, err
		}
		if g == nil {
			// No cached and no persisted graph: the execution cannot be routed.
			// The legacy call leaves this decision to the backend, whose acquire
			// refuses an execution it no longer holds; that refusal is the benign
			// no-op here. Falling back to it instead would write a lease with no
			// route check at all — the very race this capability exists to close
			// — so a store that still holds the execution but not its graph gets
			// the same benign refusal rather than an acquire made blind.
			return nil, nil
		}
		res, err := acquirer.AcquireTaskLeaseChecked(ctx, lease, LeaseAcquireOptions{AllowCycles: g.AllowCycles()})
		if err != nil {
			return nil, fmt.Errorf("acquire sub-workflow lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if !res.ExecutionFound || types.IsTerminalExecutionStatus(res.ExecutionStatus) {
			// The acquire observed the execution as gone or finished. Evict any
			// cached graph so a late dispatch cannot keep routing against it.
			e.EvictExecution(task.ExecutionID)
			return nil, nil
		}
		if !res.Acquired {
			// A stale node route — another activation advanced, or a commit is
			// in flight — exactly like a refused legacy claim: not this
			// dispatch's business.
			return nil, nil
		}
		prev = res.Previous
	} else {
		var acquired bool
		var err error
		prev, acquired, err = e.state.AcquireTaskLease(ctx, lease)
		if err != nil {
			return nil, fmt.Errorf("acquire sub-workflow lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if !acquired {
			return nil, nil
		}
	}
	lease.Attempt = 1
	if prev != nil && prev.ActivationID == task.ActivationID {
		lease.Attempt = prev.Attempt + 1
	}
	return lease, nil
}

// subWorkflowLink reads the link this node's wait may have recorded. A store
// without the capability reports no link: the caller is on the dispatch path,
// which refuses such a store before it creates anything.
func (e *Engine) subWorkflowLink(ctx context.Context, id types.ExecutionID, nodeName string) (SubWorkflowLink, bool, error) {
	store, ok := e.state.(SubWorkflowLinkStore)
	if !ok {
		return SubWorkflowLink{}, false, nil
	}
	link, found, err := store.GetSubWorkflowLink(ctx, id, nodeName)
	if err != nil {
		return SubWorkflowLink{}, false, fmt.Errorf("read sub-workflow link %q/%q: %w", id, nodeName, err)
	}
	return link, found, nil
}

// startSubWorkflowChild resolves the compiled binding, checks the runtime
// bounds, evaluates the input map, and starts the child execution if this
// generation has not already started it. generation is the caller's resolved
// generation, which is not always the task's own — see executeSubWorkflow.
//
// Every returned error is a node-start failure, and they are classified rather
// than uniform: a missing binding, a missing capability, a depth overrun and a
// definition-hash mismatch are permanent (retrying cannot change them), while a
// resolver or store failure keeps its own classification so a transient outage
// stays retriable.
func (e *Engine) startSubWorkflowChild(ctx context.Context, task *Task, g *graph.Graph, meta graph.NodeMeta, generation int) (subWorkflowChild, error) {
	binding := meta.SubWorkflow
	if binding == nil {
		return subWorkflowChild{}, fmt.Errorf("node %q: %w (%w)", task.NodeName, ErrSubWorkflowUnresolved, types.ErrPermanent)
	}
	if e.subWorkflowResolver == nil {
		return subWorkflowChild{}, fmt.Errorf("node %q: %w (%w)", task.NodeName, ErrSubWorkflowUnsupported, types.ErrPermanent)
	}
	if _, ok := e.state.(SubWorkflowLinkStore); !ok {
		return subWorkflowChild{}, fmt.Errorf("node %q: state store does not implement SubWorkflowLinkStore: %w (%w)",
			task.NodeName, ErrSubWorkflowUnsupported, types.ErrPermanent)
	}
	snap, err := e.state.GetExecution(ctx, task.ExecutionID)
	if err != nil {
		return subWorkflowChild{}, fmt.Errorf("read parent execution %q: %w", task.ExecutionID, err)
	}
	if snap == nil {
		return subWorkflowChild{}, ErrExecutionInactive
	}
	// The child identity is derived before the resolution — deliberately, not
	// incidentally: it needs nothing the resolver answers, and the canceling
	// branch below must be able to name the child an earlier dispatch may have
	// started without resolving a target it is not going to run.
	child := subWorkflowChild{
		ID:    subWorkflowChildID(task.ExecutionID, task.NodeName, task.ActivationID, generation),
		Depth: snap.Depth + 1,
	}
	if snap.Status == types.ExecutionStatusCanceling {
		// The reclaim backstop's half of the cancel cascade: the parent was
		// canceled after this task was queued and before it was consumed, so no
		// child of this generation may start (or keep running) now.
		return child, errSubWorkflowParentCanceling
	}
	if child.Depth > e.maxSubWorkflowDepth {
		// Degraded to a node failure rather than a submission refusal: the
		// compiler bounds the static reference chain, and this is the bound for
		// the chains it cannot see. Fail closed — the alternative is running an
		// unbounded recursion.
		return subWorkflowChild{}, fmt.Errorf("node %q: child execution depth %d exceeds the limit %d: %w (%w)",
			task.NodeName, child.Depth, e.maxSubWorkflowDepth, graph.ErrSubWorkflowDepth, types.ErrPermanent)
	}
	// Resolve the binding against the namespace the PARENT was registered under,
	// carried on its snapshot. The ctx namespace is not usable here: this task
	// came off a queue whose worker starts from context.Background(), so ctx
	// only names the deployment. A snapshot written before the field existed
	// carries no namespace; fall back to the ctx behavior it was created with.
	refNamespace := snap.Namespace
	if refNamespace == "" {
		refNamespace = string(namespace.FromContext(ctx))
	}
	target, err := e.subWorkflowResolver.ResolveSubWorkflow(ctx, graph.SubWorkflowRef{
		Namespace:      refNamespace,
		Name:           binding.Name,
		Version:        binding.Version,
		DefinitionHash: binding.DefinitionHash,
	})
	if err != nil {
		return subWorkflowChild{}, fmt.Errorf("resolve sub-workflow %q@%q: %w", binding.Name, binding.Version, err)
	}
	// Re-verified rather than trusted: the binding is the pin, and a resolver
	// that quietly served a newer revision would turn this node into "whatever
	// is latest", which is exactly what pinning exists to prevent.
	if target.DefinitionHash != binding.DefinitionHash {
		return subWorkflowChild{}, fmt.Errorf("sub-workflow %q@%q: resolver returned definition %q, the compiled binding pins %q: %w",
			binding.Name, binding.Version, target.DefinitionHash, binding.DefinitionHash, types.ErrPermanent)
	}
	if target.Graph == nil {
		return subWorkflowChild{}, fmt.Errorf("sub-workflow %q@%q: resolver returned no graph: %w (%w)",
			binding.Name, binding.Version, ErrSubWorkflowUnresolved, types.ErrPermanent)
	}
	if target.Graph.FAF() {
		// The compiler already rejects this shape; the check is repeated because
		// a FAF graph has no persisted state for a child to run in.
		return subWorkflowChild{}, fmt.Errorf("sub-workflow %q@%q is a FAF workflow, which cannot run as a child execution: %w",
			binding.Name, binding.Version, types.ErrPermanent)
	}
	params, err := e.subWorkflowParams(ctx, task, g, meta)
	if err != nil {
		return subWorkflowChild{}, err
	}

	existing, err := e.state.GetExecution(ctx, child.ID)
	if err != nil {
		return subWorkflowChild{}, fmt.Errorf("read child execution %q: %w", child.ID, err)
	}
	if existing != nil {
		// An earlier dispatch of this generation already created the child — a
		// crash between the create and the park, or a reclaim. Creating again
		// would overwrite its live state, because every backend's create is an
		// unconditional write, so this is a resume and not a create.
		return child, nil
	}

	entries, err := e.subWorkflowChildTasks(binding, subWorkflowEntry(meta), target.Graph, child.ID)
	if err != nil {
		return subWorkflowChild{}, err
	}
	// The resolver's explicit answer is the identity: a registry-compiled graph
	// cannot be asked for its definition (the distributed registry decodes the
	// graph through JSON, and ExpandedDefinition does not survive that), so a
	// graph-derived id would leave every production child unattributed. The
	// graph is only a fallback for a resolver that returned a compiled
	// definition-carrying graph of its own.
	childWorkflowID := target.WorkflowID
	if childWorkflowID == "" {
		if def := target.Graph.ExpandedDefinition(); def != nil {
			childWorkflowID = types.WorkflowID(def.ID)
		}
	}
	// A version with no id is a half identity no reader can resolve back to a
	// workflow, so it is recorded only alongside the id — the same rule
	// attachSubmissionMetadata applies to a directly submitted execution.
	childWorkflowVersion := ""
	if childWorkflowID != "" {
		childWorkflowVersion = target.Version
	}
	childSnap := &ExecutionSnapshot{
		ID:     child.ID,
		Graph:  target.Graph,
		Status: types.ExecutionStatusRunning,
		Params: params,
		// The child's own workflow identity, not the parent's: this execution
		// runs the resolved target, and a reader drilling down from the parent's
		// node needs to name what the child actually ran. The version is the
		// resolver's concrete answer, which may be the default for an
		// unversioned record.
		WorkflowID:      childWorkflowID,
		WorkflowVersion: childWorkflowVersion,
		// The link back to the parent. ParentID reuses the field the snapshot
		// already reserved for a sub-execution; ParentNode is what names the
		// node whose wait this child satisfies.
		ParentID:   task.ExecutionID,
		ParentNode: task.NodeName,
		Depth:      child.Depth,
		// Recorded explicitly: the child was started by a subworkflow node, not
		// by the execute face.
		TriggerKind: types.TriggerKindSubWorkflow,
		// The child is registered under the same namespace as its parent: the
		// binding that created it was resolved in that namespace.
		Namespace: snap.Namespace,
		// Trace identity is inherited so the child's spans land on the parent's
		// trace. Runtime and scope deliberately are NOT: $vars belongs to the
		// child's own definition, and its two sources would be indistinguishable
		// if the parent's were merged in. What the parent wants to pass travels
		// in params, evaluated from the node's input map.
		TraceID:      snap.TraceID,
		SpanID:       snap.SpanID,
		TraceCarrier: snap.TraceCarrier,
	}
	if _, err := e.startExecution(ctx, childSnap, entries); err != nil {
		return subWorkflowChild{}, fmt.Errorf("start child execution %q: %w", child.ID, err)
	}
	return child, nil
}

// subWorkflowChildTasks assembles the child's initial tasks, mirroring Invoke
// when the node names an entry and Submit otherwise: the named entry node runs
// and every other root is resolved as skipped, so the unselected roots neither
// hold the completion counter open nor block a fan-in they share with the entry.
func (e *Engine) subWorkflowChildTasks(binding *graph.SubWorkflowBinding, entry string, target *graph.Graph, childID types.ExecutionID) ([]initialTask, error) {
	if entry == "" {
		return submitInitialTasks(childID, target), nil
	}
	idx, ok := target.EntryIndex(entry)
	if !ok {
		// The compiler validated the entry against the target it resolved; a
		// miss here means the resolver answered with a different revision than
		// the pin, which the hash check above should have caught first.
		return nil, fmt.Errorf("sub-workflow %q@%q: entry node %q is not an entry of the resolved definition: %w",
			binding.Name, binding.Version, entry, types.ErrPermanent)
	}
	unitIdx := target.UnitIndexForNode(idx)
	var first Task
	if target.UnitKindAt(unitIdx) == graph.UnitGroup {
		gm := target.GroupMetaAt(unitIdx)
		first = Task{ExecutionID: childID, NodeName: gm.Name, NodeIdx: gm.EntryIdx, UnitIdx: unitIdx, Type: TaskTypeGroupExec}
	} else {
		node := target.NodeAt(idx)
		first = Task{ExecutionID: childID, NodeName: node.Name, NodeIdx: idx, UnitIdx: unitIdx, Type: execTaskTypeForNode(node)}
	}
	first.ActivationID = 1
	tasks := []initialTask{{
		task:      first,
		operation: fmt.Sprintf("enqueue entry node %q", first.NodeName),
	}}
	for _, skip := range UnselectedRootSkips(childID, target, unitIdx) {
		tasks = append(tasks, initialTask{
			task:      skip,
			operation: fmt.Sprintf("skip unselected root %q", skip.NodeName),
		})
	}
	return tasks, nil
}

// subWorkflowEntry reads the optional entry node out of the node's parameters.
//
// The binding deliberately does not carry it: an entry was never part of the pin
// (the same pinned revision serves an entry and a default start), and the
// compiler validated it against the resolved target, so the parameters remain
// its one source of truth.
func subWorkflowEntry(meta graph.NodeMeta) string {
	entry, _ := meta.Parameters["entry"].(string)
	return entry
}

// subWorkflowParams evaluates the node's "input" map into the child's
// submission params.
//
// The environment is the exact one the runner boundary would build for this
// node — buildInput plus exprx.BuildExprEnv — because "input" is a boundary
// parameter in every other respect (it is exempted from runner-side evaluation
// in graph.evaluableParams precisely so this single evaluation is the only
// one). Evaluating it against a different environment here would make the same
// expression mean two things depending on which side of the engine it ran on.
func (e *Engine) subWorkflowParams(ctx context.Context, task *Task, g *graph.Graph, meta graph.NodeMeta) (map[string]any, error) {
	raw, present := meta.Parameters["input"]
	if !present || raw == nil {
		return nil, nil
	}
	input, err := e.buildInput(ctx, task, g)
	if err != nil {
		return nil, fmt.Errorf("build sub-workflow input for %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	env := exprx.BuildExprEnv(input, nil)
	evaluated, err := subWorkflowEvaluateParam(raw, env)
	if err != nil {
		return nil, fmt.Errorf("evaluate sub-workflow input for %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	params, ok := evaluated.(map[string]any)
	if !ok {
		// The compiler requires a map, so this is a malformed compiled artifact
		// rather than author error; fail closed instead of running a child with
		// unbindable params.
		return nil, fmt.Errorf("node %q: parameters.input evaluated to %T, want a map: %w", task.NodeName, evaluated, types.ErrPermanent)
	}
	return params, nil
}

// subWorkflowEvaluateParam recursively renders templates in an expression map.
// It mirrors execution.evaluateParamValue deliberately — the two must agree on
// what a nested template means, and the engine/evaluation-parity test pins that
// agreement rather than trusting two copies to stay in step.
func subWorkflowEvaluateParam(value any, env map[string]any) (any, error) {
	switch v := value.(type) {
	case string:
		return exprx.RenderTemplate(v, env)
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, child := range v {
			evaluated, err := subWorkflowEvaluateParam(child, env)
			if err != nil {
				return nil, err
			}
			out[k] = evaluated
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			evaluated, err := subWorkflowEvaluateParam(child, env)
			if err != nil {
				return nil, err
			}
			out[i] = evaluated
		}
		return out, nil
	default:
		return value, nil
	}
}

// parkSubWorkflowNode moves the claimed node to waiting and records the link
// the completion path will read back.
//
// The link's lease identity is the claim's, so the completion commits under the
// same fence any other result does. Its deadline is the caller's resolved wait
// end — the node's own timeout when it declares one, the lease deadline
// otherwise — because the reclaim re-parks under a fresh lease and must not
// move the wait's end with it.
func (e *Engine) parkSubWorkflowNode(ctx context.Context, lease *TaskLease, child subWorkflowChild, generation int, waitDeadline time.Time) (bool, error) {
	store, ok := e.state.(SubWorkflowLinkStore)
	if !ok {
		// Unreachable: the plan checked the capability before creating anything.
		return false, fmt.Errorf("state store does not implement SubWorkflowLinkStore: %w", ErrSubWorkflowUnsupported)
	}
	link := SubWorkflowLink{
		ChildExecutionID: child.ID,
		ParentNode:       lease.Task.NodeName,
		Generation:       generation,
		Depth:            child.Depth,
		WaitDeadline:     waitDeadline,
		LeaseID:          lease.LeaseID,
		LeaseToken:       lease.LeaseToken,
		Attempt:          lease.Attempt,
		ActivationID:     lease.Task.ActivationID,
	}
	accepted, err := store.BeginSubWorkflowWait(ctx, lease, link, nil)
	if err != nil {
		return false, fmt.Errorf("park sub-workflow node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return accepted, nil
}

// failSubWorkflowStart turns a start failure into the node's normal failure
// pipeline: retry if the node's policy allows and the cause is retriable, else
// on_error, else a failed node that aborts the execution.
//
// Retrying is safe here in a way it is not for a partially applied expansion:
// a start failure either created nothing, or created a child under the current
// generation's derived id — and a retry advances the generation, so it can
// never re-enter the same child.
func (e *Engine) failSubWorkflowStart(ctx context.Context, lease *TaskLease, g *graph.Graph, meta graph.NodeMeta, cause error) error {
	if errors.Is(cause, ErrExecutionInactive) {
		// The parent finished or was canceled under us; nothing to commit.
		return nil
	}
	privateOutput := privateOutputForTask(g, &lease.Task)
	outcome, err := e.commitNodeErrorOutcome(ctx, lease, meta, privateOutput, cause, nil, nil,
		e.commitLegacyNodeWithClassification)
	if subWorkflowCommitDropped(outcome) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("fail sub-workflow start %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}
