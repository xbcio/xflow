package rstate

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Workflow concurrency (engine.ConcurrencyLimitedStateStore)
//
// The slot set lives in a workflow-tagged hash slot while every execution key
// lives in its own {id} slot, and Redis Cluster will not let one Lua script
// touch both. Admission and release are therefore separate transitions from the
// execution writes they bracket:
//
//	charge slot (workflow slot)  ->  create execution + tag (execution slot)
//	execution terminal (execution slot)  ->  release slot (workflow slot)
//
// Neither window is closed by atomicity; both are closed by ordering and made
// recoverable. A slot charged for an execution that never landed leaves no
// execution record, and a terminal execution whose release was lost still names
// the set it belongs to — so the reconciler can sweep both. See
// concurrency_reconcile.go.
// ---------------------------------------------------------------------------

// concurrencySlotTTL bounds how long a slot set outlives its last write. It
// tracks the execution retention, floored well above the reconciler's cadence
// so a leaked slot is always observable before the set can expire underneath
// the sweep that is supposed to find it.
func (s *Store) concurrencySlotTTL() time.Duration {
	ttl := s.execTTL
	if s.transientTTL > ttl {
		ttl = s.transientTTL
	}
	if ttl < time.Hour {
		ttl = time.Hour
	}
	return ttl
}

// concurrencySlotMember names one execution's slot inside its workflow's ZSET.
// The score is admission time, which is what a QUEUE-mode promotion orders by;
// the member is the execution ID alone because the set belongs to exactly one
// workflow.
func concurrencySlotMember(id types.ExecutionID) string {
	return string(id)
}

// concurrencyTagKey records which workflow a slot-bearing execution was
// admitted under, in the execution's own hash slot. Release has only the
// execution ID in hand while the slot key is derived from the workflow, so
// without this back-reference a terminal execution could not find the set it
// has to leave.
func concurrencyTagKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "conc_wf")
}

var _ engine.ConcurrencyLimitedStateStore = (*Store)(nil)
var _ engine.ConcurrencyQueueExpirer = (*Store)(nil)
var _ engine.ConcurrencyQueueInspector = (*Store)(nil)

// registerConcurrencyWorkflow records that a workflow has slot state, so the
// sweep can find the set it has to inspect.
//
// It runs BEFORE the charge it announces. That ordering is what makes the index
// trustworthy: registering afterwards would leave a window in which a charged
// slot is not discoverable, and a leak in that window is invisible to the only
// mechanism that repairs leaks. Registering first instead can leave a tag whose
// set never materialized (a refused admission, or a crash before the charge),
// which costs the sweep one read and is dropped the next time the set is found
// missing. A stale tag is a hint the sweep discards; a lost tag is a leak
// nobody sweeps.
//
// wait is how long this call's state has to stay discoverable beyond the slot
// TTL, and it is what keeps a long queue_timeout visible: the index is the only
// path the expiry sweep has to a workflow's line, so an index expiring on the
// slot TTL alone would hide waiters whose deadlines are further out than that
// — the tag would lapse and nothing would ever expire them. Callers that
// announce a release pass 0; the touch is extend-only, so they cannot shorten
// what an admission asked for.
//
// The write cannot join the slot script — the index key carries no hash tag by
// design, so Redis Cluster would refuse a script naming both it and the slot
// set. It is therefore best effort, and a failure is logged rather than fatal:
// the limit is enforced by the slot set, and the index only decides whether the
// repair path can see it. A workflow that is written to again is registered
// again, so a transient failure heals itself.
func (s *Store) registerConcurrencyWorkflow(ctx context.Context, t namespace.Namespace, tag string, wait time.Duration) {
	if tag == "" {
		return
	}
	indexKey := concurrencyWorkflowIndexKey(t)
	ttl := s.concurrencySlotTTL() + wait
	if err := concurrencyIndexTouchLua.Run(ctx, s.rdb, []string{indexKey}, tag, ttl.Milliseconds()).Err(); err != nil {
		if s.logger != nil {
			s.logger.Error("workflow_concurrency_index_add_failed", "namespace", string(t), "tag", tag, "err", err)
		}
	}
}

// unregisterConcurrencyWorkflow removes a tag whose slot set is gone. Like the
// registration it cannot be atomic with the set's deletion; a failure only
// costs the sweep one read of a key that does not exist.
func (s *Store) unregisterConcurrencyWorkflow(ctx context.Context, t namespace.Namespace, tag string) {
	if tag == "" {
		return
	}
	if err := s.rdb.SRem(ctx, concurrencyWorkflowIndexKey(t), tag).Err(); err != nil && s.logger != nil {
		s.logger.Error("workflow_concurrency_index_remove_failed", "namespace", string(t), "tag", tag, "err", err)
	}
}

// concurrencyAdmitKeys is the key list workflowAdmitLua declares, in order. The
// list is built in one place because a script's KEYS and the keys it actually
// names have to agree, and because every key in it must land in the workflow's
// hash slot — Redis Cluster refuses a script whose keys do not.
func concurrencyAdmitKeys(t namespace.Namespace, workflow string) []string {
	tag := workflowDigestTag(t, workflow)
	return []string{
		concurrencySlotKey(t, workflow),
		concurrencyQueueKeyForTag(t, tag),
		concurrencyQueueSeqKeyForTag(t, tag),
		concurrencyCapacityKeyForTag(t, tag),
		concurrencyDeadlineKeyForTag(t, tag),
	}
}

// concurrencyReleaseKeys is the key list workflowReleaseLua declares, in order.
// It is a prefix of the admit list with the sequence counter left out: a
// release neither reads nor advances the line's order.
func concurrencyReleaseKeys(t namespace.Namespace, tag string) []string {
	return []string{
		concurrencySlotKeyForTag(t, tag),
		concurrencyQueueKeyForTag(t, tag),
		concurrencyDeadlineKeyForTag(t, tag),
		concurrencyCapacityKeyForTag(t, tag),
	}
}

// queuedOutboxHold is the readiness score stamped on a QUEUE-mode execution's
// parked intents: far enough in the future that neither the delivery paths nor
// the discovery index treats them as work.
//
// It is a fixed instant rather than a deadline the store re-evaluates, because
// a queued execution's intents become due when it is promoted and not when some
// time passes. The alternative — scoring them available-now and gating the
// reads on the queued status — puts the parked backlog into the index as due
// work, where it spends the discovery page budget the backlog exists to
// compete for and hides the real work behind it.
var queuedOutboxHold = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// Admission reply codes, as workflowAdmitLua returns them.
const (
	concurrencyAdmitRefusedLimit     = 0
	concurrencyAdmitAdmitted         = 1
	concurrencyAdmitQueued           = 2
	concurrencyAdmitRefusedQueueFull = 3
)

// workflowAdmitResult is what one slot decision decided, in the shape both
// admission paths read it: how the submission landed, where it stands if it is
// waiting, whether this call is what put it there, and what that freed up.
type workflowAdmitResult struct {
	// code is one of the concurrencyAdmit* reply codes.
	code int64
	// position is the 1-based place in line when the submission was queued.
	position int64
	// added is true when this call put the member in the slot set or the line,
	// as opposed to finding it there from an earlier attempt of the same
	// admission. Only an addition this call made may be given back.
	added bool
	// promoted are the execution IDs this call promoted into free slots.
	promoted []string
}

// admitWorkflowSlot runs the workflow's slot decision. Both admission paths go
// through it — the ordinary submission and the entry seed — because the decision
// must not be able to differ between them: a seed that charged a slot the submit
// path would have refused, or ordered the line differently, is exactly the
// divergence the shared script and this shared call exist to prevent.
//
// The tag is registered before the charge (registerConcurrencyWorkflow), so a
// charged slot is always already discoverable by the sweeper.
func (s *Store) admitWorkflowSlot(ctx context.Context, t namespace.Namespace, id types.ExecutionID, admission engine.ConcurrencyAdmission) (workflowAdmitResult, error) {
	queueMode := admission.Mode == types.ConcurrencyModeQueue
	deadlineMs := int64(0)
	queueWaitMs := int64(0)
	if queueMode {
		// The line's keys are expired by the same wait the execution's own keys
		// are lengthened by, so a queue_timeout longer than the slot TTL cannot
		// take the parked entry away before its deadline. Passed in milliseconds
		// because that is the granularity the script compares Redis's own PTTL
		// in, and a wait rounded down to whole seconds would under-cover a
		// sub-second one.
		queueWaitMs = admission.QueueTimeout.Milliseconds()
		if admission.QueueTimeout > 0 {
			// The clock starts here, when the execution is parked. Measured from
			// promotion it could never fire — a promoted execution is not waiting for
			// anything — and a workflow whose limit never frees would hold
			// submissions past their declared bound forever.
			deadlineMs = time.Now().UTC().Add(admission.QueueTimeout).UnixMilli()
		}
	}
	reply, err := workflowAdmitLua.Run(ctx, s.rdb,
		concurrencyAdmitKeys(t, admission.Workflow),
		concurrencySlotMember(id), admission.Limit,
		time.Now().UTC().Unix(), int(s.concurrencySlotTTL().Seconds()),
		concurrencyModeFlag(queueMode), admission.MaxQueued, deadlineMs, queueWaitMs,
	).Slice()
	if err != nil && err != redis.Nil {
		return workflowAdmitResult{}, fmt.Errorf("admit workflow %q: %w", admission.Workflow, err)
	}
	return workflowAdmitResult{
		code:     luaReplyInt(reply, 0),
		position: luaReplyInt(reply, 1),
		added:    luaReplyInt(reply, 2) == 1,
		promoted: luaReplyStrings(reply, 3),
	}, nil
}

// concurrencyIndexWait is how long a workflow's index entry has to outlive the
// slot TTL for one admission: its declared queue wait, and only in queue mode.
// A REJECT-mode submission never takes a place in the line, so there is nothing
// of its own waiting to be found; both admission paths ask the same question
// through this one function so a seed cannot register a shorter index than a
// submission of the same definition would.
func concurrencyIndexWait(admission engine.ConcurrencyAdmission) time.Duration {
	if admission.Mode != types.ConcurrencyModeQueue {
		return 0
	}
	return admission.QueueTimeout
}

// concurrencyRefusal maps an admit reply code to the sentinel the caller
// reports, or nil when the code is not a refusal. It lives beside the codes so a
// new one cannot be added without a decision about what callers see.
func concurrencyRefusal(code int64) error {
	switch code {
	case concurrencyAdmitRefusedLimit:
		return engine.ErrConcurrencyLimit
	case concurrencyAdmitRefusedQueueFull:
		return engine.ErrConcurrencyQueueFull
	}
	return nil
}

// CreateExecutionWithConcurrency admits one submission against its workflow's
// slot set, then persists it.
//
// REJECT mode over the limit persists nothing and returns ErrConcurrencyLimit.
// QUEUE mode over the limit parks the submission: it is persisted in full and
// its intents are scored into the future, so it exists durably and cannot run
// until a release promotes it. A full queue is refused just as a reject-mode
// submission is — the caller pays for it with a 429, not with an execution
// waiting nowhere.
func (s *Store) CreateExecutionWithConcurrency(ctx context.Context, execution *engine.ExecutionSnapshot, entries []engine.OutboxEntry, admission engine.ConcurrencyAdmission) (engine.ConcurrencyAdmissionResult, error) {
	// The namespace is taken from the context, like every other scoped write in
	// this store. The admission's own copy is not consulted: two sources for one
	// scope could disagree, and the context's is the one a later release will
	// read back.
	t := namespace.FromContext(ctx)
	tag := workflowDigestTag(t, admission.Workflow)
	// The index is registered for the whole declared wait, not just the slot TTL:
	// a submission that ends up in the line has to stay discoverable to the
	// expiry sweep for as long as it may wait, and for an over-limit submission
	// the wait is exactly what it is about to be given.
	s.registerConcurrencyWorkflow(ctx, t, tag, concurrencyIndexWait(admission))
	admit, err := s.admitWorkflowSlot(ctx, t, execution.ID, admission)
	if err != nil {
		return engine.ConcurrencyAdmissionResult{}, err
	}
	if refused := concurrencyRefusal(admit.code); refused != nil {
		return engine.ConcurrencyAdmissionResult{}, refused
	}

	// The slot is charged (or the line place is taken). Every failure from here
	// has to give it back, or the workflow runs one short of its limit until the
	// reconciler sweeps; that release is best effort for the same reason the
	// sweep exists, and for a queued execution it also has to leave the line.
	hold := time.Time{}
	queueWait := time.Duration(0)
	if admit.code == concurrencyAdmitQueued {
		hold = queuedOutboxHold
		// A parked execution has to outlive its own wait: its keys are written
		// with the ordinary retention, which knows nothing about the line in
		// front of it, so the create path adds the declared wait on top
		// (createExecution). Without that the execution silently expires
		// mid-queue and its place in line becomes a member naming nothing.
		queueWait = admission.QueueTimeout
	}
	if err := s.createExecutionAdmitted(ctx, execution, entries, tag, hold, queueWait); err != nil {
		s.releaseWorkflowSlotBestEffort(ctx, execution.ID, t, tag)
		return engine.ConcurrencyAdmissionResult{}, err
	}
	s.completePromotions(ctx, t, tag, admit.promoted)
	if admit.code == concurrencyAdmitQueued {
		return engine.ConcurrencyAdmissionResult{Queued: true, QueuePosition: int(admit.position)}, nil
	}
	return engine.ConcurrencyAdmissionResult{}, nil
}

// concurrencyModeFlag translates the mode into the integer the Lua compares
// against: scripts have no type system to lean on, so the conversion lives in
// one place rather than at the call site.
func concurrencyModeFlag(queueMode bool) int {
	if queueMode {
		return 1
	}
	return 0
}

// luaReplyInt reads one integer element of a Lua reply. Redis replies with
// strings for numbers in some positions and integers in others (a number a
// script returned directly arrives as one; one read back from a key arrives as
// a string), so both are accepted and an unreadable element is 0.
func luaReplyInt(reply []any, i int) int64 {
	if i >= len(reply) {
		return 0
	}
	switch v := reply[i].(type) {
	case int64:
		return v
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

// luaReplyStrings reads the string elements of a Lua reply from index from on.
// Elements that are not strings are skipped rather than coerced: the only
// strings these scripts return are execution IDs.
func luaReplyStrings(reply []any, from int) []string {
	if from >= len(reply) {
		return nil
	}
	out := make([]string, 0, len(reply)-from)
	for _, raw := range reply[from:] {
		if s, ok := raw.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// createExecutionAdmitted persists an execution admitted under the workflow
// concurrency limit and records the slot back-reference.
//
// The ordinary create path does the work — it owns the transient decision, the
// SQL projection, the outbox and the counters, and a second copy here would be
// a second definition of what creating an execution means. The back-reference
// lives with the execution, in the execution's own namespace, and is written
// with the same retention as its other keys, so it is present exactly as long
// as the execution it describes.
//
// A non-zero holdUntil parks the execution: its status is persisted as queued
// and its intents are scored into the future. The status is written by this
// same call, in the same transaction as the intents, so there is no instant at
// which a parked execution's work is deliverable without its status saying so.
// queueWait is how long the execution may wait in line, which its retention has
// to cover (see createExecution).
func (s *Store) createExecutionAdmitted(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry, tag string, holdUntil time.Time, queueWait time.Duration) error {
	if !holdUntil.IsZero() {
		parked := *e
		parked.Status = types.ExecutionStatusQueued
		e = &parked
	}
	if err := s.createExecution(ctx, e, entries, holdUntil, queueWait); err != nil {
		return err
	}
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(ctx, e.ID)
	if ttl <= 0 {
		ttl = s.execTTL
	}
	if err := s.rdb.Set(ctx, concurrencyTagKey(t, e.ID), tag, ttl).Err(); err != nil {
		// The execution exists but cannot name its slot set. The member still
		// can: the reconciler reads the execution ID straight out of the slot
		// ZSET, so this is recoverable rather than fatal.
		return fmt.Errorf("record concurrency tag for %q: %w", e.ID, err)
	}
	return nil
}

// releaseWorkflowSlotBestEffort frees the slot a terminal execution held. It
// cannot return an error: the terminal transition is already durable, so
// failing the caller would make it retry a transition that succeeded.
//
// It does not stay silent either. A release that never lands lowers the
// workflow's effective limit by one until the sweep, and a silent failure is
// indistinguishable from a healthy store — the same argument
// shortenTransientCompletionTTLBestEffort makes.
func (s *Store) releaseWorkflowSlotBestEffort(ctx context.Context, id types.ExecutionID, t namespace.Namespace, tag string) {
	err := s.releaseWorkflowSlot(ctx, id, t, tag)
	if err != nil && s.logger != nil {
		s.logger.Error("workflow_slot_release_failed", "execution_id", string(id), "err", err)
	}
}

// releaseWorkflowSlot removes the execution's slot from its workflow's set.
//
// It is idempotent in both directions. An execution that never held a slot —
// every execution of an unlimited workflow — has no back-reference and
// releases nothing, and a slot already released is a ZREM miss that also
// releases nothing.
//
// The tag may be supplied by a caller that already knows it (admission
// rollback); otherwise it is read from the execution's durable back-reference,
// which is the only way a restarted process can release a slot for an execution
// it never admitted.
func (s *Store) releaseWorkflowSlot(ctx context.Context, id types.ExecutionID, t namespace.Namespace, tag string) error {
	if tag == "" {
		var err error
		tag, err = s.workflowTagFor(ctx, id, t)
		if err != nil || tag == "" {
			return err
		}
	}
	// Registered before the release for the same reason admission registers
	// before its charge: if this call turns out to drain the state, the tag has
	// to be in the index for the sweep to notice anything the drain left behind,
	// and registering after a failure here would leave nothing to notice with.
	// No wait: a release announces nothing that has to outlive the slot TTL, and
	// the touch is extend-only, so the waiters already in line keep the longer
	// expiry they registered.
	s.registerConcurrencyWorkflow(ctx, t, tag, 0)
	reply, err := workflowReleaseLua.Run(ctx, s.rdb,
		concurrencyReleaseKeys(t, tag),
		concurrencySlotMember(id), time.Now().UTC().Unix(), int(s.concurrencySlotTTL().Seconds()),
	).Slice()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("release workflow slot for %q: %w", id, err)
	}
	if luaReplyInt(reply, 1) == 1 {
		// The execution was still waiting in the line, so it never ran: the
		// intents it parked are the work of an execution that will not do it,
		// and leaving them scored into the future costs storage until the
		// execution's retention lapses with nothing ever reading them.
		s.dropParkedOutbox(ctx, t, id)
	}
	s.completePromotions(ctx, t, tag, luaReplyStrings(reply, 2))
	if luaReplyInt(reply, 0) == 1 {
		// A state this call emptied is gone, so its index entry is now a tag the
		// sweep would follow to a key that does not exist.
		s.unregisterConcurrencyWorkflow(ctx, t, tag)
	}
	return nil
}

// dropParkedOutbox discards the intents of an execution that never ran. It is
// best effort and it only ever removes intents that were never deliverable —
// an execution parked in the line can hold no lease, because every read path
// refuses to see a held intent.
//
// It is the caller-side half of a rule with one other implementation:
// promoteQueuedExecutionLua performs the same deletion when a promotion loses
// its CAS to a cancel it cannot report back here — the release had already
// moved the member into the slot set, so wasQueued is 0 by then. That one is a
// script because only a script can tell, at the instant of the lost CAS,
// whether the intents were ever released; it deletes the same two keys.
func (s *Store) dropParkedOutbox(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	if err := s.rdb.Del(ctx, outboxReadyKey(t, id), outboxBodyKey(t, id)).Err(); err != nil && s.logger != nil {
		s.logger.Error("workflow_concurrency_parked_outbox_drop_failed", "execution_id", string(id), "err", err)
	}
}

// completePromotions finishes the promotions a slot transition decided. The
// slot side of a promotion is atomic with the release that freed the capacity;
// the promoted execution's own status and outbox live in its own hash slot and
// cannot be written from there, so they are written here.
//
// Every failure is logged and left to the reconciler, which finds a promoted
// execution the same way: holding a slot while its status still reads queued.
// Failing the caller instead would report a release that has already been made
// durable as failed, encouraging a retry that would release someone else's slot.
func (s *Store) completePromotions(ctx context.Context, t namespace.Namespace, tag string, promoted []string) {
	for _, raw := range promoted {
		id := types.ExecutionID(raw)
		if err := s.promoteExecution(ctx, t, id); err != nil && s.logger != nil {
			s.logger.Error("workflow_concurrency_promotion_failed", "execution_id", raw, "workflow_tag", tag, "err", err)
		}
	}
}

// promoteExecution starts one promoted execution: the status leaves queued and
// the intents it parked become due, in one script on the execution's own keys.
//
// The CAS is on the queued status rather than applied blindly, because a cancel
// can win the race between the release that promoted the execution and this
// call. Nothing is started then — and the parked intents are dropped by the
// script itself, not by the release that promoted it: that release sees a
// HOLDER (the promotion already took the member out of the line), so its
// wasQueued flag is 0 and it has nothing to drop, and the cancel's terminal
// write, which would clear them, has not landed yet. See the script for how it
// identifies those intents.
func (s *Store) promoteExecution(ctx context.Context, t namespace.Namespace, id types.ExecutionID) error {
	ttl := s.getExecTTL(ctx, id)
	if ttl <= 0 {
		ttl = s.execTTL
	}
	reply, err := promoteQueuedExecutionLua.Run(ctx, s.rdb,
		[]string{
			execKey(t, id, "status"),
			outboxReadyKey(t, id),
			outboxBodyKey(t, id),
			execKey(t, id, "updated_at_ms"),
		},
		string(types.ExecutionStatusQueued), string(types.ExecutionStatusRunning),
		int(ttl.Seconds()), queuedOutboxHold.UnixMilli(), time.Now().UTC().UnixMilli(),
	).Slice()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("promote execution %q: %w", id, err)
	}
	if luaReplyInt(reply, 0) != 1 {
		return nil
	}
	// The discovery index remembers the score the execution had when it was
	// created, which is the held one. Re-registering publishes the score that
	// makes it findable; without it the execution would be started and its work
	// would still not be discovered until the next keyspace sweep.
	s.markOutboxReadyIndex(ctx, t, id)
	return nil
}

// ExpireQueuedExecutions drives every queued execution whose queue timeout has
// passed into the timeout terminal state, and reports how many it expired.
//
// The line is the authority on what is still waiting: a deadline whose member
// has left the line (promoted, cancelled, or expired by another replica) is
// dropped without being reported. That check is what makes the sweep safe to
// run from several control planes at once — the same member cannot be expired
// twice, because the script that removes it from the line is the one that
// reports it.
func (s *Store) ExpireQueuedExecutions(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = defaultConcurrencySweepBatch
	}
	s.concurrencySweepMu.Lock()
	defer s.concurrencySweepMu.Unlock()

	namespaces, err := s.scopedNamespaces(ctx)
	if err != nil {
		return 0, fmt.Errorf("list namespaces for queue expiry: %w", err)
	}
	expired := 0
	for _, t := range namespaces {
		if expired >= limit {
			break
		}
		n, err := s.expireQueuedForNamespace(ctx, t, limit-expired)
		if err != nil {
			return expired, err
		}
		expired += n
	}
	return expired, nil
}

func (s *Store) expireQueuedForNamespace(ctx context.Context, t namespace.Namespace, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tags, err := s.rdb.SMembers(ctx, concurrencyWorkflowIndexKey(t)).Result()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read concurrency workflow index for namespace %q: %w", t, err)
	}
	expired := 0
	now := time.Now().UTC().UnixMilli()
	for _, tag := range tags {
		if expired >= limit {
			break
		}
		raw, err := workflowQueueExpireLua.Run(ctx, s.rdb,
			[]string{concurrencyQueueKeyForTag(t, tag), concurrencyDeadlineKeyForTag(t, tag)},
			now, limit-expired,
		).StringSlice()
		if err != nil && err != redis.Nil {
			return expired, fmt.Errorf("expire queued executions for %q: %w", tag, err)
		}
		for _, id := range raw {
			// Through the store's own terminal writer, so the expiry releases
			// whatever the execution held and promotes the next in line exactly
			// as a cancellation would. Its failure is logged rather than fatal:
			// the next sweep finds the same member — the line entry is gone, so
			// it is now a slot-less execution reading queued, which the reconcile
			// pass repairs.
			if err := s.UpdateExecutionStatus(ctx, types.ExecutionID(id), types.ExecutionStatusTimeout, engine.ConcurrencyQueueTimeoutError); err != nil {
				if s.logger != nil {
					s.logger.Error("workflow_concurrency_queue_timeout_failed", "execution_id", id, "err", err)
				}
				continue
			}
			// The member is out of the line already (the script above took it),
			// so the release path cannot tell it was queued. It never ran, so
			// its parked intents are dropped here instead.
			s.dropParkedOutbox(ctx, t, types.ExecutionID(id))
			expired++
		}
	}
	return expired, nil
}

// workflowTagFor answers which workflow an execution was admitted under.
func (s *Store) workflowTagFor(ctx context.Context, id types.ExecutionID, t namespace.Namespace) (string, error) {
	tag, err := s.rdb.Get(ctx, concurrencyTagKey(t, id)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read concurrency tag for %q: %w", id, err)
	}
	return tag, nil
}

// ConcurrencyQueuePosition reports where one queued execution stands in its
// workflow's line, 1-based, or 0 when it is not queued.
//
// It reads the queue ZSET's own order rather than a stored position: the line
// is a ranked set, so a rank IS the position, and any copy of it would be stale
// the moment a release promotes the head. A missing back-reference or a member
// that is no longer in the line both answer 0 — the execution is not waiting,
// which is what the caller asked — while a real read failure is returned so a
// caller can tell "not queued" from "could not tell".
func (s *Store) ConcurrencyQueuePosition(ctx context.Context, id types.ExecutionID) (int, error) {
	return s.queuePositionFor(ctx, namespace.FromContext(ctx), id)
}

// queuePositionFor answers where one queued execution stands in its workflow's
// line, for a caller that already resolved the namespace it is asking about —
// which the seed path has, because a seed carries its own namespace and a read
// scoped by the wrong one would silently answer "not queued".
func (s *Store) queuePositionFor(ctx context.Context, t namespace.Namespace, id types.ExecutionID) (int, error) {
	tag, err := s.workflowTagFor(ctx, id, t)
	if err != nil || tag == "" {
		return 0, err
	}
	rank, err := s.rdb.ZRank(ctx, concurrencyQueueKeyForTag(t, tag), concurrencySlotMember(id)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read queue position for %q: %w", id, err)
	}
	return int(rank) + 1, nil
}

// releaseWorkflowSlotAfterTerminal is the post-terminal hook every terminal
// writer calls. It takes the status the writer just persisted rather than
// re-reading it, so it can never race the next transition into thinking a
// canceling execution finished.
func (s *Store) releaseWorkflowSlotAfterTerminal(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	if !types.IsTerminalExecutionStatus(status) {
		return
	}
	s.releaseWorkflowSlotBestEffort(ctx, id, namespace.FromContext(ctx), "")
}
