package control

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

var _ ActivationDeliveryDirectory = (*MemoryRunnerDirectory)(nil)

// memoryActivationDirectiveQueueKey identifies one runner session's pending
// directives. Both components matter: the runner ID is what the drain looks up
// and the session is what makes an old session's queue unreachable, exactly as
// the two are separate components of the Redis key.
type memoryActivationDirectiveQueueKey struct {
	runnerID  string
	sessionID string
}

// memoryActivationDirectiveQueue is the in-process equivalent of the Redis
// hash: field = "a:<identity>" / "d:<identity>", value = directive JSON. It
// carries the same TTL semantics so embedded mode does not accumulate queues
// for sessions that ended long ago — the Redis implementation gets that from
// EXPIRE, this one from a deadline refreshed on every enqueue.
type memoryActivationDirectiveQueue struct {
	fields    map[string]string
	expiresAt time.Time
}

type memoryHostedActivationsReport struct {
	sessionID  string
	items      []protocol.ActivationInventoryItem
	observedAt time.Time
}

// EnqueueActivationDirective mirrors RedisRunnerDirectory's implementation:
// dedup by activation identity, refresh the session queue's TTL.
func (d *MemoryRunnerDirectory) EnqueueActivationDirective(_ context.Context, runnerID, sessionID string, directive protocol.ActivateDirective) error {
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	payload, err := json.Marshal(directive)
	if err != nil {
		return err
	}
	return d.enqueueActivationDirectiveField(runnerID, sessionID,
		activationDirectiveFieldActivate+activationDirectiveKeyOfActivate(directive), string(payload))
}

// EnqueueDeactivationDirective mirrors RedisRunnerDirectory's implementation.
func (d *MemoryRunnerDirectory) EnqueueDeactivationDirective(_ context.Context, runnerID, sessionID string, directive protocol.DeactivateDirective) error {
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	payload, err := json.Marshal(directive)
	if err != nil {
		return err
	}
	return d.enqueueActivationDirectiveField(runnerID, sessionID,
		activationDirectiveFieldDeactivate+activationDirectiveKeyOfDeactivate(directive), string(payload))
}

func (d *MemoryRunnerDirectory) enqueueActivationDirectiveField(runnerID, sessionID, field, payload string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := memoryActivationDirectiveQueueKey{runnerID: runnerID, sessionID: sessionID}
	queue := d.activationDirectives[key]
	if queue == nil {
		queue = &memoryActivationDirectiveQueue{fields: make(map[string]string)}
		d.activationDirectives[key] = queue
	}
	queue.fields[field] = payload
	queue.expiresAt = d.clockNow().Add(activationDirectiveQueueTTL)
	return nil
}

// ActivationDirectives drains one session's queue. Drain-once is guaranteed by
// the same mutex the enqueue takes: a second concurrent drain finds the entry
// already removed.
func (d *MemoryRunnerDirectory) ActivationDirectives(_ context.Context, runnerID, sessionID string) ([]protocol.ActivateDirective, []protocol.DeactivateDirective, error) {
	if runnerID == "" || sessionID == "" {
		return nil, nil, ErrRunnerSessionRequired
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	key := memoryActivationDirectiveQueueKey{runnerID: runnerID, sessionID: sessionID}
	queue := d.activationDirectives[key]
	delete(d.activationDirectives, key)
	if queue == nil || !d.clockNow().Before(queue.expiresAt) {
		return nil, nil, nil
	}
	return resolveActivationDirectiveConflicts(queue.fields)
}

// RecordHostedActivations stores the latest report, replacing any previous one
// regardless of session: the newest heartbeat is by definition the current
// truth, and the session travels inside the report so a reader never mistakes
// a replaced session's entry for the current one's.
func (d *MemoryRunnerDirectory) RecordHostedActivations(_ context.Context, runnerID, sessionID string, items []protocol.ActivationInventoryItem) error {
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	d.hostedActivations[runnerID] = memoryHostedActivationsReport{
		sessionID:  sessionID,
		items:      append([]protocol.ActivationInventoryItem(nil), items...),
		observedAt: d.clockNow(),
	}
	return nil
}

// HostedActivations returns the report only while it is fresh, mirroring both
// Redis's TTL and the "absent or stale => ok=false" contract. An expired entry
// is dropped at read time; there is no background reaper.
func (d *MemoryRunnerDirectory) HostedActivations(_ context.Context, runnerID string) (HostedActivationsReport, bool, error) {
	if runnerID == "" {
		return HostedActivationsReport{}, false, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	report, ok := d.hostedActivations[runnerID]
	if !ok {
		return HostedActivationsReport{}, false, nil
	}
	if !d.clockNow().Before(report.observedAt.Add(activationHostedReportTTL)) {
		delete(d.hostedActivations, runnerID)
		return HostedActivationsReport{}, false, nil
	}
	return HostedActivationsReport{
		SessionID: report.sessionID,
		Items:     append([]protocol.ActivationInventoryItem(nil), report.items...),
	}, true, nil
}
