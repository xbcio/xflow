package types

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cast"
)

type TriggerEvent struct {
	ID      string            `json:"id,omitempty"`
	Kind    string            `json:"kind,omitempty"`
	Source  string            `json:"source,omitempty"`
	Time    time.Time         `json:"time,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Data    map[string]any    `json:"data,omitempty"`
	Raw     []byte            `json:"raw,omitempty"`
}

// TriggerEventData renders a trigger event as the decoded-map shape a downstream
// node sees under $input.trigger when the event crossed JSON.
//
// Every delivery path that carries an event into a seeded execution renders it
// through this ONE function — the trigger kinds that seed from a runner
// (node/trigger/internal/seedadmit) and the server-hosted webhook route
// (service/apiserver) alike — because a definition may read any of these keys,
// and a path that built the map itself would drift the moment the shape changed.
//
// Keys mirror TriggerEvent's JSON tags, and an absent field is omitted rather
// than written as a zero value, so a node testing for a key's presence reads the
// same answer on every path.
func TriggerEventData(event *TriggerEvent) map[string]any {
	out := map[string]any{}
	if event == nil {
		return out
	}
	if event.ID != "" {
		out["id"] = event.ID
	}
	if event.Kind != "" {
		out["kind"] = event.Kind
	}
	if event.Source != "" {
		out["source"] = event.Source
	}
	if !event.Time.IsZero() {
		out["time"] = event.Time.UTC().Format(time.RFC3339Nano)
	}
	if len(event.Headers) > 0 {
		out["headers"] = event.Headers
	}
	if len(event.Data) > 0 {
		out["data"] = event.Data
	}
	if len(event.Raw) > 0 {
		// Carried as the payload's own bytes rather than as the base64 JSON
		// would produce for a []byte field: on this path the raw bytes ARE the
		// message the trigger received (a Kafka batch, an HTTP body), so a reader
		// of $input.trigger.raw sees the payload itself, which a base64 envelope
		// would not give it.
		out["raw"] = string(event.Raw)
	}
	return out
}

type TriggerHandler interface {
	Handler
	Activate(ctx context.Context, input *TriggerActivateInput) (TriggerSubscription, error)
}

// ScheduledTrigger is a trigger kind whose timeline names instants in advance —
// a cron expression, a fixed interval — and that can therefore be asked to
// produce the firing of one of them outside a live activation.
//
// A backfill is the caller: an operator asks for the instants of a past range to
// run, and the kind that owns the timeline is the only code that can spell a
// firing of one of its own instants, because the event ID is the instant in that
// kind's own format. params are the node's effective activation parameters, so a
// node whose timing an operator has overridden produces the firing of the
// schedule it is actually running.
//
// A trigger kind without a timeline (a webhook has a route, not a schedule) does
// not implement this, and is not backfillable — which is the honest answer
// rather than a limitation to route around.
type ScheduledTrigger interface {
	ScheduledEvent(workflowID WorkflowID, nodeName string, params map[string]any, instant time.Time) (*TriggerEvent, error)
}

type TriggerSubscription interface {
	Close(ctx context.Context) error
}

// SubscriptionHealth is an optional extension of TriggerSubscription for
// subscriptions whose termination is observable without Close.
//
// The runner's activation tracker uses it to stop reporting an activation as
// hosted once its subscription has exited on its own — the consumer's message
// stream ending, a trigger-internal shutdown (a lost renewable lock), or the
// parent context being cancelled. Without this signal the entry stays in the
// inventory until Close or a restart, so the control plane believes the runner
// still hosts it and never redelivers the activation, even though nothing is
// consuming.
//
// It is optional per trigger kind; Kafka (both the aggregate and per-message
// shapes) and Redis implement it today, and kinds that cannot outlive their
// Close call have nothing to report. Done is NOT a liveness probe: it fires
// when the subscription's own reading loop exits, not when deliveries merely
// stop. A consumer stuck establishing its stream reports nothing here.
//
// Done is closed exactly once when the subscription stops, whether the stop
// was requested through Close or happened on its own. Implementations must
// not block in Done.
type SubscriptionHealth interface {
	Done() <-chan struct{}
}

type CloseFunc func(context.Context) error

func (f CloseFunc) Close(ctx context.Context) error { return f(ctx) }

type TriggerActivateInput struct {
	WorkflowID WorkflowID
	NodeName   string
	Params     map[string]any
	Runtime    TriggerRuntime
	// Supplies holds decoded supply content for this trigger's declared
	// dependencies (via DependsOn edges). Keyed by supply node name. Each
	// value is the JSON-decoded supply content (map[string]any). nil when no
	// supplies are declared or none have content yet.
	Supplies map[string]any
	// WorkflowTimezone is the workflow's settings.timezone (IANA name, already
	// validated at compile), or "" when the workflow declares none. Triggers
	// that evaluate wall-clock schedules use it when their own timezone
	// parameter is unset.
	WorkflowTimezone string
}

func (i *TriggerActivateInput) Emit(ctx context.Context, event *TriggerEvent) (ExecutionID, error) {
	return i.Runtime.Emit(ctx, i.WorkflowID, i.NodeName, event)
}

func (i *TriggerActivateInput) GetString(name string) string {
	return cast.ToString(i.Params[name])
}

// GetSupply returns the decoded supply content for the named supply node, or
// nil if not present. The supply node name is the one declared via DependsOn.
func (i *TriggerActivateInput) GetSupply(name string) map[string]any {
	if i.Supplies == nil {
		return nil
	}
	v, ok := i.Supplies[name]
	if !ok {
		return nil
	}
	m, _ := v.(map[string]any)
	return m
}

func (i *TriggerActivateInput) Webhooks() (WebhookRuntime, error) {
	provider, ok := i.Runtime.(interface{ Webhooks() WebhookRuntime })
	if !ok {
		return nil, fmt.Errorf("webhook runtime is not available")
	}
	rt := provider.Webhooks()
	if rt == nil {
		return nil, fmt.Errorf("webhook runtime is not available")
	}
	return rt, nil
}

type TriggerRuntime interface {
	Emit(ctx context.Context, workflowID WorkflowID, nodeName string, event *TriggerEvent) (ExecutionID, error)
	Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error)
	TryLock(ctx context.Context, key string, ttl time.Duration) (TriggerLock, bool, error)
	State(ctx context.Context, scope string) TriggerState
}

type TriggerLock interface {
	Release(ctx context.Context) error
}

type RenewableTriggerLock interface {
	TriggerLock
	Renew(ctx context.Context, ttl time.Duration) (bool, error)
}

type TriggerState interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// TriggerFireObserver counts a firing a trigger decided not to admit. A trigger
// detects the drop and knows why; what it cannot do is report it, because the
// only place a counter can live is observability, which depends on the trigger
// packages and therefore cannot be depended on by them. The trigger packages
// therefore hold an observer installed by the process that builds the runtime
// (sdk/xflow, next to the kafka trigger's observer), and call it wherever a
// firing is dropped.
//
// reason is one of the TriggerFireDropped* constants below. err is the failure
// the drop came from, or nil when the trigger itself decided (the overlap
// policy). It is passed for classification, not for display: an implementation
// that can name the engine's sentinels narrows what the trigger could not, and
// one that cannot must report reason unchanged. err's TEXT is never a label —
// it is unbounded, and it belongs in the trigger's own log line.
//
// Neither the workflow nor the node is passed: a metric label set must stay
// bounded, and both are definition data.
type TriggerFireObserver interface {
	OnTriggerFireDropped(ctx context.Context, reason string, err error)
}

// Why a trigger dropped a firing. These are the whole vocabulary: a series
// partitioned by reason is only readable if each reason means one thing.
const (
	// TriggerFireDroppedRefused is a firing the workflow's OWN admission
	// declined, on either hosting path: the control plane answered
	// EntrySeedResponse.Refused, or the embedded engine returned its concurrency
	// limit (which the observer narrows out of emit_error — see below). Nothing
	// was persisted, so the firing is behind the trigger and the next one is a
	// fresh attempt. The nearest thing to a schedule being throttled by its own
	// workflow — expected under load, and the fix is capacity or a queue mode.
	//
	// It is one reason rather than two because it is one condition: an operator
	// reading a series per deployment topology would see the same throttle
	// spelled differently depending on where the trigger ran.
	TriggerFireDroppedRefused = "refused"
	// TriggerFireDroppedOverlap is a firing the trigger's overlap policy dropped
	// because the execution it started last had not finished.
	TriggerFireDroppedOverlap = "overlap"
	// TriggerFireDroppedTransport is a firing whose admission failed after the
	// bounded retries — the control plane was unreachable, or answered something
	// the caller could not use.
	TriggerFireDroppedTransport = "transport"
	// TriggerFireDroppedEmitError is a firing the embedded engine would not take,
	// reported with the error it returned. The trigger cannot tell WHICH failure
	// it was — the engine's sentinels are not importable from node/, which the
	// engine's own tests depend on — so the observer narrows the concurrency
	// limit out of this reason (see TriggerFireObserver). What is left here is
	// the engine rejecting the submission for anything else: an unknown workflow,
	// a stopped engine.
	TriggerFireDroppedEmitError = "emit_error"
)

// ExecutionStatusReader is an optional capability of TriggerRuntime that reads
// back the lifecycle status of an execution the runtime admitted. A trigger that
// must not start a second run while the previous one is still going (the cron
// trigger's overlap policy) asks this question through a type assertion on the
// TriggerRuntime it was activated with.
//
// The capability is optional because a trigger can be hosted by a runtime with
// no execution store behind it, and a trigger that cannot see the other run's
// status must say so rather than assume. A runtime that has no store to read
// still implements this method and answers with ErrExecutionStatusUnavailable;
// a runtime that implements nothing at all fails the type assertion. Callers
// treat both the same way: fall back to the behaviour that needs no status.
//
// The shape matches engine.ExecutionStatusReader — same method, same
// found/err split — so a store that already answers it for the engine answers
// it here by being forwarded to, and the two can never disagree about what a
// status read means.
type ExecutionStatusReader interface {
	// GetExecutionStatus reports one execution's lifecycle status. found=false
	// means no such execution is recorded, which is not the zero status; an
	// error means the question could not be answered at all.
	GetExecutionStatus(ctx context.Context, id ExecutionID) (status ExecutionStatus, found bool, err error)
}

// EntrySeedRuntime is an optional capability of TriggerRuntime that supports
// entry-unit seed (single node or group node) atomic admission. Kafka trigger
// nodes check for this via type assertion when operating in seed mode. If
// absent, the trigger falls back to the standard Emit path.
type EntrySeedRuntime interface {
	// SeedExecutionFromEntry atomically admits an entry-unit result to the
	// control plane. Only after a successful (accepted/duplicate) response may
	// the caller commit the Kafka offset.
	SeedExecutionFromEntry(ctx context.Context, req EntrySeedRequest) (EntrySeedResponse, error)
}

// EntrySeedRequest is the caller-facing admission request. It wraps
// the engine-level types with the fields a Kafka trigger naturally has. The
// EntrySeedRuntime implementation maps these to the engine request.
type EntrySeedRequest struct {
	AdmissionKey    string
	WorkflowID      WorkflowID
	WorkflowVersion string
	EntryUnitID     string
	Outcome         string // "success" or "failed"
	Exits           []BoundaryExit
	Error           string
}

// BoundaryExit is one boundary output from the entry-unit execution.
type BoundaryExit struct {
	NodeName string
	Port     string
	Data     map[string]any
}

// EntrySeedResponse is the control-plane response.
//
// The four handled outcomes are mutually exclusive and one of them is always
// set when a response is returned without an error; a response that sets none
// is a protocol disagreement the caller must not treat as handled.
type EntrySeedResponse struct {
	Accepted  bool
	Duplicate bool
	Conflict  bool
	// Refused is true when the workflow's concurrency policy refused the
	// admission: nothing was persisted, and the entry-unit result is still the
	// caller's — so the caller must NOT commit the offset, and the batch is
	// redelivered and retried. It is deliberately not folded into Conflict (which
	// DOES commit: another runner already holds the result) or into the error
	// path (which is a transport fault rather than a decision the workflow made).
	Refused bool
	// QueueFull narrows Refused: the refusal was the queue-mode backlog being at
	// max_queued rather than a slot being unavailable. Both waits are retryable,
	// but they are different waits — a slot to free versus a backlog to drain —
	// and the runner reports them separately for the same reason the HTTP layer
	// gives them different codes.
	QueueFull   bool
	ExecutionID ExecutionID
	// RetryAfter is how long the control plane asked the caller to wait before
	// retrying, carried on a refusal. Zero means the control plane did not say,
	// and the caller applies its own cadence — which is what every caller in
	// this repository does, so this is for an embedder that has no cadence of
	// its own (a webhook route admitting in process, which must turn the
	// refusal into a Retry-After header on its own response).
	//
	// It is a lower bound, not a release time: when a concurrency slot frees is
	// not knowable, and the queue-mode backlog drains at the rate the running
	// executions finish.
	RetryAfter time.Duration
}

// EntrySeedObserver receives one observation per entry-seed admission attempt:
// its outcome and how long the control-plane round trip took. Implementations
// must be non-blocking and must not affect admission.
//
// It exists so an admission that FAILS is visible to the integrator, not only
// to the runner's logger. The deadline on this path is a window the runner
// applies, and the batch each admission carries is the integrator's choice, so
// an admission that starts timing out is the signal that the batch size is out
// of bounds — a signal that otherwise reaches only a throttled log line on one
// runner.
//
// One method, not a counter call plus a duration call: one attempt must always
// produce exactly one of each, so the count and the duration distribution can
// never disagree about how many attempts happened. That is the same shape
// engine.GroupObserver.OnGroupAdmission uses for the group admission path.
//
// The contract lives here rather than in service/protocol because the package
// that consumes it (observability/metrics) must not import service/ — see
// AGENTS.md's lower-layer prohibition. types is the dependency-free contract
// package both sides already share, and this sits beside EntrySeedRuntime,
// whose calls it observes.
type EntrySeedObserver interface {
	// OnEntrySeedAdmission reports one admission attempt. outcome is a closed
	// enum produced by the runtime, never free text: "accepted", "duplicate",
	// "conflict", "refused", "queue_full", "timeout", "error". "refused" and
	// "queue_full" are the workflow's own concurrency policy declining the seed —
	// a designed throttle, not a fault, and the only series in which an operator
	// can see that a correctly-configured deployment is being held back. d is how
	// long the round trip took, including any queueing inside the HTTP client.
	OnEntrySeedAdmission(ctx context.Context, outcome string, d time.Duration)
}

// GroupExecRuntime is an optional capability of TriggerRuntime that lets a
// trigger route one batch through LOCAL group execution — running the
// group's real member nodes on this runner via the embedded engine — instead
// of synthesizing boundary exits from the raw batch payload. A Kafka trigger
// operating as a trigger-group entry checks for this via type assertion
// (alongside EntrySeedRuntime) before flushing a batch; see
// node/trigger/kafka/kafka.go and spec 2026-08-07 §3.3-§3.4.
//
// Without this capability (e.g. a single standalone trigger node, not a
// group), the trigger falls back to the existing raw-exit seed path
// (seedKafkaEntryBatchMessages) — that path is unaffected by this feature.
type GroupExecRuntime interface {
	// ExecuteGroup runs one group execution locally, seeding the group's
	// entry node with input as its $input data, and returns the REAL boundary
	// exits the group's member nodes produced. A non-nil error means the
	// group could not be executed at all (e.g. its package failed to
	// resolve/compile) — this is a transient/infrastructure failure, distinct
	// from GroupExecResult.Outcome != "success" which means the group DID run
	// but a member node failed or the deadline was exceeded.
	ExecuteGroup(ctx context.Context, input map[string]any) (GroupExecResult, error)
}

// GroupExecResult is the caller-facing result of one local group execution
// via GroupExecRuntime.ExecuteGroup. Outcome mirrors engine.GroupOutcome's
// string values ("success", "failed", "timeout", "canceled") without this
// package importing the engine package (types must not depend on engine).
type GroupExecResult struct {
	Outcome string
	Exits   []BoundaryExit
	Error   string

	// FailedMember is the name of the member node whose fatal failure produced
	// Outcome != "success", copied from subgraph.Result.FailedMember by the
	// runner's ExecuteGroup. Empty when no single member names the failure: a
	// deadline or cancel is environmental, and a group that could not run at
	// all leaves via the error return instead. It is a plain node name (not an
	// index) so a Kafka batch consumer can put "which node" in front of an
	// operator without reaching into the engine's classification. Only meaningful
	// when Outcome != "success".
	FailedMember string

	// Deterministic indicates the failure is permanent — retrying the same
	// input will produce the same failure (e.g. rule compilation error,
	// schema validation failure). Consumers use this to decide whether to
	// commit offset (skip) or withhold it (redeliver). Only meaningful when
	// Outcome != "success".
	Deterministic bool
}
