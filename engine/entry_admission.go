package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ErrEntryAdmissionNotSupported is returned by Engine.SeedExecutionFromEntry
// when the backing StateStore does not implement EntryAdmissionStore.
var ErrEntryAdmissionNotSupported = errors.New("entry admission not supported by this backend")

// AdmissionKey uniquely identifies one entry-unit result submission.
// Format: namespace/workflowID/workflowVersion/entryUnitID/topic/partition/start-end
type AdmissionKey string

// BuildAdmissionKey constructs a canonical admission key for a Kafka
// entry-unit (single node or group node) batch.
func BuildAdmissionKey(ns namespace.Namespace, workflowID types.WorkflowID, workflowVersion string, entryUnitID string, topic string, partition int, startOffset, endOffset int64) AdmissionKey {
	return AdmissionKey(fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d-%d",
		ns, workflowID, workflowVersion, entryUnitID, topic, partition, startOffset, endOffset))
}

// BuildAdmissionKeySingle constructs an admission key for a single-message
// (non-batch) trigger.
func BuildAdmissionKeySingle(ns namespace.Namespace, workflowID types.WorkflowID, workflowVersion string, entryUnitID string, topic string, partition int, offset int64) AdmissionKey {
	return BuildAdmissionKey(ns, workflowID, workflowVersion, entryUnitID, topic, partition, offset, offset)
}

// AdmissionState classifies the control-plane response to an entry admission.
type AdmissionState string

const (
	// AdmissionStateAccepted means the result was admitted (first writer wins).
	AdmissionStateAccepted AdmissionState = "accepted"
	// AdmissionStateConflict means the key was already admitted with a different
	// result hash — a concurrent runner produced a different outcome.
	AdmissionStateConflict AdmissionState = "conflict"
	// AdmissionStateOverLimit means the workflow's concurrency limit refused the
	// seed in REJECT mode. Nothing was persisted for it, so the caller still owns
	// the entry-unit result: a Kafka trigger withholds the offset and retries.
	AdmissionStateOverLimit AdmissionState = "over_limit"
	// AdmissionStateQueueFull means the workflow's QUEUE-mode backlog was at
	// max_queued and the seed was refused. Like AdmissionStateOverLimit, this is
	// a retryable refusal with nothing persisted — the two are distinct because
	// they name different waits (a slot to free, a backlog to drain), exactly as
	// ErrConcurrencyLimit and ErrConcurrencyQueueFull do for submissions.
	AdmissionStateQueueFull AdmissionState = "queue_full"
)

// ResultHash is the content-addressed hash of an entry-unit result, used to
// distinguish duplicate-accepted (same key+hash) from conflict (same key,
// different hash).
type ResultHash string

// ComputeResultHash deterministically hashes the entry-unit outcome and exits.
// Exits are sorted by (NodeName, Port) for stability regardless of input order.
func ComputeResultHash(outcome GroupOutcome, exits []BoundaryExit) ResultHash {
	h := sha256.New()
	h.Write([]byte(outcome))
	// Sort exits by NodeName+Port for determinism.
	sorted := make([]BoundaryExit, len(exits))
	copy(sorted, exits)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].NodeName != sorted[j].NodeName {
			return sorted[i].NodeName < sorted[j].NodeName
		}
		return sorted[i].Port < sorted[j].Port
	})
	for _, ex := range sorted {
		fmt.Fprintf(h, "%s/%s/", ex.NodeName, ex.Port)
		// Sort data keys for determinism.
		keys := make([]string, 0, len(ex.Data))
		for k := range ex.Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(h, "%s=%v,", k, ex.Data[k])
		}
	}
	return ResultHash(hex.EncodeToString(h.Sum(nil)))
}

// DeterministicExecutionID derives a stable execution ID from an admission key.
// This ensures the admission key and all execution state keys share the same
// Redis hash slot ({execID}), enabling a single atomic Lua script.
func DeterministicExecutionID(key AdmissionKey) types.ExecutionID {
	h := sha256.Sum256([]byte(key))
	return types.ExecutionID("exec-tg-" + hex.EncodeToString(h[:16]))
}

// --- Request / Response ---

// SeedExecutionFromEntryRequest is the atomic admission request from an
// entry-unit (single node or group node) runner to the control plane. It seeds
// an execution + commits the entry-unit result in a single fenced transition.
type SeedExecutionFromEntryRequest struct {
	AdmissionKey AdmissionKey
	// Namespace is the ADMISSION namespace: the request's when supplied, the
	// authenticated ctx's otherwise. It is recorded on the execution snapshot as
	// the registration namespace sub-workflow dispatch resolves bindings in. It
	// is not read from a WorkflowDef — the entry-seed path has no definition in
	// hand — and for an HTTP-registered workflow the value is the same one the
	// definition was registered under.
	Namespace       namespace.Namespace
	WorkflowID      types.WorkflowID
	WorkflowVersion string
	EntryUnitID     string
	EntryUnitIdx    int
	Graph           *graph.Graph
	Outcome         GroupOutcome
	Exits           []BoundaryExit
	Error           string
	ResultHash      ResultHash
	// Downstream arrivals to schedule after the entry unit is admitted.
	Downstream []DownstreamArrival
	// Params for the created execution (optional metadata).
	Params  map[string]any
	Runtime *types.Runtime
	TraceID string
	SpanID  string
	// Generation is the entry-activation generation the seeding runner believes
	// it owns. The control plane fences stale generations (spec §11.6): a seed
	// whose generation is below the currently-assigned generation may not create
	// a new execution, though an already-accepted admission key is still
	// duplicate-accepted so the runner can commit its offset. It is not part of
	// the content-addressed admission (it never enters ResultHash or the key).
	Generation uint64
	// ReplicaIndex identifies the activation sibling that produced this seed.
	// It participates only in the generation fence: sibling replicas share the
	// admission key and result hash so Kafka rebalances preserve idempotency.
	ReplicaIndex uint32
}

// SeedExecutionFromEntryResponse is the control-plane response.
type SeedExecutionFromEntryResponse struct {
	// State is accepted, conflict, over_limit or queue_full.
	State AdmissionState
	// ExecutionID is the stable, deterministic execution ID for this admission key.
	ExecutionID types.ExecutionID
	// Duplicate is true when the same key+hash was already accepted (idempotent
	// retry); the same execution ID is returned.
	Duplicate bool
	// Queued is true when the admitted execution was parked in its workflow's
	// QUEUE-mode line instead of started. It is the seed-path counterpart of
	// ConcurrencyAdmissionResult.Queued and carries the same meaning: the caller
	// must not expect the execution's work to be deliverable until a release
	// promotes it.
	Queued bool
	// QueuePosition is the queued execution's 1-based place in line, or 0 when
	// Queued is false. Position 1 is the head: the execution the next release
	// promotes.
	QueuePosition int
	// Skipped reports the downstream units this admission resolved as skip (see
	// SkippedUnit). An admission is a scheduling transition like any other: it
	// decrements the new execution's in-degree counters and picks execute or
	// skip per downstream unit. Nil when the backend does not report it or when
	// nothing was skipped.
	Skipped []SkippedUnit
}

// SeedAdmissionRefusal maps a refused slot decision to the response an entry
// seed returns for it, or returns the error unchanged when it is not a refusal.
//
// It lives here rather than in each backend because it is a decision about the
// response contract, not about storage: a refusal is an administration the
// caller acts on (retry once a slot frees, or once the backlog drains) rather
// than a failed call, so it travels in State — and which sentinel means which
// state has to read the same from every backend. The response names no
// execution: nothing was persisted for it, and an ID would invite a caller to
// inspect an execution that does not exist.
func SeedAdmissionRefusal(err error) (SeedExecutionFromEntryResponse, error) {
	switch {
	case errors.Is(err, ErrConcurrencyLimit):
		return SeedExecutionFromEntryResponse{State: AdmissionStateOverLimit}, nil
	case errors.Is(err, ErrConcurrencyQueueFull):
		return SeedExecutionFromEntryResponse{State: AdmissionStateQueueFull}, nil
	}
	return SeedExecutionFromEntryResponse{}, err
}

// --- EntryAdmissionStore ---

// EntryAdmissionStore is the atomic admission capability for entry-unit (single
// node or group node) results. It is intentionally separate from GroupStateStore
// because the entry-seed path has no lease lifecycle — only first-writer-wins
// admission.
//
// Implementations must guarantee all steps in one atomic transition:
//  1. admission key unique occupancy (first-writer-wins)
//  2. create execution
//  3. initialize unit counters (remaining, failed, in-degree)
//  4. mark entry unit as success/failed
//  5. write all boundary outputs
//  6. write downstream advance outbox
//  7. return stable execution ID
type EntryAdmissionStore interface {
	SeedExecutionFromEntry(ctx context.Context, req SeedExecutionFromEntryRequest) (SeedExecutionFromEntryResponse, error)
}

// SeedExecutionFromEntry delegates an entry-unit seed admission to the
// backing StateStore when it implements EntryAdmissionStore. It mirrors the
// CommitGroupResult delegation pattern: the atomic admission logic lives in the
// backend (local mutex or Redis Lua), and the engine merely routes to it. A
// backend that does not implement EntryAdmissionStore reports
// ErrEntryAdmissionNotSupported.
//
// When req identifies a GROUP entry unit (req.Graph.UnitKindAt(req.EntryUnitIdx)
// == graph.UnitGroup), this also reports one xflow_group_admission_total /
// xflow_group_admission_duration_seconds observation, timing only the
// store.SeedExecutionFromEntry backend round trip — not the topology
// resolution or generation fence that service/control/core.go's caller
// performs before/after this call.
//
// An accepted, non-duplicate admission additionally reports the downstream
// units the backend resolved as skip through GroupObserver.OnNodeSkip with
// flow "entry". That is reported for every entry unit, group or not: the skip
// decision is made on the new execution's downstream fan-in, which has nothing
// to do with what kind of unit the seed came from.
//
// The ErrEntryAdmissionNotSupported early return is NOT observed: a backend
// lacking the capability is a static configuration fact, not an admission
// attempt, so reporting outcome="error" there would flood the error rate with
// a constant that never reflects an actual admission being tried.
//
// A nil Graph or an EntryUnitIdx outside [0, Graph.UnitCount()) is also not
// observed at all — neither as group nor non-group — because the discriminator
// cannot be evaluated safely. This is a missing value, not a wrong one: the
// resulting series is a lower bound whenever the caller could not resolve
// group membership, mirroring Cut C-1's tolerance for a legacy NodeType=="".
//
// A graph that declares settings.concurrency routes through
// ConcurrencyLimitedEntryAdmissionStore instead, so an entry-seeded execution
// contends for a slot exactly as a submitted one does. That capability is
// required, not optional, once the graph declares a limit: a backend that can
// seed entries but cannot enforce the limit fails closed with
// ErrConcurrencyUnsupported, which is the same answer every other submission
// path gives. Refusals are reported in the response's State rather than as an
// error, because they are admissions the caller can act on (retry later)
// rather than failed calls.
func (e *Engine) SeedExecutionFromEntry(ctx context.Context, req SeedExecutionFromEntryRequest) (SeedExecutionFromEntryResponse, error) {
	store, ok := e.state.(EntryAdmissionStore)
	if !ok {
		return SeedExecutionFromEntryResponse{}, ErrEntryAdmissionNotSupported
	}

	// An entry seed arrives from a runner, so an exit's privacy marker is not
	// trusted. Copy its exits, including their mutable data trees, before
	// deriving each marker from the control-plane graph. A backend may retain or
	// mutate its request without changing the runner's retryable request.
	admissionReq := req
	admissionReq.Exits = cloneEntryAdmissionExits(req.Exits)
	for i := range admissionReq.Exits {
		admissionReq.Exits[i].PrivateOutput = entryAdmissionExitPrivateOutput(admissionReq.Graph, admissionReq.Exits[i].NodeName)
	}

	isGroupEntry := req.Graph != nil &&
		req.EntryUnitIdx >= 0 &&
		req.EntryUnitIdx < req.Graph.UnitCount() &&
		req.Graph.UnitKindAt(req.EntryUnitIdx) == graph.UnitGroup

	// The admission is built from the seed's own graph, exactly as a submission
	// builds its own: the definition that will run is the definition that is
	// judged, so a limit added by a re-published version applies to the seeds
	// that resolve to that version and to no others.
	admission := concurrencyAdmissionFor(ctx, req.Graph)

	start := time.Now()
	resp, err := e.seedEntryAdmission(ctx, store, admissionReq, admission)
	d := time.Since(start)

	// The backend resolves the admission's namespace from the request first and
	// only falls back to ctx, so an embedded caller can name a namespace the
	// context does not carry. Observers read the namespace from ctx; hand them
	// the one the admission actually used.
	notifyCtx := ctx
	if req.Namespace != "" {
		notifyCtx = namespace.WithNamespace(ctx, req.Namespace)
	}

	if isGroupEntry {
		e.notifyGroupAdmission(notifyCtx, classifyAdmissionOutcome(resp, err), d)
	}
	// Only on a real admission. A duplicate retry applied no transition, so its
	// response carries no skips; a conflict or a transport error applied
	// nothing at all, and counting the skips of a transition that did not
	// happen would inflate the one series an operator alerts on. A refusal
	// applies nothing either, for the same reason and one more: nothing exists
	// on the execution side of it yet.
	if err == nil && !resp.Duplicate && resp.State == AdmissionStateAccepted {
		e.notifySkip(notifyCtx, flowEntry, resp.Skipped)
	}

	return resp, err
}

// seedEntryAdmission routes one seed to the store capability its graph calls
// for: the concurrency-limited admission when the graph declares a limit (fail
// closed when the store cannot apply one), the plain admission otherwise.
func (e *Engine) seedEntryAdmission(ctx context.Context, store EntryAdmissionStore, req SeedExecutionFromEntryRequest, admission *ConcurrencyAdmission) (SeedExecutionFromEntryResponse, error) {
	if admission == nil {
		return store.SeedExecutionFromEntry(ctx, req)
	}
	limited, ok := e.state.(ConcurrencyLimitedEntryAdmissionStore)
	if !ok {
		return SeedExecutionFromEntryResponse{}, fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyUnsupported)
	}
	return limited.SeedExecutionFromEntryWithConcurrency(ctx, req, *admission)
}

// cloneEntryAdmissionExits makes the store-owned copy of runner boundary
// outputs. BoundaryExit.Data is runtime payload data, so copying only the
// slice or struct would still let the backend mutate the caller through a map
// or nested container alias.
func cloneEntryAdmissionExits(exits []BoundaryExit) []BoundaryExit {
	if exits == nil {
		return nil
	}

	cloned := make([]BoundaryExit, len(exits))
	for i := range exits {
		cloned[i] = exits[i]
		cloned[i].Data = cloneEntryAdmissionData(exits[i].Data)
	}
	return cloned
}

func cloneEntryAdmissionData(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}

	cloned := make(map[string]any, len(data))
	for key, value := range data {
		cloned[key] = cloneEntryAdmissionValue(value)
	}
	return cloned
}

// cloneEntryAdmissionValue recursively copies ordinary runtime-data
// containers while preserving their concrete Go types. Opaque scalar values
// are immutable by value and can be reused. This deliberately mirrors the
// graph package's defensive-copy behavior without importing an internal graph
// helper into the runtime payload boundary.
func cloneEntryAdmissionValue(value any) any {
	if value == nil {
		return nil
	}

	cloned := cloneEntryAdmissionReflectValue(reflect.ValueOf(value))
	if !cloned.IsValid() || !cloned.CanInterface() {
		return value
	}
	return cloned.Interface()
}

func cloneEntryAdmissionReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		return cloneEntryAdmissionReflectValue(value.Elem())
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			source := iter.Value()
			copied := cloneEntryAdmissionReflectValue(source)
			if !copied.IsValid() || !copied.Type().AssignableTo(value.Type().Elem()) {
				copied = source
			}
			cloned.SetMapIndex(iter.Key(), copied)
		}
		return cloned
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			source := value.Index(i)
			copied := cloneEntryAdmissionReflectValue(source)
			if !copied.IsValid() || !copied.Type().AssignableTo(value.Type().Elem()) {
				copied = source
			}
			cloned.Index(i).Set(copied)
		}
		return cloned
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			source := value.Index(i)
			copied := cloneEntryAdmissionReflectValue(source)
			if !copied.IsValid() || !copied.Type().AssignableTo(value.Type().Elem()) {
				copied = source
			}
			cloned.Index(i).Set(copied)
		}
		return cloned
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.New(value.Type().Elem())
		source := value.Elem()
		copied := cloneEntryAdmissionReflectValue(source)
		if !copied.IsValid() || !copied.Type().AssignableTo(value.Type().Elem()) {
			copied = source
		}
		cloned.Elem().Set(copied)
		return cloned
	case reflect.Struct:
		cloned := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			source := value.Field(i)
			target := cloned.Field(i)
			// Do not risk corrupting opaque structs with unexported fields. Such
			// values are retained by value; normal map/slice runtime payloads do
			// not take this branch.
			if !source.CanInterface() || !target.CanSet() {
				return value
			}
			copied := cloneEntryAdmissionReflectValue(source)
			if !copied.IsValid() || !copied.Type().AssignableTo(target.Type()) {
				copied = source
			}
			target.Set(copied)
		}
		return cloned
	default:
		return value
	}
}

// entryAdmissionExitPrivateOutput resolves one runner-supplied boundary exit
// against the graph selected by the control plane. Missing or malformed graph
// metadata must fail closed: exposing an output is irreversible, whereas a
// false positive only withholds it from public projections.
//
// This deliberately has an admission-specific name rather than sharing the
// task-commit helper: entry admission must handle a nil graph and a potentially
// malformed node index before dereferencing NodeAt.
func entryAdmissionExitPrivateOutput(g *graph.Graph, nodeName string) bool {
	if g == nil {
		return true
	}

	nodeIdx, ok := g.NodeIndex(nodeName)
	if !ok || nodeIdx < 0 || nodeIdx >= g.NodeCount() {
		return true
	}

	node := g.NodeAt(nodeIdx)
	// Treat an inconsistent index as an unknown node rather than accepting a
	// policy for a different node from a malformed graph snapshot.
	if node.Name != nodeName {
		return true
	}
	return node.Output != nil && node.Output.Private
}

// classifyAdmissionOutcome maps a SeedExecutionFromEntry result to the
// xflow_group_admission_total outcome label. Order matters: a backend/transport
// error always wins ("error") regardless of what resp happens to hold;
// otherwise resp.Duplicate distinguishes an idempotent duplicate-accepted
// retry ("duplicate") from a first-time admission, which is classified by
// resp.State — AdmissionStateAccepted ("accepted"), AdmissionStateConflict
// ("conflict"), or one of the two retryable refusals, which are reported as
// "refused" rather than being folded into "accepted": a refused seed created no
// execution, and an operator counting admissions must not see it as one.
func classifyAdmissionOutcome(resp SeedExecutionFromEntryResponse, err error) string {
	if err != nil {
		return "error"
	}
	if resp.Duplicate {
		return "duplicate"
	}
	switch resp.State {
	case AdmissionStateConflict:
		return "conflict"
	case AdmissionStateOverLimit, AdmissionStateQueueFull:
		return "refused"
	}
	return "accepted"
}
