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
	AdmissionKey    AdmissionKey
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
	// State is accepted or conflict.
	State AdmissionState
	// ExecutionID is the stable, deterministic execution ID for this admission key.
	ExecutionID types.ExecutionID
	// Duplicate is true when the same key+hash was already accepted (idempotent
	// retry); the same execution ID is returned.
	Duplicate bool
	// Skipped reports the downstream units this admission resolved as skip (see
	// SkippedUnit). An admission is a scheduling transition like any other: it
	// decrements the new execution's in-degree counters and picks execute or
	// skip per downstream unit. Nil when the backend does not report it or when
	// nothing was skipped.
	Skipped []SkippedUnit
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

	start := time.Now()
	resp, err := store.SeedExecutionFromEntry(ctx, admissionReq)
	d := time.Since(start)

	if isGroupEntry {
		e.notifyGroupAdmission(ctx, classifyAdmissionOutcome(resp, err), d)
	}
	// Only on a real admission. A duplicate retry applied no transition, so its
	// response carries no skips; a conflict or a transport error applied
	// nothing at all, and counting the skips of a transition that did not
	// happen would inflate the one series an operator alerts on.
	if err == nil && !resp.Duplicate && resp.State == AdmissionStateAccepted {
		e.notifySkip(ctx, flowEntry, resp.Skipped)
	}

	return resp, err
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
// resp.State — AdmissionStateAccepted ("accepted") or AdmissionStateConflict
// ("conflict"), the only two values AdmissionState defines.
func classifyAdmissionOutcome(resp SeedExecutionFromEntryResponse, err error) string {
	if err != nil {
		return "error"
	}
	if resp.Duplicate {
		return "duplicate"
	}
	if resp.State == AdmissionStateConflict {
		return "conflict"
	}
	return "accepted"
}
