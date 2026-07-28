package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

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
