package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xbcio/xflow/types"
)

// DefaultEntrySeedRequestTimeout bounds a single seed round-trip when the
// caller does not choose its own. If the control plane does not respond within
// this window the request is treated as a transient failure (returns an error →
// the caller does NOT commit the Kafka offset, so the message is redelivered).
//
// It is only a default, not a bound on what the protocol supports. The window
// a deployment actually needs is a function of the batch size IT chose: the
// seed request carries one batch worth of exits, and its latency grows with
// that batch. Measured on a real deployment against the same control plane:
//
//	batch   samples     mean      max
//	   30   111,789    1.356s    2.620s
//	  100     7,357    2.017s    9.141s
//	  150         —         —   breached 15s
//
// At batch=150 the admission returned context.DeadlineExceeded, the offset was
// left uncommitted, and the whole batch was redelivered — which re-executes
// completed downstream work and, at that volume, raises the execution-creation
// rate rather than shedding load. A constant therefore caps the integrator's
// throughput knob at a value unrelated to its deployment. Use
// HTTPEntrySeedRuntime.RequestTimeout to raise it; see its doc for why an
// over-large value is not free.
const DefaultEntrySeedRequestTimeout = 15 * time.Second

// HTTPEntrySeedRuntime is the production implementation of
// types.EntrySeedRuntime. It posts an entry-unit seed admission to the control
// plane's POST /v1/executions endpoint and maps the response back.
//
// Response mapping:
//   - body State=="accepted" (HTTP 2xx) → EntrySeedResponse{Accepted:true} (+ Duplicate from body)
//   - HTTP 409 with body State=="conflict" → EntrySeedResponse{Conflict:true}
//     (another runner already admitted this key — handled → caller commits)
//   - HTTP 409 WITHOUT state=="conflict" (fence rejection, e.g.
//     {"error":"stale_generation"}) → non-nil error, so the caller leaves the
//     Kafka offset uncommitted and the message is redelivered to the current
//     generation owner (offset-safety; prevents silent message loss)
//   - any other non-2xx status or a transport/decode error → non-nil error, so
//     the caller leaves the Kafka offset uncommitted and Kafka redelivers.
//
// Security (org policy §7): the bearer token and the request/response bodies are
// never logged. On failure only the HTTP status code enters the returned error.
type HTTPEntrySeedRuntime struct {
	// BaseURL is the control-plane origin, e.g. "https://control.internal".
	// The endpoint path "/v1/executions" is appended.
	BaseURL string
	// Client is the HTTP client used for the round-trip. If nil, a default
	// client is used.
	Client *http.Client
	// Token, when non-empty, is sent as "Authorization: Bearer <Token>".
	Token string
	// RunnerID, when non-empty, is sent to identify an enrollment-issued
	// runner to the control plane's HTTP resource authentication middleware.
	RunnerID string
	// Namespace is the activation namespace represented by this seed request.
	// It is a declaration only; the server verifies it against the issued
	// runner policy before using it.
	Namespace string
	// Generation is the entry-activation generation this runtime serves. It is
	// stamped onto every seed request so the control-plane fence admits exactly
	// the current-generation seeds. The runtime is constructed PER ACTIVATION
	// with the directive's generation and is the authority for the activation it
	// serves (IMPORTANT-1).
	Generation uint64
	// ReplicaIndex is the sibling activation this runtime serves. Like
	// Generation, it is fixed when the activation starts and overwrites anything
	// supplied by the trigger call path.
	ReplicaIndex uint32
	// RequestTimeout bounds one seed admission round trip. Zero or negative
	// uses DefaultEntrySeedRequestTimeout, so an embedder that sets nothing
	// keeps the 15s window this runtime has always applied.
	//
	// Raise it when a batch is large enough that the control plane cannot admit
	// it in 15s: the deadline is what turns a slow-but-successful admission into
	// a redelivered batch, and redelivery re-executes the work downstream of the
	// entry unit. Do NOT raise it past what the caller can afford to wait,
	// though — the HTTP transport holds one trigger partition's flush for the
	// whole window, and the caller's http.Client.Timeout (when larger) is the
	// only thing above it.
	RequestTimeout time.Duration
	// Observer, when non-nil, receives one observation per admission attempt:
	// its outcome and the round-trip duration. nil disables observation, which
	// is the default so a host that wires no metrics pays nothing.
	//
	// The outcome is this runtime's own classification of the attempt, not
	// something the caller derives from the response: only here is a deadline
	// breach still distinguishable from the other transport failures, because
	// only here is the context error in hand when the failure is classified.
	Observer types.EntrySeedObserver
}

// requestTimeout is the deadline this runtime actually applies: the configured
// value when it is positive, otherwise DefaultEntrySeedRequestTimeout.
//
// The fallback lives here rather than at each construction site because a
// non-positive value must never become a deadline. A zero timeout reaches
// context.WithTimeout as an already-expired context, which turns every
// admission into an immediate deadline-exceeded and withholds every offset —
// an unbounded redelivery loop, not a slow one.
func (h *HTTPEntrySeedRuntime) requestTimeout() time.Duration {
	if h.RequestTimeout > 0 {
		return h.RequestTimeout
	}
	return DefaultEntrySeedRequestTimeout
}

var _ types.EntrySeedRuntime = (*HTTPEntrySeedRuntime)(nil)

// HTTPEntrySeedRuntime is also a full types.TriggerRuntime so a trigger handler
// can receive it directly as TriggerActivateInput.Runtime. In entry-seed mode
// (the only mode this runtime is used for) the trigger routes every message
// through SeedExecutionFromEntry, so the Emit/Dedup/TryLock/State methods below
// are never exercised. They are implemented as fail-closed stubs: if a trigger
// were ever mis-wired onto the legacy Emit path with this runtime, the returned
// error prevents an offset commit (the message is redelivered) rather than
// silently dropping it.
var _ types.TriggerRuntime = (*HTTPEntrySeedRuntime)(nil)

// errNonSeedPathUnsupported is returned by the legacy TriggerRuntime methods.
// This runtime only supports the entry-seed admission path.
var errNonSeedPathUnsupported = errors.New("entry-seed: runtime supports only the entry-seed admission path")

// Emit is not supported: this runtime only admits via SeedExecutionFromEntry.
func (h *HTTPEntrySeedRuntime) Emit(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
	return "", errNonSeedPathUnsupported
}

// Dedup is not supported on the entry-seed path (admission is server-side).
func (h *HTTPEntrySeedRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return false, errNonSeedPathUnsupported
}

// TryLock is not supported on the entry-seed path.
func (h *HTTPEntrySeedRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return nil, false, errNonSeedPathUnsupported
}

// State is not supported on the entry-seed path; returns nil.
func (h *HTTPEntrySeedRuntime) State(context.Context, string) types.TriggerState {
	return nil
}

// SeedExecutionFromEntry maps the caller-facing request to the wire DTO, POSTs
// it, and maps the wire response back. See the type doc for the state mapping.
//
// This function is a thin timing shell around seedExecutionFromEntry: every
// attempt is observed exactly once, through the single exit point below, so no
// future return path can be added inside without being counted. The outcome is
// classified HERE rather than by the caller because a deadline breach is only
// distinguishable from the other transport failures while the context error is
// still in hand.
func (h *HTTPEntrySeedRuntime) SeedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	start := time.Now()
	resp, err := h.seedExecutionFromEntry(ctx, req)
	h.observeAdmission(ctx, resp, err, time.Since(start))
	return resp, err
}

// observeAdmission reports one attempt to h.Observer, when one is installed. A
// nil observer is the default and costs one nil check.
func (h *HTTPEntrySeedRuntime) observeAdmission(ctx context.Context, resp types.EntrySeedResponse, err error, d time.Duration) {
	if h.Observer == nil {
		return
	}
	h.Observer.OnEntrySeedAdmission(ctx, admissionOutcome(resp, err), d)
}

// admissionOutcome classifies one admission attempt for the metrics label. It
// is a closed enum computed from the response and the error, never from a
// server-supplied or free-text value, so it cannot open unbounded label
// cardinality in a process-lifetime registry.
//
// "timeout" is separated from "error" deliberately. Both withhold the offset
// and redeliver the batch, but they call for opposite operator responses: a
// timeout means this deployment's batch has outgrown its admission window (a
// configuration fact, fixable by raising the window or lowering the batch),
// whereas a reset or a refused connection is an infrastructure fault. Folded
// together, the series cannot say which of the two is happening — which is
// exactly the position the runner's log line left an integrator in.
func admissionOutcome(resp types.EntrySeedResponse, err error) string {
	switch {
	case resp.Conflict:
		return "conflict"
	case resp.Accepted && resp.Duplicate:
		// A duplicate is a redelivery whose admission already succeeded; it is
		// still an accepted outcome for offset purposes (the caller commits),
		// but it is the only evidence of how often redelivery happens, so it
		// keeps its own label rather than folding into "accepted".
		return "duplicate"
	case resp.Accepted:
		return "accepted"
	case err == nil:
		// No error and none of the three flags: the caller treats this as a
		// protocol disagreement. Classified here as "error" so the counter
		// accounts for the attempt either way.
		return "error"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "error"
	}
}

// seedExecutionFromEntry is the admission round trip itself.
func (h *HTTPEntrySeedRuntime) seedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	exits := make([]BoundaryExit, 0, len(req.Exits))
	for _, ex := range req.Exits {
		exits = append(exits, BoundaryExit{
			NodeName: ex.NodeName,
			Port:     ex.Port,
			Data:     ex.Data,
		})
	}
	wireReq := SeedExecutionRequest{
		ProtocolVersion: EntrySeedProtocolVersion,
		WorkflowID:      string(req.WorkflowID),
		WorkflowVersion: req.WorkflowVersion,
		EntryUnitID:     req.EntryUnitID,
		AdmissionKey:    req.AdmissionKey,
		Outcome:         req.Outcome,
		Exits:           exits,
		Error:           req.Error,
		Generation:      h.Generation,
		ReplicaIndex:    h.ReplicaIndex,
	}

	body, err := json.Marshal(wireReq)
	if err != nil {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: marshal request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout())
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, h.BaseURL+"/v1/executions", bytes.NewReader(body))
	if err != nil {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if h.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+h.Token)
	}
	if h.RunnerID != "" {
		httpReq.Header.Set(RunnerIDHeader, h.RunnerID)
	}
	if h.Namespace != "" {
		httpReq.Header.Set("X-Xflow-Namespace", h.Namespace)
	}

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		// Transport error (timeout, connection reset, etc.) — transient.
		// Do not include the URL/token in the error; wrap only the low-level err.
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: request failed: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
	}()

	// HTTP 409 is overloaded by the control plane for TWO distinct situations
	// with DIFFERENT bodies, and they MUST be handled differently for offset
	// safety:
	//
	//   1. Genuine admission conflict — another runner already admitted a result
	//      for this admission key. Body: {"state":"conflict","execution_id":...}.
	//      This IS handled → return Conflict:true so the caller commits the Kafka
	//      offset (the message is consumed by the other runner's result).
	//
	//   2. Generation fence rejection (e.g. stale_generation) — this runner posted
	//      a NEW admission key at a superseded generation. Body:
	//      {"error":"stale_generation"} (errorResponse; NO "state" field, so it
	//      decodes to State==""). This is NOT handled: the correctly-generationed
	//      new owner has not processed this message yet. Returning Conflict here
	//      would make the caller commit the offset (kafka.go), so Kafka would
	//      never redeliver and the new owner would never see the message — SILENT
	//      MESSAGE LOSS during a generation upgrade / reassignment. Therefore this
	//      case must return a NON-NIL error so the caller leaves the offset
	//      UNCOMMITTED and Kafka redelivers to the new owner.
	//
	// We distinguish the two by decoding a struct carrying both the "state" and
	// "error" json fields: only the genuine-conflict body sets state=="conflict".
	if httpResp.StatusCode == http.StatusConflict {
		var body struct {
			State       string `json:"state"`
			ExecutionID string `json:"execution_id"`
			Error       string `json:"error"`
		}
		_ = json.NewDecoder(httpResp.Body).Decode(&body)
		if body.State == "conflict" {
			// Genuine admission conflict — handled; caller commits the offset.
			return types.EntrySeedResponse{
				Conflict:    true,
				ExecutionID: types.ExecutionID(body.ExecutionID),
			}, nil
		}
		// Fence rejection (stale_generation, etc.) — NOT handled. Return an error
		// so the caller does NOT commit the offset (offset-safety: Kafka must
		// redeliver to the current-generation owner). The reason string is a
		// server-controlled classifier only; it carries no token/URL/params.
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: rejected by generation fence: %s", body.Error)
	}

	// Any other non-2xx is transient/unexpected → error (no offset commit).
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: unexpected status %d", httpResp.StatusCode)
	}

	var wireResp SeedExecutionResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&wireResp); err != nil {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: decode response: %w", err)
	}

	switch wireResp.State {
	case "accepted":
		return types.EntrySeedResponse{
			Accepted:    true,
			Duplicate:   wireResp.Duplicate,
			ExecutionID: types.ExecutionID(wireResp.ExecutionID),
		}, nil
	case "conflict":
		return types.EntrySeedResponse{
			Conflict:    true,
			ExecutionID: types.ExecutionID(wireResp.ExecutionID),
		}, nil
	default:
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: unknown response state %q", wireResp.State)
	}
}
