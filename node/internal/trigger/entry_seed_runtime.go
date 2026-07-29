package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// entrySeedRequestTimeout bounds a single seed round-trip. If the control plane
// does not respond within this window the request is treated as a transient
// failure (returns an error → the caller does NOT commit the Kafka offset, so
// the message is redelivered).
const entrySeedRequestTimeout = 15 * time.Second

// HTTPEntrySeedRuntime is the production implementation of
// types.EntrySeedRuntime. It posts an entry-unit seed admission to the control
// plane's POST /v1/executions endpoint and maps the response back.
//
// Response mapping:
//   - body State=="accepted" (HTTP 2xx) → EntrySeedResponse{Accepted:true} (+ Duplicate from body)
//   - body State=="conflict" or HTTP 409 → EntrySeedResponse{Conflict:true}
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
	// Generation is the entry-activation generation this runtime serves. It is
	// stamped onto every seed request so the control-plane fence admits exactly
	// the current-generation seeds. The runtime is constructed PER ACTIVATION
	// with the directive's generation and is the authority for the activation it
	// serves (IMPORTANT-1).
	Generation uint64
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
func (h *HTTPEntrySeedRuntime) SeedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	exits := make([]protocol.BoundaryExit, 0, len(req.Exits))
	for _, ex := range req.Exits {
		exits = append(exits, protocol.BoundaryExit{
			NodeName: ex.NodeName,
			Port:     ex.Port,
			Data:     ex.Data,
		})
	}
	wireReq := protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      string(req.WorkflowID),
		WorkflowVersion: req.WorkflowVersion,
		EntryUnitID:     req.EntryUnitID,
		AdmissionKey:    req.AdmissionKey,
		Outcome:         req.Outcome,
		Exits:           exits,
		Error:           req.Error,
		Generation:      h.Generation,
	}

	body, err := json.Marshal(wireReq)
	if err != nil {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: marshal request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, entrySeedRequestTimeout)
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

	// HTTP 409 (or a body State=="conflict") means another runner already
	// admitted a result for this admission key — the admission is handled.
	if httpResp.StatusCode == http.StatusConflict {
		var wireResp protocol.SeedExecutionResponse
		_ = json.NewDecoder(httpResp.Body).Decode(&wireResp)
		return types.EntrySeedResponse{
			Conflict:    true,
			ExecutionID: types.ExecutionID(wireResp.ExecutionID),
		}, nil
	}

	// Any other non-2xx is transient/unexpected → error (no offset commit).
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: unexpected status %d", httpResp.StatusCode)
	}

	var wireResp protocol.SeedExecutionResponse
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
