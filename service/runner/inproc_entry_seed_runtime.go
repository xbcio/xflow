package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// InProcessEntrySeeder is the control-plane dependency of the in-process
// entry-seed runtime. service/runner cannot import service/control, and this is
// the whole surface it needs; *control.InProcessRunnerClient implements it.
type InProcessEntrySeeder interface {
	SeedEntry(ctx context.Context, runnerID, ns string, req protocol.SeedExecutionRequest) (types.EntrySeedResponse, error)
}

// InProcessSupplyReader is the control-plane dependency of
// InProcessSupplyFetcher. *control.InProcessRunnerClient implements it.
type InProcessSupplyReader interface {
	FetchSupply(ctx context.Context, runnerID, ns, name string) ([]byte, string, uint64, error)
}

// inProcessEntrySeedRuntime is the RunnerTransportInProc counterpart of
// protocol.HTTPEntrySeedRuntime: same fields, same response contract, same
// fail-closed legacy methods, but the admission is a call into the control
// plane's Core instead of an HTTP round trip.
//
// It exists so an embedded runner hosting triggers needs no ServerURL, token
// header or loopback listener — the transport that removed the runner-protocol
// socket would otherwise reintroduce one for every seed.
type inProcessEntrySeedRuntime struct {
	Seeder InProcessEntrySeeder
	// RunnerID identifies the runner to the control plane's resource
	// authentication.
	RunnerID string
	// Namespace is the activation namespace represented by this seed request.
	// It is a declaration only; Core verifies it against the runner policy.
	Namespace string
	// Generation and ReplicaIndex are fixed per activation, exactly as on the
	// HTTP runtime: this runtime is the authority for the activation it serves.
	Generation   uint64
	ReplicaIndex uint32
	// RequestTimeout bounds one admission round trip. Zero or negative uses
	// protocol.DefaultEntrySeedRequestTimeout.
	RequestTimeout time.Duration
	// Observer, when non-nil, receives one observation per attempt, classified
	// with protocol.AdmissionOutcome so both transports report one vocabulary.
	Observer types.EntrySeedObserver
}

var _ types.EntrySeedRuntime = (*inProcessEntrySeedRuntime)(nil)
var _ types.TriggerRuntime = (*inProcessEntrySeedRuntime)(nil)

// requestTimeout is the deadline this runtime actually applies: the configured
// value when positive, otherwise the shared protocol default. The fallback is
// here for the same reason HTTPEntrySeedRuntime has it — a zero timeout would
// reach context.WithTimeout as an already-expired context and turn every
// admission into an immediate deadline breach.
func (r *inProcessEntrySeedRuntime) requestTimeout() time.Duration {
	if r.RequestTimeout > 0 {
		return r.RequestTimeout
	}
	return protocol.DefaultEntrySeedRequestTimeout
}

// Emit is not supported: this runtime only admits via SeedExecutionFromEntry.
func (r *inProcessEntrySeedRuntime) Emit(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
	return "", protocol.ErrNonSeedPathUnsupported
}

// Dedup is not supported on the entry-seed path (admission is server-side).
func (r *inProcessEntrySeedRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return false, protocol.ErrNonSeedPathUnsupported
}

// TryLock is not supported on the entry-seed path.
func (r *inProcessEntrySeedRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return nil, false, protocol.ErrNonSeedPathUnsupported
}

// State is not supported on the entry-seed path; returns nil, matching the HTTP
// runtime (the cron watermark therefore remains unavailable on both).
func (r *inProcessEntrySeedRuntime) State(context.Context, string) types.TriggerState {
	return nil
}

// SeedExecutionFromEntry maps the caller-facing request to the wire DTO, admits
// it through the embedded control plane, and returns the caller-facing response.
// Every attempt is observed exactly once, through the single exit point below.
func (r *inProcessEntrySeedRuntime) SeedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	start := time.Now()
	resp, err := r.seedExecutionFromEntry(ctx, req)
	if r.Observer != nil {
		r.Observer.OnEntrySeedAdmission(ctx, protocol.AdmissionOutcome(resp, err), time.Since(start))
	}
	return resp, err
}

// seedExecutionFromEntry is the admission round trip itself. The request shape
// is the same one HTTPEntrySeedRuntime puts on the wire, because both faces
// feed the same control-plane Core.
func (r *inProcessEntrySeedRuntime) seedExecutionFromEntry(ctx context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	if r.Seeder == nil {
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: no in-process seeder configured")
	}
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
		Generation:      r.Generation,
		ReplicaIndex:    r.ReplicaIndex,
	}

	reqCtx, cancel := context.WithTimeout(ctx, r.requestTimeout())
	defer cancel()

	resp, err := r.Seeder.SeedEntry(reqCtx, r.RunnerID, r.Namespace, wireReq)
	if err != nil {
		// Wrapped with %w so the control plane's sentinels (stale generation,
		// unknown workflow, concurrency unavailable) remain matchable by the
		// caller, exactly as the HTTP runtime's transport errors are.
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: in-process admission failed: %w", err)
	}
	return resp, nil
}
