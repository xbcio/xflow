package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ErrEntrySeedRequestInvalid is the transport-agnostic form of the seed
// endpoint's required-field rejection. The HTTP handler maps it back to the
// 400 + bare body it has always answered with (the body is pinned by
// entry_seed_bare_shape_test), and the in-process entry point returns it to its
// caller unchanged.
var ErrEntrySeedRequestInvalid = errors.New("admission_key, workflow_id, entry_unit_id and outcome are required")

// EntrySeedRequestFromWire validates the required fields of a wire seed request
// and converts it to the engine request. It is the ONE conversion both the HTTP
// handler and the in-process entry point use, so the two faces cannot drift on
// either the required fields or the server-side ResultHash.
//
// ResultHash is always computed here from outcome + exits; a client cannot
// supply it.
func EntrySeedRequestFromWire(req protocol.SeedExecutionRequest) (engine.SeedExecutionFromEntryRequest, error) {
	if req.AdmissionKey == "" || req.WorkflowID == "" || req.EntryUnitID == "" || req.Outcome == "" {
		return engine.SeedExecutionFromEntryRequest{}, ErrEntrySeedRequestInvalid
	}
	exits := make([]engine.BoundaryExit, 0, len(req.Exits))
	for _, ex := range req.Exits {
		exits = append(exits, engine.BoundaryExit{
			NodeName: ex.NodeName,
			Port:     ex.Port,
			Data:     ex.Data,
		})
	}
	outcome := engine.GroupOutcome(req.Outcome)
	return engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey(req.AdmissionKey),
		WorkflowID:      types.WorkflowID(req.WorkflowID),
		WorkflowVersion: req.WorkflowVersion,
		EntryUnitID:     req.EntryUnitID,
		Outcome:         outcome,
		Exits:           exits,
		Error:           req.Error,
		Generation:      req.Generation,
		ReplicaIndex:    req.ReplicaIndex,
		ResultHash:      engine.ComputeResultHash(outcome, exits),
	}, nil
}

// authenticateRunnerResource authenticates a runner-protocol call that reads or
// writes a namespace-scoped resource. It is the shared first half of the
// in-process seed and supply entry points: both must answer a bad credential
// exactly as every other runner-protocol method does, through authDeny.
func (c *Core) authenticateRunnerResource(ctx context.Context, runnerID, token, op string, info TransportInfo) (store.RunnerPolicy, error) {
	policy, authErr := c.authn().AuthenticateOngoing(runnerID, token, info)
	if err := c.authDeny(ctx, runnerID, token, op, info, authErr); err != nil {
		return store.RunnerPolicy{}, err
	}
	return policy, nil
}

// authorizeRunnerNamespace resolves the namespace a runner resource call acts
// on and checks it against the authenticated policy. An empty declaration means
// the default namespace, exactly as register's effective-namespace resolution
// treats it.
//
// A namespace denial deliberately does not go through authDeny: that collapses
// every failure into ErrUnauthenticated, and "authenticated but not entitled" is
// a different answer than "not authenticated". The observable result and the log
// line match register's namespace gate so the two paths cannot be told apart by
// their metrics.
func (c *Core) authorizeRunnerNamespace(ctx context.Context, policy store.RunnerPolicy, ns, op, runnerID string) (namespace.Namespace, error) {
	effective := namespace.Namespace(ns)
	if effective == "" {
		effective = namespace.Default
	}
	if err := namespace.Validate(effective); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidNamespace, err)
	}
	if !policy.AllowsNamespace(effective) {
		c.observeAuth(ctx, op, "deny_namespace")
		if c.logger != nil {
			c.logger.Error("auth_denied",
				"op", op, "reason", "namespace_not_granted",
				"runner", runnerID, "policy", policy.Name, "namespace", string(effective))
		}
		return "", fmt.Errorf("%w: policy %q does not grant namespace %q", ErrAuthNamespaceDenied, policy.Name, effective)
	}
	return effective, nil
}

// seedEntryForRunner is the in-process equivalent of POST /v1/executions: it
// applies the same runner authentication, namespace entitlement and generation
// fence the HTTP face applies, then maps the engine's admission result onto the
// caller-facing types.EntrySeedResponse.
//
// The mapping must produce what the HTTP handler plus HTTPEntrySeedRuntime
// decode produce together (see entrySeedResponseFromEngine); errors are returned
// unchanged so errors.Is can still name ErrStaleGeneration,
// ErrEntrySeedWorkflowUnknown and the concurrency sentinels.
func (c *Core) seedEntryForRunner(ctx context.Context, runnerID, token, ns string, req protocol.SeedExecutionRequest, info TransportInfo) (types.EntrySeedResponse, error) {
	if runnerID == "" {
		return types.EntrySeedResponse{}, ErrRunnerIDRequired
	}
	policy, err := c.authenticateRunnerResource(ctx, runnerID, token, "entry_seed", info)
	if err != nil {
		return types.EntrySeedResponse{}, err
	}
	effective, err := c.authorizeRunnerNamespace(ctx, policy, ns, "entry_seed", runnerID)
	if err != nil {
		return types.EntrySeedResponse{}, err
	}
	engReq, err := EntrySeedRequestFromWire(req)
	if err != nil {
		return types.EntrySeedResponse{}, err
	}
	// Delegate to the shared seed path so topology resolution and the
	// generation fence are the same ones the HTTP face runs.
	resp, err := c.SeedExecutionFromEntry(namespace.WithNamespace(ctx, effective), engReq)
	if err != nil {
		return types.EntrySeedResponse{}, err
	}
	return entrySeedResponseFromEngine(resp)
}

// entrySeedResponseFromEngine mirrors writeSeedAdmissionResult's body shapes and
// HTTPEntrySeedRuntime's decoding, collapsed into the caller-facing response:
//
//   - accepted             → Accepted (+ Duplicate), ExecutionID
//   - conflict             → Conflict, ExecutionID
//   - over_limit           → Refused, RetryAfter=protocol.ConcurrencyRetryAfterSeconds
//   - queue_full           → Refused+QueueFull, RetryAfter=protocol.ConcurrencyRetryAfterSeconds
//   - anything else        → error (a state the wire path would answer 200 with
//     but the runtime would refuse to classify)
func entrySeedResponseFromEngine(resp engine.SeedExecutionFromEntryResponse) (types.EntrySeedResponse, error) {
	switch resp.State {
	case engine.AdmissionStateAccepted:
		return types.EntrySeedResponse{
			Accepted:    true,
			Duplicate:   resp.Duplicate,
			ExecutionID: resp.ExecutionID,
		}, nil
	case engine.AdmissionStateConflict:
		return types.EntrySeedResponse{
			Conflict:    true,
			ExecutionID: resp.ExecutionID,
		}, nil
	case engine.AdmissionStateOverLimit:
		return types.EntrySeedResponse{
			Refused:    true,
			RetryAfter: protocol.ConcurrencyRetryAfterSeconds * time.Second,
		}, nil
	case engine.AdmissionStateQueueFull:
		return types.EntrySeedResponse{
			Refused:    true,
			QueueFull:  true,
			RetryAfter: protocol.ConcurrencyRetryAfterSeconds * time.Second,
		}, nil
	default:
		return types.EntrySeedResponse{}, fmt.Errorf("entry-seed: unknown admission state %q", resp.State)
	}
}

// fetchSupplyForRunner is the in-process equivalent of GET /v1/supplies/{name}:
// the namespace is the authenticated runner's (declared here, verified against
// its policy, never taken from a request body), and a missing resource answers
// store.ErrNotFound — the HTTP path's 404, which the runner's fetchers both map
// to ErrSupplyNotFound.
func (c *Core) fetchSupplyForRunner(ctx context.Context, runnerID, token, ns, name string, info TransportInfo) ([]byte, string, uint64, error) {
	if runnerID == "" {
		return nil, "", 0, ErrRunnerIDRequired
	}
	policy, err := c.authenticateRunnerResource(ctx, runnerID, token, "supply_read", info)
	if err != nil {
		return nil, "", 0, err
	}
	effective, err := c.authorizeRunnerNamespace(ctx, policy, ns, "supply_read", runnerID)
	if err != nil {
		return nil, "", 0, err
	}
	// A server with no supply store answers exactly like one that has no record
	// for the name: the HTTP route is not even mounted in that deployment, and
	// the runner reads the absence as a cold start rather than a fault.
	if c.supplies == nil || name == "" {
		return nil, "", 0, store.ErrNotFound
	}
	rec, err := c.supplies.GetSupply(ctx, string(effective), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, "", 0, store.ErrNotFound
		}
		return nil, "", 0, normalizeRunnerError(err, c.logger, "supply_read")
	}
	return rec.Content, rec.ContentHash, rec.Revision, nil
}
