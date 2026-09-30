package control

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
)

// ErrEnrollRejected is the ONLY enrollment error that ever leaves this package.
// Unknown code, revoked code, out-of-scope request, and rate-limit lockout all
// collapse into it, with an identical message, so a prober learns nothing about
// which codes exist or which of their guesses got closest (spec §2.3.4 item 4).
var ErrEnrollRejected = errors.New("enrollment rejected")

// Enroll exchanges a registration code for a freshly generated runner identity.
//
// This endpoint is unauthenticated by construction: the caller has no credential
// yet. Its defenses are the code's 32 bytes of entropy, the per-source failure
// lockout, and the fact that every rejection looks the same from outside.
func (c *Core) Enroll(ctx context.Context, req protocol.EnrollRequest, info TransportInfo) (protocol.EnrollResponse, error) {
	if c == nil || !EnrollDeclared(c.registrationCodes, c.issuedIdentities) {
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	if info.SourceIP == "" {
		c.auditEnroll(ctx, "", false, "missing source ip", "", "")
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	if !c.enrollLimiter.Allow(info.SourceIP) {
		c.auditEnroll(ctx, "", false, "source locked out", "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}

	code, err := c.registrationCodes.ResolveByPlaintext(ctx, req.RegistrationCode)
	if err != nil {
		c.enrollLimiter.RecordFailure(info.SourceIP)
		c.auditEnroll(ctx, "", false, err.Error(), "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	if code.PoolID == "" {
		c.enrollLimiter.RecordFailure(info.SourceIP)
		c.auditEnroll(ctx, code.ID, false, "registration code has no pool", "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	return c.enrollPool(ctx, code, req, info.SourceIP)
}

func (c *Core) enrollPool(ctx context.Context, code RegistrationCode, req protocol.EnrollRequest, sourceIP string) (protocol.EnrollResponse, error) {
	reject := func(reason, runnerID string) (protocol.EnrollResponse, error) {
		c.enrollLimiter.RecordFailure(sourceIP)
		c.auditEnroll(ctx, code.ID, false, reason, runnerID, sourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	if c.pools == nil {
		return reject("runner pool store not configured", "")
	}
	pool, err := c.pools.GetPool(ctx, code.PoolID, OwnerScope{All: true})
	if err != nil {
		return reject("runner pool unavailable: "+err.Error(), "")
	}
	if pool.Paused {
		return reject("runner pool paused", "")
	}
	if req.SystemID == "" {
		return reject("system id required", "")
	}
	if req.InstanceUID == "" {
		return reject("instance uid required", "")
	}

	effective, reason := poolEnrollRequest(pool, req)
	if reason != "" {
		return reject(reason, "")
	}
	prefix := c.enrollmentRunnerIDPrefixOrDefault()
	candidateID, token, err := GenerateRegistrationCode()
	if err != nil {
		c.auditEnroll(ctx, code.ID, false, "identity generation failed", "", sourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	candidateID = prefix + candidateID
	now := time.Now().UTC()
	scope := pool.Policy()
	scope.Name = candidateID
	scope.IDPrefix = prefix
	scope.AllowedNamespaces = append([]string(nil), effective.Namespaces...)
	scope.AllowedNodeTypes = append([]string(nil), effective.NodeTypes...)

	instanceResult, err := c.pools.EnrollInstance(ctx, store.EnrollInstanceRequest{
		PoolID:            pool.ID,
		SystemID:          req.SystemID,
		InstanceUID:       req.InstanceUID,
		CandidateRunnerID: candidateID,
		Now:               now,
	})
	if err != nil {
		return reject("runner instance enroll failed: "+err.Error(), "")
	}
	runnerID := instanceResult.Instance.RunnerID
	scope.Name = runnerID

	if instanceResult.Created {
		if err := c.registrationCodes.Consume(ctx, code.ID); err != nil {
			return reject(err.Error(), runnerID)
		}
		issued := c.newIssuedIdentity(code, scope, runnerID, token, pool.ID, now)
		if err := c.issuedIdentities.Issue(ctx, issued); err != nil {
			c.auditEnroll(ctx, code.ID, false, "identity persist failed", runnerID, sourceIP)
			return protocol.EnrollResponse{}, ErrEnrollRejected
		}
		c.enrollLimiter.RecordSuccess(sourceIP)
		c.auditEnroll(ctx, code.ID, true, "first", runnerID, sourceIP)
		return enrollResponse(issued, token, effective.Namespaces, pool.Labels), nil
	}

	identity, found, err := c.issuedIdentities.Lookup(ctx, runnerID)
	if err != nil {
		c.auditEnroll(ctx, code.ID, false, "identity lookup failed", runnerID, sourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	if !found {
		issued := c.newIssuedIdentity(code, scope, runnerID, token, pool.ID, now)
		if err := c.issuedIdentities.Issue(ctx, issued); err != nil {
			c.auditEnroll(ctx, code.ID, false, "identity heal failed", runnerID, sourceIP)
			return protocol.EnrollResponse{}, ErrEnrollRejected
		}
		c.enrollLimiter.RecordSuccess(sourceIP)
		c.auditEnroll(ctx, code.ID, true, "healed", runnerID, sourceIP)
		return enrollResponse(issued, token, effective.Namespaces, pool.Labels), nil
	}
	if !identity.RevokedAt.IsZero() {
		return reject("issued identity revoked", runnerID)
	}

	generation := credentialGeneration(identity)
	generation, err = c.issuedIdentities.RotateCredential(ctx, runnerID, generation, HashSecret(token), now.Add(c.rotationGrace))
	if errors.Is(err, ErrCredentialGenerationConflict) {
		identity, found, err = c.issuedIdentities.Lookup(ctx, runnerID)
		if err == nil && found {
			if !identity.RevokedAt.IsZero() {
				err = ErrIssuedIdentityNotFound
			} else {
				generation, err = c.issuedIdentities.RotateCredential(ctx, runnerID, credentialGeneration(identity), HashSecret(token), now.Add(c.rotationGrace))
			}
		}
	}
	if err != nil || !found {
		reason := "credential rotation failed"
		if err != nil {
			reason += ": " + err.Error()
		}
		return reject(reason, runnerID)
	}
	identity.CredentialGeneration = generation
	c.enrollLimiter.RecordSuccess(sourceIP)
	c.auditEnroll(ctx, code.ID, true, "reenroll", runnerID, sourceIP)
	return enrollResponse(identity, token, effective.Namespaces, pool.Labels), nil
}

func (c *Core) newIssuedIdentity(code RegistrationCode, scope RunnerPolicy, runnerID, token, poolID string, now time.Time) IssuedIdentity {
	issued := IssuedIdentity{
		RunnerID:             runnerID,
		TokenHash:            HashSecret(token),
		Scope:                scope,
		CodeID:               code.ID,
		OwnerNamespace:       code.OwnerNamespace,
		IssuedAt:             now,
		PoolID:               poolID,
		CredentialGeneration: 1,
	}
	if c.identityTTL > 0 {
		issued.ExpiresAt = now.Add(c.identityTTL)
	}
	return issued
}

func enrollResponse(identity IssuedIdentity, token string, namespaces []string, labels map[string]string) protocol.EnrollResponse {
	resp := protocol.EnrollResponse{
		RunnerID:             identity.RunnerID,
		Token:                token,
		Namespaces:           append([]string(nil), namespaces...),
		CredentialGeneration: credentialGeneration(identity),
	}
	if !identity.ExpiresAt.IsZero() {
		resp.ExpiresAt = identity.ExpiresAt.Format(time.RFC3339)
	}
	if labels != nil {
		resp.Labels = make(map[string]string, len(labels))
		for key, value := range labels {
			resp.Labels[key] = value
		}
	}
	return resp
}

func credentialGeneration(identity IssuedIdentity) int64 {
	if identity.CredentialGeneration == 0 {
		return 1
	}
	return identity.CredentialGeneration
}

func poolEnrollRequest(pool RunnerPool, req protocol.EnrollRequest) (protocol.EnrollRequest, string) {
	policy := pool.Policy()
	if len(req.Namespaces) == 0 {
		switch {
		case pool.InheritNamespaces && concreteNamespaces(pool.AllowedNamespaces) && len(pool.AllowedNamespaces) <= store.MaxInheritedNamespaces:
			req.Namespaces = append([]string(nil), pool.AllowedNamespaces...)
		case concreteNamespaces(pool.AllowedNamespaces) && policy.AllowsNamespace(namespace.Default):
			req.Namespaces = []string{string(namespace.Default)}
		default:
			return protocol.EnrollRequest{}, "namespaces required"
		}
	}
	for _, ns := range req.Namespaces {
		if err := namespace.Validate(namespace.Namespace(ns)); err != nil {
			return protocol.EnrollRequest{}, "invalid namespace: " + ns
		}
		if !policy.AllowsNamespace(namespace.Namespace(ns)) {
			return protocol.EnrollRequest{}, "namespace outside pool scope: " + ns
		}
	}
	if len(req.NodeTypes) == 0 {
		req.NodeTypes = append([]string(nil), pool.AllowedNodeTypes...)
	}
	for _, nodeType := range req.NodeTypes {
		if !policy.Allows(nodeType) {
			return protocol.EnrollRequest{}, "node type outside pool scope: " + nodeType
		}
	}
	return req, ""
}

func concreteNamespaces(namespaces []string) bool {
	for _, ns := range namespaces {
		if ns == "*" {
			return false
		}
	}
	return true
}

// renewIdentity extends the caller's own issued identity by identityTTL.
//
// The caller is the authenticated runner, not an operator: AuthenticateOngoing
// already proved the token matches req.RunnerID and that the identity is
// neither revoked nor expired — IssuedIdentityAuthenticator.authenticate
// checks RevokedAt and ExpiresAt right after the constant-time token compare,
// and this is the exact same authenticator every other ongoing runner
// endpoint (heartbeat, poll, etc.) runs behind. That is the whole reason an
// already-revoked or already-expired identity cannot renew itself: it cannot
// get past authentication to reach this method. IssuedIdentityStore.Renew's
// own revoked/expired guard is a second, defense-in-depth layer for the rare
// race where the identity is revoked between the authentication check above
// and the store write below — it is not the first line of defense.
//
// This is also why "runner A renews runner B" needs no id comparison anywhere
// in this method: AuthenticateOngoing authenticates the (runnerID, token)
// pair as a unit, so a token that authenticates at all can only ever prove
// req.RunnerID is the identity that token belongs to. There is no second,
// independently-authenticated id in scope to compare it against.
func (c *Core) renewIdentity(ctx context.Context, req protocol.RenewIdentityRequest, info TransportInfo) (protocol.RenewIdentityResponse, error) {
	if req.RunnerID == "" {
		return protocol.RenewIdentityResponse{}, ErrRunnerIDRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "renew_identity", info, authErr); err != nil {
		return protocol.RenewIdentityResponse{}, err
	}
	// No issued-identity store configured, or no TTL configured: there is
	// nothing to extend. Reporting a zero expiry (rather than an error) is what
	// tells a well-behaved runner to stop its renewal loop entirely, the same
	// signal Enroll sends when it issues an identity with no ExpiresAt.
	if c.issuedIdentities == nil || c.identityTTL <= 0 {
		return protocol.RenewIdentityResponse{}, nil
	}
	next := time.Now().UTC().Add(c.identityTTL)
	if err := c.issuedIdentities.Renew(ctx, req.RunnerID, next); err != nil {
		return protocol.RenewIdentityResponse{}, normalizeRunnerError(err, c.logger, "renew_identity")
	}
	return protocol.RenewIdentityResponse{ExpiresAt: next.Format(time.RFC3339)}, nil
}

// enrollScopeReason returns a non-empty server-side reason when the request asks
// for more than the code permits. Scope matching goes through RunnerPolicy so
// enroll and ongoing auth cannot disagree about what a scope means.
func enrollScopeReason(code RegistrationCode, req protocol.EnrollRequest) string {
	policy := code.Policy()
	for _, ns := range req.Namespaces {
		if err := namespace.Validate(namespace.Namespace(ns)); err != nil {
			return "invalid namespace: " + ns
		}
		if !policy.AllowsNamespace(namespace.Namespace(ns)) {
			return "namespace outside code scope: " + ns
		}
	}
	for _, nt := range req.NodeTypes {
		if !policy.Allows(nt) {
			return "node type outside code scope: " + nt
		}
	}
	return ""
}

// auditEnroll records the attempt. A failure to write the audit row must not
// change the enrollment verdict — but it must not be silent either, so it goes
// to the logger.
func (c *Core) auditEnroll(ctx context.Context, codeID string, success bool, reason, runnerID, sourceIP string) {
	if c.registrationCodes == nil {
		return
	}
	err := c.registrationCodes.AppendEnrollAudit(ctx, EnrollAuditRecord{
		CodeID:   codeID,
		Success:  success,
		Reason:   reason,
		RunnerID: runnerID,
		SourceIP: sourceIP,
		At:       time.Now().UTC(),
	})
	if err != nil && c.logger != nil {
		c.logger.Warn("control: enroll audit write failed", "error", err)
	}
}
