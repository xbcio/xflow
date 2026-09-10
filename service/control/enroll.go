package control

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
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
		// Enroll was not configured on this server. Same external error as a bad
		// code: whether the feature is on is not something a caller needs to be
		// told apart from a wrong guess.
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	// The limiter buckets by source. An empty SourceIP has no source to bucket
	// by, and every enrollLimiter method (Allow/RecordFailure/RecordSuccess)
	// short-circuits to a no-op on "" — so an empty SourceIP does not merge
	// into a shared bucket, it silently bypasses the limiter entirely. This
	// endpoint is unauthenticated and the limiter is its only brute-force
	// control, so refuse rather than let any caller skip it.
	//
	// An empty SourceIP is not only "enroll was wired onto a transport that
	// doesn't populate it" (a future caller passing a zero-value TransportInfo).
	// sourceIPOf (the HTTP runner face's populator) can itself legitimately
	// return "" — on an empty or malformed RemoteAddr, on a Unix domain socket
	// listener (RemoteAddr commonly "" or "@"), or on a RemoteAddr with an
	// empty host such as ":1234". Whatever the cause, there is no attributable
	// source, so refuse.
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
		// code.ID is empty here by construction — an unresolvable code has no id
		// to attribute the attempt to. The record still lands, so a brute-force
		// run is visible server-side.
		c.enrollLimiter.RecordFailure(info.SourceIP)
		c.auditEnroll(ctx, "", false, err.Error(), "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}

	if reason := enrollScopeReason(code, req); reason != "" {
		c.enrollLimiter.RecordFailure(info.SourceIP)
		c.auditEnroll(ctx, code.ID, false, reason, "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}

	// Claim a use of the code BEFORE issuing anything. The order is deliberate
	// and fail-closed: a crash between this and Issue burns one slot without
	// enrolling a runner — visible in use_count and recoverable by minting a new
	// code — whereas issuing first and counting after would silently overissue
	// past the ceiling on exactly the crash the ceiling exists to survive.
	//
	// It runs after the scope check for the same reason it is not folded into
	// ResolveByPlaintext: a request the scope check will reject must not spend
	// one of the code's uses.
	if err := c.registrationCodes.Consume(ctx, code.ID); err != nil {
		// The reason is named in the audit trail and nowhere else; the caller
		// gets the same ErrEnrollRejected every other rejection returns. That is
		// not an existence oracle: reaching this line already proved the caller
		// holds a live, in-scope code.
		c.enrollLimiter.RecordFailure(info.SourceIP)
		c.auditEnroll(ctx, code.ID, false, err.Error(), "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}

	// The runner id is generated here and nowhere else. req.ProposedRunnerID is
	// read only for the audit trail; letting it decide the id would turn enroll
	// into "become any runner you can name".
	runnerID, token, err := GenerateRegistrationCode()
	if err != nil {
		c.auditEnroll(ctx, code.ID, false, "identity generation failed", "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	runnerID = "runner-" + runnerID

	now := time.Now().UTC()
	issued := IssuedIdentity{
		RunnerID:  runnerID,
		TokenHash: HashSecret(token),
		// The issued scope is the code's scope, narrowed to what the runner
		// actually asked for. A runner that asks for one namespace does not get
		// the code's full ceiling.
		Scope:  issuedScope(code, req, runnerID),
		CodeID: code.ID,
		// Snapshot the code's owner so revoking this identity later stays
		// inside one tenant. Copied, not joined through CodeID: the identity
		// outlives the code by design (see IssuedIdentity.Scope), so a deleted
		// code must not erase who owns the runner.
		OwnerNamespace: code.OwnerNamespace,
		IssuedAt:       now,
	}
	// c.identityTTL == 0 (the default) leaves ExpiresAt zero, meaning "never
	// expires" — the pre-feature behavior. Only WithIdentityTTL turns this on.
	if c.identityTTL > 0 {
		issued.ExpiresAt = now.Add(c.identityTTL)
	}
	if err := c.issuedIdentities.Issue(ctx, issued); err != nil {
		c.auditEnroll(ctx, code.ID, false, "identity persist failed", runnerID, info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}

	c.enrollLimiter.RecordSuccess(info.SourceIP)
	c.auditEnroll(ctx, code.ID, true, "", runnerID, info.SourceIP)
	resp := protocol.EnrollResponse{RunnerID: runnerID, Token: token}
	if !issued.ExpiresAt.IsZero() {
		resp.ExpiresAt = issued.ExpiresAt.Format(time.RFC3339)
	}
	return resp, nil
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

// issuedScope narrows the code's ceiling to what the runner asked for. An empty
// request list means "everything the code allows".
func issuedScope(code RegistrationCode, req protocol.EnrollRequest, runnerID string) RunnerPolicy {
	scope := code.Policy()
	scope.Name = runnerID
	if len(req.Namespaces) > 0 {
		scope.AllowedNamespaces = append([]string(nil), req.Namespaces...)
	}
	if len(req.NodeTypes) > 0 {
		scope.AllowedNodeTypes = append([]string(nil), req.NodeTypes...)
	}
	return scope
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
