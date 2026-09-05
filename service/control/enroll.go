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
	if c == nil || c.registrationCodes == nil || c.issuedIdentities == nil {
		// Enroll was not configured on this server. Same external error as a bad
		// code: whether the feature is on is not something a caller needs to be
		// told apart from a wrong guess.
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	// The limiter buckets by source. An empty SourceIP would merge every
	// unattributable caller into one bucket: ten failures anywhere would lock out
	// all of them, and one success anywhere would clear all of their counters.
	// This endpoint is unauthenticated and the limiter is its only brute-force
	// control, so refuse rather than bucket. Reaching here with an empty SourceIP
	// means enroll was wired onto a transport that does not populate it.
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

	// The runner id is generated here and nowhere else. req.ProposedRunnerID is
	// read only for the audit trail; letting it decide the id would turn enroll
	// into "become any runner you can name".
	runnerID, token, err := GenerateRegistrationCode()
	if err != nil {
		c.auditEnroll(ctx, code.ID, false, "identity generation failed", "", info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}
	runnerID = "runner-" + runnerID

	issued := IssuedIdentity{
		RunnerID:  runnerID,
		TokenHash: HashSecret(token),
		// The issued scope is the code's scope, narrowed to what the runner
		// actually asked for. A runner that asks for one namespace does not get
		// the code's full ceiling.
		Scope:    issuedScope(code, req, runnerID),
		CodeID:   code.ID,
		IssuedAt: time.Now().UTC(),
	}
	if err := c.issuedIdentities.Issue(ctx, issued); err != nil {
		c.auditEnroll(ctx, code.ID, false, "identity persist failed", runnerID, info.SourceIP)
		return protocol.EnrollResponse{}, ErrEnrollRejected
	}

	c.enrollLimiter.RecordSuccess(info.SourceIP)
	c.auditEnroll(ctx, code.ID, true, "", runnerID, info.SourceIP)
	return protocol.EnrollResponse{RunnerID: runnerID, Token: token}, nil
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
