package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// renewLeaseAuthTestRunnerID is the fixed runner identity used by both
// helpers below. It must satisfy the "runner-" IDPrefix configured on the
// PolicyEntry the tests build.
const renewLeaseAuthTestRunnerID = "runner-renew-auth"

// renewLeaseAuthTestSessionID is filled in by newRenewLeaseAuthTestCore with
// the real session minted by Core.register, so renewLeaseAuthTestRequest can
// build a request that passes ValidateSession and isolates the assertions to
// authentication behavior. Package-level because the two helpers don't share
// a receiver; tests in this file run sequentially (no t.Parallel), so there
// is no cross-test interleaving on this variable.
var renewLeaseAuthTestSessionID string

// newRenewLeaseAuthTestCore assembles a *Core wired with the given
// Authenticator and a fresh MemoryRunnerDirectory, then registers a real
// runner session under renewLeaseAuthTestRunnerID using the policy's good
// token. Registering (rather than fabricating a session) means a caller
// using the resulting session ID reaches ValidateSession successfully, so a
// renewLease rejection can only be attributed to authentication.
func newRenewLeaseAuthTestCore(t *testing.T, auth Authenticator) *Core {
	t.Helper()

	c := &Core{
		runners:  NewMemoryRunnerDirectory(),
		auth:     auth,
		pollWait: time.Second,
	}

	resp, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    renewLeaseAuthTestRunnerID,
		Concurrency: 1,
		AuthToken:   "good-token",
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	renewLeaseAuthTestSessionID = resp.SessionID

	return c
}

// renewLeaseAuthTestRequest builds a RenewLeaseRequest for the runner/session
// registered by newRenewLeaseAuthTestCore, carrying the given auth token. The
// lease fields are deliberately fake: whether the lease itself is found is
// irrelevant to what these tests pin (see the package doc comment on the
// positive-control test).
func renewLeaseAuthTestRequest(token string) protocol.RenewLeaseRequest {
	return protocol.RenewLeaseRequest{
		RunnerID:   renewLeaseAuthTestRunnerID,
		SessionID:  renewLeaseAuthTestSessionID,
		LeaseID:    "lease-does-not-exist",
		LeaseToken: "lease-token-does-not-exist",
		AuthToken:  token,
	}
}

// TestRenewLeaseRejectsBadToken pins that /v1/runners/lease/renew authenticates
// like every other runner-protocol path. A live session ID is not a credential:
// it is a routing key the server itself issued, and this package refuses to
// treat it as proof of identity anywhere else.
func TestRenewLeaseRejectsBadToken(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:             "r",
			IDPrefix:         "runner-",
			Token:            "good-token",
			AllowedNodeTypes: []string{"*"},
		}},
	}, false)
	if err != nil {
		t.Fatalf("policy store: %v", err)
	}

	c := newRenewLeaseAuthTestCore(t, store)

	_, gotErr := c.renewLease(context.Background(), renewLeaseAuthTestRequest("wrong-token"), TransportInfo{})
	if gotErr == nil {
		t.Fatal("renewLease accepted a wrong token; it must authenticate like heartbeat/poll/report")
	}
	// NOTE: deliberate deviation from the brief's literal assertion. authDeny
	// (core.go:163-182) intentionally collapses every specific Authenticator
	// error, including ErrAuthUnknownToken, into the transport-agnostic
	// ErrUnauthenticated sentinel before returning it to the caller — see the
	// "Wrapping keeps callers from having to know the specific policy denial
	// reason" comment at auth.go:16-17, and TestCoreAuthObserverRecordsAllowAndDeny
	// in auth_test.go, which pins exactly this behavior for heartbeat. Asserting
	// errors.Is(gotErr, ErrAuthUnknownToken) here would fail even against the
	// correct, brief-mandated implementation (verified: it does — see the task
	// report). ErrUnauthenticated is what every other runner-protocol path
	// actually surfaces, so that is what this test pins instead.
	if !errors.Is(gotErr, ErrUnauthenticated) {
		t.Fatalf("want ErrUnauthenticated, got %v", gotErr)
	}
}

// TestRenewLeaseAcceptsGoodToken is the positive control: the same call with the
// policy's token must get past authentication. Without it, Step 3's negative
// test would still pass if renewLease rejected everything unconditionally.
func TestRenewLeaseAcceptsGoodToken(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:             "r",
			IDPrefix:         "runner-",
			Token:            "good-token",
			AllowedNodeTypes: []string{"*"},
		}},
	}, false)
	if err != nil {
		t.Fatalf("policy store: %v", err)
	}

	c := newRenewLeaseAuthTestCore(t, store)

	_, gotErr := c.renewLease(context.Background(), renewLeaseAuthTestRequest("good-token"), TransportInfo{})
	if errors.Is(gotErr, ErrAuthUnknownToken) || errors.Is(gotErr, ErrAuthMissingToken) {
		t.Fatalf("good token was rejected by authentication: %v", gotErr)
	}
}
