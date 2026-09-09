package control

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// renewIdentityFixture seeds one live issued identity directly (bypassing
// Enroll, which this test does not need) and wires a Core whose sole
// authenticator is the issued-identity path, the same authenticator every
// other ongoing runner endpoint runs behind.
func renewIdentityFixture(t *testing.T, ttl time.Duration, runnerID string) (*Core, *MemoryIssuedIdentityStore, string) {
	t.Helper()
	ids := NewMemoryIssuedIdentityStore()
	token := "renew-identity-test-token-" + runnerID
	// now must be real wall-clock time, not a fixed fixture epoch: the
	// authenticator's default clock is time.Now(), so an ExpiresAt computed
	// from a stale fixed epoch would already be in the past and every
	// authentication in this file would fail as "expired" rather than for the
	// reason each test actually means to exercise.
	now := time.Now().UTC()
	issued := IssuedIdentity{
		RunnerID:  runnerID,
		TokenHash: HashSecret(token),
		IssuedAt:  now,
	}
	if ttl > 0 {
		issued.ExpiresAt = now.Add(ttl)
	}
	if err := ids.Issue(context.Background(), issued); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	core := &Core{
		issuedIdentities: ids,
		identityTTL:      ttl,
		auth:             NewIssuedIdentityAuthenticator(ids),
	}
	return core, ids, token
}

// TestRenewIdentityExtendsExpiry is the happy path: a runner holding a valid
// token renews its own identity and gets back a later expires_at than the one
// it was issued with, and the store itself reflects the extension.
func TestRenewIdentityExtendsExpiry(t *testing.T) {
	const ttl = time.Hour
	core, ids, token := renewIdentityFixture(t, ttl, "runner-a")

	before, ok, err := ids.Lookup(context.Background(), "runner-a")
	if err != nil || !ok {
		t.Fatalf("Lookup before renew: ok=%v err=%v", ok, err)
	}

	// The response's expires_at is RFC3339 (whole-second precision), while
	// `before.ExpiresAt` was computed with sub-second precision moments ago.
	// Without a real gap, the two can land in the same wall-clock second and
	// the truncated renewal would compare equal-or-before the un-truncated
	// original — a false failure, not a real one. Sleeping past the second
	// boundary makes "after" unambiguous at RFC3339's own resolution.
	time.Sleep(1100 * time.Millisecond)

	resp, err := core.renewIdentity(context.Background(), protocol.RenewIdentityRequest{
		RunnerID:  "runner-a",
		AuthToken: token,
	}, TransportInfo{SourceIP: "10.0.0.1"})
	if err != nil {
		t.Fatalf("renewIdentity: %v", err)
	}
	if resp.ExpiresAt == "" {
		t.Fatal("resp.ExpiresAt is empty, want a later RFC3339 timestamp")
	}
	got, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("resp.ExpiresAt = %q is not RFC3339: %v", resp.ExpiresAt, err)
	}
	if !got.After(before.ExpiresAt) {
		t.Fatalf("renewed ExpiresAt = %v, want after original %v", got, before.ExpiresAt)
	}

	after, ok, err := ids.Lookup(context.Background(), "runner-a")
	if err != nil || !ok {
		t.Fatalf("Lookup after renew: ok=%v err=%v", ok, err)
	}
	if !after.ExpiresAt.Truncate(time.Second).Equal(got) {
		t.Fatalf("store ExpiresAt = %v, want %v (the value returned to the caller, RFC3339-truncated)", after.ExpiresAt, got)
	}
	// Renew must not touch anything except ExpiresAt.
	if after.TokenHash != before.TokenHash {
		t.Fatal("renewIdentity rotated the token hash; it must not")
	}
}

// TestRenewIdentityZeroTTLReturnsEmptyExpiry verifies that a server with no
// TTL configured (the pre-feature default) answers a renewal request with an
// empty expires_at and no error — the signal that tells a well-behaved runner
// to stop its renewal loop, not a failure.
func TestRenewIdentityZeroTTLReturnsEmptyExpiry(t *testing.T) {
	core, _, token := renewIdentityFixture(t, 0, "runner-a")

	resp, err := core.renewIdentity(context.Background(), protocol.RenewIdentityRequest{
		RunnerID:  "runner-a",
		AuthToken: token,
	}, TransportInfo{SourceIP: "10.0.0.1"})
	if err != nil {
		t.Fatalf("renewIdentity: %v", err)
	}
	if resp.ExpiresAt != "" {
		t.Fatalf("resp.ExpiresAt = %q, want empty when identityTTL is 0", resp.ExpiresAt)
	}
}

// TestRenewIdentityCannotRenewAnotherRunner is the structural test for
// corrections §5: runner A holds A's own valid token but names runner B in
// the request body. There is no id-comparison anywhere in renewIdentity — the
// rejection has to come from AuthenticateOngoing itself, because it
// authenticates the (runnerID, token) pair as a unit and A's token was never
// issued for B. If this test passes for the wrong reason (e.g. a stray
// comparison masking a broken authenticator wire-up), mutation 1 in the task
// report is what catches it.
func TestRenewIdentityCannotRenewAnotherRunner(t *testing.T) {
	core, ids, tokenA := renewIdentityFixture(t, time.Hour, "runner-a")
	// runner-b exists too, so a bug that fell through to "unknown runner"
	// instead of "wrong pairing" would not be caught by this test alone --
	// both runners are live identities known to the same store.
	//
	// B's IssuedAt/ExpiresAt must be real, non-expired wall-clock values (not
	// a fixed historical epoch): if B's identity were already expired from the
	// store's own perspective, a bypassed-auth mutation would still be
	// rejected by IssuedIdentityStore.Renew's independent expired-guard,
	// which would make this test go red for the wrong reason -- an unrelated
	// store-side rejection, not the auth-pairing rejection this test exists
	// to pin. Real, live times keep the two failure modes distinguishable.
	bNow := time.Now().UTC()
	bExpiresAt := bNow.Add(time.Hour)
	if err := ids.Issue(context.Background(), IssuedIdentity{
		RunnerID:  "runner-b",
		TokenHash: HashSecret("runner-b-own-token"),
		IssuedAt:  bNow,
		ExpiresAt: bExpiresAt,
	}); err != nil {
		t.Fatalf("Issue runner-b: %v", err)
	}

	_, err := core.renewIdentity(context.Background(), protocol.RenewIdentityRequest{
		RunnerID:  "runner-b",
		AuthToken: tokenA,
	}, TransportInfo{SourceIP: "10.0.0.1"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("A renewing B with A's token: err = %v, want ErrUnauthenticated", err)
	}

	// B's own identity must be untouched -- confirms the request was rejected
	// before any store write, not renewed and then separately reported as an
	// error.
	b, ok, lookupErr := ids.Lookup(context.Background(), "runner-b")
	if lookupErr != nil || !ok {
		t.Fatalf("Lookup runner-b: ok=%v err=%v", ok, lookupErr)
	}
	if !b.ExpiresAt.Equal(bExpiresAt) {
		t.Fatalf("runner-b ExpiresAt changed to %v; A's rejected request must not touch B's identity", b.ExpiresAt)
	}
}

// TestRenewIdentityRevokedTokenCannotAuthenticate confirms T4/T5's guarantee
// holds on this new endpoint too: a revoked identity's token cannot get past
// AuthenticateOngoing, so renewIdentity never reaches the store's own
// revoked/expired guard for the ordinary case -- authentication is the first
// line of defense, not IssuedIdentityStore.Renew.
func TestRenewIdentityRevokedTokenCannotAuthenticate(t *testing.T) {
	core, ids, token := renewIdentityFixture(t, time.Hour, "runner-a")
	if err := ids.Revoke(context.Background(), "runner-a"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	_, err := core.renewIdentity(context.Background(), protocol.RenewIdentityRequest{
		RunnerID:  "runner-a",
		AuthToken: token,
	}, TransportInfo{SourceIP: "10.0.0.1"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("renew with revoked token: err = %v, want ErrUnauthenticated", err)
	}
}

// TestRenewIdentityMissingRunnerID verifies the same required-field guard
// every other runner-protocol Core method has.
func TestRenewIdentityMissingRunnerID(t *testing.T) {
	core, _, _ := renewIdentityFixture(t, time.Hour, "runner-a")
	_, err := core.renewIdentity(context.Background(), protocol.RenewIdentityRequest{
		AuthToken: "irrelevant",
	}, TransportInfo{SourceIP: "10.0.0.1"})
	if !errors.Is(err, ErrRunnerIDRequired) {
		t.Fatalf("err = %v, want ErrRunnerIDRequired", err)
	}
}

// TestHandleRenewIdentityHTTPUsesAuthorizationHeader is an HTTP-layer test
// that a token arriving ONLY via the Authorization header (never in the JSON
// body) is what authenticates the renewal -- proving overrideTokenFromHeader
// is actually wired into this handler, not just present elsewhere in the
// file. Mutation 3 in the task report deletes that line and this is the test
// that must go red.
func TestHandleRenewIdentityHTTPUsesAuthorizationHeader(t *testing.T) {
	core, _, token := renewIdentityFixture(t, time.Hour, "runner-a")
	srv := &Server{core: core}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// Body carries only runner_id; the token rides the header, per the
	// package's convention that Authorization takes priority.
	resp := postAuthed(t, ts.URL+protocol.RenewIdentityPath, token, protocol.RenewIdentityRequest{
		RunnerID: "runner-a",
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
