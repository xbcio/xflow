// Package storecontract holds cross-implementation contract tests for
// store.* interfaces. It exists as its own package — rather than living in
// store itself, or in service/control as the original plan called for —
// because it imports "testing", and testing must never enter cmd/server's
// production dependency graph. store/node_contract.go already establishes
// that a shared contract belongs to a non-_test.go file (so
// store/sqlstore's tests can reach it); this package extends that rule to
// contracts that need testing.T itself, which store/node_contract.go's var
// export deliberately avoided.
//
// Only _test.go files may import this package.
package storecontract

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
)

// RunRegistrationCodeStoreContract exercises every behavior Core.Enroll depends
// on. Both the in-memory store and the SQL store must pass it identically —
// otherwise dev and production disagree about what "revoked" or "constant-time
// lookup" means, and the disagreement only shows up in production.
//
// factory returns a fresh, empty store per call.
func RunRegistrationCodeStoreContract(t *testing.T, factory func(t *testing.T) store.RegistrationCodeStore) {
	t.Helper()
	ctx := context.Background()

	mk := func(t *testing.T, st store.RegistrationCodeStore, namespaces []string) (string, string) {
		t.Helper()
		id, plaintext, err := store.GenerateRegistrationCode()
		if err != nil {
			t.Fatalf("GenerateRegistrationCode: %v", err)
		}
		err = st.Create(ctx, store.RegistrationCode{
			ID: id, CodeHash: store.HashSecret(plaintext),
			AllowedNamespaces: namespaces, AllowedNodeTypes: []string{"*"},
			CreatedAt: time.Unix(1700000000, 0).UTC(),
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		return id, plaintext
	}

	t.Run("resolve returns the created code", func(t *testing.T) {
		st := factory(t)
		id, plaintext := mk(t, st, []string{"sas"})
		got, err := st.ResolveByPlaintext(ctx, plaintext)
		if err != nil {
			t.Fatalf("ResolveByPlaintext: %v", err)
		}
		if got.ID != id {
			t.Fatalf("id = %q, want %q", got.ID, id)
		}
		if !got.Policy().AllowsNamespace("sas") || got.Policy().AllowsNamespace("other") {
			t.Fatalf("scope did not round-trip: %+v", got)
		}
		if got.CodeHash != store.HashSecret(plaintext) {
			t.Fatal("CodeHash did not round-trip; a truncated column would corrupt every future lookup")
		}
	})

	t.Run("unknown code", func(t *testing.T) {
		st := factory(t)
		mk(t, st, []string{"sas"})
		if _, err := st.ResolveByPlaintext(ctx, "definitely-not-a-code"); err != store.ErrRegistrationCodeUnknown {
			t.Fatalf("err = %v, want ErrRegistrationCodeUnknown", err)
		}
	})

	t.Run("revoked code", func(t *testing.T) {
		st := factory(t)
		id, plaintext := mk(t, st, []string{"sas"})
		if err := st.Revoke(ctx, id, store.OwnerScope{All: true}); err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		if _, err := st.ResolveByPlaintext(ctx, plaintext); err != store.ErrRegistrationCodeRevoked {
			t.Fatalf("err = %v, want ErrRegistrationCodeRevoked", err)
		}
		if err := st.Revoke(ctx, "no-such-id", store.OwnerScope{All: true}); err != store.ErrRegistrationCodeNotFound {
			t.Fatalf("Revoke(missing) = %v, want ErrRegistrationCodeNotFound", err)
		}
	})

	t.Run("list never exposes plaintext", func(t *testing.T) {
		st := factory(t)
		_, plaintext := mk(t, st, []string{"sas"})
		list, err := st.List(ctx, store.OwnerScope{All: true})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(list) != 1 {
			t.Fatalf("List len = %d, want 1", len(list))
		}
		if strings.Contains(fmt.Sprintf("%#v", list[0]), plaintext) {
			t.Fatal("List leaked the plaintext code")
		}
	})

	t.Run("audit records both outcomes and filters by code", func(t *testing.T) {
		st := factory(t)
		id, _ := mk(t, st, []string{"sas"})
		at := time.Unix(1700000000, 0).UTC()
		for _, rec := range []store.EnrollAuditRecord{
			{CodeID: id, Success: false, Reason: "unknown code", SourceIP: "10.0.0.1", At: at},
			{CodeID: id, Success: true, RunnerID: "runner-x", SourceIP: "10.0.0.2", At: at},
			{CodeID: "other-code", Success: true, RunnerID: "runner-y", SourceIP: "10.0.0.3", At: at},
		} {
			if err := st.AppendEnrollAudit(ctx, rec); err != nil {
				t.Fatalf("AppendEnrollAudit: %v", err)
			}
		}
		got, err := st.EnrollAudit(ctx, id, store.OwnerScope{All: true})
		if err != nil {
			t.Fatalf("EnrollAudit: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("audit len = %d, want 2", len(got))
		}
		if got[0].Success || got[0].Reason != "unknown code" {
			t.Fatalf("failure record did not round-trip: %+v", got[0])
		}
		if got[1].RunnerID != "runner-x" || got[1].SourceIP != "10.0.0.2" {
			t.Fatalf("success record did not round-trip: %+v", got[1])
		}
	})

	t.Run("owner scope isolates namespaces", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		mk := func(id, owner string) {
			t.Helper()
			if err := s.Create(ctx, store.RegistrationCode{
				ID:             id,
				CodeHash:       store.HashSecret("plaintext-" + id),
				OwnerNamespace: owner,
				CreatedAt:      time.Now().UTC(),
			}); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
		}
		mk("code-a", "nsA")
		mk("code-b", "nsB")
		mk("code-legacy", "")

		// A tenant scope sees exactly its own row — not the other tenant's, and
		// not the legacy row whose OwnerNamespace predates the column.
		got, err := s.List(ctx, store.OwnerScope{Namespace: "nsA"})
		if err != nil {
			t.Fatalf("list nsA: %v", err)
		}
		if len(got) != 1 || got[0].ID != "code-a" {
			t.Fatalf("nsA list = %v, want exactly [code-a]", ids(got))
		}

		// The platform scope sees all three, legacy row included.
		all, err := s.List(ctx, store.OwnerScope{All: true})
		if err != nil {
			t.Fatalf("list all: %v", err)
		}
		if len(all) != 3 {
			t.Fatalf("platform list = %v, want 3 rows", ids(all))
		}

		// Cross-namespace revoke is not-found, and is genuinely a no-op.
		if err := s.Revoke(ctx, "code-b", store.OwnerScope{Namespace: "nsA"}); !errors.Is(err, store.ErrRegistrationCodeNotFound) {
			t.Fatalf("cross-namespace revoke err = %v, want ErrRegistrationCodeNotFound", err)
		}
		after, err := s.List(ctx, store.OwnerScope{Namespace: "nsB"})
		if err != nil {
			t.Fatalf("list nsB: %v", err)
		}
		if len(after) != 1 || after[0].Revoked {
			t.Fatalf("code-b revoked by a cross-namespace call: %+v", after)
		}

		// Cross-namespace audit is not-found, not an empty list.
		if _, err := s.EnrollAudit(ctx, "code-b", store.OwnerScope{Namespace: "nsA"}); !errors.Is(err, store.ErrRegistrationCodeNotFound) {
			t.Fatalf("cross-namespace audit err = %v, want ErrRegistrationCodeNotFound", err)
		}

		// The zero scope is a caller bug on every method, and fails closed.
		if _, err := s.List(ctx, store.OwnerScope{}); !errors.Is(err, store.ErrOwnerScopeUnset) {
			t.Fatalf("zero-scope list err = %v, want ErrOwnerScopeUnset", err)
		}
		if err := s.Revoke(ctx, "code-a", store.OwnerScope{}); !errors.Is(err, store.ErrOwnerScopeUnset) {
			t.Fatalf("zero-scope revoke err = %v, want ErrOwnerScopeUnset", err)
		}
		if _, err := s.EnrollAudit(ctx, "code-a", store.OwnerScope{}); !errors.Is(err, store.ErrOwnerScopeUnset) {
			t.Fatalf("zero-scope audit err = %v, want ErrOwnerScopeUnset", err)
		}
		// Both selectors set is equally a bug.
		if _, err := s.List(ctx, store.OwnerScope{All: true, Namespace: "nsA"}); !errors.Is(err, store.ErrOwnerScopeUnset) {
			t.Fatalf("over-specified scope err = %v, want ErrOwnerScopeUnset", err)
		}
	})
}

// ids extracts the ID field of each code, for compact test failure messages.
func ids(codes []store.RegistrationCode) []string {
	out := make([]string, 0, len(codes))
	for _, c := range codes {
		out = append(out, c.ID)
	}
	return out
}

// RunIssuedIdentityStoreContract holds both IssuedIdentityStore implementations
// to the same behavior.
func RunIssuedIdentityStoreContract(t *testing.T, factory func(t *testing.T) store.IssuedIdentityStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("issue then lookup", func(t *testing.T) {
		st := factory(t)
		want := store.IssuedIdentity{
			RunnerID:  "runner-1",
			TokenHash: store.HashSecret("tok"),
			Scope: store.RunnerPolicy{
				Name: "runner-1",
				// IDPrefix is deliberately non-zero here (task-7-addendum.md
				// correction 3): enroll's own issuedScope never sets it today,
				// so a contract that left it zero would not notice a SQL
				// implementation that silently drops the column — a store
				// that always writes/reads back "" would still pass. The
				// point of this field is a future ceiling copied from
				// RegistrationCode.Policy() onto Scope; if that day comes and
				// the SQL side has no id_prefix column, the drop is a silent
				// privilege escalation (dev honors the ceiling, prod does
				// not), so this test must be able to see it now.
				IDPrefix:          "runner-",
				AllowedNodeTypes:  []string{"kafka.trigger"},
				AllowedNamespaces: []string{"sas"},
			},
			CodeID:   "code-1",
			IssuedAt: time.Unix(1700000000, 0).UTC(),
		}
		if err := st.Issue(ctx, want); err != nil {
			t.Fatalf("Issue: %v", err)
		}
		got, ok, err := st.Lookup(ctx, "runner-1")
		if err != nil || !ok {
			t.Fatalf("Lookup: ok=%v err=%v", ok, err)
		}
		if got.TokenHash != want.TokenHash {
			t.Fatal("TokenHash did not round-trip")
		}
		if !got.Scope.Allows("kafka.trigger") || got.Scope.Allows("http.request") {
			t.Fatalf("node-type scope did not round-trip: %+v", got.Scope)
		}
		if !got.Scope.AllowsNamespace("sas") || got.Scope.AllowsNamespace("other") {
			t.Fatalf("namespace scope did not round-trip: %+v", got.Scope)
		}
		if got.CodeID != "code-1" {
			t.Fatalf("CodeID = %q, want code-1", got.CodeID)
		}
		// The two assertions above prove semantics (Allows / AllowsNamespace
		// still answer correctly). They do NOT prove the whole Scope struct
		// made the round trip — a store that never persists IDPrefix would
		// still pass both, because neither method reads it. DeepEqual is the
		// one assertion in this contract that would catch that: it proves
		// complete transport, not just correct-looking behavior for the
		// fields today's callers happen to exercise.
		if !reflect.DeepEqual(got.Scope, want.Scope) {
			t.Fatalf("Scope did not round-trip completely:\n got  = %+v\n want = %+v", got.Scope, want.Scope)
		}
		// Widen the comparison beyond Scope to the rest of IssuedIdentity's
		// fields too: a store that round-tripped Scope correctly but dropped
		// IssuedAt/ExpiresAt/RevokedAt would still pass every assertion above.
		// Time fields compare via Equal rather than DeepEqual/==: a SQL round
		// trip can change time.Time's internal representation (monotonic
		// reading, Location pointer identity) without changing the instant it
		// represents, and DeepEqual would false-fail on that.
		if got.RunnerID != want.RunnerID {
			t.Fatalf("RunnerID = %q, want %q", got.RunnerID, want.RunnerID)
		}
		if !got.IssuedAt.Equal(want.IssuedAt) {
			t.Fatalf("IssuedAt = %v, want %v", got.IssuedAt, want.IssuedAt)
		}
		if !got.ExpiresAt.Equal(want.ExpiresAt) {
			t.Fatalf("ExpiresAt = %v, want %v", got.ExpiresAt, want.ExpiresAt)
		}
		if !got.RevokedAt.Equal(want.RevokedAt) {
			t.Fatalf("RevokedAt = %v, want %v", got.RevokedAt, want.RevokedAt)
		}
	})

	t.Run("absent lookup", func(t *testing.T) {
		st := factory(t)
		if _, ok, err := st.Lookup(ctx, "nobody"); ok || err != nil {
			t.Fatalf("Lookup(absent) = ok=%v err=%v, want false,nil", ok, err)
		}
	})

	t.Run("list", func(t *testing.T) {
		st := factory(t)
		for _, id := range []string{"a", "b"} {
			if err := st.Issue(ctx, store.IssuedIdentity{RunnerID: id, TokenHash: store.HashSecret(id)}); err != nil {
				t.Fatalf("Issue: %v", err)
			}
		}
		list, err := st.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("List len = %d, want 2", len(list))
		}
	})

	t.Run("lifecycle fields round-trip", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		exp := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
		id := store.IssuedIdentity{
			RunnerID:       "runner-lifecycle",
			TokenHash:      store.HashSecret("tok"),
			CodeID:         "code-1",
			OwnerNamespace: "nsA",
			IssuedAt:       time.Now().UTC().Truncate(time.Millisecond),
			ExpiresAt:      exp,
		}
		if err := s.Issue(ctx, id); err != nil {
			t.Fatalf("issue: %v", err)
		}
		got, ok, err := s.Lookup(ctx, "runner-lifecycle")
		if err != nil || !ok {
			t.Fatalf("lookup: ok=%v err=%v", ok, err)
		}
		if !got.ExpiresAt.Equal(exp) {
			t.Fatalf("ExpiresAt = %v, want %v", got.ExpiresAt, exp)
		}
		if !got.RevokedAt.IsZero() {
			t.Fatalf("RevokedAt = %v, want zero", got.RevokedAt)
		}
		// OwnerNamespace must survive the round trip. The revoke predicate is
		// enforced in SQL and never reads this field back, so a repo that
		// dropped it from rowToIssuedIdentity would still pass every scope
		// assertion below while silently reporting every identity as
		// unowned.
		if got.OwnerNamespace != "nsA" {
			t.Fatalf("OwnerNamespace = %q, want nsA", got.OwnerNamespace)
		}

		// Renewal extends and only extends.
		next := exp.Add(time.Hour)
		if err := s.Renew(ctx, "runner-lifecycle", next); err != nil {
			t.Fatalf("renew: %v", err)
		}
		got, _, _ = s.Lookup(ctx, "runner-lifecycle")
		if !got.ExpiresAt.Equal(next) {
			t.Fatalf("ExpiresAt after renew = %v, want %v", got.ExpiresAt, next)
		}
		if got.TokenHash != store.HashSecret("tok") {
			t.Fatalf("renew rotated the token hash; R9 says it must not")
		}

		// Revocation is a state: the second call is a nil no-op.
		nsA := store.OwnerScope{Namespace: "nsA"}
		if err := s.Revoke(ctx, "runner-lifecycle", nsA); err != nil {
			t.Fatalf("revoke: %v", err)
		}
		if err := s.Revoke(ctx, "runner-lifecycle", nsA); err != nil {
			t.Fatalf("second revoke = %v, want nil", err)
		}
		got, _, _ = s.Lookup(ctx, "runner-lifecycle")
		if got.RevokedAt.IsZero() {
			t.Fatalf("RevokedAt still zero after revoke")
		}

		// A revoked identity cannot renew itself back.
		if err := s.Renew(ctx, "runner-lifecycle", next.Add(time.Hour)); !errors.Is(err, store.ErrIssuedIdentityNotFound) {
			t.Fatalf("renew after revoke = %v, want ErrIssuedIdentityNotFound", err)
		}
		// The rejected renew must not have moved ExpiresAt. A "write first,
		// check second" implementation would still return
		// ErrIssuedIdentityNotFound here (revoked lookups are hidden) while
		// having already advanced the row — the assertion above alone cannot
		// see that, only a follow-up Lookup can.
		afterRevokedRenew, _, err := s.Lookup(ctx, "runner-lifecycle")
		if err != nil {
			t.Fatalf("lookup after rejected renew: %v", err)
		}
		if !afterRevokedRenew.ExpiresAt.Equal(next) {
			t.Fatalf("rejected renew on revoked identity moved ExpiresAt: got %v, want %v (unchanged)", afterRevokedRenew.ExpiresAt, next)
		}

		// Neither can an already-expired one.
		past := store.IssuedIdentity{
			RunnerID:  "runner-expired",
			TokenHash: store.HashSecret("tok2"),
			CodeID:    "code-1",
			IssuedAt:  time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Millisecond),
			ExpiresAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond),
		}
		if err := s.Issue(ctx, past); err != nil {
			t.Fatalf("issue expired: %v", err)
		}
		if err := s.Renew(ctx, "runner-expired", time.Now().UTC().Add(time.Hour)); !errors.Is(err, store.ErrIssuedIdentityNotFound) {
			t.Fatalf("renew of expired = %v, want ErrIssuedIdentityNotFound", err)
		}
		// Same non-mutation requirement for the expired case.
		afterExpiredRenew, _, err := s.Lookup(ctx, "runner-expired")
		if err != nil {
			t.Fatalf("lookup after rejected renew: %v", err)
		}
		if !afterExpiredRenew.ExpiresAt.Equal(past.ExpiresAt) {
			t.Fatalf("rejected renew on expired identity moved ExpiresAt: got %v, want %v (unchanged)", afterExpiredRenew.ExpiresAt, past.ExpiresAt)
		}

		if err := s.Revoke(ctx, "no-such-runner", nsA); !errors.Is(err, store.ErrIssuedIdentityNotFound) {
			t.Fatalf("revoke unknown = %v, want ErrIssuedIdentityNotFound", err)
		}
	})

	// Revocation is namespace-scoped. Without this, any principal holding the
	// revoke scope could knock any tenant's entire fleet offline — a
	// cross-tenant DoS. The scope predicate lives in the store, not only in the
	// HTTP handler, so both implementations are held to it here.
	t.Run("revoke is confined to the owning namespace", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		mk := func(runnerID, owner string) {
			t.Helper()
			err := s.Issue(ctx, store.IssuedIdentity{
				RunnerID:       runnerID,
				TokenHash:      store.HashSecret(runnerID),
				CodeID:         "code-1",
				OwnerNamespace: owner,
				IssuedAt:       time.Now().UTC().Truncate(time.Millisecond),
			})
			if err != nil {
				t.Fatalf("issue %s: %v", runnerID, err)
			}
		}
		mk("runner-a", "nsA")
		mk("runner-b", "nsB")
		mk("runner-legacy", "")

		nsA := store.OwnerScope{Namespace: "nsA"}
		nsB := store.OwnerScope{Namespace: "nsB"}
		all := store.OwnerScope{All: true}

		// A zero scope is a caller bug, not "everything".
		if err := s.Revoke(ctx, "runner-a", store.OwnerScope{}); !errors.Is(err, store.ErrOwnerScopeUnset) {
			t.Fatalf("zero-scope revoke = %v, want ErrOwnerScopeUnset", err)
		}
		if err := s.Revoke(ctx, "runner-a", store.OwnerScope{All: true, Namespace: "nsA"}); !errors.Is(err, store.ErrOwnerScopeUnset) {
			t.Fatalf("over-specified scope revoke = %v, want ErrOwnerScopeUnset", err)
		}

		// B may not revoke A's runner, and the refusal is reported as
		// not-found: a distinct error would make this an existence oracle for
		// other tenants' runner ids.
		if err := s.Revoke(ctx, "runner-a", nsB); !errors.Is(err, store.ErrIssuedIdentityNotFound) {
			t.Fatalf("cross-tenant revoke = %v, want ErrIssuedIdentityNotFound", err)
		}
		// The refusal must be a refusal, not just a misleading error code. Only
		// a Lookup can tell "rejected" from "revoked it anyway, then said no".
		gotA, ok, err := s.Lookup(ctx, "runner-a")
		if err != nil || !ok {
			t.Fatalf("lookup runner-a: ok=%v err=%v", ok, err)
		}
		if !gotA.RevokedAt.IsZero() {
			t.Fatalf("cross-tenant revoke went through: RevokedAt = %v, want zero", gotA.RevokedAt)
		}

		// An owner_namespace of "" means "unknown", NOT "everyone's". No tenant
		// scope matches it; only All: true can revoke such a legacy row.
		if err := s.Revoke(ctx, "runner-legacy", nsA); !errors.Is(err, store.ErrIssuedIdentityNotFound) {
			t.Fatalf("tenant revoke of legacy row = %v, want ErrIssuedIdentityNotFound", err)
		}
		gotLegacy, _, _ := s.Lookup(ctx, "runner-legacy")
		if !gotLegacy.RevokedAt.IsZero() {
			t.Fatalf("tenant revoked a legacy row: RevokedAt = %v, want zero", gotLegacy.RevokedAt)
		}

		// The owner itself is still allowed, so the guard above is a scope
		// check and not a blanket refusal.
		if err := s.Revoke(ctx, "runner-a", nsA); err != nil {
			t.Fatalf("owner revoke: %v", err)
		}
		gotA, _, _ = s.Lookup(ctx, "runner-a")
		if gotA.RevokedAt.IsZero() {
			t.Fatalf("owner revoke did not stamp RevokedAt")
		}

		// And a platform principal reaches both the other tenant's row and the
		// legacy one — otherwise legacy identities would be unrevokable.
		if err := s.Revoke(ctx, "runner-b", all); err != nil {
			t.Fatalf("global revoke of nsB runner: %v", err)
		}
		if err := s.Revoke(ctx, "runner-legacy", all); err != nil {
			t.Fatalf("global revoke of legacy runner: %v", err)
		}
		for _, id := range []string{"runner-b", "runner-legacy"} {
			got, _, _ := s.Lookup(ctx, id)
			if got.RevokedAt.IsZero() {
				t.Fatalf("global revoke of %s did not stamp RevokedAt", id)
			}
		}
	})
}
