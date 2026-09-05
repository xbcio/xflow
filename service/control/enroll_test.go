package control

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// enrollFixture builds a Core wired only for enroll, plus a live code.
func enrollFixture(t *testing.T, namespaces, nodeTypes []string) (*Core, *MemoryRegistrationCodeStore, *MemoryIssuedIdentityStore, string) {
	t.Helper()
	codes := NewMemoryRegistrationCodeStore()
	ids := NewMemoryIssuedIdentityStore()
	id, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	err = codes.Create(context.Background(), RegistrationCode{
		ID:                id,
		CodeHash:          HashSecret(plaintext),
		AllowedNamespaces: namespaces,
		AllowedNodeTypes:  nodeTypes,
		CreatedAt:         time.Unix(1700000000, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	core := &Core{
		registrationCodes: codes,
		issuedIdentities:  ids,
		enrollLimiter:     newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout),
	}
	return core, codes, ids, plaintext
}

func TestEnrollIssuesAServerGeneratedIdentity(t *testing.T) {
	core, _, ids, code := enrollFixture(t, []string{"sas"}, []string{"kafka.trigger"})
	resp, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: code,
		ProposedRunnerID: "attacker-chosen-id",
		Namespaces:       []string{"sas"},
		NodeTypes:        []string{"kafka.trigger"},
	}, TransportInfo{SourceIP: "10.0.0.1"})
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if resp.RunnerID == "" || resp.Token == "" {
		t.Fatalf("response = %+v, want both id and token", resp)
	}
	// The server generates the id. Honoring proposed_runner_id would let a
	// caller enroll AS an existing runner (spec Global Constraints).
	if resp.RunnerID == "attacker-chosen-id" {
		t.Fatal("server honored proposed_runner_id; that is an identity-takeover path")
	}

	stored, ok, err := ids.Lookup(context.Background(), resp.RunnerID)
	if err != nil || !ok {
		t.Fatalf("issued identity not persisted: ok=%v err=%v", ok, err)
	}
	if stored.TokenHash != HashSecret(resp.Token) {
		t.Fatal("stored token hash does not match the issued token")
	}
	// The issued identity must be usable by the ongoing auth path immediately.
	a := NewIssuedIdentityAuthenticator(ids)
	if _, err := a.AuthenticateOngoing(resp.RunnerID, resp.Token, TransportInfo{}); err != nil {
		t.Fatalf("issued credential rejected by the authenticator: %v", err)
	}
}

func TestEnrollRejectionsAreIndistinguishable(t *testing.T) {
	// Unknown / revoked / out-of-scope must produce the SAME external error.
	// Anything else lets a prober enumerate which codes exist (spec §2.3.4).
	unknownCore, _, _, _ := enrollFixture(t, []string{"sas"}, []string{"*"})
	_, unknownErr := unknownCore.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: "no-such-code", Namespaces: []string{"sas"},
	}, TransportInfo{SourceIP: "10.0.0.1"})

	revokedCore, revokedCodes, _, revokedCode := enrollFixture(t, []string{"sas"}, []string{"*"})
	list, _ := revokedCodes.List(context.Background())
	if err := revokedCodes.Revoke(context.Background(), list[0].ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	_, revokedErr := revokedCore.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: revokedCode, Namespaces: []string{"sas"},
	}, TransportInfo{SourceIP: "10.0.0.2"})

	scopeCore, _, _, scopeCode := enrollFixture(t, []string{"sas"}, []string{"*"})
	_, scopeErr := scopeCore.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: scopeCode, Namespaces: []string{"someone-elses-namespace"},
	}, TransportInfo{SourceIP: "10.0.0.3"})

	for name, err := range map[string]error{"unknown": unknownErr, "revoked": revokedErr, "scope": scopeErr} {
		if !errors.Is(err, ErrEnrollRejected) {
			t.Fatalf("%s: err = %v, want ErrEnrollRejected", name, err)
		}
	}
	if unknownErr.Error() != revokedErr.Error() || unknownErr.Error() != scopeErr.Error() {
		t.Fatalf("rejection reasons are distinguishable:\n unknown=%q\n revoked=%q\n scope=%q",
			unknownErr, revokedErr, scopeErr)
	}
	for _, err := range []error{unknownErr, revokedErr, scopeErr} {
		for _, leak := range []string{"revoked", "namespace", "scope", "not recognized"} {
			if strings.Contains(strings.ToLower(err.Error()), leak) {
				t.Fatalf("rejection message %q leaks the server-side reason %q", err, leak)
			}
		}
	}
}

func TestEnrollAuditsBothOutcomesWithSourceIP(t *testing.T) {
	core, codes, _, code := enrollFixture(t, []string{"sas"}, []string{"*"})
	list, _ := codes.List(context.Background())
	codeID := list[0].ID

	if _, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: code, Namespaces: []string{"nope"},
	}, TransportInfo{SourceIP: "10.0.0.9"}); err == nil {
		t.Fatal("out-of-scope enroll must fail")
	}
	if _, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: code, Namespaces: []string{"sas"},
	}, TransportInfo{SourceIP: "10.0.0.9"}); err != nil {
		t.Fatalf("in-scope enroll must succeed: %v", err)
	}

	audit, err := codes.EnrollAudit(context.Background(), codeID)
	if err != nil {
		t.Fatalf("EnrollAudit: %v", err)
	}
	if len(audit) != 2 {
		t.Fatalf("audit len = %d, want 2 (the failure must be recorded too)", len(audit))
	}
	if audit[0].Success {
		t.Fatal("first record should be the failure")
	}
	if audit[0].Reason == "" {
		t.Fatal("the server-side reason must be recorded even though it is not returned")
	}
	for i, rec := range audit {
		if rec.SourceIP != "10.0.0.9" {
			t.Fatalf("audit[%d].SourceIP = %q, want 10.0.0.9", i, rec.SourceIP)
		}
	}
	if audit[1].RunnerID == "" {
		t.Fatal("the successful record must carry the issued runner id")
	}
}

// countingRegistrationCodeStore wraps a RegistrationCodeStore and counts
// ResolveByPlaintext calls. It exists only in test code — MemoryRegistrationCodeStore
// (the production type) is not touched — so a test can prove the store was
// never consulted, which "the error looked right" cannot distinguish from
// "the store was asked and said no again".
type countingRegistrationCodeStore struct {
	RegistrationCodeStore
	resolveCalls atomic.Int64
}

func (c *countingRegistrationCodeStore) ResolveByPlaintext(ctx context.Context, plaintext string) (RegistrationCode, error) {
	c.resolveCalls.Add(1)
	return c.RegistrationCodeStore.ResolveByPlaintext(ctx, plaintext)
}

func TestEnrollLockoutBlocksBeforeTouchingTheStore(t *testing.T) {
	inner := NewMemoryRegistrationCodeStore()
	ids := NewMemoryIssuedIdentityStore()
	id, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := inner.Create(context.Background(), RegistrationCode{
		ID:                id,
		CodeHash:          HashSecret(plaintext),
		AllowedNamespaces: []string{"sas"},
		AllowedNodeTypes:  []string{"*"},
		CreatedAt:         time.Unix(1700000000, 0).UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	codes := &countingRegistrationCodeStore{RegistrationCodeStore: inner}
	core := &Core{
		registrationCodes: codes,
		issuedIdentities:  ids,
		enrollLimiter:     newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout),
	}

	const ip = "10.0.0.7"
	for i := 0; i < defaultEnrollFailureLimit; i++ {
		if _, err := core.Enroll(context.Background(), protocol.EnrollRequest{
			RegistrationCode: "wrong", Namespaces: []string{"sas"},
		}, TransportInfo{SourceIP: ip}); err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", i+1)
		}
	}
	callsBeforeLockout := codes.resolveCalls.Load()
	if callsBeforeLockout != int64(defaultEnrollFailureLimit) {
		t.Fatalf("resolveCalls after %d failing attempts = %d, want %d (one store lookup per attempt while unlocked)",
			defaultEnrollFailureLimit, callsBeforeLockout, defaultEnrollFailureLimit)
	}

	// The next attempt is rejected by the limiter. It must look identical to a
	// normal rejection — a distinct "you are locked out" reply is itself a
	// signal that the prior guesses were being counted.
	_, err = core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: "wrong", Namespaces: []string{"sas"},
	}, TransportInfo{SourceIP: ip})
	if !errors.Is(err, ErrEnrollRejected) {
		t.Fatalf("locked-out err = %v, want ErrEnrollRejected", err)
	}
	if got := codes.resolveCalls.Load(); got != callsBeforeLockout {
		t.Fatalf("resolveCalls after the locked-out attempt = %d, want unchanged at %d — the store must not be touched once locked out",
			got, callsBeforeLockout)
	}

	// The bearing-weight assertion: a REAL, in-scope code — one that would
	// succeed outside the lockout window — must ALSO be rejected while this
	// source is locked out. Without this, none of the assertions above can
	// tell "the limiter blocked it" apart from "the code was wrong anyway":
	// every attempt so far used the code "wrong".
	_, err = core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: plaintext, Namespaces: []string{"sas"},
	}, TransportInfo{SourceIP: ip})
	if !errors.Is(err, ErrEnrollRejected) {
		t.Fatalf("locked-out err for a VALID, in-scope code = %v, want ErrEnrollRejected — the limiter must block even a correct code", err)
	}
	if got := codes.resolveCalls.Load(); got != callsBeforeLockout {
		t.Fatalf("resolveCalls after a valid-code attempt during lockout = %d, want unchanged at %d — the limiter must block before the store is asked",
			got, callsBeforeLockout)
	}

	// A different source is unaffected.
	if _, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: "wrong", Namespaces: []string{"sas"},
	}, TransportInfo{SourceIP: "10.0.0.8"}); !errors.Is(err, ErrEnrollRejected) {
		t.Fatalf("unrelated source err = %v, want the same ErrEnrollRejected", err)
	}
}

func TestEnrollDisabledWhenNoCodeStoreConfigured(t *testing.T) {
	core := &Core{}
	if _, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: "anything",
	}, TransportInfo{SourceIP: "10.0.0.1"}); !errors.Is(err, ErrEnrollRejected) {
		t.Fatalf("err = %v, want ErrEnrollRejected when enroll is not configured", err)
	}
}

func TestHTTPTransportInfoCarriesSourceIP(t *testing.T) {
	// The empty-output cases ("" and ":1234") pin exactly when sourceIPOf
	// legitimately returns "" — the contract the enroll empty-SourceIP guard
	// and its comment depend on (service/control/enroll.go, server.go).
	cases := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{"ipv4", "192.0.2.10:54321", "192.0.2.10"},
		{"ipv6 loopback", "[::1]:54321", "::1"},
		{"ipv6 global", "[2001:db8::1]:8080", "2001:db8::1"},
		{"empty RemoteAddr", "", ""},
		{"empty host with port", ":1234", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, protocol.EnrollPath, strings.NewReader("{}"))
			r.RemoteAddr = tc.remoteAddr
			info := httpTransportInfo(r)
			if info.SourceIP != tc.want {
				t.Fatalf("RemoteAddr = %q: SourceIP = %q, want %q", tc.remoteAddr, info.SourceIP, tc.want)
			}
		})
	}
}

func TestEnrollRejectsEmptySourceIP(t *testing.T) {
	// Not a hypothetical: TransportInfo has three construction sites and only
	// the HTTP runner face populates SourceIP. If enroll is ever wired onto
	// another one, this must fail loudly instead of silently sharing a bucket.
	core, _, _, plaintext := enrollFixture(t, []string{"sas"}, []string{"*"})
	_, err := core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: plaintext,
		Namespaces:       []string{"sas"},
	}, TransportInfo{})
	if !errors.Is(err, ErrEnrollRejected) {
		t.Fatalf("err = %v, want ErrEnrollRejected — an empty SourceIP must not reach the limiter", err)
	}
}

func TestHandleEnrollRejectionsAreByteIdentical(t *testing.T) {
	newSrv := func(t *testing.T, revoke bool) (*httptest.Server, string) {
		t.Helper()
		core, codes, _, plaintext := enrollFixture(t, []string{"sas"}, []string{"*"})
		if revoke {
			list, _ := codes.List(context.Background())
			if err := codes.Revoke(context.Background(), list[0].ID); err != nil {
				t.Fatalf("Revoke: %v", err)
			}
		}
		s := &Server{core: core}
		mux := http.NewServeMux()
		mux.HandleFunc(protocol.EnrollPath, s.HandleEnroll)
		return httptest.NewServer(mux), plaintext
	}

	post := func(t *testing.T, srv *httptest.Server, body string) (int, string) {
		t.Helper()
		resp, err := srv.Client().Post(srv.URL+protocol.EnrollPath, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	unknownSrv, _ := newSrv(t, false)
	defer unknownSrv.Close()
	unknownStatus, unknownBody := post(t, unknownSrv, `{"registration_code":"nope","namespaces":["sas"]}`)

	revokedSrv, revokedCode := newSrv(t, true)
	defer revokedSrv.Close()
	revokedStatus, revokedBody := post(t, revokedSrv, `{"registration_code":"`+revokedCode+`","namespaces":["sas"]}`)

	scopeSrv, scopeCode := newSrv(t, false)
	defer scopeSrv.Close()
	scopeStatus, scopeBody := post(t, scopeSrv, `{"registration_code":"`+scopeCode+`","namespaces":["other"]}`)

	malformedSrv, _ := newSrv(t, false)
	defer malformedSrv.Close()
	malformedStatus, malformedBody := post(t, malformedSrv, `{not json`)

	oversizedSrv, _ := newSrv(t, false)
	defer oversizedSrv.Close()
	// One byte over the MaxBytesReader cap shared with HandleReportMetrics
	// (protocol.MaxRunnerMetricsBytes). Otherwise well-formed JSON, so this
	// proves the size cap itself produces the same rejection — not merely
	// that a large body happens to fail to parse.
	oversizedPayload := `{"registration_code":"` + strings.Repeat("a", protocol.MaxRunnerMetricsBytes+1) + `"}`
	oversizedStatus, oversizedBody := post(t, oversizedSrv, oversizedPayload)

	statuses := []int{unknownStatus, revokedStatus, scopeStatus, malformedStatus, oversizedStatus}
	bodies := []string{unknownBody, revokedBody, scopeBody, malformedBody, oversizedBody}
	for i := range statuses {
		if statuses[i] != statuses[0] {
			t.Fatalf("rejection statuses differ: %v", statuses)
		}
		if bodies[i] != bodies[0] {
			t.Fatalf("rejection bodies differ:\n [0]=%q\n [%d]=%q", bodies[0], i, bodies[i])
		}
	}
	if statuses[0] != http.StatusForbidden {
		t.Fatalf("rejection status = %d, want 403", statuses[0])
	}
}
