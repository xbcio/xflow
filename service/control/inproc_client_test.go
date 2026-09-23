package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// These tests pin the in-process transport to the HTTP one. They exist because
// the in-process client is a second caller of the same Core methods the HTTP
// handler drives, and the failure mode of a divergence is silent: a runner that
// renews its lease over HTTP but not in process, or authenticates in one and
// not the other, looks healthy until the day it matters.

func newInProcContractServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	policy, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:              "contract",
			IDPrefix:          "runner-",
			Token:             "valid-token",
			AllowedNodeTypes:  []string{"*"},
			AllowedNamespaces: []string{"default"},
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	dir := NewMemoryRunnerDirectory()
	// A short poll wait keeps the empty-poll comparison from costing a second on
	// each path.
	srv := NewServer(&fakeControlEngine{}, dir, WithAuthenticator(policy), WithHTTPPollWait(10*time.Millisecond))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// statusForErr renders an error the way the HTTP handler would, so an in-process
// error can be compared against an HTTP status code without maintaining a second
// copy of the error-to-status mapping.
func statusForErr(err error) int {
	if err == nil {
		return http.StatusOK
	}
	rr := httptest.NewRecorder()
	writeRunnerError(rr, err)
	return rr.Code
}

func httpStatusFor(t *testing.T, url, token string, body any) int {
	t.Helper()
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// TestInProcessClientMatchesHTTPTransport drives both transports with identical
// requests and asserts the responses agree. Fields the server legitimately mints
// per call (a fresh session id, wall-clock server time) are normalized away;
// everything else must match, so a field one path populates and the other drops
// is a failure.
func TestInProcessClientMatchesHTTPTransport(t *testing.T) {
	srv, ts := newInProcContractServer(t)
	ctx := context.Background()
	const token = "valid-token"
	httpClient := protocol.NewClient(ts.URL, ts.Client()).WithToken(token)
	inproc := srv.InProcessRunnerClient(token)

	registerReq := protocol.RegisterRunnerRequest{
		RunnerID:           "runner-contract",
		Concurrency:        2,
		SupportsEncryption: true,
	}
	httpReg, err := httpClient.Register(ctx, registerReq)
	if err != nil {
		t.Fatalf("http register: %v", err)
	}
	inprocReg, err := inproc.Register(ctx, registerReq)
	if err != nil {
		t.Fatalf("inproc register: %v", err)
	}
	if httpReg.SessionID == "" || inprocReg.SessionID == "" {
		t.Fatalf("register must mint a session; http=%q inproc=%q", httpReg.SessionID, inprocReg.SessionID)
	}
	// Both registers are for the same runner id, so the second supersedes the
	// first; the live session is the one the in-process register just minted.
	session := inprocReg.SessionID
	httpReg.SessionID, inprocReg.SessionID = "", ""
	if !reflect.DeepEqual(httpReg, inprocReg) {
		t.Fatalf("register responses differ:\n http:   %+v\n inproc: %+v", httpReg, inprocReg)
	}

	// Heartbeat: same session-scoped request on both paths. ServerTime is
	// wall-clock derived and normalized before comparison.
	httpBeat, err := httpClient.Heartbeat(ctx, protocol.HeartbeatRequest{RunnerID: "runner-contract", SessionID: session, Capacity: 2})
	if err != nil {
		t.Fatalf("http heartbeat: %v", err)
	}
	inprocBeat, err := inproc.Heartbeat(ctx, protocol.HeartbeatRequest{RunnerID: "runner-contract", SessionID: session, Capacity: 2})
	if err != nil {
		t.Fatalf("inproc heartbeat: %v", err)
	}
	httpBeat.ServerTime, inprocBeat.ServerTime = 0, 0
	if !reflect.DeepEqual(httpBeat, inprocBeat) {
		t.Fatalf("heartbeat responses differ:\n http:   %+v\n inproc: %+v", httpBeat, inprocBeat)
	}

	// Poll with no work available: both must return the same empty wait, not
	// one an error and the other a response.
	httpPoll, err := httpClient.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-contract", SessionID: session, Capacity: 2})
	if err != nil {
		t.Fatalf("http poll: %v", err)
	}
	inprocPoll, err := inproc.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-contract", SessionID: session, Capacity: 2})
	if err != nil {
		t.Fatalf("inproc poll: %v", err)
	}
	if !reflect.DeepEqual(httpPoll, inprocPoll) {
		t.Fatalf("poll responses differ:\n http:   %+v\n inproc: %+v", httpPoll, inprocPoll)
	}
}

// TestInProcessClientErrorParity asserts that an in-process error maps to the
// same HTTP status the wire transport returns for the same condition. The error
// values themselves are richer in process (the wire transport flattens them to
// a status + body), so the comparison is on the status Core's error produces.
func TestInProcessClientErrorParity(t *testing.T) {
	srv, ts := newInProcContractServer(t)
	ctx := context.Background()
	valid := srv.InProcessRunnerClient("valid-token")

	cases := []struct {
		name  string
		path  string
		token string
		body  any
		call  func() error
	}{
		{
			name:  "wrong token",
			path:  protocol.RegisterRunnerPath,
			token: "not-the-token",
			body:  protocol.RegisterRunnerRequest{RunnerID: "runner-contract", Concurrency: 1},
			call: func() error {
				_, err := srv.InProcessRunnerClient("not-the-token").Register(ctx,
					protocol.RegisterRunnerRequest{RunnerID: "runner-contract", Concurrency: 1})
				return err
			},
		},
		{
			name:  "missing session",
			path:  protocol.HeartbeatPath,
			token: "valid-token",
			body:  protocol.HeartbeatRequest{RunnerID: "runner-contract"},
			call: func() error {
				_, err := valid.Heartbeat(ctx, protocol.HeartbeatRequest{RunnerID: "runner-contract"})
				return err
			},
		},
		{
			name:  "missing concurrency",
			path:  protocol.RegisterRunnerPath,
			token: "valid-token",
			body:  protocol.RegisterRunnerRequest{RunnerID: "runner-contract"},
			call: func() error {
				_, err := valid.Register(ctx, protocol.RegisterRunnerRequest{RunnerID: "runner-contract"})
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := httpStatusFor(t, ts.URL+tc.path, tc.token, tc.body)
			got := statusForErr(tc.call())
			if got != want {
				t.Fatalf("status mismatch: http=%d inproc=%d", want, got)
			}
		})
	}
}

// newInProcAuthServer builds a control plane with runner auth enforcing and no
// HTTP listener at all.
func newInProcAuthServer(t *testing.T) *Server {
	t.Helper()
	policy, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:              "contract",
			IDPrefix:          "runner-",
			Token:             "valid-token",
			AllowedNodeTypes:  []string{"*"},
			AllowedNamespaces: []string{"default"},
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory(), WithAuthenticator(policy))
}

// TestInProcessClientNeedsNoListener is the direct proof of the transport's
// purpose: with no HTTP listener ever started, the in-process client still
// registers and polls. If any part of the path dialed the control plane this
// would fail to connect rather than succeed.
func TestInProcessClientNeedsNoListener(t *testing.T) {
	srv := newInProcAuthServer(t)
	c := srv.InProcessRunnerClient("valid-token")
	ctx := context.Background()
	reg, err := c.Register(ctx, protocol.RegisterRunnerRequest{RunnerID: "runner-contract", Concurrency: 1})
	if err != nil {
		t.Fatalf("register with no listener: %v", err)
	}
	if _, err := c.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-contract", SessionID: reg.SessionID, Capacity: 1}); err != nil {
		t.Fatalf("poll with no listener: %v", err)
	}
}

// countingListener records how many connections were accepted. It counts a
// successful Accept, not the call itself: a serving goroutine sits blocked in
// Accept waiting for the first connection, so counting invocations would show a
// phantom "1" on a listener nobody ever dialed.
type countingListener struct {
	net.Listener
	accepts atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return c, err
}

// TestInProcessClientOpensNoTCPConnections serves the same control plane over a
// counting listener and drives a full lifecycle through the in-process client,
// asserting the listener accepted zero connections. Asserting "we did not call
// the HTTP client" would be weaker: the point is that no socket is opened on the
// path at all, even one constructed indirectly.
func TestInProcessClientOpensNoTCPConnections(t *testing.T) {
	srv := newInProcAuthServer(t)

	ts := httptest.NewUnstartedServer(srv.Handler())
	counting := &countingListener{Listener: ts.Listener}
	ts.Listener = counting
	ts.Start()
	defer ts.Close()

	c := srv.InProcessRunnerClient("valid-token")
	ctx := context.Background()
	reg, err := c.Register(ctx, protocol.RegisterRunnerRequest{RunnerID: "runner-contract", Concurrency: 1})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := c.Heartbeat(ctx, protocol.HeartbeatRequest{RunnerID: "runner-contract", SessionID: reg.SessionID, Capacity: 1}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if _, err := c.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-contract", SessionID: reg.SessionID, Capacity: 1}); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if err := c.ActivationAck(ctx, protocol.ActivationAck{RunnerID: "runner-contract", SessionID: reg.SessionID}); err != nil {
		t.Fatalf("activation ack: %v", err)
	}
	if _, err := c.RenewIdentity(ctx, protocol.RenewIdentityRequest{RunnerID: "runner-contract"}); err != nil {
		t.Fatalf("renew identity: %v", err)
	}

	if n := counting.accepts.Load(); n != 0 {
		t.Fatalf("in-process path opened %d TCP connection(s); want 0", n)
	}

	// Positive control: the instrument must register a real connection, or the
	// zero above proves nothing. One plain HTTP call to the same listener bumps
	// the count, confirming the in-process zero was a zero and not a dead meter.
	resp := httpStatusFor(t, ts.URL+protocol.HeartbeatPath, "valid-token", protocol.HeartbeatRequest{RunnerID: "runner-contract", SessionID: reg.SessionID, Capacity: 1})
	if resp == 0 {
		t.Fatal("control request did not complete")
	}
	if n := counting.accepts.Load(); n == 0 {
		t.Fatal("counting listener did not observe the control connection; zero-connection assertion is meaningless")
	}
}

// TestInProcessClientMatchesHTTPWithoutATransportIdentity pins the one place the
// two transports are not equivalent, and pins it as equivalence-with-HTTP rather
// than as a special case: the in-process path carries no TLS peer subject and no
// peer address — its TransportInfo is Kind=inproc with an empty SourceIP — so on
// the mTLS axis it authenticates exactly like an HTTP caller that presented no
// client certificate. (The Kind field is the discriminator a host uses to tell
// this apart from a peerless HTTP caller; it does not enter the mTLS decision.)
//
// That is the correct behavior, and it is fail-closed: FilePolicyStore.authenticate
// skips any entry whose mtls_subject is set when the reported subject is empty,
// so an embedded runner cannot satisfy a subject-bound policy. It cannot bypass
// one either. What it must NOT do is succeed, and this test fails if a future
// change lets an empty-subject TransportInfo satisfy a subject-bound entry.
func TestInProcessClientMatchesHTTPWithoutATransportIdentity(t *testing.T) {
	auth := mtlsPolicy(t, "cn=runner-prod")
	srv := NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory(), WithAuthenticator(auth))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	const token = "tok"
	body := protocol.RegisterRunnerRequest{RunnerID: "runner-prod", Concurrency: 1}
	httpStatus := httpStatusFor(t, ts.URL+protocol.RegisterRunnerPath, token, body)
	if httpStatus == http.StatusOK {
		t.Fatal("the HTTP control registered against a subject-bound policy without a " +
			"client certificate; this test's premise is broken")
	}

	inprocStatus := statusForErr(func() error {
		_, err := srv.InProcessRunnerClient(token).Register(context.Background(),
			protocol.RegisterRunnerRequest{RunnerID: "runner-prod", Concurrency: 1})
		return err
	}())
	if inprocStatus == http.StatusOK {
		t.Fatal("the in-process control registered against a subject-bound policy: it " +
			"reports no client-certificate subject, so this is an authentication bypass")
	}
	if inprocStatus != httpStatus {
		t.Fatalf("in-process status %d != HTTP status %d for the same absent transport "+
			"identity; the two paths disagree on a denial", inprocStatus, httpStatus)
	}
}
