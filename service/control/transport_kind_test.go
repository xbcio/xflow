package control

import (
	"context"
	"crypto/subtle"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// TestTransportInfoKindIsStampedByEveryTransport pins the discriminator an
// authenticator keys on to admit the embedded runner: each constructor stamps
// its kind server-side. The in-process kind's SourceIP is empty — but so is
// gRPC's, which is exactly why Kind rather than SourceIP must carry the answer.
func TestTransportInfoKindIsStampedByEveryTransport(t *testing.T) {
	cases := []struct {
		name string
		info TransportInfo
		want TransportKind
	}{
		{"http", httpTransportInfo(httptest.NewRequest(http.MethodPost, protocol.RegisterRunnerPath, nil)), TransportKindHTTP},
		{"grpc", grpcTransportInfo(context.Background()), TransportKindGRPC},
		{"inproc", inProcessTransportInfo(), TransportKindInProcess},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.info.Kind != tc.want {
				t.Fatalf("Kind = %q, want %q", tc.info.Kind, tc.want)
			}
		})
	}

	if got := inProcessTransportInfo().SourceIP; got != "" {
		t.Fatalf("in-process SourceIP = %q, want empty (there is no network peer)", got)
	}
	if got := httpTransportInfo(httptest.NewRequest(http.MethodPost, protocol.RegisterRunnerPath, nil)).SourceIP; got == "" {
		t.Fatal("HTTP transport dropped its SourceIP; the in-process/HTTP distinction would be unobservable")
	}
}

// localPeerAuthenticator stands in for a policy that admits only the local
// peer, mirroring SAS's localRunnerAuthenticator: it gates on the runner ID and
// the peer first — admitting TransportKindInProcess or a loopback SourceIP —
// and only then checks the token. See
// TestAuthenticatorDistinguishesInProcessFromPeerlessHTTP.
type localPeerAuthenticator struct {
	runnerID string
	token    string
}

func (a localPeerAuthenticator) check(id, token string, info TransportInfo) (RunnerPolicy, error) {
	if id != a.runnerID {
		return RunnerPolicy{}, ErrAuthIDPrefixDenied
	}
	local := info.Kind == TransportKindInProcess
	if !local {
		if ip := net.ParseIP(info.SourceIP); ip != nil && ip.IsLoopback() {
			local = true
		}
	}
	if !local {
		return RunnerPolicy{}, ErrAuthUnknownToken
	}
	// The peer gate does not replace the credential: a local claim with the
	// wrong token is still rejected, exactly as SAS's delegate would.
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.token)) != 1 {
		return RunnerPolicy{}, ErrAuthUnknownToken
	}
	return RunnerPolicy{Name: "local"}, nil
}

func (a localPeerAuthenticator) AuthenticateRegister(id, token string, info TransportInfo) (RunnerPolicy, error) {
	return a.check(id, token, info)
}

func (a localPeerAuthenticator) AuthenticateOngoing(id, token string, info TransportInfo) (RunnerPolicy, error) {
	return a.check(id, token, info)
}

// TestAuthenticatorDistinguishesInProcessFromPeerlessHTTP proves the kind is
// usable for the reason it was added: it admits the embedded runner by kind
// while still rejecting an HTTP caller that presents no peer address. Both
// present the same empty SourceIP; only Kind separates them. It also pins that
// the kind is an admission aid, not an authorization bypass — a local claim
// with a wrong token is rejected on both kinds.
func TestAuthenticatorDistinguishesInProcessFromPeerlessHTTP(t *testing.T) {
	a := localPeerAuthenticator{runnerID: "embedded", token: "tok"}

	peerless := httptest.NewRequest(http.MethodPost, protocol.RegisterRunnerPath, nil)
	peerless.RemoteAddr = ""

	cases := []struct {
		name    string
		token   string
		info    TransportInfo
		wantErr bool
	}{
		{"inproc correct token", "tok", inProcessTransportInfo(), false},
		{"loopback http correct token", "tok", httpTransportInfo(loopbackRequest()), false},
		{"peerless http correct token", "tok", httpTransportInfo(peerless), true},
		{"inproc wrong token", "wrong", inProcessTransportInfo(), true},
		{"loopback http wrong token", "wrong", httpTransportInfo(loopbackRequest()), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.AuthenticateRegister("embedded", tc.token, tc.info)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func loopbackRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, protocol.RegisterRunnerPath, nil)
	r.RemoteAddr = "127.0.0.1:40000"
	return r
}

// recordingAuthenticator records the TransportInfo Kind it sees on every call,
// so a test can assert what the in-process client actually presents on each
// method rather than only on the helper's return value.
type recordingAuthenticator struct{ kinds []TransportKind }

func (a *recordingAuthenticator) note(info TransportInfo) {
	a.kinds = append(a.kinds, info.Kind)
}

func (a *recordingAuthenticator) AuthenticateRegister(_, _ string, info TransportInfo) (RunnerPolicy, error) {
	a.note(info)
	return permissivePolicy, nil
}

func (a *recordingAuthenticator) AuthenticateOngoing(_, _ string, info TransportInfo) (RunnerPolicy, error) {
	a.note(info)
	return permissivePolicy, nil
}

// TestInProcessClientStampsItsTransportKind drives every method of the
// in-process client, not just Register. The client repeats the TransportInfo at
// nine call sites, so a slip to the zero value in any one of them (say,
// RenewLease) would otherwise pass unnoticed. Core authenticates before doing
// anything else, so the Kind is recorded even when a method then fails for an
// unrelated reason (e.g. an unknown session).
func TestInProcessClientStampsItsTransportKind(t *testing.T) {
	rec := &recordingAuthenticator{}
	srv := NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory(), WithAuthenticator(rec))
	// report-metrics short-circuits to ErrMetricsProxyDisabled when no inbox is
	// configured, before auth; wire one so that method reaches the authenticator
	// like the rest.
	srv.core.metricsInbox = NewMetricsInbox(MetricsInboxConfig{Store: NewMemoryMetricsStore()})
	c := srv.InProcessRunnerClient("tok")
	ctx := context.Background()

	calls := []struct {
		name string
		call func() error
	}{
		{"register", func() error {
			_, err := c.Register(ctx, protocol.RegisterRunnerRequest{RunnerID: "embedded", Concurrency: 1})
			return err
		}},
		{"heartbeat", func() error {
			_, err := c.Heartbeat(ctx, protocol.HeartbeatRequest{RunnerID: "embedded", SessionID: "s"})
			return err
		}},
		{"poll", func() error {
			_, err := c.Poll(ctx, protocol.PollTaskRequest{RunnerID: "embedded", SessionID: "s"})
			return err
		}},
		{"report-result", func() error {
			// report-result validates RunnerID/SessionID/Lease before auth.
			_, err := c.ReportResult(ctx, protocol.ReportResultRequest{
				RunnerID: "embedded", SessionID: "s", Lease: &engine.TaskLease{},
			})
			return err
		}},
		{"renew-lease", func() error {
			_, err := c.RenewLease(ctx, protocol.RenewLeaseRequest{RunnerID: "embedded", SessionID: "s"})
			return err
		}},
		{"activation-ack", func() error {
			return c.ActivationAck(ctx, protocol.ActivationAck{RunnerID: "embedded", SessionID: "s"})
		}},
		{"report-metrics", func() error {
			return c.ReportMetrics(ctx, "embedded", "s", []byte("body"))
		}},
		{"renew-identity", func() error {
			_, err := c.RenewIdentity(ctx, protocol.RenewIdentityRequest{RunnerID: "embedded"})
			return err
		}},
	}

	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			before := len(rec.kinds)
			_ = tc.call() // downstream errors are fine; auth runs first
			if len(rec.kinds) != before+1 {
				t.Fatalf("authenticator was not reached (calls recorded: %d, want %d)", len(rec.kinds), before+1)
			}
			if got := rec.kinds[len(rec.kinds)-1]; got != TransportKindInProcess {
				t.Fatalf("authenticator saw Kind = %q, want %q", got, TransportKindInProcess)
			}
		})
	}
}
