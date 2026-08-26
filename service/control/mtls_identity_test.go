package control

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// The exact string an operator has to put in a policy entry's mtls_subject for
// the certificate this file issues. It is Go's RFC 2253 rendering of the
// subject: no space after the commas, and the RDNs in reverse order from how
// they are written in pkix.Name — CN first, then OU, then O.
//
// Hardcoded on purpose. This is a wire format between a certificate authority
// and a hand-edited YAML file, and the whole point of the test below is that
// nothing else in the repository pins it.
const wantRunnerDN = "CN=runner-prod,OU=platform,O=Acme"

// TestMTLSSubjectPolicyIsMatchedAgainstTheFullCertificateSubject drives the one
// authentication path that no test in this repository has ever driven with a
// real certificate.
//
// The gap: TransportInfo.TLSPeerCN is populated at server.go:343 and
// grpc_server.go:181, and consulted at auth.go:276, where it is compared for
// whole-string equality against the operator-written mtls_subject. Every
// existing test — auth_test.go:125 and my own policy_and_codec_gaps_test.go
// among them — hand-builds TransportInfo{TLSPeerCN: "..."} and hands it
// straight to AuthenticateRegister. That tests the comparison but not the
// extraction, which is the "塞字段的探针证明不了接线" shape: the assertion can
// only ever agree with whatever literal the test itself supplied.
// x509.CreateCertificate does not appear anywhere in this module outside the
// module cache, so no test has ever seen what a real peer certificate produces.
//
// What the extraction actually produces is worth being blunt about, because the
// field name argues against it. The field is called TLSPeerCN; the value
// assigned to it is cert.Subject.String(), the full RFC 2253 distinguished
// name. For a certificate whose subject carries only a CN the two are
// indistinguishable — which is exactly the case auth_test.go happens to
// encode — but production certificates carry O and OU, and then an operator who
// takes the field name at its word and writes
//
//	mtls_subject: "runner-prod"
//
// gets a runner that fails to authenticate with ErrAuthUnknownToken and no
// indication anywhere that the subject was the part that did not match. The
// second sub-test pins that this is a rejection rather than a match, so the
// required form cannot be quietly relaxed to a CommonName comparison without
// this test going red in both directions at once.
//
// Reachability was checked before this was written rather than assumed: the
// mtls_subject branch is not dead config. service/apiserver/run.go:80-88 loads
// --tls-client-ca into ClientCAs and sets tls.RequireAndVerifyClientCert, and
// applies that same config to both the HTTPS listener (:180) and the gRPC one
// (:119); sdk/xflow/runner.go:834-841 loads the runner's --tls-client-cert/key
// into Certificates. Both ends of the handshake are wired.
//
// Only the HTTP transport is exercised here. grpc_server.go:181 has the
// identical assignment, and that is a statement about the code rather than
// about this test's coverage: nothing below would notice if the gRPC extractor
// were changed, and this comment should not be read as claiming otherwise.
//
// Measured, so the claim is bounded to what was actually shown. Three mutations
// were run against every package that depends on service/control, each also run
// against the tree with this file removed:
//
//   - server.go:343 Subject.String() -> Subject.CommonName: two sub-tests below
//     go red; without this file the whole scope stays green.
//   - server.go:343 assignment -> "": the first sub-test goes red; without this
//     file the whole scope stays green.
//   - auth.go:287 `if e.mtlsSubject != ""` -> `if false`: two sub-tests below go
//     red, but so does auth_test.go's
//     TestFilePolicyStoreRequiresBothTokenAndMTLSWhenConfigured, with this file
//     removed.
//
// So the extraction is what had no coverage. The enforcement was already
// pinned, and the sub-tests here that cover it are a second angle on a guarded
// branch rather than a new one.
//
// One thing this test deliberately does not cover, recorded rather than fixed:
// TLSPeerSAN is appended to at server.go:344 and grpc_server.go:182 and read
// nowhere in the module. Asserting on it would pin a field no decision consults.
func TestMTLSSubjectPolicyIsMatchedAgainstTheFullCertificateSubject(t *testing.T) {
	ca, caKey := newTestCA(t)
	clientCert := issueClientCert(t, ca, caKey, pkix.Name{
		CommonName:         "runner-prod",
		Organization:       []string{"Acme"},
		OrganizationalUnit: []string{"platform"},
	})

	// Fixture sanity, kept separate from the assertions that have teeth on
	// xflow's own code: confirm the certificate really does render as the DN
	// the policies below are written against. Without this, a mismatch shows up
	// only as an unexplained 401 several layers away.
	leaf, err := x509.ParseCertificate(clientCert.Certificate[0])
	if err != nil {
		t.Fatalf("parse issued client cert: %v", err)
	}
	if got := leaf.Subject.String(); got != wantRunnerDN {
		t.Fatalf("issued certificate subject = %q, want %q: the constant this "+
			"file pins is the exact string an operator must copy into "+
			"mtls_subject, so if the rendering has changed the operator "+
			"documentation has changed with it", got, wantRunnerDN)
	}

	t.Run("the full DN authenticates", func(t *testing.T) {
		status := registerOverTLS(t, mtlsPolicy(t, wantRunnerDN), ca, clientCert)
		if status != http.StatusOK {
			t.Fatalf("register with mtls_subject=%q status = %d, want %d: the "+
				"policy names exactly the subject the client certificate "+
				"carries, so a runner configured correctly by the book "+
				"cannot get in", wantRunnerDN, status, http.StatusOK)
		}
	})

	t.Run("the bare common name does not authenticate", func(t *testing.T) {
		status := registerOverTLS(t, mtlsPolicy(t, "runner-prod"), ca, clientCert)
		if status != http.StatusUnauthorized {
			t.Fatalf("register with mtls_subject=%q status = %d, want %d: the "+
				"comparison at auth.go:288 is whole-string equality against "+
				"cert.Subject.String(), so a bare CN must not match a subject "+
				"that also carries O and OU -- accepting it here would mean "+
				"any certificate whose CN happens to collide authenticates as "+
				"this runner", "runner-prod", status, http.StatusUnauthorized)
		}
	})

	t.Run("a plaintext connection does not authenticate", func(t *testing.T) {
		srv := httptest.NewServer(newMTLSTestHandler(mtlsPolicy(t, wantRunnerDN)))
		defer srv.Close()
		status := postRegister(t, srv.Client(), srv.URL)
		if status != http.StatusUnauthorized {
			t.Fatalf("register over plaintext HTTP status = %d, want %d: "+
				"httpTransportInfo returns a zero TransportInfo when r.TLS is "+
				"nil, and an entry with an mtls_subject must not be satisfiable "+
				"without a peer certificate at all", status, http.StatusUnauthorized)
		}
	})
}

// mtlsPolicy builds a policy store holding a single entry whose only credential
// is the given subject. No token: this isolates the mTLS comparison, so a
// failure cannot be a token problem wearing a subject's clothes.
func mtlsPolicy(t *testing.T, subject string) Authenticator {
	t.Helper()
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			IDPrefix:         "runner-",
			MTLSSubject:      subject,
			AllowedNodeTypes: []string{"*"},
		}},
	}, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStoreFromConfig() error = %v", err)
	}
	return store
}

func newMTLSTestHandler(auth Authenticator) http.Handler {
	return NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory(), WithAuthenticator(auth)).Handler()
}

// registerOverTLS runs one register call across a real mutually-authenticated
// TLS connection and returns the HTTP status. The server requires and verifies
// a client certificate, so the request only reaches the handler at all if the
// handshake succeeded — which is what makes r.TLS.PeerCertificates non-empty
// inside httpTransportInfo.
func registerOverTLS(t *testing.T, auth Authenticator, ca *x509.Certificate, clientCert tls.Certificate) int {
	t.Helper()

	pool := x509.NewCertPool()
	pool.AddCert(ca)

	srv := httptest.NewUnstartedServer(newMTLSTestHandler(auth))
	srv.TLS = &tls.Config{
		ClientCAs:  pool,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	defer srv.Close()

	client := srv.Client()
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("httptest client transport is %T, want *http.Transport", client.Transport)
	}
	tr.TLSClientConfig.Certificates = []tls.Certificate{clientCert}

	return postRegister(t, client, srv.URL)
}

func postRegister(t *testing.T, client *http.Client, baseURL string) int {
	t.Helper()
	body, err := json.Marshal(protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("marshal register request: %v", err)
	}
	resp, err := client.Post(baseURL+protocol.RegisterRunnerPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", protocol.RegisterRunnerPath, err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func newTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "xflow test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return cert, key
}

func issueClientCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, subject pkix.Name) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
