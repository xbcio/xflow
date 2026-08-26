package runner

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
)

// TestSupplyFetchCompletesTheEncryptedRoundTrip exercises the only path that
// makes supply encryption real: a fetcher holding a key, talking to a server
// that answers with an envelope.
//
// Both halves of that round trip were unpinned. supply_client.go:62-63 asks for
// ciphertext; supply_client.go:100-106 turns it back into plaintext. Every
// existing test of the fetcher (TestHTTPSupplyFetcher_OK and its four
// siblings) leaves Keyring nil, so both branches are skipped. The one test that
// does give a fetcher a real Keyring — TestHeartbeatCarriesTheSupplyKeyID —
// only drives the heartbeat and never calls Fetch. And the server-side tests in
// service/apiserver decrypt by hand with supplyenc.NewKeyring(key).Decrypt,
// which proves the server encrypts but says nothing about whether any runner
// can read it. Nothing anywhere connected the two.
//
// The consequence of the ask half breaking is a silent downgrade, which is the
// dangerous shape. The server matches the header by exact string equality
// (service/apiserver/module_supply.go:170, `r.Header.Get("Accept") ==
// AcceptEncrypted`) and otherwise serves plaintext, so deleting
// supply_client.go:62-64 does not fail anything: the runner still gets usable
// content, IsEncrypted is then false so the decrypt branch never runs, and a
// deployment that has provisioned keys and believes supply content is encrypted
// on the wire is shipping it in the clear with no error and no metric.
//
// service/runner/doc.go:85-90 warns about exactly this — that assigning a fake
// key directly to a fetcher's Keyring field bypasses the real assembly path and
// must be backed by at least one test that exercises the real one.
//
// This is NOT that test, and an earlier version of this comment claimed it was.
// The fetcher below is a struct literal with Keyring set by hand, which is
// precisely the shape doc.go calls out as proving nothing about the wiring.
// What this test proves is narrower and still worth having: that Fetch asks for
// ciphertext and can open it, *given* a keyring. The assembly path doc.go
// actually asks for — Register → SupplyKey → installSupplyKey — is covered by
// TestRegisterInstallsTheSupplyKeyItWasHanded in
// register_supply_key_wiring_test.go.
func TestSupplyFetchCompletesTheEncryptedRoundTrip(t *testing.T) {
	key, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	plaintext := []byte(`{"rules":[{"id":"r-1","match":"/v1/orders"}]}`)
	ciphertext, err := supplyenc.Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	// Setup guard: the rest of the test is meaningless if the "ciphertext" is
	// just the plaintext, since then every assertion below passes trivially.
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("Encrypt returned its input unchanged")
	}
	if !supplyenc.IsEncrypted(ciphertext) {
		t.Fatal("Encrypt produced something the fetcher would not recognise as an envelope")
	}

	var mu sync.Mutex
	var sawAccept string
	var servedCiphertext bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sawAccept = r.Header.Get("Accept")
		// Exact equality, mirroring module_supply.go:170 rather than a
		// Contains: a fetcher that sent a merely similar value would be served
		// plaintext in production, so it must be served plaintext here too.
		encrypted := sawAccept == "application/x-xflow-encrypted"
		servedCiphertext = encrypted
		mu.Unlock()

		w.Header().Set("ETag", "sha256:0123456789abcdef")
		w.Header().Set("X-Supply-Revision", "7")
		if encrypted {
			w.Header().Set("Content-Type", "application/x-xflow-encrypted")
			_, _ = w.Write(ciphertext)
			return
		}
		_, _ = w.Write(plaintext)
	}))
	defer srv.Close()

	f := &HTTPSupplyFetcher{
		BaseURL: srv.URL,
		Token:   "tok",
		Client:  srv.Client(),
		Keyring: supplyenc.NewKeyring(key),
	}
	body, hash, revision, err := f.Fetch(context.Background(), "shared-rules")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	mu.Lock()
	accept, encrypted := sawAccept, servedCiphertext
	mu.Unlock()

	if !encrypted {
		t.Fatalf("the server answered in plaintext because the fetcher sent "+
			"Accept %q, want %q: a runner that holds keys and does not ask for "+
			"ciphertext downgrades the whole deployment to cleartext supply "+
			"content silently — nothing errors and the content still works",
			accept, "application/x-xflow-encrypted")
	}
	if !bytes.Equal(body, plaintext) {
		t.Fatalf("Fetch returned %d bytes, want the %d-byte plaintext", len(body), len(plaintext))
	}
	// Not just "equal to plaintext" — also positively not still the envelope,
	// so a future change that returns the raw body on a decrypt no-op cannot
	// pass by accident on a fixture whose plaintext happens to be short.
	if supplyenc.IsEncrypted(body) {
		t.Fatal("Fetch handed the caller an undecrypted envelope; the supply " +
			"would reach the node as an opaque blob instead of its content")
	}
	if hash != "sha256:0123456789abcdef" {
		t.Fatalf("hash = %q; the ETag must survive the decrypt path, it is what "+
			"suppresses the next fetch", hash)
	}
	if revision != 7 {
		t.Fatalf("revision = %d, want 7", revision)
	}
}

// TestSupplyFetchRefusesAnEnvelopeItCannotDecrypt pins supply_client.go:102-104.
//
// A decrypt failure means one of two things: the server rotated to a key this
// runner has not been given yet, or the response did not come from the server
// we think it did. Both must surface as an error. The failure to avoid is
// passing the envelope through as if it were content — the node would then run
// against a JSON object with "v"/"alg"/"kid"/"data" keys instead of its rules,
// which is not a parse error anywhere, just a supply that silently matches
// nothing.
func TestSupplyFetchRefusesAnEnvelopeItCannotDecrypt(t *testing.T) {
	serverKey, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	runnerKey, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ciphertext, err := supplyenc.Encrypt(serverKey, []byte(`{"rules":[]}`))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(ciphertext)
	}))
	defer srv.Close()

	f := &HTTPSupplyFetcher{
		BaseURL: srv.URL,
		Client:  srv.Client(),
		Keyring: supplyenc.NewKeyring(runnerKey),
	}
	body, _, _, err := f.Fetch(context.Background(), "shared-rules")
	if err == nil {
		t.Fatalf("Fetch succeeded with a key that cannot open the envelope and "+
			"returned %d bytes; an unopenable envelope must not reach the node "+
			"as content", len(body))
	}
	if body != nil {
		t.Fatalf("Fetch returned an error and %d bytes of content; the caller "+
			"must not be able to use a body from a failed decrypt", len(body))
	}
	if !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("err = %v, want it to name the decrypt step: this is the "+
			"symptom of a key rotation the runner has not caught up with, and "+
			"an operator has to be able to tell it from a transport failure", err)
	}
}
