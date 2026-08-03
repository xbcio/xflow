package apiserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store/memstore"
)

// newEncryptedSupplyTestServer mirrors newSupplyTestServer
// (supply_endpoint_test.go:16) and additionally installs an encryptor, which
// that helper's signature does not expose.
func newEncryptedSupplyTestServer(t *testing.T, key *supplyenc.Key) (*http.ServeMux, *memstore.Store) {
	t.Helper()
	st := memstore.New()
	m := newSupplyModule(st)
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "test-user", Namespace: "ns1", Scopes: []string{"supply.write", "supply.read"},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	m.encryptor = fixedEncryptor{key: key}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux, st
}

type fixedEncryptor struct{ key *supplyenc.Key }

func (f fixedEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return supplyenc.Encrypt(f.key, plaintext)
}

func putSupplyForTest(t *testing.T, mux *http.ServeMux, content []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d, body=%s", rec.Code, rec.Body)
	}
}

// runner 带 Accept: application/x-xflow-encrypted 时，响应体必须是密文，
// 且必须能用注册时下发的那把 key 解开。这是「传输加密真的发生了」的直接
// 证据 —— round trip 成功不足以证明，明文直通也会 round trip 成功。
func TestSupplyGETReturnsCiphertextWhenRequested(t *testing.T) {
	key, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	mux, _ := newEncryptedSupplyTestServer(t, key)
	plain := []byte(`{"rules":[{"field":"authorization","action":"redact"}]}`)
	putSupplyForTest(t, mux, plain)

	req := httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil)
	req.Header.Set("Accept", AcceptEncrypted)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d", rec.Code)
	}
	body := rec.Body.Bytes()
	if bytes.Contains(body, []byte("authorization")) {
		t.Fatal("the response body contains plaintext; nothing was encrypted")
	}
	got, err := supplyenc.NewKeyring(key).Decrypt(body)
	if err != nil {
		t.Fatalf("decrypt response with the delivered key: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("decrypted body = %q, want %q", got, plain)
	}
}

// 不带 Accept 的调用方必须继续拿到明文，否则老 runner 在升级瞬间全部失效。
func TestSupplyGETStaysPlaintextWithoutAcceptHeader(t *testing.T) {
	key, err := supplyenc.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	mux, _ := newEncryptedSupplyTestServer(t, key)
	plain := []byte(`{"rules":[1]}`)
	putSupplyForTest(t, mux, plain)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))

	if !bytes.Equal(rec.Body.Bytes(), plain) {
		t.Errorf("a client that did not ask for encryption received %q, want the plaintext %q",
			rec.Body.Bytes(), plain)
	}
}
