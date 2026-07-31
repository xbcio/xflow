package apiserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store/memstore"
)

func TestSupplyAuditRecordsRevisionNotContent(t *testing.T) {
	st := memstore.New()
	sink := NewInMemoryAuditSink()
	m := newSupplyModule(st)
	m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "test-user", Namespace: "ns1", Scopes: []string{"supply.write"}}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = sink
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	const secretish = "RULE-BODY-MARKER"
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v1/supplies/rules",
			bytes.NewReader([]byte(`{"rules":["`+secretish+`"]}`))))
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT %d = %d, body=%s", i, rec.Code, rec.Body)
		}
	}

	events := sink.Events()
	var outcomes []AuditEvent
	for _, e := range events {
		if e.Phase == "outcome" {
			outcomes = append(outcomes, e)
		}
	}
	if len(outcomes) != 2 {
		t.Fatalf("outcome rows = %d, want 2", len(outcomes))
	}
	if outcomes[0].Revision != 1 || outcomes[1].Revision != 2 {
		t.Fatalf("revisions = %d,%d; want 1,2", outcomes[0].Revision, outcomes[1].Revision)
	}
	for _, e := range events {
		if strings.Contains(e.Resource, secretish) || strings.Contains(e.Reason, secretish) {
			t.Fatalf("audit leaked supply content: %+v", e)
		}
		if e.Principal == "" {
			t.Fatalf("audit row without principal: %+v", e)
		}
	}
}
