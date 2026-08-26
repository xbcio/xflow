package objectstore_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// TestHTTPStore_GetObject_PropagatesContentType proves that HTTPStore.GetObject
// forwards the origin server's actual Content-Type header into Object.ContentType
// rather than a fixed value.
//
// If this were hardcoded (e.g. always "application/octet-stream"), a downstream
// consumer that branches on Object.ContentType (or re-serves it verbatim to a
// client) would silently mislabel every artifact regardless of what the server
// actually sent — breaking content negotiation without any error anywhere in
// the call chain.
func TestHTTPStore_GetObject_PropagatesContentType(t *testing.T) {
	const wantType = "application/wasm"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", wantType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("wasm bytes"))
	}))
	defer srv.Close()

	hs := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "t", Client: srv.Client()}
	rc, obj, err := hs.GetObject(context.Background(), testKey)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	_ = rc.Close()

	if obj.ContentType != wantType {
		t.Fatalf("ContentType = %q, want %q (origin header must be propagated, not hardcoded)", obj.ContentType, wantType)
	}
}

// TestHTTPStore_HeadObject_PropagatesContentType is the HeadObject counterpart:
// the HEAD response's Content-Type must also be forwarded, not hardcoded.
func TestHTTPStore_HeadObject_PropagatesContentType(t *testing.T) {
	const wantType = "application/x-custom-artifact"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", wantType)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	hs := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "t", Client: srv.Client()}
	obj, err := hs.HeadObject(context.Background(), testKey)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}

	if obj.ContentType != wantType {
		t.Fatalf("ContentType = %q, want %q (origin header must be propagated, not hardcoded)", obj.ContentType, wantType)
	}
}
