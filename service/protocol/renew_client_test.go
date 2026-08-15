package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientRenewLease(t *testing.T) {
	var got RenewLeaseRequest
	deadline := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != RenewLeasePath {
			t.Errorf("path = %q, want %q", r.URL.Path, RenewLeasePath)
		}
		decodeClientTestJSON(t, r, &got)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RenewLeaseResponse{Renewed: true, Deadline: deadline})
	}))
	defer server.Close()

	client := NewClient(server.URL, server.Client())
	resp, err := client.RenewLease(context.Background(), RenewLeaseRequest{
		RunnerID:   "runner-1",
		SessionID:  "sess-1",
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Extend:     90_000,
	})
	if err != nil {
		t.Fatalf("RenewLease() error = %v", err)
	}
	if !resp.Renewed || !resp.Deadline.Equal(deadline) {
		t.Fatalf("RenewLease() = %+v, want renewed with the server's deadline", resp)
	}
	if got.LeaseToken != "token-1" || got.SessionID != "sess-1" || got.Extend != 90_000 {
		t.Fatalf("server saw %+v — the fence token, session, and extend must reach the server verbatim", got)
	}
}

// A refused renewal is a normal response, not a transport failure: the runner
// must be able to tell "the server says you lost the lease" (cancel the
// handler) from "the network is down" (retry). Collapsing the former into an
// error would make the renewal loop retry a lease it no longer owns.
func TestClientRenewLeaseRefusalIsNotAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RenewLeaseResponse{Renewed: false, Error: "lease not found"})
	}))
	defer server.Close()

	resp, err := NewClient(server.URL, server.Client()).RenewLease(context.Background(), RenewLeaseRequest{
		RunnerID: "runner-1", SessionID: "sess-1", LeaseID: "l", LeaseToken: "t",
	})
	if err != nil {
		t.Fatalf("RenewLease() error = %v, want a Renewed=false response", err)
	}
	if resp.Renewed {
		t.Fatal("RenewLease() reported renewed for a refusal")
	}
	if resp.Error != "lease not found" {
		t.Errorf("resp.Error = %q, want the server's reason", resp.Error)
	}
}

// The lease token is a bearer credential for the node's fence. It must never
// travel in a URL (org policy §6) and must never be echoed into an error the
// runner logs (§7).
func TestClientRenewLeaseErrorDoesNotLeakTheToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "token") {
			t.Errorf("lease token traveled in the query string: %q", r.URL.RawQuery)
		}
		http.Error(w, "stale session", http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := NewClient(server.URL, server.Client()).WithToken("auth-secret").
		RenewLease(context.Background(), RenewLeaseRequest{
			RunnerID: "runner-1", SessionID: "sess-1", LeaseID: "l", LeaseToken: "lease-secret-token",
		})
	if err == nil {
		t.Fatal("expected an error for 401")
	}
	if strings.Contains(err.Error(), "lease-secret-token") || strings.Contains(err.Error(), "auth-secret") {
		t.Fatalf("error leaks a credential: %v", err)
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error = %v, want the status code", err)
	}
}
