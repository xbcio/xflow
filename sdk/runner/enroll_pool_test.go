package runner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestResolveRunnerIdentityCarriesPoolInstanceFieldsAndAdoptsResponse(t *testing.T) {
	t.Setenv("POD_UID", "pod-uid-7")
	var seen protocol.EnrollRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{
			RunnerID:   "pool-runner-1",
			Token:      "pool-token-1",
			Namespaces: []string{"tenant-a", "tenant-b"},
			Labels:     map[string]string{"pool": "shared"},
		})
	}))
	defer srv.Close()

	cfg := enrollTestConfig(srv.URL)
	cfg.registrationToken = "pool-code"
	cfg.systemID = "system-a"
	cfg.labels = map[string]string{"zone": "east"}
	got, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{})
	if err != nil {
		t.Fatalf("resolveRunnerIdentity: %v", err)
	}

	if seen.SystemID != "system-a" || seen.InstanceUID != "pod:pod-uid-7" {
		t.Fatalf("pool instance fields = (%q, %q), want (system-a, pod:pod-uid-7)", seen.SystemID, seen.InstanceUID)
	}
	if len(seen.Namespaces) != 0 {
		t.Fatalf("request namespaces = %v, want an undeclared empty set", seen.Namespaces)
	}
	if len(got.namespaces) != 2 || got.namespaces[0] != "tenant-a" || got.namespaces[1] != "tenant-b" {
		t.Fatalf("adopted namespaces = %v, want [tenant-a tenant-b]", got.namespaces)
	}
	if got.labels["pool"] != "shared" || got.labels["zone"] != "east" {
		t.Fatalf("merged labels = %v, want pool and runner labels", got.labels)
	}
}

func TestResolveRunnerIdentitySendsDeclaredNamespacesAndKeepsThem(t *testing.T) {
	var seen protocol.EnrollRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{
			RunnerID:   "pool-runner-2",
			Token:      "pool-token-2",
			Namespaces: []string{"server-namespace"},
		})
	}))
	defer srv.Close()

	cfg := enrollTestConfig(srv.URL)
	cfg.registrationToken = "pool-code"
	cfg.namespaces = parseNamespaces([]string{"runner-namespace"})
	got, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{})
	if err != nil {
		t.Fatalf("resolveRunnerIdentity: %v", err)
	}
	if len(seen.Namespaces) != 1 || seen.Namespaces[0] != "runner-namespace" {
		t.Fatalf("request namespaces = %v, want [runner-namespace]", seen.Namespaces)
	}
	if len(got.namespaces) != 1 || got.namespaces[0] != "runner-namespace" {
		t.Fatalf("resolved namespaces = %v, want runner declaration preserved", got.namespaces)
	}
}

func TestResolveRunnerIdentityRejectsServerLabelConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{Namespaces: []string{"default"},
			RunnerID: "pool-runner-3",
			Token:    "pool-token-3",
			Labels:   map[string]string{"pool": "server"},
		})
	}))
	defer srv.Close()

	store := &ephemeralIdentityStore{}
	cfg := enrollTestConfig(srv.URL)
	cfg.registrationToken = "pool-code"
	cfg.labels = map[string]string{"pool": "manual"}
	_, err := resolveRunnerIdentity(context.Background(), cfg, store)
	if err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("error = %v, want label conflict", err)
	}
	if _, ok, loadErr := store.Load(); loadErr != nil || ok {
		t.Fatalf("stored identity after conflict = (_, %v, %v), want (_, false, nil)", ok, loadErr)
	}
}
