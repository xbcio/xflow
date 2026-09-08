package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEphemeralIdentityStoreRoundTrip(t *testing.T) {
	s := &ephemeralIdentityStore{}
	if _, ok, err := s.Load(); err != nil || ok {
		t.Fatalf("Load on a fresh ephemeral store = (_, %v, %v), want (_, false, nil)", ok, err)
	}
	want := identity{RunnerID: "runner-7", Token: "t0ken"}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := s.Load()
	if err != nil || !ok || got != want {
		t.Fatalf("Load after Save = (%+v, %v, %v), want (%+v, true, nil)", got, ok, err, want)
	}
}

func TestFileIdentityStoreRoundTripAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "identity.json")
	s := &fileIdentityStore{path: path}

	if _, ok, err := s.Load(); err != nil || ok {
		t.Fatalf("Load with no file = (_, %v, %v), want (_, false, nil)", ok, err)
	}

	want := identity{RunnerID: "runner-7", Token: "t0ken"}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("identity file mode = %v, want 0600: the file holds a bearer token", perm)
	}

	got, ok, err := s.Load()
	if err != nil || !ok || got != want {
		t.Fatalf("Load after Save = (%+v, %v, %v), want (%+v, true, nil)", got, ok, err, want)
	}
}

func TestFileIdentityStoreRejectsWorldReadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(path, []byte(`{"runner_id":"r","token":"t"}`), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := (&fileIdentityStore{path: path}).Load(); err == nil {
		t.Fatal("Load accepted a 0644 identity file; a group/world-readable bearer token must be refused")
	}
}

func TestFileIdentityStoreRejectsIncompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(path, []byte(`{"runner_id":"r"}`), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := (&fileIdentityStore{path: path}).Load(); err == nil {
		t.Fatal("Load accepted an identity file with no token; a half-written identity must not silently become an anonymous runner")
	}
}

func TestNewIdentityStoreRequiresAPathForTheFileKind(t *testing.T) {
	if _, err := newIdentityStore(runnerConfig{identityStoreKind: "file"}); err == nil {
		t.Fatal("newIdentityStore(file) with no identity file path returned nil error")
	}
	if _, err := newIdentityStore(runnerConfig{identityStoreKind: "nope"}); err == nil {
		t.Fatal("newIdentityStore accepted an unknown store kind")
	}
	s, err := newIdentityStore(runnerConfig{identityStoreKind: "ephemeral"})
	if err != nil {
		t.Fatalf("newIdentityStore(ephemeral): %v", err)
	}
	if _, ok := s.(*ephemeralIdentityStore); !ok {
		t.Fatalf("newIdentityStore(ephemeral) = %T, want *ephemeralIdentityStore", s)
	}
}
