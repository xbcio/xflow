package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	identityStoreEphemeral = "ephemeral"
	identityStoreFile      = "file"
)

// identity is this runner's durable control-plane credential: the ID the
// server knows it by and the bearer token it authenticates with. Field names
// mirror protocol.EnrollResponse so an enrollment result maps straight across
// with no renaming layer.
type identity struct {
	RunnerID string `json:"runner_id"`
	Token    string `json:"token"`
}

// identityStore is where a runner keeps the identity it was issued. Load's
// bool reports presence: (identity{}, false, nil) means "nothing stored yet",
// which is the enrollment trigger, and is distinct from an error — a corrupt
// or world-readable store must NOT read as "not enrolled yet" and silently
// re-enroll, burning a registration code every restart.
type identityStore interface {
	Load() (identity, bool, error)
	Save(identity) error
}

// ephemeralIdentityStore keeps the identity in memory only. A restart
// re-enrolls. This is the default because it is the behavior every existing
// deployment already has: no file is created anywhere unless an operator asks
// for one.
type ephemeralIdentityStore struct {
	mu  sync.Mutex
	cur identity
	set bool
}

func (s *ephemeralIdentityStore) Load() (identity, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur, s.set, nil
}

func (s *ephemeralIdentityStore) Save(id identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur, s.set = id, true
	return nil
}

// fileIdentityStore persists the identity as JSON at path, mode 0600.
type fileIdentityStore struct{ path string }

func (s *fileIdentityStore) Load() (identity, bool, error) {
	info, err := os.Stat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return identity{}, false, nil
	}
	if err != nil {
		return identity{}, false, fmt.Errorf("stat identity file: %w", err)
	}
	// Same 0o077 rule FilePolicyStore.Reload applies to the server's policy
	// file, for the same reason: the contents are a bearer credential.
	if info.Mode().Perm()&0o077 != 0 {
		return identity{}, false, fmt.Errorf(
			"identity file %q is world/group-readable (mode %v); tighten to 0600",
			s.path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return identity{}, false, fmt.Errorf("read identity file: %w", err)
	}
	var id identity
	if err := json.Unmarshal(raw, &id); err != nil {
		return identity{}, false, fmt.Errorf("parse identity file %q: %w", s.path, err)
	}
	if strings.TrimSpace(id.RunnerID) == "" || strings.TrimSpace(id.Token) == "" {
		return identity{}, false, fmt.Errorf(
			"identity file %q has no runner_id or no token; delete it to re-enroll", s.path)
	}
	return id, true, nil
}

func (s *fileIdentityStore) Save(id identity) error {
	raw, err := json.Marshal(id)
	if err != nil {
		return fmt.Errorf("encode identity: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create identity directory: %w", err)
	}
	// Temp file + rename so a crash mid-write cannot leave a truncated file
	// that Load would then reject on every subsequent start.
	tmp, err := os.CreateTemp(dir, ".identity-*")
	if err != nil {
		return fmt.Errorf("create temporary identity file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temporary identity file: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary identity file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary identity file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("install identity file: %w", err)
	}
	return nil
}

// newIdentityStore builds the store named by --identity-store.
func newIdentityStore(cfg runnerConfig) (identityStore, error) {
	switch cfg.identityStoreKind {
	case "", identityStoreEphemeral:
		return &ephemeralIdentityStore{}, nil
	case identityStoreFile:
		path := strings.TrimSpace(cfg.identityFile)
		if path == "" {
			return nil, errors.New("identity store \"file\" requires --identity-file (or identity.file in the config, or XFLOW_RUNNER_IDENTITY_FILE)")
		}
		return &fileIdentityStore{path: path}, nil
	default:
		return nil, fmt.Errorf("identity store must be %q or %q: %q",
			identityStoreEphemeral, identityStoreFile, cfg.identityStoreKind)
	}
}
