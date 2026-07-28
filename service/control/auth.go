package control

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"gopkg.in/yaml.v3"
)

// Authentication sentinels. Wrapping keeps callers from having to know the
// specific policy denial reason — every failure path becomes 401 /
// codes.Unauthenticated at the transport layer.
var (
	ErrAuthMissingToken   = errors.New("missing auth token")
	ErrAuthUnknownToken   = errors.New("unknown auth token")
	ErrAuthIDPrefixDenied = errors.New("runner id does not match policy prefix")
	ErrAuthMTLSMismatch   = errors.New("mtls subject does not match policy")
)

// TransportInfo carries transport-layer identity extracted by the HTTP or
// gRPC middleware. TLSPeerCN / TLSPeerSAN are empty when TLS is not in play;
// mTLS policies reject in that case.
type TransportInfo struct {
	TLSPeerCN  string
	TLSPeerSAN []string
}

// RunnerPolicy is the effective set of permissions bound to an authenticated
// runner. Cached on the runnerState so the dispatcher can filter node types
// without touching the policy store on every Assign.
type RunnerPolicy struct {
	// Name identifies the matched policy entry for logging. Not
	// security-relevant — the token match is what proves identity.
	Name string
	// IDPrefix is the required prefix of the runner's self-declared ID.
	IDPrefix string
	// AllowedNodeTypes is the set of node types this runner may execute.
	// A single "*" entry means all node types.
	AllowedNodeTypes []string
}

// Allows reports whether the policy permits the given node type. Called from
// the dispatcher's Assign hot path — kept O(N) with N ~= handful of types.
func (p RunnerPolicy) Allows(nodeType string) bool {
	for _, t := range p.AllowedNodeTypes {
		if t == "*" || t == nodeType {
			return true
		}
	}
	return false
}

// Authenticator resolves credentials to a RunnerPolicy. Register runs at
// registration time; Ongoing runs on every heartbeat / poll / report so
// revocation via policy reload takes effect without forcing re-register.
type Authenticator interface {
	AuthenticateRegister(runnerID, token string, info TransportInfo) (RunnerPolicy, error)
	AuthenticateOngoing(runnerID, token string, info TransportInfo) (RunnerPolicy, error)
}

// DisabledAuthenticator is the default and matches today's behavior: every
// request is accepted, with an empty-but-permissive policy. Used when the
// operator has not enabled auth (dev / MVP path).
type DisabledAuthenticator struct{}

// permissivePolicy accepts every node type. Only used by DisabledAuthenticator
// to keep the dispatcher path unchanged when auth is off.
var permissivePolicy = RunnerPolicy{Name: "auth-disabled", AllowedNodeTypes: []string{"*"}}

func (DisabledAuthenticator) AuthenticateRegister(string, string, TransportInfo) (RunnerPolicy, error) {
	return permissivePolicy, nil
}
func (DisabledAuthenticator) AuthenticateOngoing(string, string, TransportInfo) (RunnerPolicy, error) {
	return permissivePolicy, nil
}

// PolicyEntry is one YAML runners.yaml entry after env / file expansion.
type PolicyEntry struct {
	Name             string   `yaml:"name,omitempty"`
	IDPrefix         string   `yaml:"id_prefix"`
	Token            string   `yaml:"token,omitempty"`
	TokenFile        string   `yaml:"token_file,omitempty"`
	MTLSSubject      string   `yaml:"mtls_subject,omitempty"`
	AllowedNodeTypes []string `yaml:"allowed_node_types"`
}

// PolicyConfig is the parsed structure of runners.yaml. Kept as a plain
// struct so tests can construct it programmatically without touching disk.
type PolicyConfig struct {
	Version int           `yaml:"version"`
	Runners []PolicyEntry `yaml:"runners"`
}

// FilePolicyStore is a bearer-token + optional mTLS authenticator loaded
// from disk. Hot reload is not implemented yet — the swap is atomic (via
// atomic.Pointer) so a future fsnotify watcher can plug in with a single
// snapshot.Store call.
type FilePolicyStore struct {
	// dryRun logs violations but returns the permissivePolicy so the request
	// proceeds. Meant for the --auth-mode dry-run rollout path.
	dryRun bool
	snap   atomic.Pointer[policySnapshot]
}

// policySnapshot is a resolved+immutable view of the policy file. Everything
// the auth hot path needs is either a hashed token (constant-time compare) or
// a lowercase mTLS subject string.
type policySnapshot struct {
	entries []resolvedEntry
}

type resolvedEntry struct {
	name         string
	idPrefix     string
	tokenHash    [32]byte // sha256(token); empty [32]byte{} means no token bound
	hasToken     bool
	mtlsSubject  string   // lowercased for case-insensitive compare
	allowedTypes []string // stored verbatim; matched by RunnerPolicy.Allows
}

// NewFilePolicyStore reads and parses a runners.yaml. The store starts empty
// if path is empty — callers can then use Reload to load explicitly.
func NewFilePolicyStore(path string, dryRun bool) (*FilePolicyStore, error) {
	s := &FilePolicyStore{dryRun: dryRun}
	s.snap.Store(&policySnapshot{})
	if path == "" {
		return s, nil
	}
	if err := s.Reload(path); err != nil {
		return nil, err
	}
	return s, nil
}

// NewFilePolicyStoreFromConfig builds a store from an in-memory config —
// useful for tests. Env / token_file expansion is not applied.
func NewFilePolicyStoreFromConfig(cfg PolicyConfig, dryRun bool) (*FilePolicyStore, error) {
	s := &FilePolicyStore{dryRun: dryRun}
	s.snap.Store(&policySnapshot{})
	snap, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	s.snap.Store(snap)
	return s, nil
}

// Reload parses the file at path and atomically swaps in the new snapshot.
// Returns without mutating state if the file is malformed.
func (s *FilePolicyStore) Reload(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat policy file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		// The file may hold plaintext tokens. Refuse to load if group / world
		// can read — matches the spec's "insecure permissions" rule.
		return fmt.Errorf("policy file %q is world/group-readable (mode %v); tighten to 0600 or 0640", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read policy file: %w", err)
	}
	var cfg PolicyConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse policy file: %w", err)
	}
	snap, err := resolveConfig(cfg)
	if err != nil {
		return err
	}
	s.snap.Store(snap)
	return nil
}

func resolveConfig(cfg PolicyConfig) (*policySnapshot, error) {
	if cfg.Version != 0 && cfg.Version != 1 {
		return nil, fmt.Errorf("unsupported policy version %d (only 1 is supported)", cfg.Version)
	}
	entries := make([]resolvedEntry, 0, len(cfg.Runners))
	for i, r := range cfg.Runners {
		if r.IDPrefix == "" {
			return nil, fmt.Errorf("runners[%d]: id_prefix is required", i)
		}
		re := resolvedEntry{
			name:         r.Name,
			idPrefix:     r.IDPrefix,
			mtlsSubject:  strings.ToLower(r.MTLSSubject),
			allowedTypes: append([]string(nil), r.AllowedNodeTypes...),
		}
		if re.name == "" {
			re.name = r.IDPrefix
		}
		token, err := resolveToken(r)
		if err != nil {
			return nil, fmt.Errorf("runners[%d/%s]: %w", i, re.name, err)
		}
		if token != "" {
			re.tokenHash = sha256.Sum256([]byte(token))
			re.hasToken = true
		}
		if !re.hasToken && re.mtlsSubject == "" {
			return nil, fmt.Errorf("runners[%d/%s]: at least one of token, token_file, mtls_subject is required", i, re.name)
		}
		entries = append(entries, re)
	}
	return &policySnapshot{entries: entries}, nil
}

func resolveToken(r PolicyEntry) (string, error) {
	switch {
	case r.Token != "" && r.TokenFile != "":
		return "", errors.New("token and token_file are mutually exclusive")
	case r.Token != "":
		return os.ExpandEnv(r.Token), nil
	case r.TokenFile != "":
		info, err := os.Stat(r.TokenFile)
		if err != nil {
			return "", fmt.Errorf("token_file: %w", err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			return "", fmt.Errorf("token_file %q is world/group-readable (mode %v)", r.TokenFile, info.Mode().Perm())
		}
		raw, err := os.ReadFile(r.TokenFile)
		if err != nil {
			return "", fmt.Errorf("read token_file: %w", err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	return "", nil
}

// AuthenticateRegister matches (token, runnerID, transport identity) against
// the current snapshot. Returns the permissive dry-run policy when dryRun is
// on and no match is found so the request proceeds.
func (s *FilePolicyStore) AuthenticateRegister(runnerID, token string, info TransportInfo) (RunnerPolicy, error) {
	return s.authenticate(runnerID, token, info)
}

// AuthenticateOngoing is identical to Register in this implementation:
// tokens are validated on every request so revoking a policy takes effect
// without a runner-side reconnect.
func (s *FilePolicyStore) AuthenticateOngoing(runnerID, token string, info TransportInfo) (RunnerPolicy, error) {
	return s.authenticate(runnerID, token, info)
}

// TokenFingerprint returns the first 8 hex chars of sha256(token). Callers
// use this in log lines instead of the raw token so a leaked log never
// exposes credentials.
func TokenFingerprint(token string) string {
	if token == "" {
		return "none"
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:8]
}

func (s *FilePolicyStore) authenticate(runnerID, token string, info TransportInfo) (RunnerPolicy, error) {
	snap := s.snap.Load()
	if snap == nil || len(snap.entries) == 0 {
		return s.deny(ErrAuthUnknownToken)
	}
	var tokenHash [32]byte
	if token != "" {
		tokenHash = sha256.Sum256([]byte(token))
	}
	tlsSubject := strings.ToLower(info.TLSPeerCN)
	idPrefixDenied := false
	for _, e := range snap.entries {
		if e.hasToken {
			if token == "" {
				continue
			}
			if subtle.ConstantTimeCompare(e.tokenHash[:], tokenHash[:]) != 1 {
				continue
			}
		}
		if e.mtlsSubject != "" {
			if tlsSubject == "" || tlsSubject != e.mtlsSubject {
				continue
			}
		}
		// The credentials matched this entry but the runner ID is outside its
		// allowed prefix. Do not deny outright: a later entry may legitimately
		// authorize the same credentials for this runner ID. Remember the prefix
		// rejection and, only if no entry ultimately matches, deny with the
		// prefix reason.
		if runnerID != "" && !strings.HasPrefix(runnerID, e.idPrefix) {
			idPrefixDenied = true
			continue
		}
		return RunnerPolicy{
			Name:             e.name,
			IDPrefix:         e.idPrefix,
			AllowedNodeTypes: append([]string(nil), e.allowedTypes...),
		}, nil
	}
	if idPrefixDenied {
		return s.deny(ErrAuthIDPrefixDenied)
	}
	if token == "" {
		return s.deny(ErrAuthMissingToken)
	}
	return s.deny(ErrAuthUnknownToken)
}

func (s *FilePolicyStore) deny(err error) (RunnerPolicy, error) {
	if s.dryRun {
		// Still return a permissive policy so the request succeeds; the caller
		// is expected to log the underlying error via `err`.
		return permissivePolicy, dryRunDenial(err)
	}
	return RunnerPolicy{}, err
}

// dryRunError wraps the underlying denial so callers can detect that this
// verdict is being suppressed. Only used from FilePolicyStore.deny.
type dryRunError struct{ inner error }

func (e *dryRunError) Error() string { return "dry-run: " + e.inner.Error() }
func (e *dryRunError) Unwrap() error { return e.inner }

func dryRunDenial(err error) error { return &dryRunError{inner: err} }

// IsDryRunDenial reports whether an authenticate result is a dry-run
// suppression. Callers can log the denial without blocking the request.
func IsDryRunDenial(err error) bool {
	var d *dryRunError
	return errors.As(err, &d)
}

// IsDryRun reports whether this authenticator suppresses denials for rollout.
func (s *FilePolicyStore) IsDryRun() bool {
	if s == nil {
		return false
	}
	return s.dryRun
}
