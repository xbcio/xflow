package runner

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
)

// HandlerInventory is the exact capability declaration used to validate group
// packages before execution. Has() must not fall back to latest version.
type HandlerInventory interface {
	Has(nodeType string, version int) bool
	Runtimes() []string
	Resources() []string
	Credentials() []string
}

// PackageCacheConfig controls the bounded cache behavior.
type PackageCacheConfig struct {
	MaxEntries      int
	MaxPackageBytes int
	AllowedRuntimes []string
}

type cacheEntry struct {
	graph   *graph.Graph
	pkg     *graph.GroupPackage
	useCount int
}

// PackageCache validates and caches compiled group packages keyed by hash.
type PackageCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	order   []string
	config  PackageCacheConfig
}

// NewPackageCache creates a bounded package cache.
func NewPackageCache(cfg PackageCacheConfig) *PackageCache {
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 64
	}
	return &PackageCache{
		entries: make(map[string]*cacheEntry),
		config:  cfg,
	}
}

// ErrPackageMissing is returned when the payload has no package and the cache
// does not contain the hash. This is retryable (server may resend).
var ErrPackageMissing = errors.New("group package not in payload and not cached")

// Resolve validates the group package against the handler inventory and
// returns the compiled graph. Results are cached by PackageHash.
func (c *PackageCache) Resolve(payload *engine.GroupLeasePayload, inv HandlerInventory) (*graph.Graph, *graph.GroupPackage, error) {
	if payload == nil {
		return nil, nil, errors.New("nil group lease payload")
	}

	c.mu.Lock()
	if entry, ok := c.entries[payload.PackageHash]; ok {
		entry.useCount++
		c.mu.Unlock()
		return entry.graph, entry.pkg, nil
	}
	c.mu.Unlock()

	if payload.Package == nil {
		return nil, nil, ErrPackageMissing
	}

	pkg := payload.Package

	if err := c.validatePackage(pkg, payload.PackageHash, inv); err != nil {
		return nil, nil, err
	}

	compiled, err := graph.CompileProjectedPackage(pkg)
	if err != nil {
		return nil, nil, &PackageValidationError{Reason: fmt.Sprintf("compile: %v", err)}
	}

	c.mu.Lock()
	c.evictIfNeeded()
	c.entries[payload.PackageHash] = &cacheEntry{graph: compiled, pkg: pkg}
	c.order = append(c.order, payload.PackageHash)
	c.mu.Unlock()

	return compiled, pkg, nil
}

// Known returns the package hashes currently cached.
func (c *PackageCache) Known() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.entries))
	for k := range c.entries {
		out = append(out, k)
	}
	return out
}

func (c *PackageCache) evictIfNeeded() {
	for len(c.entries) >= c.config.MaxEntries && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
}

func (c *PackageCache) validatePackage(pkg *graph.GroupPackage, expectedHash string, inv HandlerInventory) error {
	// Verify hash matches by recomputing from the package.
	computedHash, hashErr := recomputePackageHash(pkg)
	if hashErr != nil {
		return &PackageValidationError{Reason: fmt.Sprintf("hash computation: %v", hashErr)}
	}
	if computedHash != expectedHash {
		return &PackageValidationError{Reason: fmt.Sprintf("hash mismatch: got %s, want %s", computedHash, expectedHash)}
	}

	// Validate requirements against handler inventory.
	for _, req := range pkg.Requirements {
		if req.NodeType == graph.NodeTypeGroupExit {
			continue
		}
		if !inv.Has(req.NodeType, req.NodeVersion) {
			return &PackageValidationError{
				Reason: fmt.Sprintf("handler not available: type=%s version=%d", req.NodeType, req.NodeVersion),
			}
		}
		if req.Runtime != "" && !contains(inv.Runtimes(), req.Runtime) {
			return &PackageValidationError{
				Reason: fmt.Sprintf("runtime not available: %s (required by %s)", req.Runtime, req.NodeType),
			}
		}
		if req.Resource != "" && !contains(inv.Resources(), req.Resource) {
			return &PackageValidationError{
				Reason: fmt.Sprintf("resource not available: %s (required by %s)", req.Resource, req.NodeType),
			}
		}
		for _, cred := range req.Credentials {
			if !contains(inv.Credentials(), cred) {
				return &PackageValidationError{
					Reason: fmt.Sprintf("credential not declared: %s (required by %s)", cred, req.NodeType),
				}
			}
		}
	}

	// Validate allowed runtimes.
	if len(c.config.AllowedRuntimes) > 0 {
		for _, art := range pkg.Artifacts {
			if art.Runtime != "" && !contains(c.config.AllowedRuntimes, art.Runtime) {
				return &PackageValidationError{
					Reason: fmt.Sprintf("runtime %s not in allowlist", art.Runtime),
				}
			}
		}
	}

	// Validate max package size.
	if c.config.MaxPackageBytes > 0 {
		for _, art := range pkg.Artifacts {
			if art.Size > c.config.MaxPackageBytes {
				return &PackageValidationError{
					Reason: fmt.Sprintf("artifact %s exceeds max size: %d > %d", art.NodeName, art.Size, c.config.MaxPackageBytes),
				}
			}
		}
	}

	return nil
}

// PackageValidationError is a permanent configuration failure.
type PackageValidationError struct {
	Reason string
}

func (e *PackageValidationError) Error() string {
	return "package validation failed: " + e.Reason
}

func recomputePackageHash(pkg *graph.GroupPackage) (string, error) {
	return graph.ComputePackageHash(pkg)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
