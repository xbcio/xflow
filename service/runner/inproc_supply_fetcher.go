package runner

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// InProcessSupplyFetcher reads supply content straight from the control plane
// in the same process. It is the RunnerTransportInProc counterpart of
// HTTPSupplyFetcher: same namespace scoping, same not-found mapping, same
// content cap, but no socket and therefore no encryption envelope (there is no
// wire to protect, so it does not participate in supply key rotation).
type InProcessSupplyFetcher struct {
	Reader InProcessSupplyReader
	// RunnerID identifies the runner to the control plane's resource
	// authentication.
	RunnerID string
}

var _ SupplyFetcher = (*InProcessSupplyFetcher)(nil)

func (f *InProcessSupplyFetcher) Fetch(ctx context.Context, name string) ([]byte, string, uint64, error) {
	if name == "" {
		return nil, "", 0, fmt.Errorf("supply fetch: empty name")
	}
	if f.Reader == nil {
		return nil, "", 0, fmt.Errorf("supply fetch: no in-process reader configured")
	}
	// The namespace is the one the activation loop put on ctx — never a
	// parameter of this call, so a caller cannot address another tenant.
	ns := namespace.FromContext(ctx)
	ctx, cancel := context.WithTimeout(ctx, supplyFetchTimeout)
	defer cancel()

	content, hash, revision, err := f.Reader.FetchSupply(ctx, f.RunnerID, string(ns), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, "", 0, ErrSupplyNotFound
		}
		return nil, "", 0, fmt.Errorf("supply fetch: %w", err)
	}
	// The server-side store does not cap content, so cap it here: the HTTP
	// fetcher refuses the same size after reading it, and a consumer expecting
	// a bounded snapshot must not receive an unbounded one just because the
	// content never crossed a socket.
	if len(content) > maxSupplyResponseBytes {
		return nil, "", 0, fmt.Errorf("supply fetch: content exceeds %d bytes", maxSupplyResponseBytes)
	}
	return content, hash, revision, nil
}
