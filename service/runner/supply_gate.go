package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/supply"
)

// SupplyFetcher retrieves one supply's current content. The runner uses it at
// activation time; a second implementation will serve pull mode.
type SupplyFetcher interface {
	// Fetch returns the content, its content hash, and the server-side revision.
	// A missing resource must return ErrSupplyNotFound so the gate can tell
	// "never written" from a transport failure in its logs.
	Fetch(ctx context.Context, name string) (content []byte, hash string, revision uint64, err error)
}

// NotReadyError reports that this runner declined an activation because one or
// more required supplies have no content here. Missing holds the supply NODE
// names, sorted.
//
// Declining is the whole point: the alternative is to accept the activation, let
// the Kafka consumer commit offsets, and fail per-message — which silently
// evaporates traffic (a node with no retry policy does not retry, does not go to
// a dead-letter queue, and the failed execution expires without a trace).
type NotReadyError struct {
	Missing []string
}

func (e *NotReadyError) Error() string {
	return fmt.Sprintf("supply not ready: %s", strings.Join(e.Missing, ", "))
}

// SupplyGate is the activation-time readiness gate. It fetches any required
// supply this process does not have yet, publishes it into the registry, and
// reports whether the activation may proceed.
//
// It is also the first-fetch path for a supply this runner has never seen, which
// is why the gate — not a change notification — is what resolves every cold-start
// race: content that was never written, a freshly started runner, and a consumer
// activated before its supply all reduce to "fetch it now, synchronously".
type SupplyGate struct {
	fetcher  SupplyFetcher
	registry *supply.Registry
	logger   *slog.Logger
}

func NewSupplyGate(f SupplyFetcher, reg *supply.Registry, logger *slog.Logger) *SupplyGate {
	if reg == nil {
		reg = supply.Default
	}
	return &SupplyGate{fetcher: f, registry: reg, logger: logger}
}

// Admit reports whether the activation may proceed. It returns *NotReadyError
// listing every required-but-absent supply — all of them, so an operator does not
// need one reconcile round per missing supply to discover the next.
//
// Content is cached under the supply NODE name, not the resource name: the node
// name is the identity that $supplies expressions and the consumer registry use.
func (g *SupplyGate) Admit(ctx context.Context, reqs []engine.SupplyRequirement) error {
	if len(reqs) == 0 {
		return nil
	}
	var missing []string
	for _, req := range reqs {
		if _, ok := g.registry.Get(req.Node); ok {
			continue
		}
		content, hash, revision, err := g.fetcher.Fetch(ctx, req.Resource)
		if err != nil {
			// Both "never written" and "cannot reach the server" mean the same
			// thing for the decision: this runner has no content. They differ only
			// in what an operator should look at, hence the log level split.
			if errors.Is(err, ErrSupplyNotFound) {
				g.log(ctx, slog.LevelInfo, "supply has no content yet", req, "")
			} else {
				g.log(ctx, slog.LevelWarn, "supply fetch failed", req, "")
			}
			if req.RequireReady {
				missing = append(missing, req.Node)
			}
			continue
		}
		if err := g.registry.Apply(ctx, supply.Snapshot{
			Name:      req.Node,
			Content:   content,
			Hash:      hash,
			Revision:  revision,
			FetchedAt: time.Now(),
		}); err != nil {
			// A consumer rejected the content (e.g. wasm configure returned -1).
			// The snapshot is cached regardless, but a consumer that could not
			// apply it has no usable derived state, so a required supply is not
			// ready.
			g.log(ctx, slog.LevelWarn, "supply content rejected by a consumer", req, hash)
			if req.RequireReady {
				missing = append(missing, req.Node)
			}
			continue
		}
		g.log(ctx, slog.LevelInfo, "supply ready", req, hash)
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return &NotReadyError{Missing: missing}
}

// log emits one gate event. It records the supply name and at most a hash
// PREFIX — never the content, which is not a secret but is also never worth
// putting in a log line, and never an operator's to read from there.
func (g *SupplyGate) log(_ context.Context, level slog.Level, msg string, req engine.SupplyRequirement, hash string) {
	if g.logger == nil {
		return
	}
	attrs := []any{"supply_node", req.Node, "resource", req.Resource, "require_ready", req.RequireReady}
	if hash != "" {
		attrs = append(attrs, "hash", hashPrefix(hash))
	}
	g.logger.Log(context.Background(), level, msg, attrs...)
}

// hashPrefix shortens a content hash for logging. 12 hex chars after the
// algorithm prefix is enough to correlate two log lines and far too little to
// reveal anything about the content.
func hashPrefix(hash string) string {
	const keep = 12
	body := hash
	if i := strings.IndexByte(hash, ':'); i >= 0 {
		body = hash[i+1:]
	}
	if len(body) > keep {
		body = body[:keep]
	}
	return body
}
