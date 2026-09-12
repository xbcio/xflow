// Package redisx contains Redis helpers shared by distributed backend workers.
package redisx

import (
	"context"
	"sort"
	"sync"

	"github.com/redis/go-redis/v9"
)

type scanner interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
}

// ScanAll returns every key matching pattern without sharing cursor state
// between Redis Cluster masters. SCAN is node-local: a ClusterClient routes a
// bare SCAN to only one master, and cursors returned by one master have no
// meaning on another. ForEachMaster therefore runs an independent, complete
// cursor loop on every master. Its callbacks run concurrently, so results are
// merged under a mutex and deduplicated before being returned in stable order.
// Non-cluster clients retain the ordinary single-node cursor loop.
//
// ScanAll materializes all matched keys before returning. Callers that need
// bounded incremental work should use ScanPage instead.
func ScanAll(ctx context.Context, rdb redis.Cmdable, pattern string, count int64) ([]string, error) {
	cluster, ok := rdb.(*redis.ClusterClient)
	if !ok {
		return scanNode(ctx, rdb, pattern, count)
	}

	return scanMasters(ctx, pattern, count, func(ctx context.Context, visit func(context.Context, scanner) error) error {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return visit(ctx, master)
		})
	})
}

type forEachMaster func(context.Context, func(context.Context, scanner) error) error

func scanMasters(ctx context.Context, pattern string, count int64, forEach forEachMaster) ([]string, error) {
	seen := make(map[string]struct{})
	var mu sync.Mutex
	err := forEach(ctx, func(ctx context.Context, master scanner) error {
		keys, err := scanNode(ctx, master, pattern, count)
		if err != nil {
			return err
		}
		mu.Lock()
		for _, key := range keys {
			seen[key] = struct{}{}
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sortedKeys(seen), nil
}

// Cursors records one node-local SCAN cursor per Redis master. Callers should
// treat values as opaque and only feed a set back to ScanPage for the same
// logical scan.
type Cursors map[string]uint64

// ScanPage advances one page on every current Redis Cluster master and merges
// the pages. The requested count is divided among masters (with a minimum of
// one per master); Redis treats COUNT as a hint, so the returned key count can
// still exceed it. For a non-cluster client, ScanPage advances the cursor under
// the single empty node identifier.
//
// Cursor progress is returned only when every master succeeds. A topology
// change naturally drops departed node cursors and starts new masters at zero.
func ScanPage(ctx context.Context, rdb redis.Cmdable, cursors Cursors, pattern string, count int64) ([]string, Cursors, error) {
	cluster, ok := rdb.(*redis.ClusterClient)
	if !ok {
		keys, next, err := scanPage(ctx, rdb, cursors[""], pattern, count)
		if err != nil {
			return nil, nil, err
		}
		return keys, Cursors{"": next}, nil
	}

	counts, err := clusterMasterCounts(ctx, cluster, count)
	if err != nil {
		return nil, nil, err
	}
	return scanMasterPages(ctx, cursors, pattern, count, counts,
		func(ctx context.Context, visit func(context.Context, string, scanner) error) error {
			return cluster.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return visit(ctx, masterID(master), master)
			})
		},
	)
}

type forEachNamedMaster func(context.Context, func(context.Context, string, scanner) error) error

func scanMasterPages(
	ctx context.Context,
	cursors Cursors,
	pattern string,
	count int64,
	counts map[string]int64,
	forEach forEachNamedMaster,
) ([]string, Cursors, error) {
	seen := make(map[string]struct{})
	nextCursors := make(Cursors, len(counts))
	var mu sync.Mutex
	err := forEach(ctx, func(ctx context.Context, id string, master scanner) error {
		masterCount, known := counts[id]
		if !known {
			// The topology may change between discovering masters and scanning
			// them. A newly added master starts at cursor zero and still receives
			// a valid positive COUNT hint.
			masterCount = count
			if masterCount > 0 {
				masterCount = 1
			}
		}
		keys, next, err := scanPage(ctx, master, cursors[id], pattern, masterCount)
		if err != nil {
			return err
		}
		mu.Lock()
		for _, key := range keys {
			seen[key] = struct{}{}
		}
		nextCursors[id] = next
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return sortedKeys(seen), nextCursors, nil
}

func clusterMasterCounts(ctx context.Context, cluster *redis.ClusterClient, count int64) (map[string]int64, error) {
	return collectMasterCounts(ctx, count,
		func(ctx context.Context, visit func(context.Context, string, scanner) error) error {
			return cluster.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return visit(ctx, masterID(master), master)
			})
		},
	)
}

func collectMasterCounts(ctx context.Context, count int64, forEach forEachNamedMaster) (map[string]int64, error) {
	var mu sync.Mutex
	ids := make(map[string]struct{})
	if err := forEach(ctx, func(_ context.Context, id string, _ scanner) error {
		mu.Lock()
		ids[id] = struct{}{}
		mu.Unlock()
		return nil
	}); err != nil {
		return nil, err
	}
	return splitScanCount(ids, count), nil
}

func splitScanCount(ids map[string]struct{}, count int64) map[string]int64 {
	ordered := sortedKeys(ids)
	counts := make(map[string]int64, len(ordered))
	if len(ordered) == 0 {
		return counts
	}
	if count <= 0 {
		for _, id := range ordered {
			counts[id] = count
		}
		return counts
	}
	if count < int64(len(ordered)) {
		for _, id := range ordered {
			counts[id] = 1
		}
		return counts
	}

	base := count / int64(len(ordered))
	remainder := count % int64(len(ordered))
	for i, id := range ordered {
		counts[id] = base
		if int64(i) < remainder {
			counts[id]++
		}
	}
	return counts
}

func masterID(master *redis.Client) string {
	if addr := master.NodeAddress(); addr != "" {
		return addr
	}
	return master.Options().Addr
}

func scanPage(ctx context.Context, rdb scanner, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	keys, next, err := rdb.Scan(ctx, cursor, pattern, count).Result()
	if err != nil {
		return nil, 0, err
	}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		seen[key] = struct{}{}
	}
	return sortedKeys(seen), next, nil
}

func scanNode(ctx context.Context, rdb scanner, pattern string, count int64) ([]string, error) {
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			seen[key] = struct{}{}
		}
		cursor = next
		if cursor == 0 {
			return sortedKeys(seen), nil
		}
	}
}

func sortedKeys(seen map[string]struct{}) []string {
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
