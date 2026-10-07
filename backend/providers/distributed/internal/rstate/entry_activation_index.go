package rstate

// Entry activation index rebuild.
//
// Activation records live under two key layouts (legacy identity-tagged,
// modern workflow-tagged) scattered across the namespace's slots, so
// enumerating them with SCAN MATCH costs a full keyspace sweep per call — the
// server evaluates the pattern against every key, and a more precise pattern
// does not make it cheaper. List runs on the reconciler, heartbeat, and
// projection paths, which made the sweep their dominant cost.
//
// The index replaces the sweep with sets that List reads directly:
//
//   - the workflow enumeration set (entryactidx:wfs) naming every workflow
//     digest tag that has a per-workflow index;
//   - the per-workflow index (entryactidx:{wf-<sha>}) holding the modern record
//     key names of that workflow, maintained from the same Lua call that writes
//     the record (same hash tag, same slot);
//   - the legacy set (entryactidx:legacy) holding the frozen pre-migration
//     record key names, written only by the rebuild because the legacy layout
//     has no writers left.
//
// The ready gate (entryactidx:ready) is set once a rebuild has populated all
// three sets from a complete scan; List falls back to scanning until then. A
// cross-process lock serializes rebuilds, and a failed rebuild deliberately
// leaves its lock to expire: List keeps serving the scan path meanwhile, and
// the lock doubles as retry backoff against a persistent fault.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/namespace"
)

const (
	// entryActivationIndexRebuildLockTTL bounds one rebuild attempt's lock. It
	// outlives entryActivationIndexRebuildTimeout so an attempt that runs out of
	// time releases its lock by token while it still owns it.
	entryActivationIndexRebuildLockTTL = 30 * time.Minute
	// entryActivationIndexRebuildTimeout bounds one rebuild attempt. An attempt
	// that overruns abandons the rebuild; the lock expires on its own so a
	// later attempt can retry.
	entryActivationIndexRebuildTimeout = 25 * time.Minute
	// entryActivationIndexRebuildScanCount is the SCAN COUNT hint per cursor
	// step during a rebuild. The rebuild pays one sweep and amortizes it; a
	// larger page than the read path's shrinks the round-trip count.
	entryActivationIndexRebuildScanCount = 500
	// entryActivationIndexReadBatch bounds one HGETALL pipeline page on the
	// indexed read path.
	entryActivationIndexReadBatch = 512
)

// releaseEntryActivationIndexRebuildLockLua removes the rebuild lock only when
// it still carries the caller's token. An expired lock may already belong to a
// successor, and deleting it blind would let a third process rebuild
// concurrently.
//
// KEYS: 1=rebuild lock
const releaseEntryActivationIndexRebuildLockLuaSrc = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`

var releaseEntryActivationIndexRebuildLockLua = redis.NewScript(releaseEntryActivationIndexRebuildLockLuaSrc)

// triggerEntryActivationIndexRebuild launches a best-effort, single-flight
// index rebuild in the background. List must never wait for a rebuild: the
// caller already received the scan-based result, and the rebuild only improves
// later calls. The goroutine uses its own bounded context so a caller that
// cancels its request context cannot abort the rebuild.
func (s *EntryActivationStore) triggerEntryActivationIndexRebuild(ns namespace.Namespace) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), entryActivationIndexRebuildTimeout)
		defer cancel()
		_ = s.rebuildEntryActivationIndex(ctx, ns)
	}()
}

// rebuildEntryActivationIndex scans both key layouts once, populates the
// per-workflow indexes, the workflow enumeration set, and the legacy set, then
// flips the ready gate that moves List onto the indexed path. It is idempotent
// and guarded by a cross-process lock; a concurrent rebuild loses the lock and
// returns without touching anything.
func (s *EntryActivationStore) rebuildEntryActivationIndex(ctx context.Context, ns namespace.Namespace) error {
	lockKey := entryActivationIndexRebuildLockRedisKey(ns)
	lockToken := strconv.FormatInt(time.Now().UnixNano(), 10)
	acquired, err := s.rdb.SetNX(ctx, lockKey, lockToken, entryActivationIndexRebuildLockTTL).Result()
	if err != nil {
		return fmt.Errorf("acquire entry activation index rebuild lock %q: %w", lockKey, err)
	}
	if !acquired {
		return nil
	}

	ready, err := s.rdb.Exists(ctx, entryActivationIndexReadyRedisKey(ns)).Result()
	if err != nil {
		return fmt.Errorf("check entry activation index readiness: %w", err)
	}
	if ready > 0 {
		// A concurrent rebuild finished while this one waited for the lock.
		return s.releaseEntryActivationIndexRebuildLock(ctx, lockKey, lockToken)
	}

	keys, err := s.scanEntryActivationKeys(ctx, ns)
	if err != nil {
		return err
	}
	if err := s.writeEntryActivationIndex(ctx, ns, keys); err != nil {
		return err
	}
	if err := s.rdb.Set(ctx, entryActivationIndexReadyRedisKey(ns), lockToken, 0).Err(); err != nil {
		return fmt.Errorf("mark entry activation index ready %q: %w", entryActivationIndexReadyRedisKey(ns), err)
	}
	return s.releaseEntryActivationIndexRebuildLock(ctx, lockKey, lockToken)
}

// releaseEntryActivationIndexRebuildLock releases the lock while this attempt
// still owns it; see releaseEntryActivationIndexRebuildLockLua.
func (s *EntryActivationStore) releaseEntryActivationIndexRebuildLock(ctx context.Context, lockKey, token string) error {
	if _, err := releaseEntryActivationIndexRebuildLockLua.Run(ctx, s.rdb, []string{lockKey}, token).Result(); err != nil {
		return fmt.Errorf("release entry activation index rebuild lock %q: %w", lockKey, err)
	}
	return nil
}

// scanEntryActivationKeys returns every activation key of the namespace under
// both layouts, deduplicated and sorted. The patterns mirror the scan path's:
// the legacy layout is keyed by the raw namespace, the modern layout by the
// escaped one, and they only coincide when the namespace needs no escaping.
func (s *EntryActivationStore) scanEntryActivationKeys(ctx context.Context, ns namespace.Namespace) ([]string, error) {
	patterns := []string{entryActivationScanPattern(ns)}
	if modernPattern := workflowScopedEntryActivationScanPattern(ns); modernPattern != patterns[0] {
		patterns = append(patterns, modernPattern)
	}
	seen := make(map[string]struct{})
	for _, pattern := range patterns {
		keys, err := redisx.ScanAll(ctx, s.rdb, pattern, entryActivationIndexRebuildScanCount)
		if err != nil {
			return nil, fmt.Errorf("scan entry activations: %w", err)
		}
		for _, key := range keys {
			seen[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

// modernEntryActivationKeyTag extracts the workflow digest tag from a modern
// record key name of this namespace, returning "" for anything else. Legacy
// keys cannot pass: their hash tag is "<wf>|<ver>|<unit>" and always contains
// pipe separators, and only the modern layout continues with ":act:" after the
// tag — the tag itself is checked for the exact wf-<64 lowercase hex> shape for
// good measure.
func modernEntryActivationKeyTag(ns namespace.Namespace, key string) string {
	rest, ok := strings.CutPrefix(key, "xflow:ns:"+entryActivationNamespacePath(ns)+":entryact:{")
	if !ok {
		return ""
	}
	tag, suffix, ok := strings.Cut(rest, "}")
	if !ok || !strings.HasPrefix(suffix, ":act:") {
		return ""
	}
	if len(tag) != len("wf-")+2*sha256.Size || !strings.HasPrefix(tag, "wf-") {
		return ""
	}
	for i := len("wf-"); i < len(tag); i++ {
		if c := tag[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return tag
}

// filterLegacyEntryActivationKeys drops keys whose stored identity belongs to
// another namespace. A pattern built from a namespace containing Redis glob
// metacharacters over-matches, and only the stored namespace field can
// distinguish those keys — the same trust-the-record rule the scan path
// applies when it decodes a record.
func (s *EntryActivationStore) filterLegacyEntryActivationKeys(ctx context.Context, ns namespace.Namespace, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	namespaces := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		namespaces[i] = pipe.HGet(ctx, key, "namespace")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read legacy entry activation namespaces: %w", err)
	}
	kept := make([]string, 0, len(keys))
	for i, cmd := range namespaces {
		value, err := cmd.Result()
		if err == redis.Nil {
			// The key expired between the scan and this read.
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read legacy entry activation namespace %q: %w", keys[i], err)
		}
		if value == string(ns) {
			kept = append(kept, keys[i])
		}
	}
	return kept, nil
}

// entryActivationIndexTTL is one per-workflow index key's derived expiry: the
// remaining time of its longest-lived member, or permanent when a member has no
// expiry to bound it.
type entryActivationIndexTTL struct {
	ttl       time.Duration
	permanent bool
}

// entryActivationIndexTTLs derives each index key's expiry from its members'
// remaining record TTLs. Mirroring the records keeps the invariant the write
// path maintains — an index never expires before a record it must expose —
// across TTL reconfigurations: a record written under a longer previous TTL
// keeps its index alive past the new store TTL. A member without an expiry
// makes the index permanent, because no derived bound exists; a missing member
// contributes nothing, and the store TTL is the floor so an index whose members
// all expired still expires.
func (s *EntryActivationStore) entryActivationIndexTTLs(ctx context.Context, modernByTag map[string][]string) (map[string]entryActivationIndexTTL, error) {
	members := make([]string, 0, len(modernByTag))
	for _, keys := range modernByTag {
		members = append(members, keys...)
	}
	pipe := s.rdb.Pipeline()
	pttls := make([]*redis.DurationCmd, len(members))
	for i, member := range members {
		pttls[i] = pipe.PTTL(ctx, member)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("read entry activation record ttls: %w", err)
	}
	// go-redis maps Redis' PTTL -1 (key exists without expiry) and -2 (key
	// missing) replies to time.Duration(-1) and time.Duration(-2).
	remaining := make(map[string]time.Duration, len(members))
	for i, cmd := range pttls {
		value, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("read entry activation record ttl %q: %w", members[i], err)
		}
		remaining[members[i]] = value
	}

	ttls := make(map[string]entryActivationIndexTTL, len(modernByTag))
	for tag, keys := range modernByTag {
		derived := entryActivationIndexTTL{ttl: s.ttl}
		for _, key := range keys {
			switch value := remaining[key]; {
			case value == -1:
				derived.permanent = true
			case value > derived.ttl:
				derived.ttl = value
			}
		}
		ttls[tag] = derived
	}
	return ttls, nil
}

// writeEntryActivationIndex populates all three index sets from one scan's
// keys. Modern keys are classified by key name alone; legacy keys are
// confirmed against their stored namespace. The sets are written in one
// pipeline, and each per-workflow index's expiry follows its members' records.
func (s *EntryActivationStore) writeEntryActivationIndex(ctx context.Context, ns namespace.Namespace, keys []string) error {
	modernByTag := make(map[string][]string)
	var legacyCandidates []string
	for _, key := range keys {
		if tag := modernEntryActivationKeyTag(ns, key); tag != "" {
			modernByTag[tag] = append(modernByTag[tag], key)
			continue
		}
		if strings.HasPrefix(key, entryActivationLegacyKeyPrefix(ns)) {
			legacyCandidates = append(legacyCandidates, key)
		}
	}

	legacyKeys, err := s.filterLegacyEntryActivationKeys(ctx, ns, legacyCandidates)
	if err != nil {
		return err
	}

	tags := make([]string, 0, len(modernByTag))
	for tag := range modernByTag {
		tags = append(tags, tag)
	}
	sort.Strings(tags)

	indexTTLs, err := s.entryActivationIndexTTLs(ctx, modernByTag)
	if err != nil {
		return err
	}

	pipe := s.rdb.Pipeline()
	if len(legacyKeys) > 0 {
		pipe.SAdd(ctx, entryActivationLegacyIndexRedisKey(ns), stringArgs(legacyKeys)...)
	}
	if len(tags) > 0 {
		pipe.SAdd(ctx, entryActivationWorkflowSetRedisKey(ns), stringArgs(tags)...)
	}
	for _, tag := range tags {
		indexKey := entryActivationWorkflowIndexRedisKeyForTag(ns, tag)
		pipe.SAdd(ctx, indexKey, stringArgs(modernByTag[tag])...)
		// The expiry must follow the SADD for the same key, and a pipeline
		// preserves per-key order, so an empty index key cannot survive.
		if ttl := indexTTLs[tag]; !ttl.permanent {
			pipe.PExpire(ctx, indexKey, ttl.ttl)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("write entry activation index: %w", err)
	}
	return nil
}

// stringArgs widens a string slice for Redis command variadics.
func stringArgs(values []string) []any {
	args := make([]any, len(values))
	for i, value := range values {
		args[i] = value
	}
	return args
}
