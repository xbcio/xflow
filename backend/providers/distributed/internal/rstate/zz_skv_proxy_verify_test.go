package rstate

// Manual compatibility harness for pruneOrphanLeaseIndex against a REAL skv
// proxy (the shared test-environment Redis proxy SAS talks to in cluster mode).
// It is NOT part of the regular suite: with XFLOW_SKV_VERIFY != "1" the test
// skips before any network I/O, so `go test ./...` can never reach a remote
// instance by accident.
//
// Environment:
//
//	XFLOW_SKV_VERIFY=1          enable the harness (anything else -> t.Skip)
//	XFLOW_SKV_VERIFY_ADDRS      comma-separated host:port list (required)
//	XFLOW_SKV_VERIFY_PASSWORD   optional password; never logged or echoed
//	XFLOW_SKV_VERIFY_MODE       "single" (default, redis.NewClient on addrs[0])
//	                            or "cluster" (redis.NewClusterClient, all addrs)
//	XFLOW_SKV_VERIFY_SAMPLE_NS  optional namespace for a READ-ONLY sample of a
//	                            real lease-expiry index
//
// What is verified: the prune's ZRANGEBYSCORE with a growing LIMIT offset, its
// non-transactional pipeline of single-key EXISTS across per-execution hash
// tags (different slots), the variadic ZREM of the whole round's orphans, and
// the verify-then-rearm path. The proxy forbids MULTI/WATCH, which is why the
// production code uses Pipeline() and why this harness never issues TxPipeline.
//
// Safety rules, because the target is a SHARED test Redis:
//   - No whole-database flush and no key-enumeration commands (KEYS/SCAN)
//     anywhere in this file.
//   - Every key this test writes or deletes must start with the private
//     namespace prefix xflow:ns:zz-verify-<unixnano>:, asserted by
//     skvVerifyGuard before the command is queued.
//   - Cleanup deletes only the keys this test created, one key per DEL command:
//     a variadic DEL would be CROSSSLOT on a cluster proxy.
//   - The sample path issues only ZRANGE / ZCOUNT / ZCARD / EXISTS and never
//     writes to the sampled namespace.
//
// The 5000/100 split below is deliberate: the 100 live members score older than
// the orphans and therefore sit at the head of the ZSET, while the orphans trail
// behind them. That is the offset=kept shape the production defect produced.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const (
	skvVerifyEnableEnv   = "XFLOW_SKV_VERIFY"
	skvVerifyAddrsEnv    = "XFLOW_SKV_VERIFY_ADDRS"
	skvVerifyPasswordEnv = "XFLOW_SKV_VERIFY_PASSWORD"
	skvVerifyModeEnv     = "XFLOW_SKV_VERIFY_MODE"
	skvVerifySampleNSEnv = "XFLOW_SKV_VERIFY_SAMPLE_NS"

	skvVerifyOrphanCount   = 5000
	skvVerifyLiveCount     = 100
	skvVerifySeedBatchSize = 500
	skvVerifySampleCount   = 2000
)

// skvVerifyLogger records Error calls. pruneOrphanLeaseIndex reports every
// failure through noteLeaseIndexFailure at Error level, so any recorded error
// means the prune did not complete on this backend.
type skvVerifyLogger struct {
	mu     sync.Mutex
	errors []string
}

func (l *skvVerifyLogger) record(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, msg)
}

func (l *skvVerifyLogger) Error(msg string, args ...any) {
	l.record(fmt.Sprintf("%s %v", msg, args))
}

func (l *skvVerifyLogger) Errorf(format string, args ...any) {
	l.record(fmt.Sprintf(format, args...))
}

func (l *skvVerifyLogger) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.errors...)
}

func (l *skvVerifyLogger) Debug(string, ...any)  {}
func (l *skvVerifyLogger) Debugf(string, ...any) {}
func (l *skvVerifyLogger) Info(string, ...any)   {}
func (l *skvVerifyLogger) Infof(string, ...any)  {}
func (l *skvVerifyLogger) Warn(string, ...any)   {}
func (l *skvVerifyLogger) Warnf(string, ...any)  {}
func (l *skvVerifyLogger) Panic(string, ...any)  {}
func (l *skvVerifyLogger) Panicf(string, ...any) {}

var _ engine.Logger = (*skvVerifyLogger)(nil)

func skvVerifyRequireNoErrors(t *testing.T, log *skvVerifyLogger, what string) {
	t.Helper()
	if errs := log.recorded(); len(errs) > 0 {
		t.Fatalf("SKV-VERIFY %s: store logged %d error(s); first = %q", what, len(errs), errs[0])
	}
}

// skvVerifyGuard refuses any write or delete outside the test's private
// namespace. A cluster proxy spreads keys by hash tag, so a stray key would be
// invisible to a keyspace check and impossible to unwind here.
type skvVerifyGuard struct {
	t  *testing.T
	ns namespace.Namespace
}

func (g skvVerifyGuard) prefix() string {
	return "xflow:ns:" + string(g.ns) + ":"
}

func (g skvVerifyGuard) key(key string) string {
	if !strings.HasPrefix(key, g.prefix()) {
		g.t.Fatalf("SKV-VERIFY refusing to touch key %q outside private prefix %q", key, g.prefix())
	}
	return key
}

// skvVerifyWritten tracks exactly the keys this test creates, so cleanup can
// delete them without a scan. Only the test goroutine appends.
type skvVerifyWritten struct {
	keys []string
}

func skvVerifyCleanup(t *testing.T, rdb redis.UniversalClient, guard skvVerifyGuard, written *skvVerifyWritten) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		keys := append([]string(nil), written.keys...)
		if len(keys) == 0 {
			return
		}
		// One key per DEL command: a variadic DEL of keys in different slots is
		// rejected by a cluster proxy. Failures are reported with t.Errorf so a
		// bad batch never skips the remaining cleanup.
		for start := 0; start < len(keys); start += skvVerifySeedBatchSize {
			end := start + skvVerifySeedBatchSize
			if end > len(keys) {
				end = len(keys)
			}
			pipe := rdb.Pipeline()
			for _, key := range keys[start:end] {
				pipe.Del(ctx, guard.key(key))
			}
			if _, err := pipe.Exec(ctx); err != nil {
				t.Errorf("SKV-VERIFY cleanup: DEL batch [%d:%d] failed: %v", start, end, err)
			}
		}
		pipe := rdb.Pipeline()
		exists := make([]*redis.IntCmd, len(keys))
		for i, key := range keys {
			exists[i] = pipe.Exists(ctx, key)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Errorf("SKV-VERIFY cleanup: EXISTS verification failed: %v", err)
		}
		var residual []string
		for i, cmd := range exists {
			if cmd.Val() != 0 {
				residual = append(residual, keys[i])
			}
		}
		if len(residual) > 0 {
			shown := residual
			if len(shown) > 10 {
				shown = shown[:10]
			}
			t.Errorf("SKV-VERIFY cleanup: %d key(s) still exist in namespace %q after DEL: %v", len(residual), guard.ns, shown)
			return
		}
		t.Logf("SKV-VERIFY cleanup: deleted %d private key(s); EXISTS verification all 0", len(keys))
	}
}

type skvVerifyConfig struct {
	addrs    []string
	password string
	mode     string
	sampleNS namespace.Namespace
}

func skvVerifyConfigFromEnv(t *testing.T) skvVerifyConfig {
	t.Helper()
	if os.Getenv(skvVerifyEnableEnv) != "1" {
		t.Skipf("%s != 1: refusing to contact a remote skv proxy (set %s=1 and %s to run)",
			skvVerifyEnableEnv, skvVerifyEnableEnv, skvVerifyAddrsEnv)
	}
	var addrs []string
	for _, addr := range strings.Split(os.Getenv(skvVerifyAddrsEnv), ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		t.Fatalf("SKV-VERIFY %s is required when %s=1", skvVerifyAddrsEnv, skvVerifyEnableEnv)
	}
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(skvVerifyModeEnv)))
	if mode == "" {
		mode = "single"
	}
	if mode != "single" && mode != "cluster" {
		t.Fatalf("SKV-VERIFY %s = %q, want \"single\" or \"cluster\"", skvVerifyModeEnv, mode)
	}
	return skvVerifyConfig{
		addrs:    addrs,
		password: os.Getenv(skvVerifyPasswordEnv),
		mode:     mode,
		sampleNS: namespace.Namespace(strings.TrimSpace(os.Getenv(skvVerifySampleNSEnv))),
	}
}

func skvVerifyNewClient(t *testing.T, cfg skvVerifyConfig) redis.UniversalClient {
	t.Helper()
	var rdb redis.UniversalClient
	switch cfg.mode {
	case "cluster":
		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: cfg.addrs, Password: cfg.password})
	default:
		rdb = redis.NewClient(&redis.Options{Addr: cfg.addrs[0], Password: cfg.password})
	}
	t.Cleanup(func() {
		if err := rdb.Close(); err != nil {
			t.Logf("SKV-VERIFY close client: %v", err)
		}
	})
	return rdb
}

// skvVerifyExistsPipelineHook fires fn once, right after a pipeline containing
// an EXISTS of key has returned. For the prune that is exactly "the execution
// was read as an orphan and the ZREM has not been sent yet" — where a producer
// would slip its per-execution ZSET in. The hook is registered on the
// UniversalClient, which exposes AddHook for both *redis.Client and
// *redis.ClusterClient (the latter runs it above its per-node pipelines).
type skvVerifyExistsPipelineHook struct {
	key   string
	once  sync.Once
	fired atomic.Bool
	fn    func(context.Context)
}

func (h *skvVerifyExistsPipelineHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *skvVerifyExistsPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *skvVerifyExistsPipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			if cmd.Name() == "exists" && len(cmd.Args()) > 1 && fmt.Sprint(cmd.Args()[1]) == h.key {
				h.once.Do(func() {
					h.fired.Store(true)
					h.fn(ctx)
				})
				break
			}
		}
		return err
	}
}

// TestSkvProxyPruneVerify checks the prune against the configured backend. It
// writes only under zz-verify-<unixnano>, so it is safe on shared Redis.
func TestSkvProxyPruneVerify(t *testing.T) {
	cfg := skvVerifyConfigFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdb := skvVerifyNewClient(t, cfg)
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("SKV-VERIFY ping (mode=%q, addrs=%d) failed: %v", cfg.mode, len(cfg.addrs), err)
	}

	log := &skvVerifyLogger{}
	store := New(rdb, nil, time.Minute)
	store.SetLogger(log)

	ns := namespace.Namespace(fmt.Sprintf("zz-verify-%d", time.Now().UnixNano()))
	guard := skvVerifyGuard{t: t, ns: ns}
	written := &skvVerifyWritten{}
	indexKey := leaseExpiryExecIndexKey(ns)
	written.keys = append(written.keys, guard.key(indexKey))
	t.Cleanup(skvVerifyCleanup(t, rdb, guard, written))

	t.Logf("SKV-VERIFY target: mode=%q addrs=%d password_set=%t private_ns=%q",
		cfg.mode, len(cfg.addrs), cfg.password != "", ns)

	t.Run("bulk_prune", func(t *testing.T) {
		skvVerifyBulkPrune(t, ctx, rdb, store, log, ns, written)
	})
	t.Run("verify_concurrent_registration", func(t *testing.T) {
		skvVerifyConcurrentRegistration(t, ctx, rdb, store, log, ns, written)
	})
	if cfg.sampleNS != "" {
		t.Run("sample_readonly", func(t *testing.T) {
			skvVerifySampleReadOnly(t, ctx, rdb, cfg.sampleNS)
		})
	} else {
		t.Logf("SKV-VERIFY sample: skipped (%s not set)", skvVerifySampleNSEnv)
	}
}

// skvVerifyBulkPrune seeds 5000 orphans behind 100 live members and pins that
// one prune call clears exactly the orphans, then that a second call is a no-op.
func skvVerifyBulkPrune(t *testing.T, ctx context.Context, rdb redis.UniversalClient, store *Store, log *skvVerifyLogger, ns namespace.Namespace, written *skvVerifyWritten) {
	t.Helper()
	guard := skvVerifyGuard{t: t, ns: ns}
	now := time.Now().UTC()
	indexKey := leaseExpiryExecIndexKey(ns)
	guard.key(indexKey)

	// Live members score OLDER than the orphans, so they occupy the head of the
	// ZSET and the orphans trail behind them exactly as in the production shape.
	orphanScore := float64(now.Add(-30 * time.Minute).UnixMilli())
	liveScore := float64(now.Add(-90 * time.Minute).UnixMilli())
	liveLeaseScore := float64(now.Add(-2 * time.Hour).UnixMilli())

	orphanIDs := make([]types.ExecutionID, skvVerifyOrphanCount)
	for i := range orphanIDs {
		// Every id is a distinct {exec-id} hash tag, so the per-execution EXISTS
		// keys of the live members span many slots on a cluster proxy.
		orphanIDs[i] = types.ExecutionID(fmt.Sprintf("zz-verify-orphan-%05d", i))
	}
	liveIDs := make([]types.ExecutionID, skvVerifyLiveCount)
	for i := range liveIDs {
		liveIDs[i] = types.ExecutionID(fmt.Sprintf("zz-verify-live-%05d", i))
	}

	for start := 0; start < len(orphanIDs); start += skvVerifySeedBatchSize {
		end := start + skvVerifySeedBatchSize
		if end > len(orphanIDs) {
			end = len(orphanIDs)
		}
		pipe := rdb.Pipeline()
		for _, id := range orphanIDs[start:end] {
			pipe.ZAdd(ctx, indexKey, redis.Z{Score: orphanScore, Member: string(id)})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("SKV-VERIFY seed orphan index members [%d:%d]: %v", start, end, err)
		}
	}

	pipe := rdb.Pipeline()
	for _, id := range liveIDs {
		zkey := leaseExpiryZSetKey(ns, id)
		written.keys = append(written.keys, guard.key(zkey))
		pipe.ZAdd(ctx, indexKey, redis.Z{Score: liveScore, Member: string(id)})
		pipe.ZAdd(ctx, zkey, redis.Z{Score: liveLeaseScore, Member: leaseExpiryMember(id, "run")})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("SKV-VERIFY seed %d live index members and their lease zsets: %v", len(liveIDs), err)
	}

	wantBefore := int64(skvVerifyOrphanCount + skvVerifyLiveCount)
	before, err := rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("SKV-VERIFY ZCARD before prune: %v", err)
	}
	if before != wantBefore {
		t.Fatalf("SKV-VERIFY seeded ZCARD = %d, want %d (without a prune the orphans would stay)", before, wantBefore)
	}
	t.Logf("SKV-VERIFY seed: orphans=%d live=%d zcard_before_prune=%d",
		skvVerifyOrphanCount, skvVerifyLiveCount, before)

	maxMs := fmt.Sprintf("%d", now.UnixMilli())
	pruneStart := time.Now()
	store.pruneOrphanLeaseIndex(ctx, ns, maxMs)
	pruneElapsed := time.Since(pruneStart)

	after, err := rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("SKV-VERIFY ZCARD after prune: %v", err)
	}
	if after != int64(skvVerifyLiveCount) {
		t.Fatalf("SKV-VERIFY ZCARD after prune = %d, want %d (orphans dropped, live kept)", after, skvVerifyLiveCount)
	}
	members, err := rdb.ZRange(ctx, indexKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("SKV-VERIFY ZRANGE after prune: %v", err)
	}
	if len(members) != skvVerifyLiveCount {
		t.Fatalf("SKV-VERIFY ZRANGE after prune returned %d members, want %d", len(members), skvVerifyLiveCount)
	}
	live := make(map[string]struct{}, len(liveIDs))
	for _, id := range liveIDs {
		live[string(id)] = struct{}{}
	}
	for _, member := range members {
		if _, ok := live[member]; !ok {
			t.Fatalf("SKV-VERIFY prune left unexpected member %q in the index", member)
		}
	}
	skvVerifyRequireNoErrors(t, log, "first prune")
	t.Logf("SKV-VERIFY prune#1: elapsed=%s zcard_before=%d zcard_after=%d removed=%d prune_failure_logs=false",
		pruneElapsed, before, after, before-after)

	secondStart := time.Now()
	store.pruneOrphanLeaseIndex(ctx, ns, fmt.Sprintf("%d", time.Now().UnixMilli()))
	secondElapsed := time.Since(secondStart)
	afterSecond, err := rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("SKV-VERIFY ZCARD after second prune: %v", err)
	}
	if afterSecond != after {
		t.Fatalf("SKV-VERIFY second prune changed ZCARD: %d -> %d", after, afterSecond)
	}
	skvVerifyRequireNoErrors(t, log, "second prune")
	t.Logf("SKV-VERIFY prune#2 (idempotent): elapsed=%s zcard_after=%d prune_failure_logs=false",
		secondElapsed, afterSecond)
}

// skvVerifyConcurrentRegistration pins the verify step on the real backend: a
// per-execution lease ZSET that appears after the orphan EXISTS but before the
// ZREM must be re-armed, not lost with the orphan.
func skvVerifyConcurrentRegistration(t *testing.T, ctx context.Context, rdb redis.UniversalClient, store *Store, log *skvVerifyLogger, ns namespace.Namespace, written *skvVerifyWritten) {
	t.Helper()
	guard := skvVerifyGuard{t: t, ns: ns}
	const id = types.ExecutionID("zz-verify-race")
	indexKey := leaseExpiryExecIndexKey(ns)
	zkey := leaseExpiryZSetKey(ns, id)
	guard.key(indexKey)
	written.keys = append(written.keys, guard.key(zkey))

	if err := rdb.ZAdd(ctx, indexKey, redis.Z{Score: 1, Member: string(id)}).Err(); err != nil {
		t.Fatalf("SKV-VERIFY seed race member: %v", err)
	}

	hook := &skvVerifyExistsPipelineHook{key: zkey, fn: func(hookCtx context.Context) {
		if err := rdb.ZAdd(hookCtx, zkey, redis.Z{Score: 4242, Member: leaseExpiryMember(id, "run")}).Err(); err != nil {
			t.Errorf("SKV-VERIFY hook: inject concurrent lease zset: %v", err)
		}
	}}
	// AddHook is part of redis.UniversalClient, so this works for both the
	// single client and the cluster client (which applies the hook above its
	// per-node pipelines).
	rdb.AddHook(hook)

	store.pruneOrphanLeaseIndex(ctx, ns, fmt.Sprintf("%d", time.Now().UnixMilli()))

	if !hook.fired.Load() {
		t.Fatalf("SKV-VERIFY the EXISTS pipeline hook never fired; the concurrent-registration path was not exercised")
	}
	score, err := rdb.ZScore(ctx, indexKey, string(id)).Result()
	if err == redis.Nil {
		t.Fatalf("SKV-VERIFY index member %q was removed with the orphan instead of being re-armed", id)
	}
	if err != nil {
		t.Fatalf("SKV-VERIFY ZSCORE race member: %v", err)
	}
	if score != 4242 {
		t.Fatalf("SKV-VERIFY race member restored at score %v, want the exact minimum 4242", score)
	}
	skvVerifyRequireNoErrors(t, log, "verify re-arm path")
	t.Logf("SKV-VERIFY verify path: concurrent lease zset appeared after the orphan read; index member restored at score=%v", score)
}

// skvVerifySampleReadOnly inspects a real namespace's lease-expiry index
// WITHOUT writing to it. Allowed commands are exactly ZRANGE, ZCOUNT, ZCARD and
// EXISTS; the offset stays at 0 and at most skvVerifySampleCount members are
// read, so the O(offset) cost of ZRANGEBYSCORE is bounded.
func skvVerifySampleReadOnly(t *testing.T, ctx context.Context, rdb redis.UniversalClient, sampleNS namespace.Namespace) {
	t.Helper()
	indexKey := fmt.Sprintf("xflow:ns:%s:%s", sampleNS, leaseExpiryExecIndexSuffix)
	members, err := rdb.ZRange(ctx, indexKey, 0, skvVerifySampleCount-1).Result()
	if err != nil {
		t.Fatalf("SKV-VERIFY sample ZRANGE %q: %v", indexKey, err)
	}
	if len(members) > 0 {
		pipe := rdb.Pipeline()
		exists := make([]*redis.IntCmd, len(members))
		for i, member := range members {
			exists[i] = pipe.Exists(ctx, leaseExpiryZSetKey(sampleNS, types.ExecutionID(member)))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("SKV-VERIFY sample pipeline of %d EXISTS across exec hash tags: %v", len(members), err)
		}
		orphans := 0
		for _, cmd := range exists {
			if cmd.Val() == 0 {
				orphans++
			}
		}
		card, err := rdb.ZCard(ctx, indexKey).Result()
		if err != nil {
			t.Fatalf("SKV-VERIFY sample ZCARD %q: %v", indexKey, err)
		}
		count, err := rdb.ZCount(ctx, indexKey, "(0", fmt.Sprintf("%d", time.Now().UnixMilli())).Result()
		if err != nil {
			t.Fatalf("SKV-VERIFY sample ZCOUNT %q: %v", indexKey, err)
		}
		t.Logf("SKV-VERIFY sample (read-only, ns=%q): sampled=%d orphans=%d orphan_ratio=%.4f zcard=%d zcount_(0,now]=%d",
			sampleNS, len(members), orphans, float64(orphans)/float64(len(members)), card, count)
		return
	}
	card, err := rdb.ZCard(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("SKV-VERIFY sample ZCARD %q: %v", indexKey, err)
	}
	t.Logf("SKV-VERIFY sample (read-only, ns=%q): sampled=0 zcard=%d", sampleNS, card)
}
