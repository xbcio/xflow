package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestMiniredisBatchOutputContract runs the shared batch-output contract
// against the Redis backend. The backend is the reason the capability exists:
// every name is a GET, so a fan-in of F edges cost F round trips.
func TestMiniredisBatchOutputContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statestoretest.RunBatchOutputContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.BatchOutputBackend {
		return statestoretest.BatchOutputBackend{Store: New(rdb, nil, time.Minute), Namespaced: true}
	})
}

// TestRedisBatchOutputContract runs the same contract against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set. MGET is an ordinary command, but this backend's
// output values may be zstd frames and its keys carry a hash tag, so the real
// server is worth one run.
func TestRedisBatchOutputContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunBatchOutputContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.BatchOutputBackend {
		return statestoretest.BatchOutputBackend{Store: New(freshRealRedis(t, addr), nil, time.Minute), Namespaced: true}
	})
}

// mgetCapture is a go-redis hook that records the MGET commands a store issues
// and the keys they carry, plus how many single GETs it issued. Round-trip
// assertions cannot distinguish one MGET from N GETs by outcome; only the
// command stream can.
type mgetCapture struct {
	mu     sync.Mutex
	mgets  [][]string
	single int
}

func (h *mgetCapture) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *mgetCapture) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.record(cmd)
		return next(ctx, cmd)
	}
}

func (h *mgetCapture) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *mgetCapture) record(cmd redis.Cmder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch strings.ToLower(cmd.Name()) {
	case "mget":
		args := cmd.Args()
		keys := make([]string, 0, len(args)-1)
		for _, arg := range args[1:] {
			keys = append(keys, fmt.Sprint(arg))
		}
		h.mgets = append(h.mgets, keys)
	case "get":
		h.single++
	}
}

// reset clears the recording so a test observes only the reads it triggers.
func (h *mgetCapture) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mgets = nil
	h.single = 0
}

func (h *mgetCapture) snapshot() ([][]string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]string(nil), h.mgets...), h.single
}

// TestGetOutputsIssuesOneMGet pins the round-trip budget: three distinct names
// (with one repeated) are one MGET carrying three keys and zero GETs, and every
// key keeps the execution's {id} hash tag that makes the batch one Cluster slot.
func TestGetOutputsIssuesOneMGet(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	hook := &mgetCapture{}
	rdb.AddHook(hook)

	ctx := context.Background()
	id := types.ExecutionID("exec-batch-mget")
	for name, value := range map[string]string{"alpha": "a", "beta": "b"} {
		if err := state.PutOutput(ctx, id, name, map[string]any{"value": value}); err != nil {
			t.Fatalf("PutOutput(%q) error = %v", name, err)
		}
	}
	hook.reset()

	got, err := state.GetOutputs(ctx, id, []string{"alpha", "beta", "alpha", "missing"})
	if err != nil {
		t.Fatalf("GetOutputs() error = %v", err)
	}
	if len(got) != 2 || got["alpha"]["value"] != "a" || got["beta"]["value"] != "b" {
		t.Fatalf("GetOutputs() = %#v, want alpha=a and beta=b", got)
	}

	mgets, single := hook.snapshot()
	if len(mgets) != 1 {
		t.Fatalf("issued %d MGET commands, want exactly 1:\n%v", len(mgets), mgets)
	}
	if single != 0 {
		t.Fatalf("issued %d single GET commands, want 0 — the batch read must not fall back "+
			"to per-name reads when the capability is implemented", single)
	}
	keys := mgets[0]
	if len(keys) != 3 {
		t.Fatalf("MGET carried %d keys %v, want 3 (alpha, beta, missing; the duplicate alpha is "+
			"read once)", len(keys), keys)
	}
	tag := "{" + string(id) + "}"
	for _, key := range keys {
		if !strings.Contains(key, tag) {
			t.Errorf("MGET key %q does not carry the execution hash tag %q; without the shared "+
				"tag a Cluster client cannot serve the batch as one command", key, tag)
		}
	}
}

// TestGetOutputsNamesTheCorruptOutput pins the error contract: a value that
// cannot be decoded fails the whole batch and names which output was corrupt,
// so an operator can go straight to the bad key.
func TestGetOutputsNamesTheCorruptOutput(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("exec-batch-corrupt")
	if err := state.PutOutput(ctx, id, "good", map[string]any{"ok": true}); err != nil {
		t.Fatalf("PutOutput(good) error = %v", err)
	}
	// Write garbage straight to the output key; PutOutput always encodes valid
	// JSON, so this is the only way to reach the decode-failure branch.
	if err := rdb.Set(ctx, outputKey(namespace.FromContext(ctx), id, "corrupt"), "{not-json", 0).Err(); err != nil {
		t.Fatalf("set corrupt output: %v", err)
	}

	_, err := state.GetOutputs(ctx, id, []string{"good", "corrupt"})
	if err == nil {
		t.Fatal("GetOutputs() error = nil for a corrupt output; a partial result would let the " +
			"node run on a silently truncated fan-in")
	}
	if !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("GetOutputs() error = %v, want it to name the corrupt output", err)
	}
}

// TestGetOutputsDecodesCompressedValues pins that the batch path shares the
// per-name codec: a zstd-compressed output (what large captured payloads become
// when compression is enabled) decodes through MGET exactly as through GET.
func TestGetOutputsDecodesCompressedValues(t *testing.T) {
	state, _ := testStoreWithRedis(t)
	state.ConfigureOutputCompression(true)
	ctx := context.Background()
	id := types.ExecutionID("exec-batch-compressed")
	want := capturedBatchOutput(150)
	if err := state.PutOutput(ctx, id, "collect", want); err != nil {
		t.Fatalf("PutOutput: %v", err)
	}

	got, err := state.GetOutputs(ctx, id, []string{"collect"})
	if err != nil {
		t.Fatalf("GetOutputs: %v", err)
	}
	gotJSON, err := json.Marshal(got["collect"])
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatal("GetOutputs returned a different payload than PutOutput stored; the batch read " +
			"must decode the same compressed frames GetOutput does")
	}
}
