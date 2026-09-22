package rstate

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// testStoreWithRedis builds a Store over a throwaway miniredis. Node outputs go
// through PutOutput/GetOutput, which use a plain SET and GET rather than Lua, so
// these tests need no script support.
func testStoreWithRedis(t *testing.T) (*Store, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, nil, time.Hour), rdb
}

// capturedBatchOutput mimics the shape this feature exists for: a node output
// carrying a batch of captured HTTP exchanges. The redundancy is the point --
// repeated header names, repeated JSON keys, near-identical records -- so it
// compresses the way the real values measured in production do.
func capturedBatchOutput(items int) map[string]any {
	results := make([]any, 0, items)
	for i := 0; i < items; i++ {
		results = append(results, map[string]any{
			"req": map[string]any{
				"method":  "POST",
				"url":     fmt.Sprintf("https://api.example.com/v1/orders/%d?trace=%d", i, i),
				"host":    "api.example.com",
				"headers": map[string]any{"Content-Type": "application/json", "X-Trace-Id": fmt.Sprintf("trace-%d", i)},
				"body":    map[string]any{"order_id": i, "status": "paid", "amount": 1999, "currency": "CNY"},
			},
			"resp": map[string]any{
				"status_code": 200,
				"headers":     map[string]any{"Content-Type": "application/json", "Server": "nginx"},
				"body":        map[string]any{"code": 0, "message": "ok", "data": map[string]any{"order_id": i}},
			},
		})
	}
	return map[string]any{"results": results, "count": items}
}

// TestOutputCompressionShrinksTheStoredValueAndRoundTrips is the test that fails
// if compression silently stops happening.
//
// It asserts the value in Redis itself -- that the bytes are a zstd frame and
// that they are materially smaller than the JSON they replace -- rather than
// only asserting the round trip. A round-trip assertion alone would pass just as
// well with the feature switched off, which is exactly the failure mode worth
// guarding: the switch can be wired up wrong, or dropped by a refactor, and
// every read would still return the right map while the keyspace kept growing.
func TestOutputCompressionShrinksTheStoredValueAndRoundTrips(t *testing.T) {
	state, rdb := testStoreWithRedis(t)
	state.ConfigureOutputCompression(true)

	ctx := context.Background()
	id := types.ExecutionID("exec-compressed")
	want := capturedBatchOutput(150)

	if err := state.PutOutput(ctx, id, "collect", want); err != nil {
		t.Fatalf("PutOutput: %v", err)
	}

	plain, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	tg := namespace.FromContext(ctx)
	stored, err := rdb.Get(ctx, outputKey(tg, id, "collect")).Bytes()
	if err != nil {
		t.Fatalf("read stored value: %v", err)
	}

	if !bytes.HasPrefix(stored, zstdFrameMagic) {
		t.Fatalf("stored value is not a zstd frame (starts %q): compression is enabled "+
			"but the value was written verbatim, so the keyspace keeps its full size",
			string(stored[:min(16, len(stored))]))
	}
	if len(stored) >= len(plain) {
		t.Errorf("stored %d bytes vs %d plain -- compression must not be used when it "+
			"does not shrink the value", len(stored), len(plain))
	}
	t.Logf("  %d B plain -> %d B stored (%.2fx, %.1f%% of original)",
		len(plain), len(stored), float64(len(plain))/float64(len(stored)),
		float64(len(stored))/float64(len(plain))*100)

	got, err := state.GetOutput(ctx, id, "collect")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	gotJSON, _ := json.Marshal(got)
	if !bytes.Equal(gotJSON, plain) {
		t.Errorf("round trip changed the value: got %d bytes, want %d", len(gotJSON), len(plain))
	}
}

// TestOutputCompressionReadsValuesWrittenBeforeItWasEnabled covers the rolling
// upgrade, and it is the reason the read path ignores the switch.
//
// A deployment enables compression only after every process can decode it. Until
// then -- and for every value already in Redis at that moment -- the keyspace
// holds plain JSON while writers may be emitting frames. A reader that trusted
// its own configuration instead of looking at the bytes would fail every read of
// the other form: the plain values would be fed to the decoder, or the frames to
// json.Unmarshal. Both are data loss on a live execution, so this test writes
// under one setting and reads under the other in both directions.
func TestOutputCompressionReadsValuesWrittenBeforeItWasEnabled(t *testing.T) {
	state, _ := testStoreWithRedis(t)
	ctx := context.Background()

	// Written by a process that predates the feature, or by this one before the
	// switch was flipped.
	state.ConfigureOutputCompression(false)
	oldID := types.ExecutionID("exec-written-plain")
	oldValue := capturedBatchOutput(40)
	if err := state.PutOutput(ctx, oldID, "collect", oldValue); err != nil {
		t.Fatalf("PutOutput (plain): %v", err)
	}

	// The switch is flipped. The value already in Redis must still be readable.
	state.ConfigureOutputCompression(true)
	got, err := state.GetOutput(ctx, oldID, "collect")
	if err != nil {
		t.Fatalf("GetOutput of a value written before compression was enabled: %v", err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(oldValue)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Error("a pre-existing plain value did not read back identically once " +
			"compression was enabled; enabling the switch must not strand the keyspace")
	}

	// And the other direction: a value written compressed must stay readable
	// after the switch is turned back off, so disabling it is not a migration.
	newID := types.ExecutionID("exec-written-compressed")
	newValue := capturedBatchOutput(40)
	if err := state.PutOutput(ctx, newID, "collect", newValue); err != nil {
		t.Fatalf("PutOutput (compressed): %v", err)
	}

	state.ConfigureOutputCompression(false)
	got, err = state.GetOutput(ctx, newID, "collect")
	if err != nil {
		t.Fatalf("GetOutput of a compressed value after compression was disabled: %v", err)
	}
	gotJSON, _ = json.Marshal(got)
	wantJSON, _ = json.Marshal(newValue)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Error("a compressed value did not read back identically once compression " +
			"was disabled; turning the switch off must be a no-op, not a data loss")
	}
}

// TestOutputCompressionLeavesSmallValuesAlone pins the size floor.
//
// Most node outputs are small -- a status, a count, a single scalar. A zstd frame
// has a header and a checksum of its own, so compressing those costs CPU and can
// make the value larger. They must be stored as plain JSON, which also keeps them
// readable by the miniredis tooling an operator reaches for.
func TestOutputCompressionLeavesSmallValuesAlone(t *testing.T) {
	state, rdb := testStoreWithRedis(t)
	state.ConfigureOutputCompression(true)

	ctx := context.Background()
	id := types.ExecutionID("exec-small")
	small := map[string]any{"count": 1, "ok": true}

	if err := state.PutOutput(ctx, id, "tiny", small); err != nil {
		t.Fatalf("PutOutput: %v", err)
	}

	tg := namespace.FromContext(ctx)
	stored, err := rdb.Get(ctx, outputKey(tg, id, "tiny")).Bytes()
	if err != nil {
		t.Fatalf("read stored value: %v", err)
	}
	if bytes.HasPrefix(stored, zstdFrameMagic) {
		t.Errorf("a %d-byte value was compressed; values below %d bytes must be "+
			"stored verbatim", len(stored), outputCompressMinBytes)
	}

	// Byte-identical to plain JSON, not merely equal once parsed.
	want, _ := json.Marshal(small)
	if !bytes.Equal(stored, want) {
		t.Errorf("stored %q, want the plain JSON %q", stored, want)
	}
}

// TestOutputCompressionNeverEnlargesAValue pins the fallback for data that does
// not compress.
//
// A node output is arbitrary captured traffic, so "compressible" cannot be
// assumed. zstd output can exceed its input, and storing a larger value would
// make the feature a pessimisation on exactly the payloads -- random, encrypted
// or already-compressed bodies -- that are hardest to notice. Here the payload is
// random bytes, which json.Marshal renders as base64: base64 of random data has
// no exploitable redundancy, so this is the incompressible case.
func TestOutputCompressionNeverEnlargesAValue(t *testing.T) {
	state, rdb := testStoreWithRedis(t)
	state.ConfigureOutputCompression(true)

	random := make([]byte, 64<<10)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"body": random}

	ctx := context.Background()
	id := types.ExecutionID("exec-incompressible")
	if err := state.PutOutput(ctx, id, "collect", value); err != nil {
		t.Fatalf("PutOutput: %v", err)
	}

	plain, _ := json.Marshal(value)
	tg := namespace.FromContext(ctx)
	stored, err := rdb.Get(ctx, outputKey(tg, id, "collect")).Bytes()
	if err != nil {
		t.Fatalf("read stored value: %v", err)
	}
	if len(stored) > len(plain) {
		t.Errorf("stored %d bytes for a %d-byte value: an incompressible payload must "+
			"fall back to plain JSON rather than grow", len(stored), len(plain))
	}
	t.Logf("  incompressible: %d B plain -> %d B stored (compressed=%v)",
		len(plain), len(stored), bytes.HasPrefix(stored, zstdFrameMagic))

	got, err := state.GetOutput(ctx, id, "collect")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	if got["body"] == nil {
		t.Error("round trip lost the body")
	}
}

// TestOutputCompressionSurvivesTheCommitPath closes the gap between "the codec
// works" and "a real commit is compressed and still readable".
//
// PutOutput is the store's direct write API, but the engine does not use it for
// node results: a node's output is committed through CommitNode, which hands the
// bytes to a Lua script as an argument. That hop is the one place this feature
// can corrupt data rather than merely fail, because a zstd frame is arbitrary
// bytes -- it contains NULs and invalid UTF-8 by construction -- and a layer that
// treated the payload as a text string would silently truncate or mangle it. The
// assertion on the frame header below is what proves the bytes arrived intact
// and were stored as written; the round trip alone would not, since a mangled
// frame usually fails loudly while a truncated one need not.
//
// It also pins that the decompression happens on the store side only: the value
// the engine reads back is the plain map, so no node handler ever sees a frame.
func TestOutputCompressionSurvivesTheCommitPath(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	state.ConfigureOutputCompression(true)

	ctx := context.Background()
	id := types.ExecutionID("exec-compressed-commit")
	g := privateRedisOutputGraph(t, "collect")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	idx, ok := g.NodeIndex("collect")
	if !ok {
		t.Fatal("collect node missing from graph")
	}
	lease := privateRedisLease(id, "collect", idx, "lease-collect", "token-collect")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1

	want := capturedBatchOutput(150)
	result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "collect",
		NodeIdx:     idx,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     lease.Attempt,
		Status:      types.NodeStatusSuccess,
		Output:      want,
		StoreOutput: true,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !result.Applied {
		t.Fatalf("CommitNode() = %+v, want an applied commit", result)
	}

	tg := namespace.FromContext(ctx)
	stored, err := rdb.Get(ctx, outputKey(tg, id, "collect")).Bytes()
	if err != nil {
		t.Fatalf("read committed output: %v", err)
	}
	plain, _ := json.Marshal(want)
	if !bytes.HasPrefix(stored, zstdFrameMagic) {
		t.Fatalf("the committed output is not a zstd frame (starts %q): either "+
			"compression did not run, or the Lua hop mangled the frame",
			string(stored[:min(16, len(stored))]))
	}
	if len(stored) >= len(plain) {
		t.Errorf("committed %d bytes vs %d plain", len(stored), len(plain))
	}
	t.Logf("  commit path: %d B plain -> %d B stored (%.1f%%)",
		len(plain), len(stored), float64(len(stored))/float64(len(plain))*100)

	// The engine's read of that node must be the plain map again.
	got, err := state.GetOutput(ctx, id, "collect")
	if err != nil {
		t.Fatalf("GetOutput() after a compressed commit: %v", err)
	}
	gotJSON, _ := json.Marshal(got)
	if !bytes.Equal(gotJSON, plain) {
		t.Errorf("a compressed commit did not read back identically: got %d bytes, "+
			"want %d -- compression must be invisible above the store", len(gotJSON), len(plain))
	}
}

// TestOutputCompressionOnRealCapturedPayloads measures the real ratio.
//
// Every other test here builds its input, and a built input is exactly the wrong
// thing to size a compression feature with: this feature's whole value is a ratio,
// and a ratio is a property of the data. A fixture with repeated header names and
// near-identical records compresses far better than real captured traffic, so a
// fixture-derived number overstates the benefit and would be the basis for a
// capacity decision.
//
// This test therefore reads payloads captured from a live pipeline, pointed at by
// XFLOW_REAL_PAYLOAD_DIR. It is skipped when that is unset, so it never blocks a
// normal run — but when the directory is given, it asserts the round trip on that
// real data and REPORTS the measured ratio rather than asserting a threshold: the
// number is a property of the corpus, and a threshold would fail on a different
// one for no reason.
func TestOutputCompressionOnRealCapturedPayloads(t *testing.T) {
	dir := os.Getenv("XFLOW_REAL_PAYLOAD_DIR")
	if dir == "" {
		t.Skip("XFLOW_REAL_PAYLOAD_DIR not set; skipping real-payload measurement")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	state, _ := testStoreWithRedis(t)
	state.ConfigureOutputCompression(true)

	var totalPlain, totalCompressed int
	var encodeTotal, decodeTotal time.Duration
	measured := 0

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var value map[string]any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatalf("%s is not a JSON object: %v", e.Name(), err)
		}
		// Marshal the parsed map, not the file: the store's input is the map, and
		// the file's own formatting is not what it would hold.
		plain, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		encoded, err := state.encodeOutputValue(value)
		encodeTotal += time.Since(start)
		if err != nil {
			t.Fatalf("encodeOutputValue(%s): %v", e.Name(), err)
		}

		start = time.Now()
		decoded, err := state.decodeOutputValue([]byte(encoded))
		decodeTotal += time.Since(start)
		if err != nil {
			t.Fatalf("decodeOutputValue(%s): %v", e.Name(), err)
		}

		got, err := json.Marshal(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("%s did not survive the round trip: %d bytes in, %d bytes out",
				e.Name(), len(plain), len(got))
		}
		if !bytes.HasPrefix([]byte(encoded), zstdFrameMagic) {
			t.Errorf("%s (%d B) was stored uncompressed; a payload this size is "+
				"exactly what the feature exists for", e.Name(), len(plain))
		}

		t.Logf("  %-18s %9d B -> %8d B  %5.2fx",
			e.Name(), len(plain), len(encoded), float64(len(plain))/float64(len(encoded)))
		totalPlain += len(plain)
		totalCompressed += len(encoded)
		measured++
	}

	if measured == 0 {
		t.Fatalf("no .json payloads found in %s", dir)
	}
	if totalCompressed == 0 {
		t.Fatal("compressed total is zero")
	}
	t.Logf("  %d real payloads: %d B -> %d B  (%.2fx, %.1f%% of original), "+
		"encode %v total, decode %v total",
		measured, totalPlain, totalCompressed,
		float64(totalPlain)/float64(totalCompressed),
		float64(totalCompressed)/float64(totalPlain)*100,
		encodeTotal.Round(time.Microsecond), decodeTotal.Round(time.Microsecond))
}

// TestOutputCompressionOffIsByteForByteTheOldBehaviour pins that the feature is
// inert when disabled, so the default deployment writes exactly what it wrote
// before and the change cannot regress an un-upgraded fleet.
func TestOutputCompressionOffIsByteForByteTheOldBehaviour(t *testing.T) {
	state, rdb := testStoreWithRedis(t)

	ctx := context.Background()
	id := types.ExecutionID("exec-default-off")
	value := capturedBatchOutput(150)

	if err := state.PutOutput(ctx, id, "collect", value); err != nil {
		t.Fatalf("PutOutput: %v", err)
	}

	tg := namespace.FromContext(ctx)
	stored, err := rdb.Get(ctx, outputKey(tg, id, "collect")).Bytes()
	if err != nil {
		t.Fatalf("read stored value: %v", err)
	}
	want, _ := json.Marshal(value)
	if !bytes.Equal(stored, want) {
		t.Errorf("with compression off the stored value must be the plain JSON the "+
			"previous implementation wrote; got %d bytes, want %d", len(stored), len(want))
	}
	if strings.HasPrefix(string(stored), string(zstdFrameMagic)) {
		t.Error("compression ran with the switch off")
	}
}
