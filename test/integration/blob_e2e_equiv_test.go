//go:build integration

package integration

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// blobE2EExecute assembles a run, serves it and drives one execution to its end.
func blobE2EExecute(t *testing.T, addr string, blobOn bool, batchSize int) (*blobE2ERun, blobE2EOutcome) {
	t.Helper()
	r := newBlobE2ERun(t, addr, blobOn)
	r.serve(t)
	return r, r.run(t, batchSize)
}

// blobE2EExpected is the map node's results as the workflow defines them: item
// n (1-based) yields {id: n, sq: n*n}, in item order.
func blobE2EExpected() []any {
	out := make([]any, blobE2EItems)
	for i := range out {
		n := float64(i + 1)
		out[i] = map[string]any{"id": n, "sq": n * n}
	}
	return out
}

// blobE2ENormalize round-trips v through JSON so two outputs compare by the
// values a client would read, whatever Go types the stores produced.
func blobE2ENormalize(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// blobE2EAssertSuccess checks the execution succeeded, the map node reports all
// its items, and its results are exactly the expected ones in item order.
func blobE2EAssertSuccess(t *testing.T, out blobE2EOutcome) {
	t.Helper()
	if out.res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success; fan node %s: %q", out.res.Status, out.fan.Status, out.fan.Error)
	}
	fan, _ := blobE2ENormalize(t, out.res.Output["fan"]).(map[string]any)
	if fan["count"] != float64(blobE2EItems) {
		t.Fatalf("fan count = %v, want %d", fan["count"], blobE2EItems)
	}
	if got := fan["results"]; !reflect.DeepEqual(got, blobE2EExpected()) {
		t.Fatalf("fan results = %v\nwant %v", got, blobE2EExpected())
	}
}

// blobE2EPayloads returns every outbox body and queue payload the run captured,
// as text, and fails the test if the capture itself failed.
func blobE2EPayloads(t *testing.T, r *blobE2ERun) (bodies, queued []string) {
	t.Helper()
	b, q, errs := r.cap.snapshot()
	if len(errs) > 0 {
		t.Fatalf("capture problems: %v", errs)
	}
	for _, v := range b {
		bodies = append(bodies, string(v))
	}
	for _, v := range q {
		queued = append(queued, string(v))
	}
	return bodies, queued
}

// blobE2EAssertBlobOff proves a run with writing off never touched the blob
// layer, and that the scan it relies on does see the items: the batch payloads
// carry the whole array inline and the marker is found in Redis.
func blobE2EAssertBlobOff(t *testing.T, r *blobE2ERun) {
	t.Helper()
	if puts, _, _ := r.store.counts(); puts != 0 {
		t.Errorf("blob puts = %d, want 0 with blob off", puts)
	}
	if objs := blobE2EObjects(r.dir); len(objs) != 0 {
		t.Errorf("blob objects = %v, want none with blob off", objs)
	}
	bodies, queued := blobE2EPayloads(t, r)
	for _, p := range append(bodies, queued...) {
		if strings.Contains(p, "items_ref") {
			t.Fatalf("a payload carries items_ref with blob off: %.200s", p)
		}
	}
	if !blobE2EAnyContains(queued, "all_items") || !blobE2EAnyContains(queued, blobE2EMarker) {
		t.Errorf("queue payloads do not carry the items inline with blob off")
	}
	r.mu.Lock()
	holders := len(r.holders)
	r.mu.Unlock()
	if holders == 0 {
		t.Errorf("no Redis key held the items marker with blob off: the scan cannot see the items")
	}
}

func blobE2EAnyContains(list []string, sub string) bool {
	for _, s := range list {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// blobE2EAssertBlobOn proves the items went through the blob layer and nowhere
// else: the store holds an object, every batch payload carries items_ref and
// neither the payloads nor any Redis key contain the large-array marker.
func blobE2EAssertBlobOn(t *testing.T, r *blobE2ERun, batches int) {
	t.Helper()
	if puts, _, _ := r.store.counts(); puts == 0 {
		t.Errorf("blob puts = 0, want the items stored with blob on")
	}
	if objs := blobE2EObjects(r.dir); len(objs) != 1 {
		t.Errorf("blob objects = %v, want exactly one (the items array, stored once)", objs)
	}
	bodies, queued := blobE2EPayloads(t, r)
	for _, p := range append(bodies, queued...) {
		if strings.Contains(p, blobE2EMarker) || strings.Contains(p, "all_items") {
			t.Fatalf("a payload carries the items inline with blob on: %.200s", p)
		}
	}
	if n := blobE2ECountBatches(queued, "items_ref"); n != batches {
		t.Errorf("queue payloads with items_ref = %d, want %d batch tasks", n, batches)
	}
	if n := blobE2ECountBatches(bodies, "items_ref"); n != batches {
		t.Errorf("outbox bodies with items_ref = %d, want %d batch tasks", n, batches)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.probes == 0 {
		t.Errorf("no batch task was probed, so the Redis scan proved nothing")
	}
	if len(r.holders) != 0 {
		t.Errorf("Redis keys held the items marker with blob on: %v", r.holders)
	}
}

// blobE2ECountBatches counts the batch payloads in list that contain sub.
func blobE2ECountBatches(list []string, sub string) int {
	n := 0
	for _, s := range list {
		if strings.Contains(s, `"_batch_exec"`) && strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

// blobE2EEquivalence runs the workflow with blob off and with blob on at the
// same batch size and requires the two executions to end identically: the same
// status and the same output for every node, the map's results included.
func blobE2EEquivalence(t *testing.T, batchSize int) {
	t.Helper()
	addr := requireRedis(t)
	off, offOut := blobE2EExecute(t, addr, false, batchSize)
	on, onOut := blobE2EExecute(t, addr, true, batchSize)
	blobE2EAssertSuccess(t, offOut)
	blobE2EAssertSuccess(t, onOut)
	if offOut.res.Status != onOut.res.Status || offOut.fan.Status != onOut.fan.Status {
		t.Errorf("status differs: off %s/%s, on %s/%s", offOut.res.Status, offOut.fan.Status, onOut.res.Status, onOut.fan.Status)
	}
	for _, node := range blobE2ENodes {
		a := blobE2ENormalize(t, offOut.res.Output[node])
		b := blobE2ENormalize(t, onOut.res.Output[node])
		if !reflect.DeepEqual(a, b) {
			t.Errorf("output of %q differs:\n off %v\n on  %v", node, a, b)
		}
	}
	batches := (blobE2EItems + batchSize - 1) / batchSize
	blobE2EAssertBlobOff(t, off)
	blobE2EAssertBlobOn(t, on, batches)
}

// TestBlobE2EEquivalenceSingleBatch: the whole array is one batch.
func TestBlobE2EEquivalenceSingleBatch(t *testing.T) {
	blobE2EEquivalence(t, blobE2EItems)
}

// TestBlobE2EEquivalenceMultiBatch: 30 items in batches of 7 are five windows
// (7, 7, 7, 7, 2) of one stored array, and the results must reassemble in item
// order, which is what the expected-results comparison checks.
func TestBlobE2EEquivalenceMultiBatch(t *testing.T) {
	blobE2EEquivalence(t, 7)
}
