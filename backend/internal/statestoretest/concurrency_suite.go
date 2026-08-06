//go:build concurrency

// Package statestoretest — concurrency suite for engine.StateStore
// implementations.
//
// The cases below stress the atomicity guarantees that the distributed backend
// realizes via Lua scripts and the memory backend realizes via mutex
// discipline. Default `go test ./...` skips these cases; run them with
// `make test-concurrency` (build tag `concurrency`, -race, -count=3).
//
// Spec: .claude/specs/lua-concurrency-tests.md
package statestoretest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// StoreFactory returns a fresh StateStore for one sub-test. The factory must
// also register a cleanup with t.Cleanup so the underlying resource (in-memory
// state, miniredis instance, etc.) is released.
type StoreFactory func(t *testing.T) engine.StateStore

// RunStateStoreConcurrencySuite executes every concurrent contract case
// against the given factory. The factory is called once per sub-test, so
// shared state cannot leak across cases.
func RunStateStoreConcurrencySuite(t *testing.T, newStore StoreFactory) {
	t.Helper()
	t.Run("DecrementInDegreeAtomic", func(t *testing.T) {
		decrementInDegreeAtomicCase(t, newStore(t))
	})
	t.Run("ClaimTaskLeaseSingleWinner", func(t *testing.T) {
		claimTaskLeaseSingleWinnerCase(t, newStore(t))
	})
	t.Run("SuspendThenDeliverSignal", func(t *testing.T) {
		suspendThenDeliverSignalCase(t, newStore(t))
	})
	t.Run("RevokeSignalIdempotent", func(t *testing.T) {
		revokeSignalIdempotentCase(t, newStore(t))
	})
	t.Run("CheckCompletionConvergence", func(t *testing.T) {
		checkCompletionConvergenceCase(t, newStore(t))
	})
}

// barrier returns a start-fn and a wait-fn that together implement a
// many-goroutines-release-at-once primitive: every worker calls wait() at the
// top, then start() unblocks them simultaneously. Maximizes collisions
// compared to scheduling each goroutine separately.
func barrier(n int) (start func(), wait func()) {
	gate := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(n)
	start = func() {
		close(gate)
		wg.Wait()
	}
	wait = func() {
		<-gate
		wg.Done()
	}
	return
}

// linearGraph compiles a graph with `width` nodes where node 0 fans out to
// every other node (so each downstream has in-degree 1).
// Returns the compiled graph; panics on compile failure (used only by tests).
func linearGraph(width int) *graph.Graph {
	defNodes := make([]types.NodeDef, 0, width)
	conns := make(types.Connections)
	defNodes = append(defNodes, types.NodeDef{Name: "n0", Type: "test.fanout"})
	for i := 1; i < width; i++ {
		name := fmt.Sprintf("n%d", i)
		defNodes = append(defNodes, types.NodeDef{Name: name, Type: "test.echo"})
		if conns["n0"] == nil {
			conns["n0"] = make(map[string]types.PortConnections)
		}
		// A map index expression yields a non-addressable struct, so the slice
		// has to be lifted out, appended to, and written back.
		pc := conns["n0"]["main"]
		pc.Targets = append(pc.Targets, types.Connection{Node: name, Input: "main"})
		conns["n0"]["main"] = pc
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name:        "concurrency",
		Nodes:       defNodes,
		Connections: conns,
	})
	if err != nil {
		panic(err)
	}
	return g
}

// inDegreeGraph compiles a graph whose target node has in-degree equal to
// `inDeg`. Used by DecrementInDegreeAtomic to seed counters.
func inDegreeGraph(inDeg int) *graph.Graph {
	defNodes := []types.NodeDef{{Name: "target", Type: "test.echo"}}
	conns := make(types.Connections)
	for i := 0; i < inDeg; i++ {
		src := fmt.Sprintf("src%d", i)
		defNodes = append(defNodes, types.NodeDef{Name: src, Type: "test.fanout"})
		if conns[src] == nil {
			conns[src] = map[string]types.PortConnections{}
		}
		conns[src]["main"] = types.PortConnections{Targets: []types.Connection{{Node: "target", Input: "main"}}}
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name:        "in-degree",
		Nodes:       defNodes,
		Connections: conns,
	})
	if err != nil {
		panic(err)
	}
	return g
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

func decrementInDegreeAtomicCase(t *testing.T, state engine.StateStore) {
	const inDeg = 64
	ctx := context.Background()
	id := types.ExecutionID("dec-in-deg")
	g := inDegreeGraph(inDeg)

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	targetIdx, _ := g.NodeIndex("target")

	// Each goroutine performs exactly one decrement with portActive=true.
	// Atomicity expectations:
	//  - sum of all observed `remainingInDeg` values is the triangular number
	//    (inDeg-1 + inDeg-2 + ... + 0) = inDeg*(inDeg-1)/2 — i.e. no lost
	//    updates, no double-decrements.
	//  - exactly one caller observes remainingInDeg == 0
	//  - all callers observe arrivedActiveIn between 1 and inDeg, each value
	//    appears exactly once
	start, wait := barrier(inDeg)
	results := make(chan [2]int, inDeg)
	for i := 0; i < inDeg; i++ {
		go func() {
			wait()
			remaining, active, err := state.DecrementInDegree(ctx, id, targetIdx, true)
			if err != nil {
				t.Errorf("DecrementInDegree() error = %v", err)
				results <- [2]int{-1, -1}
				return
			}
			results <- [2]int{remaining, active}
		}()
	}
	start()

	var sumRemaining int
	zeros := 0
	seenActive := make(map[int]int)
	for i := 0; i < inDeg; i++ {
		r := <-results
		if r[0] < 0 {
			t.Fatalf("decrement reported error in goroutine")
		}
		sumRemaining += r[0]
		if r[0] == 0 {
			zeros++
		}
		seenActive[r[1]]++
	}
	want := inDeg * (inDeg - 1) / 2
	if sumRemaining != want {
		t.Fatalf("sum of remainingInDeg = %d, want %d (lost or duplicated decrements)", sumRemaining, want)
	}
	if zeros != 1 {
		t.Fatalf("expected exactly one caller to observe remaining==0, got %d", zeros)
	}
	if len(seenActive) != inDeg {
		t.Fatalf("expected %d distinct arrivedActiveIn values, got %d (atomicity break)", inDeg, len(seenActive))
	}
}

func claimTaskLeaseSingleWinnerCase(t *testing.T, state engine.StateStore) {
	const racers = 24
	ctx := context.Background()
	id := types.ExecutionID("lease-winner")
	g := linearGraph(2)
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	// Seed the node as running with a known lease token. ClaimTaskLease should
	// only let one racer transition it to committing; the winning token remains
	// stored so a crash in that state can be fenced and reclaimed.
	token := engine.LeaseToken("lease-tok-1")
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id, Name: "n1", NodeIdx: 1,
		Status: types.NodeStatusRunning, LeaseToken: token,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	start, wait := barrier(racers)
	var wins int64
	results := make(chan bool, racers)
	for i := 0; i < racers; i++ {
		go func() {
			wait()
			_, valid, err := state.ClaimTaskLease(ctx, &engine.TaskLease{
				LeaseToken: token,
				Task: engine.Task{
					ExecutionID: id, NodeName: "n1", NodeIdx: 1,
				},
			})
			if err != nil {
				t.Errorf("ClaimTaskLease() error = %v", err)
			}
			if valid {
				atomic.AddInt64(&wins, 1)
			}
			results <- valid
		}()
	}
	start()
	for i := 0; i < racers; i++ {
		<-results
	}
	if got := atomic.LoadInt64(&wins); got != 1 {
		t.Fatalf("ClaimTaskLease winners = %d, want 1", got)
	}
}

func suspendThenDeliverSignalCase(t *testing.T, state engine.StateStore) {
	const pairs = 32
	ctx := context.Background()
	id := types.ExecutionID("suspend-deliver")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: linearGraph(2), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Each pair has its own node name + signal name, so cross-talk between
	// pairs is impossible. The race is purely between the suspend and the
	// deliver for the *same* signal name. After both run, every suspend must
	// have observed its signal exactly once: either via SuspendOrConsume's
	// returned payload (deliver landed first) or via DeliverSignal's
	// returned resume node + payload (suspend landed first).
	type result struct {
		nodeName  string
		consumed  bool // SuspendOrConsume saw the payload itself
		delivered bool // DeliverSignal returned this node
	}
	results := make(chan result, pairs*2)
	start, wait := barrier(pairs * 2)

	for i := 0; i < pairs; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		sigName := fmt.Sprintf("sig-%d", i)
		go func(node, sig string) {
			wait()
			spec := &types.SuspendSpec{
				Mode:    node1Mode,
				Signals: []string{sig},
			}
			payload, err := state.SuspendOrConsume(ctx, id, node, spec)
			if err != nil {
				t.Errorf("SuspendOrConsume() error = %v", err)
			}
			results <- result{nodeName: node, consumed: payload != nil}
		}(nodeName, sigName)
		go func(node, sig string) {
			wait()
			resume, _, err := state.DeliverSignal(ctx, id, sig, map[string]any{"k": sig})
			if err != nil {
				t.Errorf("DeliverSignal() error = %v", err)
			}
			results <- result{nodeName: node, delivered: resume == node}
		}(nodeName, sigName)
	}
	start()

	// Each (node, sig) pair contributes 2 results. Across them, exactly one
	// side must have observed the rendezvous: either the suspender consumed
	// the pre-delivered signal, or the deliverer resumed the suspender.
	seen := make(map[string]int)
	for i := 0; i < pairs*2; i++ {
		r := <-results
		if r.consumed || r.delivered {
			seen[r.nodeName]++
		}
	}
	if len(seen) != pairs {
		t.Fatalf("rendezvous observed for %d/%d pairs", len(seen), pairs)
	}
	for n, c := range seen {
		if c != 1 {
			t.Fatalf("node %q saw %d rendezvous, want 1 (double resume)", n, c)
		}
	}
}

func revokeSignalIdempotentCase(t *testing.T, state engine.StateStore) {
	const racers = 16
	ctx := context.Background()
	id := types.ExecutionID("revoke-idempotent")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: linearGraph(2), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	// Pre-deliver a signal with no waiter, so it's stored. Then race the
	// revokes: exactly one must succeed.
	if _, _, err := state.DeliverSignal(ctx, id, "approval", map[string]any{"by": "alice"}); err != nil {
		t.Fatalf("DeliverSignal() error = %v", err)
	}

	start, wait := barrier(racers)
	var wins int64
	done := make(chan bool, racers)
	for i := 0; i < racers; i++ {
		go func() {
			wait()
			ok, err := state.RevokeSignal(ctx, id, "approval")
			if err != nil {
				t.Errorf("RevokeSignal() error = %v", err)
			}
			if ok {
				atomic.AddInt64(&wins, 1)
			}
			done <- ok
		}()
	}
	start()
	for i := 0; i < racers; i++ {
		<-done
	}
	if got := atomic.LoadInt64(&wins); got != 1 {
		t.Fatalf("RevokeSignal winners = %d, want 1 (idempotency break)", got)
	}
}

func checkCompletionConvergenceCase(t *testing.T, state engine.StateStore) {
	const width = 12
	ctx := context.Background()
	id := types.ExecutionID("check-completion")
	g := linearGraph(width)
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Fan out width goroutines each upserting one node to a terminal state.
	start, wait := barrier(width)
	for i := 0; i < width; i++ {
		idx := i
		name := fmt.Sprintf("n%d", idx)
		go func() {
			wait()
			_ = state.UpsertNode(ctx, &engine.NodeSnapshot{
				ExecutionID: id, Name: name, NodeIdx: idx,
				Status: types.NodeStatusSuccess,
				Output: map[string]any{"i": idx},
				Port:   "main",
			})
		}()
	}
	start()

	// Spin briefly waiting for CheckCompletion to see all nodes terminal —
	// memory writes are concurrent, so we allow a short backoff.
	deadline := timeAfter(2 * time.Second)
	for {
		allDone, hasFailed, err := state.CheckCompletion(ctx, id, width)
		if err != nil {
			t.Fatalf("CheckCompletion() error = %v", err)
		}
		if allDone {
			if hasFailed {
				t.Fatalf("CheckCompletion reported hasFailed=true with all successes")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("CheckCompletion did not converge after fan-in completion")
		case <-shortSleep(5 * time.Millisecond):
		}
	}
}

// node1Mode is the suspend mode used by suspendThenDeliverSignalCase. Kept as
// a package-level var so the cases read cleanly.
var node1Mode = types.ModeSignal

// timeAfter and shortSleep are shimmed so callers can swap in fakes for very
// long suites if needed; today they delegate to time.After.
func timeAfter(d time.Duration) <-chan time.Time { return time.After(d) }
func shortSleep(d time.Duration) <-chan time.Time {
	t := time.NewTimer(d)
	return t.C
}
