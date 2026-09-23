package rstate

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// commandCounter is a go-redis hook that records the command names a store
// issues. It is how these tests measure ROUND TRIPS rather than outcomes: the
// whole point of reporting the ready set from the claim script is that a
// claim-finds-nothing flush stops paying for a second read, and only a command
// count can see that.
type commandCounter struct {
	mu    sync.Mutex
	names []string
}

func (c *commandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *commandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.mu.Lock()
		c.names = append(c.names, strings.ToLower(cmd.Name()))
		c.mu.Unlock()
		return next(ctx, cmd)
	}
}

func (c *commandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		c.mu.Lock()
		for _, cmd := range cmds {
			c.names = append(c.names, strings.ToLower(cmd.Name()))
		}
		c.mu.Unlock()
		return next(ctx, cmds)
	}
}

// count returns how many commands of the given names were issued, and resets.
func (c *commandCounter) count(names ...string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	want := make(map[string]struct{}, len(names))
	for _, name := range names {
		want[name] = struct{}{}
	}
	n := 0
	for _, got := range c.names {
		if _, ok := want[got]; ok {
			n++
		}
	}
	c.names = nil
	return n
}

// TestLeaseOutboxRearmsTheIndexWithoutASecondRead pins the round-trip budget of
// a claim that finds nothing.
//
// That claim is the outbox hot path — every flushed execution ends in one, and
// the drain walks a page of them serially — so the read it used to issue to
// learn the ready set's score was a round trip paid once per execution per
// drain. The claim script now reports that score in the same command, so the
// re-arm must cost no ZRANGE at all. On a link whose round trip is ~80ms this is
// the difference between a dispatcher that keeps up with its backlog and one
// that falls permanently behind it.
func TestLeaseOutboxRearmsTheIndexWithoutASecondRead(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, _ := newOutboxIndexTestStore(t)
	counter := &commandCounter{}
	rdb.AddHook(counter)

	id := types.ExecutionID("lease-rearm")
	entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Minute))

	// First claim takes the entry and leases it into the future.
	entries, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 256)
	if err != nil {
		t.Fatalf("first LeaseOutbox() error = %v", err)
	}
	if len(entries) != 1 || entries[0].ID != entryID {
		t.Fatalf("first claim = %v, want the seeded entry %q", entries, entryID)
	}

	// Second claim finds nothing, because the lease pushed the score past now.
	// This is the call under test.
	counter.count() // discard the seed and first-claim traffic
	entries, err = state.LeaseOutbox(ctx, id, time.Now().UTC(), 256)
	if err != nil {
		t.Fatalf("second LeaseOutbox() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("second claim = %v, want nothing: the entry is leased", entries)
	}
	if got := counter.count("zrange"); got != 0 {
		t.Fatalf("ZRANGE calls on a claim that found nothing = %d, want 0 -- the claim "+
			"script reports the ready set's score, so re-arming the index must not read it again", got)
	}

	// The member survives, re-armed to the leased score so the next drain's due
	// page does not pick it up while the lease is outstanding.
	score, ok := indexScore(t, ctx, rdb, namespace.Default, id)
	if !ok {
		t.Fatalf("index member was removed, want it re-armed to the leased score")
	}
	if score <= float64(time.Now().UTC().UnixMilli()) {
		t.Fatalf("index score = %v, want the future leased score so the member is not due", score)
	}
}

// TestLeaseOutboxPrunesTheIndexWhenTheReadySetEmpties is the other branch: the
// claim script's empty-ready-set sentinel has to run the full prune, so a
// finished execution leaves the index instead of being rediscovered forever.
func TestLeaseOutboxPrunesTheIndexWhenTheReadySetEmpties(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, _ := newOutboxIndexTestStore(t)

	id := types.ExecutionID("lease-prune")
	entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Minute))
	if _, ok := indexScore(t, ctx, rdb, namespace.Default, id); !ok {
		t.Fatalf("seed did not register the execution in the index")
	}

	entries, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 256)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("claim = %v, want the seeded entry", entries)
	}
	if err := state.AckOutbox(ctx, id, entryID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}

	// The ack removed the last ready member, so this claim sees an empty ready
	// set and must prune the index member.
	again, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 256)
	if err != nil {
		t.Fatalf("second LeaseOutbox() error = %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second claim = %v, want nothing", again)
	}
	if score, ok := indexScore(t, ctx, rdb, namespace.Default, id); ok {
		t.Fatalf("index score = %v after the ready set emptied, want the member pruned", score)
	}
}

// TestRefreshOutboxReadyIndexStillWorksWithoutAClaim guards the shared
// implementation: the callers that have NOT just read the ready set atomically
// (suspend, execution create) still go through the read-then-apply path, so the
// refactor must not have changed what they observe.
func TestRefreshOutboxReadyIndexStillWorksWithoutAClaim(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, _ := newOutboxIndexTestStore(t)

	id := types.ExecutionID("refresh-direct")
	seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Minute))

	// Force the index member out of agreement with the ready set, then let the
	// read-then-apply path put it back.
	if err := rdb.ZRem(ctx, outboxReadyIndexKey(namespace.Default), string(id)).Err(); err != nil {
		t.Fatalf("ZRem() error = %v", err)
	}
	state.refreshOutboxReadyIndex(ctx, namespace.Default, id)

	score, ok := indexScore(t, ctx, rdb, namespace.Default, id)
	if !ok {
		t.Fatalf("refreshOutboxReadyIndex() left the member out of the index")
	}
	if score > float64(time.Now().UTC().UnixMilli()) {
		t.Fatalf("index score = %v, want the entry's past availability score", score)
	}
}

// TestParseReadyScore treats the script's empty string as "gone", not as zero.
// A zero score is a legitimate past score that keeps a member due, so conflating
// the two would prune a member that still has work.
func TestParseReadyScore(t *testing.T) {
	if _, ready := parseReadyScore(""); ready {
		t.Fatalf(`parseReadyScore("") = ready, want the empty string to mean the ready set is gone`)
	}
	score, ready := parseReadyScore("0")
	if !ready || score != 0 {
		t.Fatalf(`parseReadyScore("0") = (%v, %v), want (0, true): a zero score is due work`, score, ready)
	}
	if _, ready := parseReadyScore("not-a-number"); ready {
		t.Fatalf("parseReadyScore(garbage) = ready, want not ready")
	}
}

// TestSplitLeaseOutboxReply covers the reply shape, including a truncated reply,
// so a script or driver that drops the trailing field degrades to "not ready"
// (which prunes) rather than panicking.
func TestSplitLeaseOutboxReply(t *testing.T) {
	claimed, earliest := splitLeaseOutboxReply([]any{[]any{"a", "b", "c"}, "1712345678000"})
	if len(claimed) != 3 || earliest != "1712345678000" {
		t.Fatalf("splitLeaseOutboxReply = (%v, %q), want the triples and the score", claimed, earliest)
	}
	claimed, earliest = splitLeaseOutboxReply(nil)
	if claimed != nil || earliest != "" {
		t.Fatalf("splitLeaseOutboxReply(nil) = (%v, %q), want (nil, empty)", claimed, earliest)
	}
}
