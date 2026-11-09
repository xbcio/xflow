package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/namespace"
)

// twoStores returns two Stores on one miniredis, each with its own client and
// SCAN/DBSIZE counting hook, over a keyspace big enough that a backfill takes
// several pages while a steady step takes one.
func twoStores(t *testing.T) (*miniredis.Miniredis, [2]*Store, [2]*scanCountHook, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	var stores [2]*Store
	var hooks [2]*scanCountHook
	var first *redis.Client
	for i := range stores {
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		hooks[i] = &scanCountHook{pages: 10}
		rdb.AddHook(hooks[i])
		stores[i] = New(rdb, nil, 24*time.Hour)
		if first == nil {
			first = rdb
		}
	}
	ctx := context.Background()
	for i := 0; i < 3500; i++ {
		if err := first.Set(ctx, fmt.Sprintf("unrelated:%d", i), "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	return mr, stores, hooks, first
}

type walkCase struct {
	name string
	kind string
	// call runs one walk-driving call on a Store.
	call func(s *Store) error
	// stepBack makes the next paced call on s due without sleeping.
	stepBack func(s *Store)
}

func walkCases() []walkCase {
	ctx := context.Background()
	return []walkCase{
		{
			name: "leases", kind: leaseWalkKind,
			call: func(s *Store) error { _, err := s.ListExpiredLeases(ctx, time.Now()); return err },
			stepBack: func(s *Store) {
				s.leaseWalkMu.Lock()
				defer s.leaseWalkMu.Unlock()
				if st := s.leaseWalks[namespace.Default]; st != nil {
					st.lastStep = time.Now().Add(-2 * leaseWalkStepInterval)
				}
			},
		},
		{
			name: "outbox", kind: outboxWalkKind,
			call: func(s *Store) error { _, err := s.OutboxMetrics(ctx); return err },
			stepBack: func(s *Store) {
				s.outboxMetricsWalkMu.Lock()
				defer s.outboxMetricsWalkMu.Unlock()
				if w := s.outboxMetricsWalks[namespace.Default]; w != nil {
					w.last = time.Now().Add(-outboxMetricsWalkRound)
				}
			},
		},
	}
}

func TestWalkSingleHolderPerKind(t *testing.T) {
	for _, tc := range walkCases() {
		t.Run(tc.name, func(t *testing.T) {
			mr, stores, hooks, rdb := twoStores(t)
			ctx := context.Background()

			// First holder, no marker: full backfill (several pages), marker set.
			if err := tc.call(stores[0]); err != nil {
				t.Fatal(err)
			}
			if n := hooks[0].take(); n < 11 {
				t.Fatalf("first holder without marker issued %d SCAN, want a full 11-page backfill", n)
			}
			hooks[0].takeDBSize()
			if ok, _ := redisx.WalkRoundRecent(ctx, rdb, redisx.WalkDoneKey(string(namespace.Default), tc.kind)); !ok {
				t.Fatal("round-done marker not written after the backfill round")
			}

			// The other replica runs the same calls: zero SCAN and zero DBSIZE.
			for i := 0; i < 5; i++ {
				tc.stepBack(stores[1])
				tc.stepBack(stores[0])
				if err := tc.call(stores[1]); err != nil {
					t.Fatal(err)
				}
				if err := tc.call(stores[0]); err != nil {
					t.Fatal(err)
				}
			}
			if n, d := hooks[1].take(), hooks[1].takeDBSize(); n != 0 || d != 0 {
				t.Fatalf("non-holder issued %d SCAN and %d DBSIZE, want 0 and 0", n, d)
			}
			if hooks[0].take() == 0 {
				t.Fatal("holder stopped walking")
			}

			// Holder stops; lock expires; the other takes over WITHOUT a backfill.
			hooks[1].take()
			mr.FastForward(redisx.WalkLockTTL + time.Second)
			if err := tc.call(stores[1]); err != nil {
				t.Fatal(err)
			}
			if n := hooks[1].take(); n > 2 {
				t.Fatalf("takeover step issued %d SCAN, want at most one steady step (marker present)", n)
			}
			// ... and the former holder is now a non-holder.
			hooks[0].take()
			hooks[0].takeDBSize()
			tc.stepBack(stores[0])
			if err := tc.call(stores[0]); err != nil {
				t.Fatal(err)
			}
			if n, d := hooks[0].take(), hooks[0].takeDBSize(); n != 0 || d != 0 {
				t.Fatalf("deposed holder issued %d SCAN and %d DBSIZE, want 0 and 0", n, d)
			}

			// Marker lost (flush): the next holder backfills at full speed.
			rdb.Del(ctx, redisx.WalkDoneKey(string(namespace.Default), tc.kind))
			mr.FastForward(redisx.WalkLockTTL + time.Second)
			hooks[0].take()
			if err := tc.call(stores[0]); err != nil {
				t.Fatal(err)
			}
			if n := hooks[0].take(); n < 11 {
				t.Fatalf("holder without marker issued %d SCAN, want a full 11-page backfill", n)
			}
		})
	}
}

func TestWalkFailedRenewStopsWalk(t *testing.T) {
	_, stores, hooks, rdb := twoStores(t)
	ctx := context.Background()
	// Another owner holds the lock: stores[0] must not walk at all.
	key := redisx.WalkLockKey(string(namespace.Default), leaseWalkKind)
	if err := rdb.Set(ctx, key, "someone-else", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := stores[0].ListExpiredLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if n := hooks[0].take(); n != 0 {
		t.Fatalf("store without the lock issued %d SCAN", n)
	}
}

func TestLeaseWalkLostRegistrationRecoveredAfterTakeover(t *testing.T) {
	mr, stores, _, rdb := twoStores(t)
	ctx := context.Background()
	mustUpsertRunning(t, stores[0], "e-lost", "n", "tok", time.Now().Add(-time.Minute), time.Second)
	if _, err := stores[0].ListExpiredLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(redisx.WalkLockTTL + time.Second)
	dropLeaseIndex(t, rdb)
	// stores[1] takes over paced; its first step is due immediately. The
	// registration comes back within the documented bound: here, by driving
	// steps until a full round has passed.
	var got int
	for i := 0; i < 15 && got == 0; i++ {
		res, err := stores[1].ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		got = len(res)
		walkCases()[0].stepBack(stores[1])
	}
	if got != 1 {
		t.Fatalf("lost registration not recovered by the new holder")
	}
}

func TestWalkPagesAtMeasuredKeyspace(t *testing.T) {
	pages := redisx.WalkPagesPerStep(2_770_000, outboxMetricsWalkPage, outboxMetricsWalkRound, leaseWalkStepInterval)
	if pages < 23 || pages > 25 {
		t.Fatalf("pages per step = %d, want about 24", pages)
	}
	if got := redisx.WalkPagesPerStep(2_770_000, leaseWalkPageCount, leaseWalkRound, leaseWalkStepInterval); got != pages {
		t.Fatalf("lease walk pages %d != outbox %d", got, pages)
	}
}
