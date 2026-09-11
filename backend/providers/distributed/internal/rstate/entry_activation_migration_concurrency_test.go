package rstate

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

type entryActivationMigrationOperationKey struct{}

// pauseAfterEntryActivationReadHook pauses a matching Redis read after Redis
// has produced its result. The caller therefore holds a stable decision or
// snapshot while the test completes the competing operation.
type pauseAfterEntryActivationReadHook struct {
	key         string
	operation   string
	commands    map[string]struct{}
	intercepted atomic.Bool
	reached     chan struct{}
	release     chan struct{}
}

func (h *pauseAfterEntryActivationReadHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *pauseAfterEntryActivationReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		_, matchesCommand := h.commands[cmd.Name()]
		if err == nil && matchesCommand && ctx.Value(entryActivationMigrationOperationKey{}) == h.operation && len(args) > 1 && fmt.Sprint(args[1]) == h.key && h.intercepted.CompareAndSwap(false, true) {
			close(h.reached)
			select {
			case <-h.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return err
	}
}

func (h *pauseAfterEntryActivationReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRedisEntryActivationLegacyMigrationSerializesAssignmentTransitions(t *testing.T) {
	transitions := []struct {
		name       string
		transition func(context.Context, *EntryActivationStore, engine.EntryActivationKey, time.Time) error
		assert     func(*testing.T, engine.EntryActivation, time.Time, string)
	}{
		{
			name: "assign",
			transition: func(ctx context.Context, store *EntryActivationStore, key engine.EntryActivationKey, deadline time.Time) error {
				assigned, err := store.Assign(ctx, key, "runner-new", "session-new", 5, deadline)
				if err != nil {
					return err
				}
				if !assigned {
					return fmt.Errorf("Assign returned false")
				}
				return nil
			},
			assert: func(t *testing.T, got engine.EntryActivation, deadline time.Time, assignedPackageHash string) {
				t.Helper()
				if got.RunnerID != "runner-new" || got.SessionID != "session-new" || got.Generation != 5 || !got.LeaseDeadline.Equal(deadline) || got.AssignedPackageHash != assignedPackageHash {
					t.Fatalf("concurrent Assign was not preserved: %+v", got)
				}
			},
		},
		{
			name: "renew",
			transition: func(ctx context.Context, store *EntryActivationStore, key engine.EntryActivationKey, deadline time.Time) error {
				renewed, err := store.Renew(ctx, key, 4, deadline)
				if err != nil {
					return err
				}
				if !renewed {
					return fmt.Errorf("Renew returned false")
				}
				return nil
			},
			assert: func(t *testing.T, got engine.EntryActivation, deadline time.Time, _ string) {
				t.Helper()
				if got.RunnerID != "runner-old" || got.SessionID != "session-old" || got.Generation != 4 || !got.LeaseDeadline.Equal(deadline) || got.AssignedPackageHash != "pkg-legacy" {
					t.Fatalf("concurrent Renew was not preserved: %+v", got)
				}
			},
		},
		{
			name: "fence",
			transition: func(ctx context.Context, store *EntryActivationStore, key engine.EntryActivationKey, _ time.Time) error {
				return store.Fence(ctx, key, 5)
			},
			assert: func(t *testing.T, got engine.EntryActivation, _ time.Time, _ string) {
				t.Helper()
				if got.RunnerID != "" || got.SessionID != "" || got.Generation != 5 || !got.LeaseDeadline.IsZero() || got.AssignedPackageHash != "" {
					t.Fatalf("concurrent Fence was not preserved: %+v", got)
				}
			},
		},
	}

	interleavings := []struct {
		name            string
		pausedOperation string
		commands        map[string]struct{}
	}{
		{
			name:            "upsert_snapshot_first",
			pausedOperation: "upsert",
			commands:        map[string]struct{}{"hgetall": {}},
		},
		{
			name:            "transition_snapshot_first",
			pausedOperation: "transition",
			// The legacy implementation selected a key with EXISTS; the fixed
			// implementation obtains migration fields with HGETALL.
			commands: map[string]struct{}{"exists": {}, "hgetall": {}},
		},
	}

	for _, interleaving := range interleavings {
		t.Run(interleaving.name, func(t *testing.T) {
			for _, transition := range transitions {
				t.Run(transition.name, func(t *testing.T) {
					ctx := context.Background()
					store, rdb := newEntryActivationRevisionRedisStore(t)
					activation := engine.EntryActivation{
						Namespace:        namespace.Namespace("legacy-migration-ns-" + interleaving.name + "-" + transition.name),
						WorkflowID:       "wf-legacy-migration",
						WorkflowVersion:  "v1",
						EntryUnitID:      "entry",
						NodeType:         "kafka.source",
						PackageHash:      "pkg-modern",
						Desired:          true,
						RegistryRevision: 1,
					}
					key := entryActivationKeyFromActivation(activation)
					legacyKey := store.legacyKeyFor(key)
					modernKey := store.keyFor(key)
					if legacySlot, modernSlot := entryActivationTestRedisSlot(legacyKey), entryActivationTestRedisSlot(modernKey); legacySlot == modernSlot {
						t.Fatalf("test requires cross-slot migration keys: legacy=%q modern=%q slot=%d", legacyKey, modernKey, legacySlot)
					}

					initialDeadline := time.Unix(1_800_000_000, 0)
					if err := rdb.HSet(ctx, legacyKey, map[string]any{
						"namespace":             string(activation.Namespace),
						"workflow_id":           string(activation.WorkflowID),
						"workflow_version":      activation.WorkflowVersion,
						"entry_unit_id":         activation.EntryUnitID,
						"replica_index":         activation.ReplicaIndex,
						"node_type":             activation.NodeType,
						"package_hash":          "pkg-legacy",
						"desired":               "1",
						"registry_revision":     "1",
						"runner_id":             "runner-old",
						"session_id":            "session-old",
						"generation":            "4",
						"lease_deadline":        initialDeadline.UnixNano(),
						"assigned_package_hash": "pkg-legacy",
					}).Err(); err != nil {
						t.Fatalf("seed legacy activation: %v", err)
					}

					hook := &pauseAfterEntryActivationReadHook{
						key:       legacyKey,
						operation: interleaving.pausedOperation,
						commands:  interleaving.commands,
						reached:   make(chan struct{}),
						release:   make(chan struct{}),
					}
					rdb.AddHook(hook)
					upsertCtx := context.WithValue(ctx, entryActivationMigrationOperationKey{}, "upsert")
					transitionCtx := context.WithValue(ctx, entryActivationMigrationOperationKey{}, "transition")
					transitionDeadline := initialDeadline.Add(time.Hour)

					if interleaving.pausedOperation == "upsert" {
						upsertDone := make(chan error, 1)
						go func() {
							upsertDone <- store.Upsert(upsertCtx, activation)
						}()
						waitForEntryActivationTestSignal(t, hook.reached, "Upsert legacy snapshot")
						transitionErr := transition.transition(transitionCtx, store, key, transitionDeadline)
						close(hook.release)
						upsertErr := waitForEntryActivationTestResult(t, upsertDone, "Upsert completion")
						if transitionErr != nil {
							t.Fatalf("concurrent %s: %v", transition.name, transitionErr)
						}
						if upsertErr != nil {
							t.Fatalf("Upsert: %v", upsertErr)
						}
					} else {
						transitionDone := make(chan error, 1)
						go func() {
							transitionDone <- transition.transition(transitionCtx, store, key, transitionDeadline)
						}()
						waitForEntryActivationTestSignal(t, hook.reached, transition.name+" legacy snapshot")
						upsertErr := store.Upsert(upsertCtx, activation)
						close(hook.release)
						transitionErr := waitForEntryActivationTestResult(t, transitionDone, transition.name+" completion")
						if upsertErr != nil {
							t.Fatalf("concurrent Upsert: %v", upsertErr)
						}
						if transitionErr != nil {
							t.Fatalf("%s: %v", transition.name, transitionErr)
						}
					}

					got, ok, err := store.Get(ctx, key)
					if err != nil || !ok {
						t.Fatalf("Get modern activation: ok=%v err=%v", ok, err)
					}
					if got.PackageHash != "pkg-modern" {
						t.Fatalf("modern desired state was not applied: %+v", got)
					}
					expectedAssignedPackageHash := "pkg-legacy"
					if interleaving.pausedOperation == "transition" {
						expectedAssignedPackageHash = "pkg-modern"
					}
					transition.assert(t, got, transitionDeadline, expectedAssignedPackageHash)
				})
			}
		})
	}
}

func entryActivationTestRedisSlot(key string) uint16 {
	if tag := firstRedisHashTag(key); tag != "" {
		key = tag
	}
	var crc uint16
	for i := 0; i < len(key); i++ {
		crc ^= uint16(key[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc % 16384
}

func waitForEntryActivationTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForEntryActivationTestResult[T any](t *testing.T, result <-chan T, description string) T {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case value := <-result:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}
