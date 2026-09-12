package rstate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/namespace"
)

func TestRepairLeaseIndexPersistsNodeLocalCursor(t *testing.T) {
	hook := &leaseRepairScanHook{pages: map[uint64]leaseRepairScanPage{
		0:  {next: 41},
		41: {next: 0},
	}}
	rdb := redis.NewClient(&redis.Options{Addr: "standalone.invalid:6379"})
	rdb.AddHook(hook)
	t.Cleanup(func() { _ = rdb.Close() })
	state := New(rdb, nil, time.Hour)
	tenant := namespace.Namespace("cursor-tenant")
	ctx := namespace.WithNamespace(context.Background(), tenant)

	for range 2 {
		reconciled, err := state.repairLeaseIndexForNamespace(ctx, tenant, 8)
		if err != nil {
			t.Fatalf("repairLeaseIndexForNamespace() error = %v", err)
		}
		if reconciled != 0 {
			t.Fatalf("repairLeaseIndexForNamespace() = %d, want 0 for empty pages", reconciled)
		}
	}

	wantCalls := []leaseRepairScanCall{
		{cursor: 0, pattern: execScanPattern(tenant, "node:*:status"), count: 8},
		{cursor: 41, pattern: execScanPattern(tenant, "node:*:status"), count: 8},
	}
	if got := hook.callsSnapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("lease repair SCAN calls = %+v, want %+v", got, wantCalls)
	}
	if want := (redisx.Cursors{"": 0}); !reflect.DeepEqual(state.leaseRepairCursors[tenant], want) {
		t.Fatalf("stored lease repair cursors = %v, want %v", state.leaseRepairCursors[tenant], want)
	}
}

func TestRepairLeaseIndexDoesNotAdvanceCursorOnScanError(t *testing.T) {
	wantErr := errors.New("scan unavailable")
	hook := &leaseRepairScanHook{pages: map[uint64]leaseRepairScanPage{
		17: {err: wantErr},
	}}
	rdb := redis.NewClient(&redis.Options{Addr: "standalone.invalid:6379"})
	rdb.AddHook(hook)
	t.Cleanup(func() { _ = rdb.Close() })
	state := New(rdb, nil, time.Hour)
	tenant := namespace.Namespace("error-tenant")
	state.leaseRepairCursors[tenant] = redisx.Cursors{"": 17}

	_, err := state.repairLeaseIndexForNamespace(context.Background(), tenant, 8)
	if !errors.Is(err, wantErr) {
		t.Fatalf("repairLeaseIndexForNamespace() error = %v, want %v", err, wantErr)
	}
	if want := (redisx.Cursors{"": 17}); !reflect.DeepEqual(state.leaseRepairCursors[tenant], want) {
		t.Fatalf("stored lease repair cursors after error = %v, want unchanged %v", state.leaseRepairCursors[tenant], want)
	}
}

type leaseRepairScanPage struct {
	next uint64
	err  error
}

type leaseRepairScanCall struct {
	cursor  uint64
	pattern string
	count   int64
}

type leaseRepairScanHook struct {
	mu    sync.Mutex
	pages map[uint64]leaseRepairScanPage
	calls []leaseRepairScanCall
}

func (h *leaseRepairScanHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *leaseRepairScanHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		scan, ok := cmd.(*redis.ScanCmd)
		if !ok {
			return next(ctx, cmd)
		}
		call, err := leaseRepairScanCallFromArgs(scan.Args())
		if err != nil {
			return err
		}
		h.mu.Lock()
		h.calls = append(h.calls, call)
		page, ok := h.pages[call.cursor]
		h.mu.Unlock()
		if !ok {
			return fmt.Errorf("unexpected SCAN cursor %d", call.cursor)
		}
		scan.SetVal(nil, page.next)
		return page.err
	}
}

func (h *leaseRepairScanHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *leaseRepairScanHook) callsSnapshot() []leaseRepairScanCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]leaseRepairScanCall(nil), h.calls...)
}

func leaseRepairScanCallFromArgs(args []any) (leaseRepairScanCall, error) {
	if len(args) < 2 {
		return leaseRepairScanCall{}, errors.New("SCAN command missing cursor")
	}
	cursor, err := strconv.ParseUint(fmt.Sprint(args[1]), 10, 64)
	if err != nil {
		return leaseRepairScanCall{}, fmt.Errorf("parse SCAN cursor: %w", err)
	}
	call := leaseRepairScanCall{cursor: cursor}
	for i := 2; i+1 < len(args); i += 2 {
		switch fmt.Sprint(args[i]) {
		case "match":
			call.pattern = fmt.Sprint(args[i+1])
		case "count":
			call.count, err = strconv.ParseInt(fmt.Sprint(args[i+1]), 10, 64)
			if err != nil {
				return leaseRepairScanCall{}, fmt.Errorf("parse SCAN count: %w", err)
			}
		}
	}
	return call, nil
}
