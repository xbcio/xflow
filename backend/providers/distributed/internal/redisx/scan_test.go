package redisx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestScanAllUsesSingleNodePathForOrdinaryClient(t *testing.T) {
	script := &scriptedScanner{pages: map[uint64]scriptedPage{
		0:  {keys: []string{"wanted:b", "wanted:a"}, next: 17},
		17: {keys: []string{"wanted:c", "wanted:a"}, next: 0},
	}}
	rdb := redis.NewClient(&redis.Options{Addr: "standalone.invalid:6379"})
	rdb.AddHook(scanHook{scanner: script})
	t.Cleanup(func() { _ = rdb.Close() })

	got, err := ScanAll(context.Background(), rdb, "wanted:*", 1)
	if err != nil {
		t.Fatalf("ScanAll() error = %v", err)
	}
	want := []string{"wanted:a", "wanted:b", "wanted:c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanAll() = %v, want %v", got, want)
	}
	wantCalls := []scanCall{
		{cursor: 0, pattern: "wanted:*", count: 1},
		{cursor: 17, pattern: "wanted:*", count: 1},
	}
	if calls := script.callsSnapshot(); !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("single-node SCAN calls = %+v, want %+v", calls, wantCalls)
	}
}

func TestScanPageUsesSingleNodeCursorForOrdinaryClient(t *testing.T) {
	script := &scriptedScanner{pages: map[uint64]scriptedPage{
		41: {keys: []string{"wanted:b", "wanted:a", "wanted:a"}, next: 73},
	}}
	rdb := redis.NewClient(&redis.Options{Addr: "standalone.invalid:6379"})
	rdb.AddHook(scanHook{scanner: script})
	t.Cleanup(func() { _ = rdb.Close() })

	keys, next, err := ScanPage(context.Background(), rdb, Cursors{"": 41}, "wanted:*", 9)
	if err != nil {
		t.Fatalf("ScanPage() error = %v", err)
	}
	if want := []string{"wanted:a", "wanted:b"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("ScanPage() keys = %v, want %v", keys, want)
	}
	if want := (Cursors{"": 73}); !reflect.DeepEqual(next, want) {
		t.Fatalf("ScanPage() cursors = %v, want %v", next, want)
	}
	wantCalls := []scanCall{{cursor: 41, pattern: "wanted:*", count: 9}}
	if calls := script.callsSnapshot(); !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("single-node SCAN calls = %+v, want %+v", calls, wantCalls)
	}
}

func TestScanPageReturnsSingleNodeErrorWithoutCursorProgress(t *testing.T) {
	wantErr := errors.New("single-node page failed")
	script := &scriptedScanner{pages: map[uint64]scriptedPage{
		41: {keys: []string{"partial"}, next: 73, err: wantErr},
	}}
	rdb := redis.NewClient(&redis.Options{Addr: "standalone.invalid:6379"})
	rdb.AddHook(scanHook{scanner: script})
	t.Cleanup(func() { _ = rdb.Close() })

	keys, next, err := ScanPage(context.Background(), rdb, Cursors{"": 41}, "wanted:*", 9)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ScanPage() error = %v, want %v", err, wantErr)
	}
	if keys != nil || next != nil {
		t.Fatalf("ScanPage() partial keys/cursors = %v/%v, want nil/nil", keys, next)
	}
}

func TestScanMastersUsesIndependentCursorsAndDeduplicatesConcurrentResults(t *testing.T) {
	masters := []*scriptedScanner{
		{pages: map[uint64]scriptedPage{
			0:  {keys: []string{"wanted:c", "wanted:duplicate"}, next: 11},
			11: {keys: []string{"wanted:a"}, next: 0},
		}},
		{pages: map[uint64]scriptedPage{
			0:  {keys: []string{"wanted:b", "wanted:duplicate"}, next: 22},
			22: {keys: []string{"wanted:d"}, next: 33},
			33: {keys: []string{"wanted:b"}, next: 0},
		}},
		{pages: map[uint64]scriptedPage{
			0: {keys: []string{"wanted:e"}, next: 0},
		}},
	}

	got, err := scanMasters(context.Background(), "wanted:*", 2, concurrentMasters(masters...))
	if err != nil {
		t.Fatalf("scanMasters() error = %v", err)
	}
	want := []string{"wanted:a", "wanted:b", "wanted:c", "wanted:d", "wanted:duplicate", "wanted:e"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scanMasters() = %v, want merged, deduplicated keys %v", got, want)
	}
	wantCursors := [][]uint64{{0, 11}, {0, 22, 33}, {0}}
	for i, want := range wantCursors {
		if got := masters[i].cursorsSnapshot(); !reflect.DeepEqual(got, want) {
			t.Errorf("master %d SCAN cursors = %v, want independent sequence %v", i, got, want)
		}
	}
}

func TestScanMastersReturnsMasterErrorWithoutPartialResults(t *testing.T) {
	wantErr := errors.New("master scan failed")
	masters := []*scriptedScanner{
		{pages: map[uint64]scriptedPage{0: {keys: []string{"partial:a"}, next: 0}}},
		{pages: map[uint64]scriptedPage{0: {err: wantErr}}},
	}

	got, err := scanMasters(context.Background(), "*", 10, concurrentMasters(masters...))
	if !errors.Is(err, wantErr) {
		t.Fatalf("scanMasters() error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Fatalf("scanMasters() partial result = %v, want nil", got)
	}
}

func TestScanMasterPagesUsesPerMasterCursorsAndHandlesTopologyChange(t *testing.T) {
	masters := []namedScanner{
		{id: "master-b", scanner: &scriptedScanner{pages: map[uint64]scriptedPage{
			22: {keys: []string{"wanted:b", "wanted:duplicate"}, next: 202},
		}}},
		{id: "master-a", scanner: &scriptedScanner{pages: map[uint64]scriptedPage{
			11: {keys: []string{"wanted:a", "wanted:duplicate"}, next: 101},
		}}},
		// master-c appeared after the count-discovery pass. It must start at
		// cursor zero and receive a positive fallback count.
		{id: "master-c", scanner: &scriptedScanner{pages: map[uint64]scriptedPage{
			0: {keys: []string{"wanted:c"}, next: 303},
		}}},
	}
	cursors := Cursors{
		"master-a":        11,
		"master-b":        22,
		"departed-master": 99,
	}
	counts := map[string]int64{"master-a": 3, "master-b": 2}

	keys, next, err := scanMasterPages(
		context.Background(), cursors, "wanted:*", 5, counts,
		concurrentNamedMasters(masters...),
	)
	if err != nil {
		t.Fatalf("scanMasterPages() error = %v", err)
	}
	if want := []string{"wanted:a", "wanted:b", "wanted:c", "wanted:duplicate"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("scanMasterPages() keys = %v, want %v", keys, want)
	}
	wantNext := Cursors{"master-a": 101, "master-b": 202, "master-c": 303}
	if !reflect.DeepEqual(next, wantNext) {
		t.Fatalf("scanMasterPages() cursors = %v, want %v", next, wantNext)
	}
	wantCalls := map[string][]scanCall{
		"master-a": {{cursor: 11, pattern: "wanted:*", count: 3}},
		"master-b": {{cursor: 22, pattern: "wanted:*", count: 2}},
		"master-c": {{cursor: 0, pattern: "wanted:*", count: 1}},
	}
	for _, master := range masters {
		if calls := master.scanner.callsSnapshot(); !reflect.DeepEqual(calls, wantCalls[master.id]) {
			t.Errorf("%s SCAN calls = %+v, want %+v", master.id, calls, wantCalls[master.id])
		}
	}
	if cursors["departed-master"] != 99 {
		t.Fatalf("input cursors were mutated: %v", cursors)
	}
}

func TestScanMasterPagesReturnsErrorWithoutKeysOrCursorProgress(t *testing.T) {
	wantErr := errors.New("page failed")
	masters := []namedScanner{
		{id: "master-a", scanner: &scriptedScanner{pages: map[uint64]scriptedPage{
			4: {keys: []string{"partial"}, next: 40},
		}}},
		{id: "master-b", scanner: &scriptedScanner{pages: map[uint64]scriptedPage{
			5: {err: wantErr},
		}}},
	}

	keys, next, err := scanMasterPages(
		context.Background(),
		Cursors{"master-a": 4, "master-b": 5},
		"*", 8,
		map[string]int64{"master-a": 4, "master-b": 4},
		concurrentNamedMasters(masters...),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("scanMasterPages() error = %v, want %v", err, wantErr)
	}
	if keys != nil || next != nil {
		t.Fatalf("scanMasterPages() partial keys/cursors = %v/%v, want nil/nil", keys, next)
	}
}

func TestCollectMasterCountsDeduplicatesConcurrentMasters(t *testing.T) {
	masters := []namedScanner{
		{id: "master-b", scanner: &scriptedScanner{}},
		{id: "master-a", scanner: &scriptedScanner{}},
		{id: "master-b", scanner: &scriptedScanner{}},
	}

	got, err := collectMasterCounts(context.Background(), 5, concurrentNamedMasters(masters...))
	if err != nil {
		t.Fatalf("collectMasterCounts() error = %v", err)
	}
	want := map[string]int64{"master-a": 3, "master-b": 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collectMasterCounts() = %v, want %v", got, want)
	}
}

func TestCollectMasterCountsReturnsDiscoveryError(t *testing.T) {
	wantErr := errors.New("master discovery failed")
	got, err := collectMasterCounts(context.Background(), 5,
		func(context.Context, func(context.Context, string, scanner) error) error {
			return wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("collectMasterCounts() error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Fatalf("collectMasterCounts() = %v, want nil on discovery error", got)
	}
}

func TestSplitScanCount(t *testing.T) {
	ids := map[string]struct{}{"master-c": {}, "master-a": {}, "master-b": {}}
	tests := []struct {
		name  string
		count int64
		want  map[string]int64
	}{
		{
			name:  "remainder follows stable master order",
			count: 8,
			want:  map[string]int64{"master-a": 3, "master-b": 3, "master-c": 2},
		},
		{
			name:  "minimum one per master",
			count: 2,
			want:  map[string]int64{"master-a": 1, "master-b": 1, "master-c": 1},
		},
		{
			name:  "zero passes through",
			count: 0,
			want:  map[string]int64{"master-a": 0, "master-b": 0, "master-c": 0},
		},
		{
			name:  "negative passes through",
			count: -1,
			want:  map[string]int64{"master-a": -1, "master-b": -1, "master-c": -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitScanCount(ids, tt.count); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitScanCount(%d) = %v, want %v", tt.count, got, tt.want)
			}
		})
	}
	if got := splitScanCount(nil, 8); len(got) != 0 {
		t.Fatalf("splitScanCount(nil, 8) = %v, want empty", got)
	}
}

func TestScanNodeReturnsScanErrorWithoutPartialResults(t *testing.T) {
	wantErr := errors.New("scan failed")
	rdb := &scriptedScanner{pages: map[uint64]scriptedPage{
		0: {keys: []string{"partial"}, next: 12, err: wantErr},
	}}

	got, err := scanNode(context.Background(), rdb, "*", 10)
	if !errors.Is(err, wantErr) {
		t.Fatalf("scanNode() error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Fatalf("scanNode() partial result = %v, want nil", got)
	}
}

func concurrentMasters(masters ...*scriptedScanner) forEachMaster {
	return func(ctx context.Context, visit func(context.Context, scanner) error) error {
		var wg sync.WaitGroup
		errCh := make(chan error, len(masters))
		for _, master := range masters {
			wg.Add(1)
			go func(master scanner) {
				defer wg.Done()
				if err := visit(ctx, master); err != nil {
					errCh <- err
				}
			}(master)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			return err
		}
		return nil
	}
}

type namedScanner struct {
	id      string
	scanner *scriptedScanner
}

func concurrentNamedMasters(masters ...namedScanner) forEachNamedMaster {
	return func(ctx context.Context, visit func(context.Context, string, scanner) error) error {
		var wg sync.WaitGroup
		errCh := make(chan error, len(masters))
		for _, master := range masters {
			wg.Add(1)
			go func(master namedScanner) {
				defer wg.Done()
				if err := visit(ctx, master.id, master.scanner); err != nil {
					errCh <- err
				}
			}(master)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			return err
		}
		return nil
	}
}

type scanCall struct {
	cursor  uint64
	pattern string
	count   int64
}

type scriptedPage struct {
	keys []string
	next uint64
	err  error
}

type scriptedScanner struct {
	mu    sync.Mutex
	pages map[uint64]scriptedPage
	calls []scanCall
}

func (s *scriptedScanner) Scan(_ context.Context, cursor uint64, pattern string, count int64) *redis.ScanCmd {
	page := s.page(scanCall{cursor: cursor, pattern: pattern, count: count})
	return redis.NewScanCmdResult(page.keys, page.next, page.err)
}

func (s *scriptedScanner) page(call scanCall) scriptedPage {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
	page, ok := s.pages[call.cursor]
	if !ok {
		return scriptedPage{err: fmt.Errorf("unexpected cursor %d", call.cursor)}
	}
	return page
}

func (s *scriptedScanner) callsSnapshot() []scanCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]scanCall(nil), s.calls...)
}

func (s *scriptedScanner) cursorsSnapshot() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	cursors := make([]uint64, 0, len(s.calls))
	for _, call := range s.calls {
		cursors = append(cursors, call.cursor)
	}
	return cursors
}

type scanHook struct {
	scanner *scriptedScanner
}

func (h scanHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h scanHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		scan, ok := cmd.(*redis.ScanCmd)
		if !ok {
			return next(ctx, cmd)
		}
		call, err := scanCallFromArgs(scan.Args())
		if err != nil {
			return err
		}
		page := h.scanner.page(call)
		scan.SetVal(page.keys, page.next)
		return page.err
	}
}

func (h scanHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func scanCallFromArgs(args []any) (scanCall, error) {
	if len(args) < 2 {
		return scanCall{}, errors.New("SCAN command missing cursor")
	}
	cursor, err := strconv.ParseUint(fmt.Sprint(args[1]), 10, 64)
	if err != nil {
		return scanCall{}, fmt.Errorf("parse SCAN cursor: %w", err)
	}
	call := scanCall{cursor: cursor}
	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToLower(fmt.Sprint(args[i])) {
		case "match":
			call.pattern = fmt.Sprint(args[i+1])
		case "count":
			call.count, err = strconv.ParseInt(fmt.Sprint(args[i+1]), 10, 64)
			if err != nil {
				return scanCall{}, fmt.Errorf("parse SCAN count: %w", err)
			}
		}
	}
	return call, nil
}

func TestMasterIDUsesReportedNodeAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		node string
		want string
	}{
		{name: "reported address", addr: "dial.internal:6379", node: "redis.example:6379", want: "redis.example:6379"},
		{name: "dial address fallback", addr: "dial.internal:6379", want: "dial.internal:6379"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := redis.NewClient(&redis.Options{Addr: tt.addr, NodeAddress: tt.node})
			t.Cleanup(func() { _ = client.Close() })
			if got := masterID(client); got != tt.want {
				t.Fatalf("masterID() = %q, want %q", got, tt.want)
			}
		})
	}
}
