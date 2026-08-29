package xflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// newSupplyAccessTestServer builds a *Server backed by an in-memory store,
// mirroring the shortest existing Server construction used by the
// TestServerUpdateSupply* tests in server_test.go. No HTTP transport is
// started: these tests only exercise the in-process supply read/write
// methods.
func newSupplyAccessTestServer(t *testing.T) *Server {
	t.Helper()
	ms := memstore.New()
	srv, err := NewServer(ServerConfig{Store: ms}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

// literalNamespaceStore wraps memstore.New() for every store.Store method
// except GetSupply/PutSupply, which it reimplements to key rows by the exact
// namespace string given — no folding of "" to "default".
//
// memstore.Store (store/memstore/supply.go) independently normalizes "" to
// "default" inside its own GetSupply/PutSupply. That is a legitimate
// defensive measure at the store layer, but it means a memstore-backed
// Server masks whether Server.GetSupply performs its own normalization:
// deleting the "if ns == \"\" { ns = namespace.Default }" guard in
// Server.GetSupply does not turn TestGetSupplyNormalizesEmptyNamespaceLikeUpdate
// red when the backing store folds "" itself regardless. This wrapper exists
// solely to isolate Server's own normalization from the store's, so that test
// has teeth. See task-1-report.md for the mutation-testing run that found this.
type literalNamespaceStore struct {
	store.Store
	mu   sync.Mutex
	rows map[string]*store.SupplyResource
}

func newLiteralNamespaceStore() *literalNamespaceStore {
	return &literalNamespaceStore{Store: memstore.New(), rows: map[string]*store.SupplyResource{}}
}

func (l *literalNamespaceStore) key(ns, name string) string { return ns + "\x00" + name }

func (l *literalNamespaceStore) GetSupply(_ context.Context, ns, name string) (*store.SupplyResource, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.rows[l.key(ns, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	cp.Content = append([]byte(nil), rec.Content...)
	return &cp, nil
}

func (l *literalNamespaceStore) PutSupply(_ context.Context, rec *store.SupplyResource, ifMatch *uint64) (*store.SupplyResource, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := l.key(rec.Namespace, rec.Name)
	cur, exists := l.rows[key]
	var curRev uint64
	if exists {
		curRev = cur.Revision
	}
	if ifMatch != nil && *ifMatch != curRev {
		return nil, store.ErrRevisionConflict
	}
	next := *rec
	next.Content = append([]byte(nil), rec.Content...)
	next.Revision = curRev + 1
	next.ContentHash = store.ContentHash(next.Content)
	next.UpdatedAt = time.Now().UTC()
	l.rows[key] = &next
	out := next
	out.Content = append([]byte(nil), next.Content...)
	return &out, nil
}

// newSupplyAccessTestServerLiteralNamespace is like newSupplyAccessTestServer
// but backs the Server with literalNamespaceStore instead of memstore.New()
// directly, so tests asserting Server-level namespace normalization are not
// masked by the store's own (independent) normalization.
func newSupplyAccessTestServerLiteralNamespace(t *testing.T) *Server {
	t.Helper()
	srv, err := NewServer(ServerConfig{Store: newLiteralNamespaceStore()}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

// GetSupply 必须和 UpdateSupply 用同一套 namespace 归一化规则。写路径把 ""
// 折成 default 而读路径不折，会让「写完立刻用 "" 回读」恒定 not found —— 这个
// 形状在 UpdateSupply 自己身上出现过一次，这里把对称性钉死。
func TestGetSupplyNormalizesEmptyNamespaceLikeUpdate(t *testing.T) {
	srv := newSupplyAccessTestServerLiteralNamespace(t)
	ctx := context.Background()

	if err := srv.UpdateSupply(ctx, "", "rules", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("UpdateSupply: %v", err)
	}

	// 用 "" 回读：必须命中写路径落进 default 的那一行。
	got, err := srv.GetSupply(ctx, "", "rules")
	if err != nil {
		t.Fatalf(`GetSupply(ctx, "", "rules") = %v; 写路径把 "" 折成 default 而读路径没折`, err)
	}
	if string(got.Content) != `{"v":1}` {
		t.Fatalf("Content = %q, want %q", got.Content, `{"v":1}`)
	}
	if got.Namespace != string(namespace.Default) {
		t.Fatalf("Namespace = %q, want %q", got.Namespace, namespace.Default)
	}
	if got.Revision == 0 {
		t.Fatal("Revision = 0；运营页要靠它做 CAS，写完必须是非零")
	}
	if got.ContentHash == "" {
		t.Fatal("ContentHash 为空；§4.4 的收敛判据就是拿它和 runner 上报的哈希比")
	}

	// 显式写 default 与写 "" 必须落在同一行上，而不是两行。
	explicit, err := srv.GetSupply(ctx, string(namespace.Default), "rules")
	if err != nil {
		t.Fatalf("GetSupply(default): %v", err)
	}
	if explicit.Revision != got.Revision {
		t.Fatalf(`GetSupply("") 的 revision=%d，GetSupply("default") 的 revision=%d；两者必须是同一行`,
			got.Revision, explicit.Revision)
	}
}

// 被封掉的 HTTP PUT 有 If-Match → CAS，UpdateSupply 没有。这条测试钉住新入口
// 真的把 revision 传给了 store，而不是像 UpdateSupply 那样传 nil。
func TestUpdateSupplyIfMatchRejectsStaleRevision(t *testing.T) {
	srv := newSupplyAccessTestServer(t)
	ctx := context.Background()

	first, err := srv.UpdateSupplyIfMatch(ctx, "ns1", "pointer", []byte("sha256:aaa"), 0)
	if err != nil {
		t.Fatalf("首次写（ifMatch=0 = 仅当不存在时创建）: %v", err)
	}
	if first.Revision == 0 {
		t.Fatalf("Revision = 0，want 非零")
	}

	// 拿正确的 revision 写：成功，且 revision 前进。
	second, err := srv.UpdateSupplyIfMatch(ctx, "ns1", "pointer", []byte("sha256:bbb"), first.Revision)
	if err != nil {
		t.Fatalf("CAS 写（ifMatch=%d）: %v", first.Revision, err)
	}
	if second.Revision <= first.Revision {
		t.Fatalf("Revision 没有前进：%d -> %d", first.Revision, second.Revision)
	}

	// 拿已经过期的 revision 再写：必须冲突，且内容不得被改动。
	_, err = srv.UpdateSupplyIfMatch(ctx, "ns1", "pointer", []byte("sha256:ccc"), first.Revision)
	if !errors.Is(err, store.ErrRevisionConflict) {
		t.Fatalf("用陈旧 revision 写 = %v, want store.ErrRevisionConflict", err)
	}
	after, err := srv.GetSupply(ctx, "ns1", "pointer")
	if err != nil {
		t.Fatalf("GetSupply: %v", err)
	}
	if string(after.Content) != "sha256:bbb" {
		t.Fatalf("冲突之后内容 = %q，want sha256:bbb（冲突写不得落地）", after.Content)
	}
}

// ifMatch=0 的语义是「仅当不存在时创建」，不是「无条件写」。目标已存在时必须冲突。
// 若实现把 0 当成 nil 传下去，这条会绿着放行一次无条件覆盖。
func TestUpdateSupplyIfMatchZeroIsCreateOnly(t *testing.T) {
	srv := newSupplyAccessTestServer(t)
	ctx := context.Background()

	if _, err := srv.UpdateSupplyIfMatch(ctx, "ns1", "pointer", []byte("first"), 0); err != nil {
		t.Fatalf("创建: %v", err)
	}
	_, err := srv.UpdateSupplyIfMatch(ctx, "ns1", "pointer", []byte("second"), 0)
	if !errors.Is(err, store.ErrRevisionConflict) {
		t.Fatalf("对已存在的 supply 用 ifMatch=0 = %v, want store.ErrRevisionConflict", err)
	}
}

func TestGetSupplyMissingReturnsNotFound(t *testing.T) {
	srv := newSupplyAccessTestServer(t)
	_, err := srv.GetSupply(context.Background(), "ns1", "never-written")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetSupply(不存在) = %v, want store.ErrNotFound", err)
	}
}

// Server.SupplyObserved 的 apiserver 层透传已经由
// service/apiserver/supply_observed_passthrough_test.go 用真实 register+heartbeat
// 全链路钉住。这里只钉 SDK 这一层没有再引入自己的短路（比如直接 return nil），
// 因为 sdk/xflow 包内没有别的测试会调用这个方法——它是本次改动新增的方法，不存在
// 就没有测试能碰到它。
//
// newSupplyAccessTestServer 用 NewServer(ServerConfig{Store: ms})：Store 非 nil
// 时 apiserver.buildServerAPIConfig 把它同时接成 Supplies，而 apiserver 自建的
// control plane（未注入 WithControlPlane）总会带上 EntryActivationStore
// （service/apiserver/apiserver.go:319 本地路径），所以这条路径下
// supplyObserved 必然非 nil——不依赖任何我们手工补的装配。
func TestServerSupplyObservedNonNilWithStore(t *testing.T) {
	srv := newSupplyAccessTestServer(t)
	if sink := srv.SupplyObserved(); sink == nil {
		t.Fatal("SupplyObserved() = nil，但 Server 是带 Store 装配的")
	}
}

// 没有 Store 时，apiserver.Config.Supplies 保持 nil，control plane 侧的
// supplyObserved 也保持 nil；SDK 必须原样透传这个 nil，不能自己造一个空 sink
// 掩盖「模块未就绪」这件事。
func TestServerSupplyObservedNilWithoutStore(t *testing.T) {
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if sink := srv.SupplyObserved(); sink != nil {
		t.Fatalf("SupplyObserved() = %#v, want nil（未配置 Store）", sink)
	}
}
