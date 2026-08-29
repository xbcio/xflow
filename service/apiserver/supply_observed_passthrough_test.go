package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store/memstore"
)

// APIServer.SupplyObserved 必须返回控制面**正在写入的那一个** sink，不是一个
// 形状相同的新对象。所以这里不比对象、不读内部字段，而是驱动一次真实的
// 注册 + 心跳（心跳带 supply_observed），再从访问器读回来。访问器若返回一个
// 新建的空 sink，快照就是空的，测试红。
func TestSupplyObservedPassthroughReturnsTheLiveSink(t *testing.T) {
	supplies := memstore.New()
	// Supplies 必须同时给控制面：supplyObserved 是在 ControlPlane 装配里按
	// cfg.Supplies 建的（service/control/controlplane.go:405-413），只给
	// apiserver.Config 不会让它变成非 nil。
	//
	// 装配还要求 cfg.EntryActivationStore 非 nil（controlplane.go:406：
	// `cfg.EntryActivationStore != nil && cfg.Supplies != nil` 两者都要），否则
	// supplyObserved 仍是 nil——这一条 brief 原文没写，是读代码补上的。
	cp, err := control.NewControlPlane(control.Config{
		Backend:              local.New(),
		Supplies:             supplies,
		EntryActivationStore: control.NewMemoryEntryActivationStore(),
	})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cp.Start(ctx); err != nil {
		t.Fatalf("cp.Start: %v", err)
	}
	defer func() { _ = cp.Shutdown(context.Background()) }()

	srv, err := New(Config{
		Supplies: supplies,
		PrincipalAuth: staticPrincipalAuth{principal: Principal{
			Subject: "test-user", Namespace: "ns1", Scopes: []string{"supply.read"},
		}},
		Authorizer: ScopeAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	mux := srv.Handler()

	// 1) 注册一个 runner，拿它的 session id。
	regBody, _ := json.Marshal(protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
	})
	regRec := httptest.NewRecorder()
	mux.ServeHTTP(regRec, httptest.NewRequest(http.MethodPost, protocol.RegisterRunnerPath, bytes.NewReader(regBody)))
	if regRec.Code != http.StatusOK {
		t.Fatalf("register = %d, body=%s", regRec.Code, regRec.Body)
	}
	// runner protocol 写的是裸 JSON，不套 Envelope（service/control/server.go:191 writeJSON）。
	var reg protocol.RegisterRunnerResponse
	if err := json.Unmarshal(regRec.Body.Bytes(), &reg); err != nil {
		t.Fatalf("decode register response %s: %v", regRec.Body, err)
	}
	if reg.SessionID == "" {
		t.Fatalf("register response 没有 session_id: %s", regRec.Body)
	}

	// 2) 心跳上报「我已经应用了 rules → sha256:abc」。
	hbBody, _ := json.Marshal(protocol.HeartbeatRequest{
		RunnerID:       "runner-1",
		SessionID:      reg.SessionID,
		Capacity:       1,
		SupplyObserved: map[string]string{"rules": "sha256:abc"},
	})
	hbRec := httptest.NewRecorder()
	mux.ServeHTTP(hbRec, httptest.NewRequest(http.MethodPost, protocol.HeartbeatPath, bytes.NewReader(hbBody)))
	if hbRec.Code != http.StatusOK {
		t.Fatalf("heartbeat = %d, body=%s", hbRec.Code, hbRec.Body)
	}

	// 3) 从访问器读回来。
	sink := srv.SupplyObserved()
	if sink == nil {
		t.Fatal("SupplyObserved() = nil，但控制面是带 Supplies 装配的")
	}
	snap := sink.Snapshot()
	if got := snap["runner-1"]["rules"]; got != "sha256:abc" {
		t.Fatalf(`Snapshot()["runner-1"]["rules"] = %q, want "sha256:abc"；`+
			`访问器返回的不是控制面正在写入的那个 sink，snapshot=%#v`, got, snap)
	}
}

// 没有配 Supplies 时返回 nil 而不是 panic：嵌入方要能靠这个判断该不该渲染
// 「模块已就绪」那一列。
func TestSupplyObservedIsNilWithoutSupplies(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}
	srv, err := New(Config{AuditSink: NewInMemoryAuditSink()}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	if sink := srv.SupplyObserved(); sink != nil {
		t.Fatalf("SupplyObserved() = %#v, want nil（未配置 Supplies）", sink)
	}
}
