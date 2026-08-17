package main

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// baseProductionDeps mirrors the all-present baseline in production_test.go.
// Real types, not fakes: the reconciler is the same worker type the SDK builds,
// so the "present" case here is the one production actually gets.
func baseProductionDeps() productionDeps {
	return productionDeps{
		principalAuth: apiserver.NewBearerPrincipalAuth("tok", "op", []string{"workflow"}),
		authorizer:    apiserver.NamespaceAwareAuthorizer{},
		auditSink:     apiserver.NewSQLAuditSink(nil),
		durableAudit:  true,
		reconciler:    reconcilerOrNil(control.NewAuditReconcileWorker(nil, nil, control.AuditReconcileConfig{})),
		masterKey:     true,
	}
}

// production 模式下缺 KEK 必须拒绝启动。静默降级会让落库变成明文而外表
// 一切正常 —— 与 on_invalid 那条同一个原则:声明了要加密的配置绝不能被
// 悄悄降级成不加密。
func TestProductionRequiresMasterKey(t *testing.T) {
	deps := baseProductionDeps()
	deps.masterKey = false

	err := validateProduction("production", deps)
	if err == nil {
		t.Fatal("production started without a master key; supply content would be stored in plaintext")
	}
	if !strings.Contains(err.Error(), "XFLOW_MASTER_KEY") {
		t.Errorf("error %q does not name the variable the operator must set", err)
	}
}

func TestProductionAcceptsMasterKey(t *testing.T) {
	if err := validateProduction("production", baseProductionDeps()); err != nil {
		t.Fatalf("validateProduction: %v", err)
	}
}

// dev 模式允许没有 KEK(落库明文 + 警告),否则本地开发要先造密钥。
func TestDevAllowsMissingMasterKey(t *testing.T) {
	if err := validateProduction("dev", productionDeps{}); err != nil {
		t.Fatalf("dev mode rejected a missing master key: %v", err)
	}
}
