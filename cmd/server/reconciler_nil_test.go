package main

import (
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// The reconciler now comes from the SDK as a *control.AuditReconcileWorker,
// which is nil when no durable audit store is configured. productionDeps holds
// it behind an interface, and a nil pointer in an interface is NOT a nil
// interface: `deps.reconciler != nil` is true for a (*T)(nil).
//
// That would turn the production gate into a no-op for exactly the case it
// exists to catch — a production server with a durable audit sink and nothing
// to settle its crash-orphaned admissions would start clean, with the check
// reporting the reconciler as present.
func TestProductionRejectsATypedNilReconciler(t *testing.T) {
	var absent *control.AuditReconcileWorker // what newAuditReconciler returns with no store

	deps := productionDeps{
		principalAuth: apiserver.NewBearerPrincipalAuth("tok", "op", []string{"workflow"}),
		authorizer:    apiserver.NamespaceAwareAuthorizer{},
		auditSink:     apiserver.NewSQLAuditSink(nil),
		durableAudit:  true,
		reconciler:    reconcilerOrNil(absent),
		masterKey:     true,
	}
	if err := validateProduction("production", deps); err == nil {
		t.Fatal("production accepted an absent reconciler: a nil *AuditReconcileWorker " +
			"stored in the interface reads as present, so the fail-closed check that " +
			"exists to catch an unsettled audit backlog passes unconditionally")
	}
}

// And a real worker must still satisfy it, so the guard is not a blanket
// rejection that would make production unstartable.
func TestProductionAcceptsARealReconciler(t *testing.T) {
	worker := control.NewAuditReconcileWorker(nil, nil, control.AuditReconcileConfig{})

	deps := productionDeps{
		principalAuth:        apiserver.NewBearerPrincipalAuth("tok", "op", []string{"workflow"}),
		authorizer:           apiserver.NamespaceAwareAuthorizer{},
		auditSink:            apiserver.NewSQLAuditSink(nil),
		durableAudit:         true,
		reconciler:           reconcilerOrNil(worker),
		masterKey:            true,
		runnerAuthConfigured: true,
	}
	if err := validateProduction("production", deps); err != nil {
		t.Fatalf("production rejected a real reconciler: %v", err)
	}
}
