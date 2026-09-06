package xflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

// reconcilableTestStore stands in for a *sqlstore.Provider: a Store that also
// satisfies store.AuditReconciler, which is what makes the audit-reconciler
// requirement satisfiable and what makes newAuditReconciler build a worker.
type reconcilableTestStore struct {
	store.Store
}

func (reconcilableTestStore) ListUnreconciledAdmissions(context.Context, time.Time, uint64, int) ([]*store.AuditRecord, error) {
	return nil, nil
}

func (reconcilableTestStore) AppendOutcomeIfAbsent(context.Context, *store.AuditRecord) (bool, error) {
	return false, nil
}

func (reconcilableTestStore) CountUnreconciledAdmissions(context.Context, time.Time) (int, time.Time, error) {
	return 0, time.Time{}, nil
}

// The production posture is defined and tested in service/apiserver. What this
// file proves is that WithServerProduction actually reaches it: an option that
// sets a field nothing forwards would leave every embedder unprotected while
// reading, at the call site, exactly like protection.

// TestWithServerProductionReachesTheGate: a server that declares production
// but supplies none of what production needs must not be constructible.
func TestWithServerProductionReachesTheGate(t *testing.T) {
	_, err := NewServer(ServerConfig{},
		WithServerInsecureNoRunnerAuth(),
		WithServerProduction(apiserver.ProductionDeclaration{}))

	var gate *apiserver.ProductionGateError
	if !errors.As(err, &gate) {
		t.Fatalf("NewServer with WithServerProduction and nothing configured: err = %v, want *apiserver.ProductionGateError", err)
	}
	// Spot-check that the reported requirements are this config's, not a
	// constant: insecure-no-runner-auth is exactly what production forbids.
	if !gate.Has(apiserver.RequireRunnerAuth) {
		t.Fatalf("unmet = %v, want it to include %s", gate.Unmet, apiserver.RequireRunnerAuth)
	}
}

// The declaration must travel, not just the on/off bit. A declaration that was
// dropped on the way through would make every declared fact read as false, and
// the resulting server would be unstartable for reasons the embedder already
// addressed.
func TestServerProductionDeclarationIsForwarded(t *testing.T) {
	sc := &serverConfig{}
	WithServerProduction(apiserver.ProductionDeclaration{
		DurableAudit:            true,
		MultiTokenPrincipalAuth: true,
		SupplyEncryptionAtRest:  true,
	})(sc)

	got := buildServerAPIConfig(ServerConfig{}, sc)
	if !got.Production {
		t.Fatal("Production not forwarded to apiserver.Config")
	}
	if got.ProductionDeclaration != (apiserver.ProductionDeclaration{
		DurableAudit:            true,
		MultiTokenPrincipalAuth: true,
		SupplyEncryptionAtRest:  true,
	}) {
		t.Fatalf("ProductionDeclaration forwarded as %+v, want all three facts", got.ProductionDeclaration)
	}
}

// Without WithServerProduction nothing changes: every existing embedder and
// every test that builds a server keeps working.
func TestServerWithoutProductionOptionIsNotGated(t *testing.T) {
	sc := &serverConfig{}
	if got := buildServerAPIConfig(ServerConfig{}, sc); got.Production {
		t.Fatal("Production is on without WithServerProduction")
	}
}

// A server that meets the posture must still be constructible — a gate that
// rejects everything is indistinguishable from a broken one until someone
// tries to deploy.
func TestProductionServerWithFullPostureConstructs(t *testing.T) {
	auth, err := control.NewStaticTokenAuthenticator("runner-", "tok", []string{"default"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(ServerConfig{Store: reconcilableTestStore{}},
		WithServerAuth(auth),
		WithServerPrincipalAuth(
			apiserver.NewBearerPrincipalAuthMulti(nil),
			apiserver.NamespaceAwareAuthorizer{},
			apiserver.NewSQLAuditSink(nil),
		),
		WithServerProduction(apiserver.ProductionDeclaration{
			DurableAudit:            true,
			MultiTokenPrincipalAuth: true,
			SupplyEncryptionAtRest:  true,
		}))
	if err != nil {
		t.Fatalf("fully-configured production server rejected: %v", err)
	}
	if srv == nil {
		t.Fatal("NewServer returned nil without an error")
	}
	// The gate accepted the store as reconcilable, so the SDK must have built
	// the worker that acts on it. Passing the gate and building no worker
	// leaves a pending-audit backlog nothing ever drains.
	if srv.Reconciler() == nil {
		t.Fatal("production server passed the audit-reconciler requirement but built no reconcile worker")
	}
}
