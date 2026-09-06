package apiserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

// configuredRunnerAuth is a real authenticator, not a stub: "configured" in
// these tests must mean what control.IsConfigured means in production.
func configuredRunnerAuth(t *testing.T) control.Authenticator {
	t.Helper()
	auth, err := control.NewStaticTokenAuthenticator("runner-", "tok", []string{"default"}, nil)
	if err != nil {
		t.Fatalf("NewStaticTokenAuthenticator: %v", err)
	}
	return auth
}

// reconcilableStore is a Store that also satisfies store.AuditReconciler —
// what a *sqlstore.Provider is to the gate. The embedded nil Store supplies
// the rest of the method set; the gate only ever type-asserts, never calls.
type reconcilableStore struct {
	store.Store
}

func (reconcilableStore) ListUnreconciledAdmissions(context.Context, time.Time, uint64, int) ([]*store.AuditRecord, error) {
	return nil, nil
}

func (reconcilableStore) AppendOutcomeIfAbsent(context.Context, *store.AuditRecord) (bool, error) {
	return false, nil
}

func (reconcilableStore) CountUnreconciledAdmissions(context.Context, time.Time) (int, time.Time, error) {
	return 0, time.Time{}, nil
}

// plainStore is a Store with no reconcile capability — the in-memory dev path.
type plainStore struct {
	store.Store
}

// productionBaseline is a Config that meets every production requirement.
// Requirement tests mutate exactly one field so a failure names one cause.
func productionBaseline(t *testing.T) Config {
	t.Helper()
	return Config{
		Production: true,
		ProductionDeclaration: ProductionDeclaration{
			DurableAudit:            true,
			MultiTokenPrincipalAuth: true,
			SupplyEncryptionAtRest:  true,
		},
		Auth:          configuredRunnerAuth(t),
		Store:         reconcilableStore{},
		PrincipalAuth: NewBearerPrincipalAuth("tok", "op", []string{"workflow"}),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewSQLAuditSink(nil),
	}
}

func TestProductionBaselineIsAccepted(t *testing.T) {
	if err := validateProductionPosture(productionBaseline(t)); err != nil {
		t.Fatalf("baseline production config rejected: %v", err)
	}
}

// Each case removes exactly one requirement and asserts the gate reports that
// requirement — not merely "some error". An assertion on the error being
// non-nil would pass even if the gate tripped for an unrelated reason, which
// is how a check that no longer works keeps its test green.
func TestProductionRequiresEachComponent(t *testing.T) {
	cases := []struct {
		name string
		want ProductionRequirement
		mut  func(*Config)
	}{
		{
			name: "no runner authenticator",
			want: RequireRunnerAuth,
			mut:  func(c *Config) { c.Auth = nil },
		},
		{
			// DisabledAuthenticator is non-nil, so a nil check alone would
			// read it as configured. It authenticates nothing.
			name: "runner authenticator explicitly disabled",
			want: RequireRunnerAuth,
			mut:  func(c *Config) { c.Auth = control.DisabledAuthenticator{} },
		},
		{
			name: "no principal authenticator",
			want: RequirePrincipalAuth,
			mut:  func(c *Config) { c.PrincipalAuth = nil },
		},
		{
			name: "single shared token",
			want: RequireMultiTokenPrincipalAuth,
			mut:  func(c *Config) { c.ProductionDeclaration.MultiTokenPrincipalAuth = false },
		},
		{
			name: "no authorizer",
			want: RequireAuthorizer,
			mut:  func(c *Config) { c.Authorizer = nil },
		},
		{
			name: "no audit sink",
			want: RequireAuditSink,
			mut:  func(c *Config) { c.AuditSink = nil },
		},
		{
			name: "audit sink not durable",
			want: RequireDurableAudit,
			mut: func(c *Config) {
				c.AuditSink = NewInMemoryAuditSink()
				c.ProductionDeclaration.DurableAudit = false
			},
		},
		{
			name: "no store to reconcile against",
			want: RequireAuditReconciler,
			mut:  func(c *Config) { c.Store = nil },
		},
		{
			name: "store without reconcile capability",
			want: RequireAuditReconciler,
			mut:  func(c *Config) { c.Store = plainStore{} },
		},
		{
			name: "no supply encryption at rest",
			want: RequireSupplyEncryptionAtRest,
			mut:  func(c *Config) { c.ProductionDeclaration.SupplyEncryptionAtRest = false },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := productionBaseline(t)
			tc.mut(&cfg)

			err := validateProductionPosture(cfg)
			if err == nil {
				t.Fatalf("want %s reported unmet, got nil", tc.want)
			}
			var gate *ProductionGateError
			if !errors.As(err, &gate) {
				t.Fatalf("error %v is not a *ProductionGateError", err)
			}
			if !gate.Has(tc.want) {
				t.Fatalf("unmet = %v, want it to contain %s", gate.Unmet, tc.want)
			}
			// The reason must reach the operator: an error that names a
			// requirement slug but explains nothing is a lookup task.
			if !strings.Contains(err.Error(), string(tc.want)) {
				t.Fatalf("Error() = %q, does not name %s", err.Error(), tc.want)
			}
		})
	}
}

// Enrollment is a complete runner-auth posture on its own. Requiring a static
// policy on top would make enroll-only production unstartable, and the
// workaround — an empty policy file — is auth theater.
func TestProductionAcceptsEnrollmentAsRunnerAuth(t *testing.T) {
	cfg := productionBaseline(t)
	cfg.Auth = nil
	cfg.RegistrationCodes = control.NewMemoryRegistrationCodeStore()
	cfg.IssuedIdentities = control.NewMemoryIssuedIdentityStore()

	if err := validateProductionPosture(cfg); err != nil {
		t.Fatalf("enroll-only production rejected: %v", err)
	}
}

// Half-wired enrollment is not a posture: an authenticator with no identity
// store to authenticate against, or the reverse, authenticates nothing.
func TestProductionRejectsHalfWiredEnrollment(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
	}{
		{"codes only", func(c *Config) { c.RegistrationCodes = control.NewMemoryRegistrationCodeStore() }},
		{"identities only", func(c *Config) { c.IssuedIdentities = control.NewMemoryIssuedIdentityStore() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := productionBaseline(t)
			cfg.Auth = nil
			tc.mut(&cfg)

			err := validateProductionPosture(cfg)
			var gate *ProductionGateError
			if err == nil || !errors.As(err, &gate) || !gate.Has(RequireRunnerAuth) {
				t.Fatalf("half-wired enrollment accepted as runner auth: err = %v", err)
			}
		})
	}
}

// A nil pointer inside a non-nil interface reads as present to `!= nil`. The
// gate exists to catch a production server with nothing to settle its
// crash-orphaned admissions; a typed nil would let exactly that case through.
func TestProductionRejectsTypedNilStore(t *testing.T) {
	cfg := productionBaseline(t)
	var absent *reconcilableStorePtr
	cfg.Store = absent

	err := validateProductionPosture(cfg)
	var gate *ProductionGateError
	if err == nil || !errors.As(err, &gate) || !gate.Has(RequireAuditReconciler) {
		t.Fatalf("typed-nil store accepted: err = %v", err)
	}
}

// reconcilableStorePtr is the pointer-receiver form, so a (*T)(nil) still
// satisfies both interfaces and only a nil-value check can reject it.
type reconcilableStorePtr struct {
	store.Store
}

func (*reconcilableStorePtr) ListUnreconciledAdmissions(context.Context, time.Time, uint64, int) ([]*store.AuditRecord, error) {
	return nil, nil
}

func (*reconcilableStorePtr) AppendOutcomeIfAbsent(context.Context, *store.AuditRecord) (bool, error) {
	return false, nil
}

func (*reconcilableStorePtr) CountUnreconciledAdmissions(context.Context, time.Time) (int, time.Time, error) {
	return 0, time.Time{}, nil
}

// Production is opt-in: a config that never declares it is untouched, which is
// what keeps every existing embedder and every dev server working.
func TestNonProductionConfigIsNotGated(t *testing.T) {
	if err := validateProductionPosture(Config{}); err != nil {
		t.Fatalf("non-production empty config rejected: %v", err)
	}
}

// All unmet requirements are reported at once. Failing on the first one turns
// a mis-configured deployment into N start-fix-restart cycles.
func TestProductionReportsEveryUnmetRequirement(t *testing.T) {
	cfg := Config{Production: true}

	err := validateProductionPosture(cfg)
	var gate *ProductionGateError
	if err == nil || !errors.As(err, &gate) {
		t.Fatalf("empty production config: err = %v, want *ProductionGateError", err)
	}
	want := []ProductionRequirement{
		RequireRunnerAuth,
		RequirePrincipalAuth,
		RequireMultiTokenPrincipalAuth,
		RequireAuthorizer,
		RequireAuditSink,
		RequireDurableAudit,
		RequireAuditReconciler,
		RequireSupplyEncryptionAtRest,
	}
	if len(gate.Unmet) != len(want) {
		t.Fatalf("unmet = %v (%d), want all %d requirements", gate.Unmet, len(gate.Unmet), len(want))
	}
	for i, r := range want {
		if gate.Unmet[i] != r {
			t.Fatalf("unmet[%d] = %s, want %s (order must be stable for operator diffing)", i, gate.Unmet[i], r)
		}
	}
}

// The gate reports flags nothing: it names requirements, and cmd/server maps
// those to its own flag hints. A bearer token reaching the message would be
// logged by whatever prints the startup error.
func TestProductionErrorDoesNotLeakTokens(t *testing.T) {
	cfg := Config{
		Production:    true,
		PrincipalAuth: NewBearerPrincipalAuth("secret-tok-xyz", "op", []string{"workflow"}),
	}

	err := validateProductionPosture(cfg)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if strings.Contains(err.Error(), "secret-tok-xyz") {
		t.Fatalf("production gate error leaked a token: %v", err)
	}
}

// The gate must be reachable from the constructor every embedder passes
// through — a validator nothing calls protects nothing.
func TestNewEnforcesProductionPosture(t *testing.T) {
	_, err := New(Config{Production: true, RedisAddr: ""})
	var gate *ProductionGateError
	if err == nil || !errors.As(err, &gate) {
		t.Fatalf("New with an unmet production posture: err = %v, want *ProductionGateError", err)
	}
}

// Every requirement the gate can report must carry an explanation. A new
// requirement added to the order map without a reason would otherwise reach an
// operator as a bare slug at start-up.
func TestEveryProductionRequirementHasAReason(t *testing.T) {
	all := AllProductionRequirements()
	if len(all) == 0 {
		t.Fatal("AllProductionRequirements is empty")
	}
	for i, r := range all {
		if r == "" {
			t.Fatalf("AllProductionRequirements[%d] is empty: the order map is not densely indexed from 0", i)
		}
		if r.Reason() == "" {
			t.Errorf("requirement %q has no reason", r)
		}
	}
}

// AllProductionRequirements must cover exactly what validateProductionPosture
// can report, or a front end that iterates it would silently miss a check.
func TestAllProductionRequirementsMatchesWhatTheGateReports(t *testing.T) {
	err := validateProductionPosture(Config{Production: true})
	var gate *ProductionGateError
	if !errors.As(err, &gate) {
		t.Fatalf("empty production config: err = %v, want *ProductionGateError", err)
	}
	all := AllProductionRequirements()
	if len(all) != len(gate.Unmet) {
		t.Fatalf("AllProductionRequirements has %d entries, the gate reports %d", len(all), len(gate.Unmet))
	}
	for i := range all {
		if all[i] != gate.Unmet[i] {
			t.Fatalf("index %d: AllProductionRequirements has %s, gate reports %s", i, all[i], gate.Unmet[i])
		}
	}
}
