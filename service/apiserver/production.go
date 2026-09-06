package apiserver

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

// ProductionRequirement names one thing a production deployment must have.
//
// The gate reports requirements rather than remediation because remediation is
// front-end specific: cmd/server tells an operator which flag to pass, an
// embedder such as an SDK host tells them which option to set, and neither
// vocabulary is meaningful to the other. See ProductionGateError.
type ProductionRequirement string

const (
	// RequireRunnerAuth: the runner protocol authenticates its callers.
	// Without it any process that can reach the server registers as a runner
	// and claims work — which means executing whatever workflows are queued.
	RequireRunnerAuth ProductionRequirement = "runner-auth"
	// RequirePrincipalAuth: the workflow/management API resolves callers to a
	// principal. Without it the API — which registers workflow definitions and
	// seeds executions — accepts anonymous callers.
	RequirePrincipalAuth ProductionRequirement = "principal-auth"
	// RequireMultiTokenPrincipalAuth: that principal authenticator binds each
	// token to its own subject, namespace and scopes. A single shared token
	// self-grants every scope, so possession of it is total authority.
	RequireMultiTokenPrincipalAuth ProductionRequirement = "multi-token-principal-auth"
	// RequireAuthorizer: an authorizer decides allow/deny per operation.
	RequireAuthorizer ProductionRequirement = "authorizer"
	// RequireAuditSink: mutations are audited before they execute.
	RequireAuditSink ProductionRequirement = "audit-sink"
	// RequireDurableAudit: that audit survives a restart. An in-memory sink
	// satisfies RequireAuditSink and still loses the record of every mutation
	// when the process exits, which is when the record matters most.
	RequireDurableAudit ProductionRequirement = "durable-audit"
	// RequireAuditReconciler: the store can settle admissions orphaned by a
	// crash between a successful mutation and its outcome row. Without it
	// those rows accumulate as permanently-pending admissions that nothing
	// resolves, visible only by querying the audit table by hand.
	RequireAuditReconciler ProductionRequirement = "audit-reconciler"
	// RequireSupplyEncryptionAtRest: supply content is encrypted in the store.
	// Supply content carries credentials; unencrypted, a store dump is a
	// credential dump.
	RequireSupplyEncryptionAtRest ProductionRequirement = "supply-encryption-at-rest"
)

// productionRequirementReason explains each requirement in terms of what goes
// wrong without it. The text names no flag and no option: it is read by
// operators of every front end.
var productionRequirementReason = map[ProductionRequirement]string{
	RequireRunnerAuth:              "runner-protocol authentication is not configured; any process that can reach the server could register as a runner and claim work",
	RequirePrincipalAuth:           "no PrincipalAuthenticator; the workflow API would accept anonymous callers, and a submitted workflow runs on every connected runner",
	RequireMultiTokenPrincipalAuth: "the PrincipalAuthenticator is a single shared token that self-grants every scope; production needs a per-token subject/namespace/scope registry",
	RequireAuthorizer:              "no Authorizer; per-operation authorization would not be enforced",
	RequireAuditSink:               "no AuditSink; mutations would execute unaudited",
	RequireDurableAudit:            "the AuditSink is not durable; the record of every mutation would be lost on restart",
	RequireAuditReconciler:         "the Store cannot reconcile audit rows; admissions orphaned by a crash would stay pending forever",
	RequireSupplyEncryptionAtRest:  "supply content encryption at rest is not enabled; supply content, which carries credentials, would be stored in plaintext",
}

// Reason explains, in front-end-neutral terms, what goes wrong when r is not
// met. Front ends compose this with their own remediation (a flag name, an
// option name) rather than restating it.
func (r ProductionRequirement) Reason() string { return productionRequirementReason[r] }

// AllProductionRequirements returns every requirement the gate checks, in
// report order. Front ends that map requirements to their own remediation
// text use this to prove the map is exhaustive — a requirement added here and
// forgotten there would otherwise surface as an empty hint at start-up, in
// front of the operator who can least afford it.
func AllProductionRequirements() []ProductionRequirement {
	all := make([]ProductionRequirement, len(productionRequirementOrder))
	for r, i := range productionRequirementOrder {
		all[i] = r
	}
	return all
}

// ProductionDeclaration states the facts about caller-supplied dependencies
// that New cannot determine by looking at them.
//
// Each field records how the caller BUILT a dependency, not a property the
// dependency exposes. NewBearerPrincipalAuth and NewBearerPrincipalAuthMulti
// return the same type; a durable audit sink and an in-memory one both satisfy
// AuditSink; and at-rest encryption is a store construction option that the
// store.Store interface does not surface. Sniffing concrete types instead
// would make every custom implementation fail for being unrecognized.
//
// The zero value is the safe one: nothing declared means nothing granted.
type ProductionDeclaration struct {
	// DurableAudit: the AuditSink is backed by durable storage.
	DurableAudit bool
	// MultiTokenPrincipalAuth: the PrincipalAuthenticator resolves each token
	// to its own subject/namespace/scopes, rather than being one shared token.
	MultiTokenPrincipalAuth bool
	// SupplyEncryptionAtRest: the Store was built with supply content
	// encryption (a master key was loaded and threaded into it).
	SupplyEncryptionAtRest bool
}

// ProductionGateError reports every production requirement a Config fails to
// meet. All of them, not the first: a server that reports one missing piece
// per start turns a mis-configured deployment into N start-fix-restart cycles.
//
// Front ends translate Unmet into their own vocabulary — a flag name, an
// option name — and should do so rather than printing Error() verbatim, whose
// text is deliberately front-end neutral.
type ProductionGateError struct {
	Unmet []ProductionRequirement
}

func (e *ProductionGateError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiserver: production posture not met (%d requirement(s)):", len(e.Unmet))
	for _, r := range e.Unmet {
		fmt.Fprintf(&b, "\n  - %s: %s", r, productionRequirementReason[r])
	}
	return b.String()
}

// Has reports whether r is among the unmet requirements.
func (e *ProductionGateError) Has(r ProductionRequirement) bool {
	if e == nil {
		return false
	}
	for _, got := range e.Unmet {
		if got == r {
			return true
		}
	}
	return false
}

// AuditReconcilable reports whether st can settle crash-orphaned audit
// admissions. It is one function with two call sites — this gate, and whatever
// builds the reconcile worker — because the two must never disagree: a server
// that passes the gate but builds no worker has a pending-audit backlog that
// nothing will ever drain, and each site would consider itself correct.
//
// The nil check is by value, not by interface: a (*sqlstore.Provider)(nil)
// stored in a store.Store is a non-nil interface holding a nil pointer, and it
// satisfies store.AuditReconciler. Reading that as "reconcilable" would make
// this check pass for exactly the case it exists to catch.
func AuditReconcilable(st store.Store) bool {
	if isNilValue(st) {
		return false
	}
	_, ok := st.(store.AuditReconciler)
	return ok
}

// isNilValue reports whether v is nil, including a nil pointer, map, slice,
// func or channel held inside a non-nil interface.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
		return rv.IsNil()
	default:
		return false
	}
}

// validateProductionPosture reports every production requirement cfg fails to
// meet, or nil when cfg is not a production config or meets them all.
//
// This is the single implementation of the production posture. cmd/server and
// sdk/xflow are both façades over apiserver.New, so putting the checks here
// rather than in either one is what keeps them from drifting apart — an
// embedder and an operator get the same guarantees because they run the same
// code, not because two lists were kept in sync by hand.
func validateProductionPosture(cfg Config) error {
	if !cfg.Production {
		return nil
	}
	decl := cfg.ProductionDeclaration

	var unmet []ProductionRequirement
	add := func(ok bool, r ProductionRequirement) {
		if !ok {
			unmet = append(unmet, r)
		}
	}

	// Runner auth is satisfied by either a configured authenticator or
	// enrollment. Requiring a static policy on top of enrollment would make
	// enroll-only production unstartable, and the workaround — an empty policy
	// file — is auth theater. IsConfigured, not a nil check: DisabledAuthenticator
	// is a non-nil Authenticator that authenticates nothing.
	add(control.IsConfigured(cfg.Auth) ||
		control.EnrollDeclared(cfg.RegistrationCodes, cfg.IssuedIdentities),
		RequireRunnerAuth)

	add(cfg.PrincipalAuth != nil, RequirePrincipalAuth)
	add(decl.MultiTokenPrincipalAuth, RequireMultiTokenPrincipalAuth)
	add(cfg.Authorizer != nil, RequireAuthorizer)
	add(cfg.AuditSink != nil, RequireAuditSink)
	add(decl.DurableAudit, RequireDurableAudit)
	add(AuditReconcilable(cfg.Store), RequireAuditReconciler)
	add(decl.SupplyEncryptionAtRest, RequireSupplyEncryptionAtRest)

	if len(unmet) == 0 {
		return nil
	}
	sort.SliceStable(unmet, func(i, j int) bool {
		return productionRequirementOrder[unmet[i]] < productionRequirementOrder[unmet[j]]
	})
	return &ProductionGateError{Unmet: unmet}
}

// productionRequirementOrder fixes the reported order so a operator diffing two
// startup failures sees only what actually changed.
var productionRequirementOrder = map[ProductionRequirement]int{
	RequireRunnerAuth:              0,
	RequirePrincipalAuth:           1,
	RequireMultiTokenPrincipalAuth: 2,
	RequireAuthorizer:              3,
	RequireAuditSink:               4,
	RequireDurableAudit:            5,
	RequireAuditReconciler:         6,
	RequireSupplyEncryptionAtRest:  7,
}
