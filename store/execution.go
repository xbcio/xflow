package store

import (
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ErrInvalidNamespace is returned by any namespace-scoped read whose scope is
// empty or malformed. It is a fail-closed refusal, never a fallback: the
// alternative reading of an empty scope — "all namespaces" — is a cross-tenant
// enumeration, which is exactly what an unscoped GET /v1/executions would be.
var ErrInvalidNamespace = errors.New("store: invalid namespace scope")

// ValidateNamespaceScope checks a scope that arrived as a typed
// namespace.Namespace: how the API layer hands one over, straight from
// namespace.FromContext with no conversion in between.
func ValidateNamespaceScope(ns namespace.Namespace) error {
	return ValidateNamespaceScopeString(string(ns))
}

// ValidateNamespaceScopeString checks a scope held as a plain string — how it
// arrives back off a persisted row, or from any caller that has no
// namespace.Namespace to hand. Both forms must agree exactly; one of them
// guarding a query while the other does not is the failure this pair prevents.
//
// The rules mirror namespace.Validate: non-empty, at most namespace.MaxNameLen
// bytes, and free of the key-schema delimiters and Redis glob meta-characters.
// They are restated here rather than delegated, because store cannot treat
// namespace as its only authority: a value read back from a row that predates
// this package's conventions may not have passed Validate, and store must still
// refuse to widen it. Rejecting is deliberately coarser than normalizing — a
// scope is not something to repair on the caller's behalf.
func ValidateNamespaceScopeString(ns string) error {
	switch {
	case ns == "":
		return fmt.Errorf("%w: empty", ErrInvalidNamespace)
	case len(ns) > namespace.MaxNameLen:
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalidNamespace, namespace.MaxNameLen)
	case hasNamespaceDelimiter(ns):
		return fmt.Errorf("%w: contains a key-schema delimiter or glob metacharacter", ErrInvalidNamespace)
	}
	return nil
}

func hasNamespaceDelimiter(ns string) bool {
	for i := 0; i < len(ns); i++ {
		switch ns[i] {
		case ':', '{', '}', '*', '?', '[', ']', '\\':
			return true
		}
	}
	return false
}

// ExecutionFilter narrows an execution listing. It exists so that adding a
// filter is a field addition rather than a second interface method, and so that
// every backend applies the same field set.
//
// namespace is NOT a field here on purpose. It is a required scope, not an
// optional filter (the distinction store/artifact.go's ArtifactIndex draws for
// its own namespace argument): a zero-valued filter must mean "no narrowing
// within this namespace", and a filter whose namespace defaulted to "" could
// only either fail or widen. ListExecutions and CountExecutions take it as a
// separate, validation-guarded parameter instead.
//
// Only fields the persisted row genuinely supports are here. See the
// ListExecutions doc in store/interfaces.go for the filter that is still
// rejected and the column-level reason.
type ExecutionFilter struct {
	// Status narrows to one lifecycle status. Empty means "every status".
	// A non-empty value must be one of the types.ExecutionStatus constants; an
	// unrecognized one is rejected rather than matched, because a typo would
	// otherwise return an empty page that is indistinguishable from "no
	// executions match a real status".
	Status types.ExecutionStatus
	// WorkflowID narrows to one workflow by its server-assigned id, matched by
	// EXACT equality. Empty means "every workflow".
	//
	// A row whose stored WorkflowID is empty (unattributed: written before the
	// column existed, or submitted without a definition id) matches NO non-empty
	// filter value. That is the same fail-closed choice Namespace makes: the
	// filter never guesses an owner, it only narrows within one it can prove.
	WorkflowID types.WorkflowID
	// CreatedAfter and CreatedBefore bound created_at. Zero means unbounded on
	// that side. The bounds are exclusive on both ends — an execution created
	// exactly at a bound is not returned — so that a caller walking adjacent
	// windows ([t0,t1), [t1,t2)) cannot double-count a row that lands on t1.
	CreatedAfter  time.Time
	CreatedBefore time.Time
}

// MaxExecutionWorkflowIDLen is the declared width of the persisted
// workflow_id column (VARCHAR(64)). A value longer than this can never equal a
// stored id, and MySQL would silently truncate the comparison rather than
// report the mistake, so it is refused at the boundary instead. The bound is
// counted in characters, matching VARCHAR(64) and the OpenAPI maxLength, not
// in bytes.
const MaxExecutionWorkflowIDLen = 64

// ErrInvalidWorkflowID is returned when an execution row is written with a
// workflow id the persistence column cannot hold. It is a fail-closed refusal:
// silently truncating the identity would attribute the execution to a
// different (or no) workflow.
var ErrInvalidWorkflowID = errors.New("store: invalid execution workflow id")

// ValidateExecutionWorkflowID checks a workflow id about to be persisted on an
// execution row. Empty is valid and means UNATTRIBUTED; the value is otherwise
// opaque to this package (ids are server-assigned) except for the column bound
// above.
func ValidateExecutionWorkflowID(id string) error {
	if utf8.RuneCountInString(id) > MaxExecutionWorkflowIDLen {
		return fmt.Errorf("%w: longer than %d characters", ErrInvalidWorkflowID, MaxExecutionWorkflowIDLen)
	}
	return nil
}

// ValidateExecutionWorkflowIDFilter checks a workflow id used to narrow an
// execution listing. Empty is valid and means "no workflow narrowing"; the
// value is otherwise opaque to this package (ids are server-assigned) except
// for the column bound above.
func ValidateExecutionWorkflowIDFilter(id string) error {
	if err := ValidateExecutionWorkflowID(id); err != nil {
		return fmt.Errorf("%w (as a listing filter)", err)
	}
	return nil
}

// Validate reports whether the filter can be applied as written. Both backends
// call it before touching their storage, so an unusable filter gets one answer
// rather than one per backend — and so a status typo fails loudly instead of
// reading back as "no executions match".
func (f ExecutionFilter) Validate() error {
	if f.Status != "" && !knownExecutionStatus(f.Status) {
		return fmt.Errorf("store: unknown execution status filter %q", f.Status)
	}
	return ValidateExecutionWorkflowIDFilter(string(f.WorkflowID))
}

func knownExecutionStatus(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusPending,
		types.ExecutionStatusQueued,
		types.ExecutionStatusPaused,
		types.ExecutionStatusRunning,
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceling,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout:
		return true
	}
	return false
}

// ExecutionOrder documents the order every execution listing is returned in.
// It is a constant string rather than prose because both backends must produce
// it and a divergence between them is the kind of bug that only shows up as a
// duplicated row on page 2 of a real deployment.
//
// newest first, created_at DESC then id DESC. created_at is DATETIME(3) and
// GORM stamps it client-side, so two executions created in the same millisecond
// compare equal; id is the AUTO_INCREMENT primary key, hence unique and
// strictly increasing, and it is the tiebreak that makes the order TOTAL.
// A total order is what makes offset pagination sound: with it, page N and page
// N+1 of one unchanging result set partition the rows exactly (no row appears
// twice, none is skipped), because "the row at offset k" has one answer.
// Ordering by a non-unique column alone does not give that — MySQL is free to
// return same-key rows in any order, and it may pick different orders for two
// LIMIT/OFFSET queries in the same session.
//
// created_at DESC rather than id DESC alone: an id order would be equivalent
// for rows this process inserted, but not for a restore/import that reseeds
// ids, and "newest first" is what a list page means to a reader. The id
// tiebreak is what carries the soundness.
const ExecutionOrder = "created_at DESC, id DESC"
