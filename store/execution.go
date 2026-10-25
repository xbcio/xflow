package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	//
	// Statuses is the multi-select form; at most one of the two may be set.
	Status types.ExecutionStatus
	// Statuses narrows to ANY of the listed lifecycle statuses (SQL IN).
	// Empty means "no status narrowing". Every member must be a known status;
	// duplicates are harmless (they cannot change an IN set) and an unknown
	// member is rejected for the same reason a single unknown Status is.
	Statuses []types.ExecutionStatus
	// Query narrows to executions whose EXECUTION ID or WORKFLOW ID starts with
	// this value. The two column predicates are ORed: one search box answers
	// "which execution is this, or which runs belong to this workflow?" without
	// asking the caller to know which identity they hold. Each side is matched
	// as a PREFIX (`LIKE 'value%'` with the pattern escaped) and each identity
	// is an opaque server-assigned string. Empty means no narrowing.
	//
	// The value is restricted to the id alphabet (letters, digits, '_' and '-')
	// and the persisted column width, so it can only ever be a prefix of an id
	// this package writes. Both backends still bind it as a parameter with LIKE
	// metacharacters escaped; the alphabet is the second line of defense, not
	// the first. A row whose workflow id is the unattributed sentinel "" can
	// still match through its execution id, but never through the empty
	// workflow id — and Query never matches the empty string, because an empty
	// Query means "no narrowing".
	Query string
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

// MaxExecutionIDQueryLen is the longest execution search PREFIX (the `q`
// parameter) a listing filter accepts. Both identity columns are VARCHAR(64), so
// a longer value can never be a prefix of a stored id; refusing it keeps the
// refusal a 400 at the API layer instead of an expensive no-op scan.
const MaxExecutionIDQueryLen = 64

// ErrInvalidExecutionIDQuery is returned when a `q` prefix filter cannot name a
// stored identity: it is too long, or contains a character outside the id
// alphabet. It is a caller-input refusal (HTTP 400), never a storage error.
var ErrInvalidExecutionIDQuery = errors.New("store: invalid execution id query")

// ValidateExecutionID checks an execution id used as a batch lookup key. Empty
// and over-long ids name no persisted row (the column is VARCHAR(64)) and are
// meant to be dropped before building an IN list. Unlike ValidateExecutionQuery
// it does NOT restrict the alphabet: stored ids include server-minted forms
// beyond the simple "exec-…" shape, and a lookup by identity must not
// re-litigate how the id was minted.
func ValidateExecutionID(id string) error {
	if utf8.RuneCountInString(id) > MaxExecutionIDQueryLen {
		return fmt.Errorf("%w: longer than %d characters", ErrInvalidExecutionIDQuery, MaxExecutionIDQueryLen)
	}
	return nil
}

// ValidateExecutionQuery checks a `q` search filter. Empty is valid and means
// "no prefix narrowing"; otherwise the value must be at most
// MaxExecutionIDQueryLen bytes drawn from [A-Za-z0-9_-], the alphabet execution
// and workflow ids are minted from. Restricting the alphabet is what keeps LIKE
// metacharacters out in the first place; EscapeLikePrefix remains a second line
// of defense for any caller that skips this check.
func ValidateExecutionQuery(query string) error {
	if len(query) > MaxExecutionIDQueryLen {
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalidExecutionIDQuery, MaxExecutionIDQueryLen)
	}
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return fmt.Errorf("%w: character %q is not allowed", ErrInvalidExecutionIDQuery, c)
		}
	}
	return nil
}

// EscapeLikePrefix escapes the LIKE metacharacters in s so it matches only
// itself as a literal prefix. The escape character is '!' rather than the
// conventional backslash because MySQL and SQLite disagree about how a
// backslash is written in a string literal, while both accept an explicit
// `ESCAPE '!'` clause. Callers append '%' after this function returns.
func EscapeLikePrefix(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '!', '%', '_':
			b.WriteByte('!')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// ExecutionQueryLikePattern builds the bound LIKE pattern for a validated
// search prefix: the escaped prefix followed by '%'. The same pattern is bound
// to both identity columns, so a LIKE metacharacter can never be interpreted on
// either side of the OR.
func ExecutionQueryLikePattern(query string) string {
	return EscapeLikePrefix(query) + "%"
}

// Validate reports whether the filter can be applied as written. Both backends
// call it before touching their storage, so an unusable filter gets one answer
// rather than one per backend — and so a status typo fails loudly instead of
// reading back as "no executions match".
func (f ExecutionFilter) Validate() error {
	if f.Status != "" && len(f.Statuses) > 0 {
		return errors.New("store: status and statuses are mutually exclusive filters")
	}
	if f.Status != "" && !knownExecutionStatus(f.Status) {
		return fmt.Errorf("store: unknown execution status filter %q", f.Status)
	}
	for _, status := range f.Statuses {
		if !knownExecutionStatus(status) {
			return fmt.Errorf("store: unknown execution status filter %q", status)
		}
	}
	if err := ValidateExecutionQuery(f.Query); err != nil {
		return err
	}
	return ValidateExecutionWorkflowIDFilter(string(f.WorkflowID))
}

// StatusSet returns the matchable statuses of the filter as a slice: Statuses
// when the multi-select form is used, otherwise the single Status when set, and
// nil when no status narrowing applies. Both backends call this so the two
// forms can never diverge.
func (f ExecutionFilter) StatusSet() []types.ExecutionStatus {
	if len(f.Statuses) > 0 {
		return f.Statuses
	}
	if f.Status != "" {
		return []types.ExecutionStatus{f.Status}
	}
	return nil
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

// LatestExecutionReader is an optional capability of an execution store: the
// batch read behind a workflow list's "latest run" column.
//
// It is deliberately NOT a method on Executions. Adding it there would oblige
// every store.Store implementation and every test double to grow a method that
// only the workflow-list endpoint calls; a backend that cannot answer it can
// omit it, and the caller type-asserts. The price of that choice is that "this
// backend supports the column" is invisible from the required interface alone,
// so every backend that implements it MUST be run through
// store/storetest.LatestExecutionsContract. The rules that contract pins —
// namespace scope, newest-first meaning, and the total tie-break — are what
// keep this column from disagreeing with ListExecutions about which rows exist.
type LatestExecutionReader interface {
	// LatestExecutions returns, for each requested workflow id, the newest
	// execution in ns. Newest means CreatedAt descending, ties broken by
	// ExecutionID ascending so repeated reads are deterministic. Ids with no
	// execution in ns are absent from the map (not an error). An empty ids
	// slice returns an empty map and nil error.
	//
	// ns is a REQUIRED SCOPE with the same fail-closed rule as ListExecutions:
	// empty/malformed => ErrInvalidNamespace, and it never widens across
	// namespaces. The scope is validated even when ids is empty, so "nothing
	// to look up" is never an answer to an unusable scope. A row whose stored
	// namespace is empty (unattributed) matches no scope, exactly as it matches
	// no ListExecutions scope.
	//
	// An id longer than MaxExecutionWorkflowIDLen can match nothing and must
	// not be passed to SQL. An empty id names no workflow either — it could
	// only select unattributed rows, which no workflow lookup may claim — so
	// both are dropped before storage access and are therefore absent from the
	// result.
	//
	// The returned map is always non-nil, including for an empty request:
	// callers index it without a nil check. The map is keyed by the requested
	// id, and each value is a snapshot, not a handle into the store.
	//
	// The record is a NARROW projection: ExecutionID, Namespace, WorkflowID,
	// Status, CreatedAt and UpdatedAt are populated; every other field —
	// WorkflowName, WorkflowDef, Params, Runtime, TraceID, SpanID, Error, ID —
	// is not part of this contract and may be zero. The narrow shape is what
	// lets a SQL backend answer a 200-workflow page without reading the
	// definition/params/runtime JSON columns; a caller that needs them reads the
	// execution directly.
	LatestExecutions(ctx context.Context, ns namespace.Namespace, workflowIDs []types.WorkflowID) (map[types.WorkflowID]*ExecutionRecord, error)
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
