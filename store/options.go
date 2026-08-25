package store

// maxListLimit is the ceiling Normalized applies to an explicit positive limit.
// It does not make every list query bounded: a limit of zero means unbounded and
// is left alone, so this caps callers who asked for a page, not callers who
// asked for everything.
const maxListLimit = 1000

// ListOptions controls pagination for list queries.
type ListOptions struct {
	// Limit is the maximum number of records to return. Zero means unbounded —
	// implementations must return every match rather than none. That is not the
	// natural reading of a SQL LIMIT, and store/sqlstore got it wrong on all
	// three of its paginated queries by passing the zero straight through to
	// GORM, which emits a literal `LIMIT 0`.
	Limit  int
	Offset int
}

// DefaultListOptions returns sensible defaults (limit=100, offset=0).
func DefaultListOptions() ListOptions {
	return ListOptions{Limit: 100, Offset: 0}
}

// Normalized returns a copy of the options with offset and limit clamped: a
// negative offset becomes 0, a negative limit becomes 0, and a positive limit is
// capped at maxListLimit. List implementations should normalize before slicing
// or building a SQL LIMIT/OFFSET.
//
// Note that clamping a negative limit to 0 makes it UNBOUNDED, since 0 is the
// unbounded sentinel — the two are indistinguishable afterwards. A caller who
// passes -1 meaning "no results" gets the whole table. Nothing in the tree does
// that today, but a normalization that turns a nonsense value into the widest
// possible query is worth knowing about before adding a caller that computes its
// limit rather than writing it as a literal.

func (o ListOptions) Normalized() ListOptions {
	if o.Offset < 0 {
		o.Offset = 0
	}
	if o.Limit < 0 {
		o.Limit = 0
	}
	if o.Limit > maxListLimit {
		o.Limit = maxListLimit
	}
	return o
}
