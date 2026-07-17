package store

// maxListLimit is the upper bound applied by ListOptions.Normalized to prevent
// unbounded list queries.
const maxListLimit = 1000

// ListOptions controls pagination for list queries.
type ListOptions struct {
	Limit  int
	Offset int
}

// DefaultListOptions returns sensible defaults (limit=100, offset=0).
func DefaultListOptions() ListOptions {
	return ListOptions{Limit: 100, Offset: 0}
}

// Normalized returns a copy of the options with offset/limit clamped to safe
// values: negative offset becomes 0, negative limit becomes 0 (unbounded is
// preserved only when the caller explicitly sets a limit of 0), and limit is
// capped at maxListLimit. Calling List methods should normalize their input
// before slicing or applying SQL LIMIT/OFFSET.
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
