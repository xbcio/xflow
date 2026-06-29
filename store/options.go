package store

// ListOptions controls pagination for list queries.
type ListOptions struct {
	Limit  int
	Offset int
}

// DefaultListOptions returns sensible defaults (limit=100, offset=0).
func DefaultListOptions() ListOptions {
	return ListOptions{Limit: 100, Offset: 0}
}
