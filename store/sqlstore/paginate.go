package sqlstore

import (
	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// applyPagination applies a ListOptions to a query, normalizing it first.
//
// It exists because `.Limit(opts.Limit)` is wrong for the zero value and the
// mistake is invisible at the call site. store.ListOptions documents a limit of
// zero as unbounded and store/memstore implements it that way (paginate slices
// only when `Limit > 0`), but GORM treats a zero limit as a literal one:
// clause/limit.go emits LIMIT whenever the value is `>= 0`, so `LIMIT 0`
// reaches the database and it answers with no rows at all. The three list
// queries that took ListOptions each did this, so a caller passing
// store.ListOptions{} got an empty slice and no error — and an empty slice is
// exactly what "nothing matched" looks like, which is why a test asserting
// absence passes against it unconditionally.
//
// Offset is passed through as-is: a zero offset means "from the start" in both
// implementations and in SQL, so it has no equivalent ambiguity.
func applyPagination(db *gorm.DB, opts store.ListOptions) *gorm.DB {
	opts = opts.Normalized()
	if opts.Limit > 0 {
		db = db.Limit(opts.Limit)
	}
	return db.Offset(opts.Offset)
}
