package sqlstore

import (
	"math"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// unboundedOffsetLimit is the LIMIT stand-in used when a caller asks to skip
// rows without asking for a page.
//
// A zero limit means "every match", but SQL has no way to write OFFSET on its
// own: the grammar only accepts it as part of `LIMIT <n> OFFSET <m>`, and
// GORM's clause/limit.go emits a bare `OFFSET ?` when no limit is set. MySQL
// answers that with error 1064 and SQLite with a syntax error, so an
// offset-only listing (store.ListOptions{Offset: n}) failed outright instead of
// returning the rows after the offset. Substituting an upper bound no list can
// reach keeps the offset valid while preserving "return everything that
// remains" — this is a stand-in for the absent LIMIT, not a page size.
// math.MaxInt32 is accepted by both dialects and is far beyond any row count
// these tables hold; store.maxListLimit (1000) would silently truncate and is
// not exported anyway.
const unboundedOffsetLimit = math.MaxInt32

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
// A zero offset means "from the start" in both implementations and in SQL, so
// it adds no pagination clause. A non-zero offset is not passed through alone:
// SQL accepts OFFSET only as part of `LIMIT <n> OFFSET <m>` (see
// unboundedOffsetLimit), so an offset without a limit gets an unreachable
// LIMIT instead of a clause the server cannot parse.
func applyPagination(db *gorm.DB, opts store.ListOptions) *gorm.DB {
	opts = opts.Normalized()
	if opts.Limit > 0 {
		db = db.Limit(opts.Limit)
	} else if opts.Offset > 0 {
		db = db.Limit(unboundedOffsetLimit)
	}
	return db.Offset(opts.Offset)
}
