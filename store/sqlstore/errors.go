package sqlstore

import (
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// wrapDBErr normalizes a GORM error for a given operation. gorm.ErrRecordNotFound
// is mapped to store.ErrNotFound so callers see a consistent sentinel regardless
// of whether the missing row surfaced from a First (read) or, defensively, from
// a write path. Any other non-nil error is wrapped with the operation name and
// %w so errors.Is/As keep working. A nil err returns (nil, nil).
func wrapDBErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%s: %w", op, store.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", op, err)
}
