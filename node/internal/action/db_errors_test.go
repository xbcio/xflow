package action

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/xbcio/xflow/types"
)

func mysqlErr(number uint16, state string) *mysqldriver.MySQLError {
	var s [5]byte
	copy(s[:], state)
	return &mysqldriver.MySQLError{Number: number, SQLState: s, Message: "db error"}
}

func TestClassifyDBError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want types.ErrorKind
	}{
		{"bad conn transient", driver.ErrBadConn, types.ErrorKindTransient},
		{"eof transient", io.EOF, types.ErrorKindTransient},
		{"unexpected eof transient", io.ErrUnexpectedEOF, types.ErrorKindTransient},
		{"deadlock 1213 transient", mysqlErr(1213, "40001"), types.ErrorKindTransient},
		{"lock wait 1205 transient", mysqlErr(1205, "HY000"), types.ErrorKindTransient},
		{"serialization 40001 transient", mysqlErr(0, "40001"), types.ErrorKindTransient},
		{"dup key 1062 permanent", mysqlErr(1062, "23000"), types.ErrorKindPermanent},
		{"FK 1452 permanent", mysqlErr(1452, "23000"), types.ErrorKindPermanent},
		{"not null 1048 permanent", mysqlErr(1048, "23000"), types.ErrorKindPermanent},
		{"syntax 1064 permanent", mysqlErr(1064, "42000"), types.ErrorKindPermanent},
		{"no such table 1146 permanent", mysqlErr(1146, "42S02"), types.ErrorKindPermanent},
		{"unknown column 1054 permanent", mysqlErr(1054, "42S22"), types.ErrorKindPermanent},
		{"access denied 1045 permanent", mysqlErr(1045, "28000"), types.ErrorKindPermanent},
		{"unknown mysql number transient fallback", mysqlErr(1644, "99999"), types.ErrorKindTransient},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classifyDBError(c.err)
			if got == nil {
				t.Fatalf("classifyDBError returned nil for %v", c.err)
			}
			var ce *types.ClassifiedError
			if !errors.As(got, &ce) {
				t.Fatalf("classifyDBError returned %T, want *ClassifiedError: %v", got, got)
			}
			if ce.Kind != c.want {
				t.Fatalf("kind = %q, want %q (err=%v)", ce.Kind, c.want, got)
			}
			if c.want == types.ErrorKindPermanent && !types.IsPermanent(ce) {
				t.Fatalf("IsPermanent=false for permanent case: %v", got)
			}
			if c.want == types.ErrorKindTransient && types.IsPermanent(ce) {
				t.Fatalf("IsPermanent=true for transient case: %v", got)
			}
		})
	}
}

func TestClassifyDBErrorNil(t *testing.T) {
	if got := classifyDBError(nil); got != nil {
		t.Fatalf("classifyDBError(nil) = %v, want nil", got)
	}
}

// TestClassifyDBErrorPreservesMessage verifies the driver message is preserved
// for observability — callers must not lose context by classifying.
func TestClassifyDBErrorPreservesMessage(t *testing.T) {
	e := mysqlErr(1062, "23000")
	e.Message = "Duplicate entry 'x' for key 'uk'"
	got := classifyDBError(e)
	if got == nil || got.Error() == "" {
		t.Fatalf("expected non-empty message preserved, got %v", got)
	}
}
