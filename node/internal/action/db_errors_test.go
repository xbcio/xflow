package action

import (
	"database/sql/driver"
	"errors"
	"io"
	"strings"
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
		// 1051 is the one entry in the permanent number list that nothing else
		// rescues. The SQLState switch above it matches only "40001", "23000",
		// "23001", "42000" — real MySQL sends 1051 with 42S02, which none of
		// those cover, so deleting 1051 from db_errors.go:81 drops it straight
		// through to the conservative transient fallback. "Unknown table" is a
		// schema fact that will be just as untrue on the next attempt: the node
		// would then be retried to exhaustion instead of failing once.
		//
		// The neighbouring uncovered numbers (1451, 1061, 1586) are NOT added
		// here: their real SQLStates are 23000/42000, which the state switch
		// already classifies permanent, so a row for them could not fail.
		{"unknown table 1051 permanent", mysqlErr(1051, "42S02"), types.ErrorKindPermanent},
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
//
// This used to be the whole coverage of the message content, and it asserted
// only that the result was non-empty. That is a shape it cannot fail: it built
// the exact duplicate-key message that leaks a column value, and passed. See
// TestClassifyDBErrorRedactsDuplicateEntryValue for the assertion that has
// teeth; this one is kept for the case where nothing is redacted at all.
func TestClassifyDBErrorPreservesMessage(t *testing.T) {
	e := mysqlErr(1054, "42S22")
	e.Message = "Unknown column 'email' in 'field list'"
	got := classifyDBError(e)
	if got == nil {
		t.Fatal("expected a classified error")
	}
	// An identifier, not a value: it must survive whole.
	if !strings.Contains(got.Error(), "Unknown column 'email' in 'field list'") {
		t.Fatalf("the driver message was altered for an error that quotes only "+
			"identifiers: %v", got)
	}
}

// TestClassifyDBErrorRedactsDuplicateEntryValue pins the one MySQL message that
// quotes data rather than schema.
//
// The value in a duplicate-key error is whatever the workflow tried to insert,
// which for a workflow inserting an upstream node's output is that output. The
// committed error text reaches xflow_executions.error_msg and the body of GET
// /v1/executions/{id}, so this is the string that has to not contain it.
func TestClassifyDBErrorRedactsDuplicateEntryValue(t *testing.T) {
	const collided = "alice@example.test"

	for _, tc := range []struct {
		name string
		msg  string
	}{
		{"plain", "Duplicate entry '" + collided + "' for key 'users.uk_email'"},
		// MySQL does not escape the value, so an apostrophe inside it produces
		// a message with three quoted runs. A leftmost scan ends at the wrong
		// one and leaves the tail of the value in place.
		{"value contains a quote", "Duplicate entry 'O'Brien " + collided + "' for key 'users.uk_email'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := mysqlErr(1062, "23000")
			e.Message = tc.msg
			got := classifyDBError(e)
			if got == nil {
				t.Fatal("expected a classified error")
			}
			// This is the exact string engine.ApplyOnError commits.
			text := got.Error()

			if strings.Contains(text, collided) {
				t.Errorf("the committed error text carries the collided column value "+
					"verbatim, so it reaches xflow_executions.error_msg and the "+
					"executions API: %s", text)
			}

			// Teeth. Redacting the whole message would pass the check above and
			// destroy the reason the message is kept at all.
			//
			// The last entry is spelled out rather than written as
			// redactedValue. Using the production constant made the check move
			// with the code it was checking: if redactedValue degenerated to "",
			// strings.Contains(text, "") is true for every possible text, and
			// the one assertion that says "something was removed, not absent"
			// would have been the first to stop meaning anything.
			for _, want := range []string{
				"1062",            // which error
				"23000",           // which SQLState
				"users.uk_email",  // WHICH unique index rejected the row
				"Duplicate entry", // what kind of failure
				"REDACTED",        // and that something was removed, not absent
			} {
				if !strings.Contains(text, want) {
					t.Errorf("the error text dropped %q, which is diagnostic rather "+
						"than sensitive: %s", want, text)
				}
			}
		})
	}
}

// TestRedactDuplicateEntryValueLeavesOtherMessagesAlone guards the blast radius.
//
// The redaction keys off the message shape, so the thing to check is that the
// shape does not accidentally match messages whose quoted runs are identifiers.
func TestRedactDuplicateEntryValueLeavesOtherMessagesAlone(t *testing.T) {
	for _, msg := range []string{
		"Column 'email' cannot be null",
		"Table 'app.orders' doesn't exist",
		"Unknown column 'email' in 'field list'",
		"Access denied for user 'svc'@'10.0.0.9' (using password: YES)",
		"Cannot add or update a child row: a foreign key constraint fails (`app`.`orders`, CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`))",
	} {
		if got := redactDuplicateEntryValue(msg); got != msg {
			t.Errorf("redacted a message that quotes identifiers rather than values:\n"+
				" in: %s\nout: %s", msg, got)
		}
	}
}
