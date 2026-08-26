package action

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
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
		// 1051's SQLState (42S02) used to reach nothing but the number list,
		// which made this row the only thing pinning 1051 there. The class-42
		// prefix arm now catches 42S02 first, so this row alone no longer
		// proves the number entry is live — see the empty-SQLState rows below
		// for the shape that still reaches it.
		//
		// The neighbouring numbers (1451, 1061, 1586) are not added with their
		// real SQLStates: those are 23000/42000, which the state switch
		// already classifies permanent, so such a row could not fail. They
		// are covered instead by the empty-SQLState rows further down, which
		// is the shape that does reach the number switch.
		{"unknown table 1051 permanent", mysqlErr(1051, "42S02"), types.ErrorKindPermanent},
		{"unknown mysql number transient fallback", mysqlErr(1644, "99999"), types.ErrorKindTransient},

		// classifyDBError's net.Error branch (db_errors.go:48-51) had zero
		// coverage: nothing in this table produced an error implementing
		// net.Error. *net.DNSError is what net.Dial actually returns for an
		// unresolvable host, which is exactly what "connect to db.invalid"
		// does at the network layer before a MySQL handshake ever starts.
		{"dns lookup failure transient", &net.DNSError{Err: "no such host", Name: "db.invalid", IsNotFound: true}, types.ErrorKindTransient},

		// The final fallback in classifyDBError (db_errors.go:59, "unknown
		// driver error") also had zero coverage: every prior case in this
		// table is either a sentinel/net error or a *mysqldriver.MySQLError.
		// A plain error reaches none of those branches.
		{"generic non-driver error transient fallback", errors.New("boom"), types.ErrorKindTransient},

		// classifyMySQLError has two switches on e.Number that both return
		// permanent: db_errors.go:79 (dup key, FK, not-null, dup index) and
		// db_errors.go:81 (syntax, no such table, unknown column, access
		// denied). The first one had zero coverage even though the table
		// above already had "permanent" cases for 1062, 1452 and 1048: every
		// one of those cases used SQLState "23000", which the earlier state
		// switch (db_errors.go:73, case "23000", "23001") already catches
		// and returns from — the number switch at line 79 was never reached
		// by any existing case.
		//
		// The SQLState is optional on the wire (go-sql-driver/mysql
		// packets.go handleErrorPacket only copies it when the server sends
		// the "#" marker byte; otherwise MySQLError.SQLState stays the zero
		// value, which sqlStateString renders as ""), so a MySQLError with
		// an empty SQLState is a real shape the driver produces, not a
		// fabricated one. These cases drive the number switch directly by
		// using that shape, covering the four numbers the table above never
		// reached (1062, 1452, 1048) and the three the earlier comment
		// explicitly called out as untested (1451, 1061, 1586).
		{"dup key 1062 without sqlstate, number fallback permanent", mysqlErr(1062, ""), types.ErrorKindPermanent},
		{"FK 1451 without sqlstate, number fallback permanent", mysqlErr(1451, ""), types.ErrorKindPermanent},
		{"FK 1452 without sqlstate, number fallback permanent", mysqlErr(1452, ""), types.ErrorKindPermanent},
		{"not null 1048 without sqlstate, number fallback permanent", mysqlErr(1048, ""), types.ErrorKindPermanent},
		{"dup keyname 1061 without sqlstate, number fallback permanent", mysqlErr(1061, ""), types.ErrorKindPermanent},
		{"dup entry keyname 1586 without sqlstate, number fallback permanent", mysqlErr(1586, ""), types.ErrorKindPermanent},

		// The second number switch (db_errors.go, "syntax, no such table,
		// unknown column, access denied") needs the same treatment for the
		// same reason: 1064 arrives as 42000 and 1146/1054/1051 arrive as
		// 42S02/42S22/42S02, all of which the class-42 prefix arm above now
		// catches and returns from. Without these rows, deleting any of those
		// four numbers from the list would go unnoticed. 1045 is left out —
		// its SQLState is 28000, so the existing row above still reaches the
		// number switch for it.
		{"syntax 1064 without sqlstate, number fallback permanent", mysqlErr(1064, ""), types.ErrorKindPermanent},
		{"no such table 1146 without sqlstate, number fallback permanent", mysqlErr(1146, ""), types.ErrorKindPermanent},
		{"unknown column 1054 without sqlstate, number fallback permanent", mysqlErr(1054, ""), types.ErrorKindPermanent},
		{"unknown table 1051 without sqlstate, number fallback permanent", mysqlErr(1051, ""), types.ErrorKindPermanent},

		// The class-42 prefix arm itself. Both of these are SQLSTATE class 42
		// with an error number that appears in NEITHER number list, so before
		// the arm existed they fell through to the conservative transient
		// fallback: a schema statement the server rejects the same way every
		// time would be retried to exhaustion. The old `case "42xxx"` label
		// could not catch them — SQLState is five bytes copied verbatim off
		// the wire (go-sql-driver/mysql@v1.9.3 packets.go:610-613), so the
		// literal "42xxx" is a value no server can send.
		{"class 42 subclass 42S01 outside both number lists permanent", mysqlErr(1050, "42S01"), types.ErrorKindPermanent},
		{"class 42 subclass 42S21 outside both number lists permanent", mysqlErr(1060, "42S21"), types.ErrorKindPermanent},

		// Contrast: the prefix must be a prefix of the CLASS, not a substring
		// anywhere in the state. A state that merely contains "42" is not
		// class 42 and must keep falling through to the transient fallback,
		// otherwise the arm would over-reach into unrelated states.
		{"state containing 42 but not class 42 stays transient", mysqlErr(1644, "HY420"), types.ErrorKindTransient},
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

// TestRedactDuplicateEntryValueNoKeyClause exercises the "shape recognised
// but no key clause" fallback (db_errors.go:147-152), which had zero
// coverage: both cases in TestClassifyDBErrorRedactsDuplicateEntryValue use a
// message with a "for key '...'" clause, so both take the other branch.
//
// Why the branch exists at all is the part worth stating precisely, because
// it is easy to write a confident sentence here that nobody checked. What is
// verifiable from this repo: dupEntryKeySep is the literal "' for key '", so
// any duplicate-entry text whose key is not single-quoted misses it, and
// production chose to redact to the end rather than return the message
// unchanged. A message ending in a bare "for key 2" is one such text. Whether
// some particular MySQL version emits that exact form is a claim about the
// server that this repo cannot settle, so the test does not make it — the
// input is chosen to reach the branch, and the assertion is about what the
// branch does with it.
func TestRedactDuplicateEntryValueNoKeyClause(t *testing.T) {
	const collided = "alice@example.test"
	msg := "Duplicate entry '" + collided + "' for key 2"

	got := redactDuplicateEntryValue(msg)

	if strings.Contains(got, collided) {
		t.Fatalf("collided value survived the no-key-clause fallback: %q", got)
	}
	// db_errors.go documents this branch as "redact to the end", and the
	// exact output is a single well-known string. Pinning it matters: an
	// assertion that only checks "doesn't contain collided" would also pass
	// if the function returned "" or dropped the message entirely.
	want := "Duplicate entry 'REDACTED'"
	if got != want {
		t.Fatalf("redactDuplicateEntryValue(%q) = %q, want %q", msg, got, want)
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
