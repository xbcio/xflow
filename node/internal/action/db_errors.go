package action

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/xbcio/xflow/types"
)

// classifyDBError maps a database driver error to a ClassifiedError per the
// error taxonomy matrix (2026-07-18 remediation §6.4). Classification is by
// MySQL SQLState / error number, never by error text.
//
// PostgreSQL is NOT supported, and the stronger statement is the useful one: no
// PG driver is in the module graph at all, so sql.Open("postgres", ...) fails
// with `unknown driver` before a connection exists and no PG error ever reaches
// this function. That matters when asking whether a fix here covers PG — the
// question does not arise, rather than being answered "no, it falls through to
// the transient fallback with its text intact". Adding a PG driver would change
// that in one import, and would need its own typed classifier, because PG
// SQLState differs (40P01 deadlock / 23505 unique_violation) and its
// unique-violation Detail quotes the collided value the same way MySQL's does.
// Out of scope; see error_taxonomy §6.
//
//   - connection-lost (driver.ErrBadConn, net errors, EOF) -> transient
//   - deadlock / lock wait / serialization failure (1213, 1205, 40001) ->
//     transient
//   - constraint violations (1062 dup, 1452/1451 FK, 1048 not-null, 23000) ->
//     permanent (data error, retrying with the same payload cannot help)
//   - syntax/config (1064 syntax, 1146 no table, SQLSTATE class 42) -> permanent
//   - unknown -> conservative transient (retryable) + caller may observe
//
// This is the single outlet for database IO errors; the database action routes
// driver errors through it instead of emitting a legacy error-port output.
func classifyDBError(err error) error {
	if err == nil {
		return nil
	}
	// Connection lost at the driver level — retry may succeed on a fresh conn.
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return types.NewTransientError("database.connection_lost", err.Error())
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Timeouts and transient network failures are retryable.
		return types.NewTransientError("database.network", err.Error())
	}
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		return classifyMySQLError(mysqlErr)
	}
	// Unknown driver error: conservative transient so a one-off glitch can
	// retry, but the message is preserved for observability.
	return types.NewTransientError("database.unknown", err.Error())
}

func classifyMySQLError(e *mysqldriver.MySQLError) error {
	code := fmtCode("mysql", e.Number)
	msg := safeMySQLMessage(e)
	state := sqlStateString(e.SQLState)
	switch e.Number {
	case 1205, 1213: // lock wait timeout, deadlock
		return types.NewTransientError(code, msg)
	}
	switch state {
	case "40001": // serialization failure
		return types.NewTransientError(code, msg)
	case "23000", "23001": // integrity constraint violation
		return types.NewPermanentError(code, msg)
	}
	// SQLSTATE class 42 is "syntax error or access rule violation" in its
	// entirety. Every subclass of it describes a statement the server will
	// reject identically on the next attempt, so none of them is retryable.
	//
	// This was previously spelled `case "42xxx", "42000"`. "42xxx" is
	// placeholder notation, not a value that can arrive: MySQLError.SQLState is
	// five bytes copied verbatim off the wire when the server sends the 0x23
	// marker (go-sql-driver/mysql@v1.9.3 packets.go:610-613), so no server ever
	// sends that literal and the label matched nothing. The subclasses it was
	// meant to stand for are not all in the number list below either — 42S01
	// "table already exists" (1050) and 42S21 "duplicate column name" (1060)
	// are in neither — so those fell through to the conservative transient
	// fallback and were retried to exhaustion against a schema fact that will
	// be just as true every time.
	if strings.HasPrefix(state, "42") {
		return types.NewPermanentError(code, msg)
	}
	switch e.Number {
	case 1062, 1451, 1452, 1048, 1061, 1586: // dup key, FK, not-null, dup index
		return types.NewPermanentError(code, msg)
	case 1064, 1146, 1054, 1051, 1045: // syntax, no such table, unknown column, access denied
		return types.NewPermanentError(code, msg)
	}
	// Unknown MySQL error: conservative transient.
	return types.NewTransientError(code, msg)
}

func sqlStateString(b [5]byte) string {
	return strings.TrimRight(string(b[:]), "\x00")
}

// safeMySQLMessage renders a driver error without the one thing in it that is
// data rather than schema.
//
// MySQL's duplicate-key text is "Duplicate entry 'V' for key 'K'", where V is
// the actual column value that collided. For a workflow inserting an upstream
// node's output — an email, an order id, whatever the previous node produced —
// V is that output. It reaches xflow_executions.error_msg and the body of GET
// /v1/executions/{id}, because engine/errorpolicy.go does errMsg =
// sysErr.Error() on whatever the node returned and ClassifiedError.Error()
// renders Message verbatim.
//
// The obvious two options are both bad: keeping V persists business data, and
// dropping the message loses the diagnostic it exists for. This takes the third
// one, the same one xflow.http already takes for query strings a few files over
// (safeURLString): keep the key, replace the value. K survives, so an operator
// still learns which unique index rejected the row, which is the actionable
// half. Every other MySQL message this classifier sees quotes identifiers
// rather than values — "Column 'c' cannot be null", "Table 't' doesn't exist" —
// and is passed through whole.
//
// Two things this deliberately does not cover, so they are not mistaken for
// handled. 1045 access-denied still carries user and host: that is topology,
// argued separately from workflow data. And a DSN with interpolateParams=true
// makes the driver splice parameter values into the SQL it sends, so a 1064
// syntax error can quote them back — the fix for that is the connection
// setting, not a wider regexp here.
func safeMySQLMessage(e *mysqldriver.MySQLError) string {
	safe := redactDuplicateEntryValue(e.Message)
	if safe == e.Message {
		return e.Error()
	}
	// Re-render through the driver's own framing rather than reproducing its
	// format string, which would drift the first time it changes.
	return strings.Replace(e.Error(), e.Message, safe, 1)
}

const (
	dupEntryPrefix = "Duplicate entry '"
	dupEntryKeySep = "' for key '"
)

// redactDuplicateEntryValue returns msg with the collided value replaced, or
// msg unchanged when it is not a duplicate-entry message.
//
// It matches on the message shape rather than on the error number because more
// than one number produces this text (1062, 1586, 1022) and a list of numbers
// is a thing that goes stale. Scanning for the LAST separator matters: MySQL
// does not escape the value, so a value containing an apostrophe would end a
// leftmost scan early and leave the tail of it in the message.
func redactDuplicateEntryValue(msg string) string {
	if !strings.HasPrefix(msg, dupEntryPrefix) {
		return msg
	}
	rest := msg[len(dupEntryPrefix):]
	i := strings.LastIndex(rest, dupEntryKeySep)
	if i < 0 {
		// Shape recognised but no key clause. Redact to the end rather than
		// give up: whatever follows is the value, and the diagnostic left is
		// the error number, which the code already carries.
		return dupEntryPrefix + redactedValue + "'"
	}
	return dupEntryPrefix + redactedValue + rest[i:]
}

const redactedValue = "REDACTED"

func fmtCode(prefix string, n uint16) string {
	// avoid strconv import churn in this small helper
	if n == 0 {
		return prefix
	}
	var buf [6]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return prefix + "." + string(buf[pos:])
}
