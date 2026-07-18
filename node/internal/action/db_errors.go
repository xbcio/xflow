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
//   - connection-lost (driver.ErrBadConn, net errors, EOF) -> transient
//   - deadlock / serialization failure (1213, 1205, 40001, 40P01) -> transient
//   - constraint violations (1062 dup, 1452/1451 FK, 1048 not-null, 23000) ->
//     permanent (data error, retrying with the same payload cannot help)
//   - syntax/config (1064 syntax, 1146 no table, 42xxx) -> permanent
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
	msg := e.Error()
	state := sqlStateString(e.SQLState)
	switch e.Number {
	case 1205, 1213: // lock wait timeout, deadlock
		return types.NewTransientError(code, msg)
	}
	switch state {
	case "40001", "40P01": // serialization failure (MySQL/PG)
		return types.NewTransientError(code, msg)
	case "23000", "23001": // integrity constraint violation
		return types.NewPermanentError(code, msg)
	case "42xxx", "42000": // syntax / invalid table (kept broad; specific numbers below)
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
