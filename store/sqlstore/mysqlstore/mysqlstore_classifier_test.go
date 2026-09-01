package mysqlstore

import (
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestIsMySQLDeadlock_DeadlockIsTransient and
// TestIsMySQLDeadlock_NonDeadlockIsNotTransient pin isMySQLDeadlock's
// contract directly -- the MySQL-specific knowledge that used to live in
// store/sqlstore/errors.go before task-56 fix-1.md's Defect 3 moved it here,
// to keep sqlstore's own core free of any SQL driver import. These are pure
// unit tests of the classifier function itself; they do not prove this
// package's init() actually registers it with sqlstore's core (a fake
// construction that called isMySQLDeadlock directly here would prove only
// the contract, not the wiring) -- that end-to-end wiring proof is
// TestNewRegistersMySQLDeadlockClassifier in mysqlstore_registration_test.go,
// which requires a real MySQL connection to reproduce an actual 1213 and
// observe it classified through the real Provider.ArtifactIndex().Bind call
// path.
func TestIsMySQLDeadlock_DeadlockIsTransient(t *testing.T) {
	deadlock := &mysqldriver.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}
	if !isMySQLDeadlock(deadlock) {
		t.Fatalf("isMySQLDeadlock(%v) = false, want true for MySQL error 1213", deadlock)
	}
}

func TestIsMySQLDeadlock_NonDeadlockIsNotTransient(t *testing.T) {
	dup := &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry"}
	if isMySQLDeadlock(dup) {
		t.Fatalf("isMySQLDeadlock(%v) = true, want false for a non-deadlock MySQL error (1062)", dup)
	}
}

func TestIsMySQLDeadlock_NonMySQLErrorIsNotTransient(t *testing.T) {
	plain := errPlain("boom")
	if isMySQLDeadlock(plain) {
		t.Fatalf("isMySQLDeadlock(%v) = true, want false for an error that is not *mysql.MySQLError at all", plain)
	}
}

// errPlain is a trivial error type distinct from *mysqldriver.MySQLError, so
// errors.As in isMySQLDeadlock cannot match it by accident.
type errPlain string

func (e errPlain) Error() string { return string(e) }
