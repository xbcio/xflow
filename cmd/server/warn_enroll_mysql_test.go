package main

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// TestWarnIfEnrollStoresIgnoreMySQLDSN pins fix2 review item 2: the comment
// above registrationCodeStore/issuedIdentityStore in runServer used to claim
// those stores "match every other in-memory store this binary falls back to
// without --mysql-dsn" — false, since they never look at cfg.mysqlDSN at all.
// --enroll --mysql-dsn ... issues identities that still do not survive a
// restart, and an operator running that combination deserves to be told at
// startup, not to discover it later.
func TestWarnIfEnrollStoresIgnoreMySQLDSN(t *testing.T) {
	run := func(cfg serverConfig) string {
		var buf bytes.Buffer
		orig := log.Writer()
		log.SetOutput(&buf)
		defer log.SetOutput(orig)
		warnIfEnrollStoresIgnoreMySQLDSN(cfg)
		return buf.String()
	}

	t.Run("enroll and mysql-dsn both set", func(t *testing.T) {
		out := run(serverConfig{enroll: true, mysqlDSN: "user:pass@tcp(127.0.0.1:3306)/xflow"})
		if !strings.Contains(out, "WARNING") || !strings.Contains(out, "--enroll") || !strings.Contains(out, "--mysql-dsn") {
			t.Fatalf("log output = %q, want a WARNING mentioning both --enroll and --mysql-dsn", out)
		}
		if !strings.Contains(out, "in-memory") {
			t.Fatalf("log output = %q, want it to say the stores are in-memory regardless of --mysql-dsn", out)
		}
	})

	t.Run("enroll without mysql-dsn: no warning needed", func(t *testing.T) {
		out := run(serverConfig{enroll: true, mysqlDSN: ""})
		if out != "" {
			t.Fatalf("log output = %q, want empty — --enroll alone (no --mysql-dsn) has nothing surprising to warn about", out)
		}
	})

	t.Run("mysql-dsn without enroll: no warning", func(t *testing.T) {
		out := run(serverConfig{enroll: false, mysqlDSN: "user:pass@tcp(127.0.0.1:3306)/xflow"})
		if out != "" {
			t.Fatalf("log output = %q, want empty — enrollment is off, so its stores are not in play at all", out)
		}
	})

	t.Run("neither set: no warning", func(t *testing.T) {
		out := run(serverConfig{})
		if out != "" {
			t.Fatalf("log output = %q, want empty", out)
		}
	})
}
