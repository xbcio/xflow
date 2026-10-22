package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/spf13/cobra"
)

// connectionSecretMarker stands in for the password embedded in a MySQL DSN or
// Redis URL. Any occurrence of it in command output is a credential leak.
const connectionSecretMarker = "pw-marker"

// TestConnectionHelpNeverPrintsEnvValues is the regression guard for the
// connection-flag defaults: reading XFLOW_MYSQL_DSN or XFLOW_REDIS_ADDR at
// flag-registration time makes pflag render `(default "<value>")` in --help,
// and both routinely carry a password. Every connection flag now has a literal
// default, and the environment is resolved in the run path instead.
//
// dead-letter --help covers the persistent flags inherited by list/replay; the
// per-command entries also catch a default reintroduced on a subcommand's own
// flagset.
func TestConnectionHelpNeverPrintsEnvValues(t *testing.T) {
	t.Setenv("XFLOW_MYSQL_DSN", "root:"+connectionSecretMarker+"@tcp(db.invalid:3306)/xflow?parseTime=true")
	t.Setenv("XFLOW_REDIS_ADDR", "redis://:"+connectionSecretMarker+"@redis.invalid:6379")

	for _, args := range [][]string{
		{"dead-letter", "--help"},
		{"dead-letter", "list", "--help"},
		{"dead-letter", "replay", "--help"},
		{"dead-letter", "reconcile", "--help"},
		{"supply", "keys", "status", "--help"},
		{"supply", "reseal", "--help"},
		{"audit", "reconcile", "--help"},
		{"execution", "diff", "--help"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var out bytes.Buffer
			if err := executeRootWith(&out, args...); err != nil {
				t.Fatalf("executeRootWith(%v): %v", args, err)
			}
			// newRootCommand wires cobra's stdout and stderr to the same writer,
			// so this buffer covers both streams.
			rendered := out.String()
			if rendered == "" {
				t.Fatalf("executeRootWith(%v) produced no help output", args)
			}
			if strings.Contains(rendered, connectionSecretMarker) {
				t.Fatalf("help for %v printed a connection env value; a flag default read "+
					"from the environment is echoed in --help:\n%s", args, rendered)
			}
		})
	}

	// The break-glass fallback must stay visible: emptying the default to hide
	// the environment would also hide localhost:6379 from the operator.
	t.Run("the break-glass literal default is still advertised", func(t *testing.T) {
		var out bytes.Buffer
		if err := executeRootWith(&out, "dead-letter", "--help"); err != nil {
			t.Fatalf("executeRootWith(dead-letter --help): %v", err)
		}
		if !strings.Contains(out.String(), `(default "localhost:6379")`) {
			t.Fatalf("dead-letter --help no longer advertises the break-glass default:\n%s", out.String())
		}
	})
}

// TestResolveEnvFlagPrecedence pins the resolution order every connection flag
// relies on: an explicitly set flag, then the environment, then the literal
// default. The explicit-equals-default case is the one that distinguishes
// resolveEnvFlag from a plain "non-empty value wins" check, which would hand
// the environment priority over an operator who typed the default themselves.
func TestResolveEnvFlagPrecedence(t *testing.T) {
	const envKey = "XFLOW_TEST_RESOLVE_ENV_FLAG"
	const literalDefault = "localhost:6379"

	resolve := func(t *testing.T, flagDefault string, args ...string) string {
		t.Helper()
		var got string
		cmd := &cobra.Command{
			Use: "probe",
			Run: func(cmd *cobra.Command, _ []string) {
				got = resolveEnvFlag(cmd, "probe-flag", envKey, flagDefault)
			},
		}
		cmd.Flags().String("probe-flag", flagDefault, "")
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("execute probe: %v", err)
		}
		return got
	}

	t.Run("an explicit flag wins over the environment", func(t *testing.T) {
		t.Setenv(envKey, "from-env")
		if got := resolve(t, literalDefault, "--probe-flag", "from-flag"); got != "from-flag" {
			t.Fatalf("resolved %q, want from-flag", got)
		}
	})

	t.Run("an explicit flag equal to the default still wins", func(t *testing.T) {
		t.Setenv(envKey, "from-env")
		if got := resolve(t, literalDefault, "--probe-flag", literalDefault); got != literalDefault {
			t.Fatalf("resolved %q, want the explicit %q: the environment must not "+
				"override a value the operator typed", got, literalDefault)
		}
	})

	t.Run("an explicit empty flag suppresses the environment", func(t *testing.T) {
		t.Setenv(envKey, "from-env")
		if got := resolve(t, literalDefault, "--probe-flag", ""); got != "" {
			t.Fatalf("resolved %q, want the explicit empty value: the environment must not "+
				"override a flag the operator set to empty", got)
		}
	})

	t.Run("the environment wins over the literal default", func(t *testing.T) {
		t.Setenv(envKey, "from-env")
		if got := resolve(t, literalDefault); got != "from-env" {
			t.Fatalf("resolved %q, want from-env", got)
		}
	})

	t.Run("the literal default is used when the environment is empty", func(t *testing.T) {
		t.Setenv(envKey, "")
		if got := resolve(t, literalDefault); got != literalDefault {
			t.Fatalf("resolved %q, want the literal default %q", got, literalDefault)
		}
	})

	t.Run("an empty flag default still picks up the environment", func(t *testing.T) {
		t.Setenv(envKey, "from-env")
		if got := resolve(t, ""); got != "from-env" {
			t.Fatalf("resolved %q, want from-env", got)
		}
	})
}

// TestDeadLetterDSNPrecedence pins the three-source order on the command that
// resolves --mysql-dsn. The cases stay hermetic by pointing at closed loopback
// ports: the dial error names the address that was actually used, so a
// precedence inversion is visible without a database. The empty-flag row is the
// one a plain "if the value is empty, read the environment" lookup fails.
func TestDeadLetterDSNPrecedence(t *testing.T) {
	const (
		flagDSN = "root:pw@tcp(127.0.0.1:1)/xflow?parseTime=true&timeout=1s"
		envDSN  = "root:pw@tcp(127.0.0.1:2)/xflow?parseTime=true&timeout=1s"
	)
	cases := []struct {
		name      string
		envDSN    string
		args      []string
		wantError string
		wantNot   string
	}{
		{
			name:      "a flag DSN beats the environment",
			envDSN:    envDSN,
			args:      []string{"dead-letter", "reconcile", "--mysql-dsn", flagDSN},
			wantError: "127.0.0.1:1",
			wantNot:   "127.0.0.1:2",
		},
		{
			name:      "the environment DSN is used when the flag is absent",
			envDSN:    envDSN,
			args:      []string{"dead-letter", "reconcile"},
			wantError: "127.0.0.1:2",
			wantNot:   "127.0.0.1:1",
		},
		{
			name:      "an explicitly empty DSN suppresses the environment",
			envDSN:    envDSN,
			args:      []string{"dead-letter", "reconcile", "--mysql-dsn", ""},
			wantError: "--mysql-dsn (or XFLOW_MYSQL_DSN) is required for reconcile",
			wantNot:   "127.0.0.1:2",
		},
		{
			name:      "the literal empty default is used when neither source is set",
			envDSN:    "",
			args:      []string{"dead-letter", "reconcile"},
			wantError: "--mysql-dsn (or XFLOW_MYSQL_DSN) is required for reconcile",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XFLOW_MYSQL_DSN", tc.envDSN)
			// The SQL open fails before Redis is touched, but the environment is
			// cleared so an ambient address cannot influence the case.
			t.Setenv("XFLOW_REDIS_ADDR", "")

			var out bytes.Buffer
			err := executeRootWith(&out, tc.args...)
			if err == nil {
				t.Fatalf("executeRootWith(%v) succeeded, want the connection failure "+
					"that names the resolved DSN host", tc.args)
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantError)
			}
			if tc.wantNot != "" && strings.Contains(err.Error(), tc.wantNot) {
				t.Fatalf("error = %v also contains %q: the wrong source supplied the DSN",
					err, tc.wantNot)
			}
		})
	}
}

// TestDeadLetterBreakGlassRedisPrecedence pins the same order for --redis-addr
// on the break-glass path. Two miniredis instances hold different dead-letter
// entries, so the listed entry ID names the source that actually won. The
// literal localhost:6379 default is covered by the --help test only: dialing it
// here would depend on whatever the developer's machine happens to run.
func TestDeadLetterBreakGlassRedisPrecedence(t *testing.T) {
	const (
		execID    = "exec-precedence"
		flagEntry = "execute/exec-precedence/flag/1"
		envEntry  = "execute/exec-precedence/env/1"
	)
	cases := []struct {
		name        string
		includeFlag bool
		wantEntry   string
	}{
		{"a flag address beats the environment", true, flagEntry},
		{"the environment address is used when the flag is absent", false, envEntry},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flagRedis, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer flagRedis.Close()
			seedDeadLetterRedis(t, flagRedis.Addr(), "default", execID, flagEntry)

			envRedis, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer envRedis.Close()
			seedDeadLetterRedis(t, envRedis.Addr(), "default", execID, envEntry)

			t.Setenv("XFLOW_REDIS_ADDR", envRedis.Addr())
			args := []string{"dead-letter", "--break-glass"}
			if tc.includeFlag {
				args = append(args, "--redis-addr", flagRedis.Addr())
			}
			args = append(args, "list", "--execution", execID)

			var out bytes.Buffer
			if err := executeRootWith(&out, args...); err != nil {
				t.Fatalf("break-glass list with %v: %v", args, err)
			}
			assertListedID(t, out.String(), tc.wantEntry)
		})
	}
}

// TestDeadLetterReconcileRedisAddrPrecedence pins the same order on the
// reconcile subcommand's own --redis-addr. It reaches resolveEnvFlag through an
// `if addr == ""` pre-check rather than calling it directly (the break-glass
// path above calls it directly), so the explicit-empty row is the one that
// proves the two paths agree: a value-based fallback in the pre-check would
// hand XFLOW_REDIS_ADDR priority over an operator who typed the empty value.
//
// runReconcile is a recorder here. The real body opens MySQL before it dials
// Redis, so an executed case cannot name the Redis address it resolved without
// a live database; the environment therefore points at a closed loopback port
// and nothing is contacted.
func TestDeadLetterReconcileRedisAddrPrecedence(t *testing.T) {
	const envAddr = "127.0.0.1:6390"
	t.Setenv("XFLOW_REDIS_ADDR", envAddr)
	t.Setenv("XFLOW_MYSQL_DSN", "root:pw@tcp(127.0.0.1:1)/xflow?parseTime=true&timeout=1s")

	orig := runDeadLetterReconcile
	t.Cleanup(func() { runDeadLetterReconcile = orig })

	cases := []struct {
		name     string
		args     []string
		wantAddr string
	}{
		{
			name:     "an explicitly empty flag suppresses the environment",
			args:     []string{"dead-letter", "reconcile", "--redis-addr", ""},
			wantAddr: "",
		},
		{
			name:     "the environment is used when the flag is absent",
			args:     []string{"dead-letter", "reconcile"},
			wantAddr: envAddr,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotAddr string
			var called bool
			runDeadLetterReconcile = func(_ *deadLetterOptions, addr string, _ bool) error {
				called = true
				gotAddr = addr
				return nil
			}

			var out bytes.Buffer
			if err := executeRootWith(&out, tc.args...); err != nil {
				t.Fatalf("dead-letter reconcile with %v: %v", tc.args, err)
			}
			if !called {
				t.Fatal("the run path never reached runReconcile; this case proves nothing")
			}
			if gotAddr != tc.wantAddr {
				t.Fatalf("resolved redis address = %q, want %q (XFLOW_REDIS_ADDR=%q): "+
					"the environment must not override a flag the operator set to empty, "+
					"and must be used when the flag is absent", gotAddr, tc.wantAddr, envAddr)
			}
		})
	}
}

// TestConnectionCommandsResolveTheEnvironmentAtRunTime proves the post-parse
// lookup actually reaches each command's run path. Every case sets both
// connection variables and then triggers a validation failure that sits behind
// the env-resolved values, so a dropped lookup shows up as the wrong error
// (--mysql-dsn/--redis-addr reported missing) rather than as a silent success.
// No live database is involved: the DSNs point at a closed port.
func TestConnectionCommandsResolveTheEnvironmentAtRunTime(t *testing.T) {
	t.Setenv("XFLOW_MYSQL_DSN", "root:"+connectionSecretMarker+"@tcp(127.0.0.1:1)/xflow?parseTime=true&timeout=1s")
	t.Setenv("XFLOW_REDIS_ADDR", "127.0.0.1:1")

	requireEnvResolved := func(t *testing.T, err error, want string) {
		t.Helper()
		if err == nil {
			t.Fatalf("command succeeded; want a validation failure behind the environment values")
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want it to contain %q", err, want)
		}
		for _, missing := range []string{
			"--mysql-dsn (or XFLOW_MYSQL_DSN) is required",
			"--redis-addr (or XFLOW_REDIS_ADDR) is required",
		} {
			if strings.Contains(err.Error(), missing) {
				t.Fatalf("err = %v reports %q even though the environment supplied it", err, missing)
			}
		}
		if strings.Contains(err.Error(), connectionSecretMarker) {
			t.Fatalf("err = %v echoes the environment credential", err)
		}
	}

	t.Run("supply keys status", func(t *testing.T) {
		t.Setenv("XFLOW_MASTER_KEY", "")
		t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", "")
		var out bytes.Buffer
		requireEnvResolved(t, executeRootWith(&out, "supply", "keys", "status"), "master key")
	})

	t.Run("supply reseal", func(t *testing.T) {
		t.Setenv("XFLOW_MASTER_KEY", "")
		t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", "")
		var out bytes.Buffer
		requireEnvResolved(t, executeRootWith(&out, "supply", "reseal"), "master key")
	})

	t.Run("audit reconcile", func(t *testing.T) {
		var out bytes.Buffer
		requireEnvResolved(t,
			executeRootWith(&out, "audit", "reconcile", "--batch", "0"),
			"--batch must be positive")
	})

	t.Run("execution diff", func(t *testing.T) {
		var out bytes.Buffer
		requireEnvResolved(t,
			executeRootWith(&out, "execution", "diff"),
			"--namespace is required")
	})

	t.Run("dead-letter reconcile", func(t *testing.T) {
		// The DSN points at a closed port, so this fails on the first MySQL
		// open without touching a database. Reaching "open mysql" means the
		// required-flag check accepted the env-supplied DSN.
		var out bytes.Buffer
		requireEnvResolved(t,
			executeRootWith(&out, "dead-letter", "reconcile"),
			"open mysql")
	})
}
