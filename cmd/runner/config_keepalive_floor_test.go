package main

import (
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestLoadRunnerConfigRejectsAKeepaliveTimeGrpcWouldIgnore covers a value an
// operator can write today and get no answer about.
//
// grpc-go's WithKeepaliveParams raises any client ping interval below 10s to
// 10s (dialoptions.go:562) and says so only through the grpc library logger.
// node/resource/pool.go:96 passes the configured KeepaliveTime straight into
// that call, so `keepalive_time: "2s"` used to load cleanly, appear in the
// config struct as 2s, and ping every 10s. Somebody who lowered it to catch a
// dead peer faster would have had no way to find out it did nothing.
//
// The existing coverage cannot see this: config_test.go:640 and
// run_propagation_test.go:63 both use values above the floor (30s and 37s),
// so they pass whether or not any floor check exists, and
// node/resource/pool_defaults_test.go asserts only on the struct
// normalizeConfig returns, never on what reaches the dial.
//
// The check has to live here rather than in normalizeConfig because this is
// the only layer with the operator's own text in hand — it can quote the value
// back and name the floor. normalizeConfig receives a time.Duration from
// several callers, including programmatic SDK ones where silently raising is
// the friendlier behaviour.
//
// Measured, Arm A only, and the asymmetry is deliberate: the guard is added by
// the same change as this test, so running the old tests against a mutation of
// a branch that does not exist at HEAD would prove nothing. What was measured
// is that each sub-test has teeth — disabling the check reddens the first,
// turning `<` into `<=` reddens the second, and copying the check onto the
// timeout branch reddens the third.
func TestLoadRunnerConfigRejectsAKeepaliveTimeGrpcWouldIgnore(t *testing.T) {
	t.Run("below the floor is refused", func(t *testing.T) {
		_, err := loadRunnerConfigFromBytes([]byte(`
resource_pool:
  grpc:
    keepalive_time: "2s"
`))
		if err == nil {
			t.Fatal("keepalive_time: 2s loaded without error; grpc-go would ping " +
				"every 10s regardless, so accepting it hands the operator a config " +
				"file that states an interval the process does not use")
		}
		// The message has to carry both halves or it does not help: which
		// setting, and what the floor actually is.
		if !strings.Contains(err.Error(), "keepalive_time") {
			t.Fatalf("error = %q, want it to name resource_pool.grpc.keepalive_time", err)
		}
		if !strings.Contains(err.Error(), types.MinGRPCKeepaliveTime.String()) {
			t.Fatalf("error = %q, want it to name the %s floor so the operator knows "+
				"what to change the value to", err, types.MinGRPCKeepaliveTime)
		}
	})

	t.Run("exactly the floor is accepted", func(t *testing.T) {
		// The boundary matters on its own: a `<=` here would reject the one
		// value grpc-go does honour verbatim, and the sub-test above would
		// still pass.
		cfg, err := loadRunnerConfigFromBytes([]byte(`
resource_pool:
  grpc:
    keepalive_time: "10s"
`))
		if err != nil {
			t.Fatalf("keepalive_time: 10s = %v, want accepted: it is exactly the "+
				"floor grpc-go enforces, so it takes effect unchanged", err)
		}
		if cfg.resourcePoolConfig.GRPC.KeepaliveTime != types.MinGRPCKeepaliveTime {
			t.Fatalf("grpc.KeepaliveTime = %v, want %v",
				cfg.resourcePoolConfig.GRPC.KeepaliveTime, types.MinGRPCKeepaliveTime)
		}
	})

	t.Run("keepalive_timeout has no such floor", func(t *testing.T) {
		// Only Time is clamped by grpc-go. Timeout is the ack deadline and is
		// honoured as written, so the check must not have been applied to both
		// fields — a short ack deadline is a legitimate tuning choice.
		cfg, err := loadRunnerConfigFromBytes([]byte(`
resource_pool:
  grpc:
    keepalive_timeout: "2s"
`))
		if err != nil {
			t.Fatalf("keepalive_timeout: 2s = %v, want accepted: grpc-go clamps "+
				"only the ping interval, not the ack deadline", err)
		}
		if cfg.resourcePoolConfig.GRPC.KeepaliveTimeout != 2*time.Second {
			t.Fatalf("grpc.KeepaliveTimeout = %v, want 2s",
				cfg.resourcePoolConfig.GRPC.KeepaliveTimeout)
		}
	})
}
