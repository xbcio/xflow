package resource

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// normalizeConfig default guards
//
// Before this file, every literal in normalizeConfig's five `if x <= 0 { x =
// literal }` branches (SQL.MaxOpenConns=25, SQL.MaxIdleConns=5,
// SQL.ConnMaxLifetime=30m, GRPC.KeepaliveTime=30s, GRPC.KeepaliveTimeout=10s)
// could be edited to any other value -- including a negative one, or the
// `<= 0` guard loosened to `< 0` -- with `go test ./node/resource/...`
// staying fully green. Nothing anywhere in the package asserted on any of
// these five values.
//
// SQL.MaxOpenConns is the sharpest edge of the five: database/sql's
// (*DB).SetMaxOpenConns documents that n <= 0 means "no limit", not "a
// small pool". So if the `<= 0` guard on that one branch is ever loosened,
// or the replacement default is itself <= 0, the resulting failure mode is
// not a smaller connection pool -- it is an UNBOUNDED one, silently, with
// nothing in the process reporting it. See
// https://pkg.go.dev/database/sql#DB.SetMaxOpenConns.
// ---------------------------------------------------------------------------

// TestNormalizeConfig_ZeroValueAppliesDefaults pins the documented default
// for every field normalizeConfig fills in when the caller supplies the zero
// value (i.e. never configured the pool at all -- the common case).
func TestNormalizeConfig_ZeroValueAppliesDefaults(t *testing.T) {
	cfg := normalizeConfig(types.ResourcePoolConfig{})

	t.Run("SQL_MaxOpenConns", func(t *testing.T) {
		if cfg.SQL.MaxOpenConns != 25 {
			t.Fatalf("normalizeConfig(zero value).SQL.MaxOpenConns = %d, want 25: "+
				"database/sql.(*DB).SetMaxOpenConns(n) treats n<=0 as UNLIMITED connections, "+
				"so this default is what stands between the pool and no connection cap at all",
				cfg.SQL.MaxOpenConns)
		}
	})
	t.Run("SQL_MaxIdleConns", func(t *testing.T) {
		if cfg.SQL.MaxIdleConns != 5 {
			t.Fatalf("normalizeConfig(zero value).SQL.MaxIdleConns = %d, want 5", cfg.SQL.MaxIdleConns)
		}
	})
	t.Run("SQL_ConnMaxLifetime", func(t *testing.T) {
		if cfg.SQL.ConnMaxLifetime != 30*time.Minute {
			t.Fatalf("normalizeConfig(zero value).SQL.ConnMaxLifetime = %v, want 30m", cfg.SQL.ConnMaxLifetime)
		}
	})
	t.Run("GRPC_KeepaliveTime", func(t *testing.T) {
		if cfg.GRPC.KeepaliveTime != 30*time.Second {
			t.Fatalf("normalizeConfig(zero value).GRPC.KeepaliveTime = %v, want 30s", cfg.GRPC.KeepaliveTime)
		}
	})
	t.Run("GRPC_KeepaliveTimeout", func(t *testing.T) {
		if cfg.GRPC.KeepaliveTimeout != 10*time.Second {
			t.Fatalf("normalizeConfig(zero value).GRPC.KeepaliveTimeout = %v, want 10s", cfg.GRPC.KeepaliveTimeout)
		}
	})
}

// TestNormalizeConfig_NegativeValuesAlsoFallBackToDefaults asserts that a
// negative input is clamped to the same default as the zero value, not
// passed through. This is the case that matters most for SQL.MaxOpenConns:
// database/sql reads n<=0 as "unlimited", so a negative value must never
// reach SetMaxOpenConns -- it has to be caught here, before the config ever
// leaves normalizeConfig.
func TestNormalizeConfig_NegativeValuesAlsoFallBackToDefaults(t *testing.T) {
	cfg := normalizeConfig(types.ResourcePoolConfig{
		SQL:  types.SQLPoolConfig{MaxOpenConns: -1, MaxIdleConns: -1, ConnMaxLifetime: -1},
		GRPC: types.GRPCPoolConfig{KeepaliveTime: -1, KeepaliveTimeout: -1},
	})

	if cfg.SQL.MaxOpenConns != 25 {
		t.Errorf("normalizeConfig(MaxOpenConns=-1).SQL.MaxOpenConns = %d, want 25: the guard here is "+
			"`<= 0`, not `< 0`, precisely so a negative input -- which database/sql would otherwise read "+
			"as UNLIMITED connections, not a smaller pool -- never reaches SetMaxOpenConns",
			cfg.SQL.MaxOpenConns)
	}
	if cfg.SQL.MaxIdleConns != 5 {
		t.Errorf("normalizeConfig(MaxIdleConns=-1).SQL.MaxIdleConns = %d, want 5", cfg.SQL.MaxIdleConns)
	}
	if cfg.SQL.ConnMaxLifetime != 30*time.Minute {
		t.Errorf("normalizeConfig(ConnMaxLifetime=-1).SQL.ConnMaxLifetime = %v, want 30m", cfg.SQL.ConnMaxLifetime)
	}
	if cfg.GRPC.KeepaliveTime != 30*time.Second {
		t.Errorf("normalizeConfig(KeepaliveTime=-1).GRPC.KeepaliveTime = %v, want 30s", cfg.GRPC.KeepaliveTime)
	}
	if cfg.GRPC.KeepaliveTimeout != 10*time.Second {
		t.Errorf("normalizeConfig(KeepaliveTimeout=-1).GRPC.KeepaliveTimeout = %v, want 10s", cfg.GRPC.KeepaliveTimeout)
	}
}

// TestNormalizeConfig_PreservesExplicitPositiveValues is the other half of
// the contract: normalizeConfig must leave a caller's explicit positive
// tuning alone. Without this test, a mutation that unconditionally
// overwrites every field with the package default (deleting the `<= 0`
// guards entirely) would still pass the two tests above.
func TestNormalizeConfig_PreservesExplicitPositiveValues(t *testing.T) {
	in := types.ResourcePoolConfig{
		SQL:  types.SQLPoolConfig{MaxOpenConns: 7, MaxIdleConns: 3, ConnMaxLifetime: 2 * time.Minute},
		GRPC: types.GRPCPoolConfig{KeepaliveTime: 4 * time.Second, KeepaliveTimeout: 9 * time.Second},
	}
	cfg := normalizeConfig(in)
	if cfg != in {
		t.Fatalf("normalizeConfig(%+v) = %+v, want unchanged: a caller-supplied positive value must "+
			"survive normalization, not be silently replaced by the package default", in, cfg)
	}
}

// TestResourcePool_SQLAppliesDefaultMaxOpenConns is the end-to-end version of
// the SQL.MaxOpenConns guard above: it does not just check what
// normalizeConfig computes, it checks what actually lands on the live
// *sql.DB returned by the pool, through the real SQL() wiring
// (db.SetMaxOpenConns(p.cfg.SQL.MaxOpenConns)). *sql.DB exposes the applied
// limit via Stats().MaxOpenConnections, so this is observable without ever
// opening a real connection.
//
// This also guards the wiring itself: a mutation that computes the right
// default in normalizeConfig but stops applying it in SQL() would pass the
// tests above yet still leave the pool unbounded in production.
func TestResourcePool_SQLAppliesDefaultMaxOpenConns(t *testing.T) {
	p := NewDefaultResourcePool(types.ResourcePoolConfig{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	db, err := p.SQL(context.Background(), poolDriverName, poolDSN)
	if err != nil {
		t.Fatalf("SQL() error = %v", err)
	}
	if got := db.Stats().MaxOpenConnections; got != 25 {
		t.Fatalf("db.Stats().MaxOpenConnections = %d, want 25: this is the default as actually applied "+
			"to the live *sql.DB, not merely what the config struct holds in memory. "+
			"database/sql.(*DB).SetMaxOpenConns(n) treats n<=0 as UNLIMITED, so a regression here means "+
			"the pool imposes no connection cap at all in production, not just a bigger one", got)
	}
}
