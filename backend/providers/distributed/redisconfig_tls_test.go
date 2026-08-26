package distributed

import (
	"crypto/tls"
	"testing"

	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// RedisConfig.TLSConfig is threaded into five constructed values —
// three in AsAsynqConnOpt (redisconfig.go:97,108,116) and two in
// newRedisClient (:137,164) — and nothing in the repo ever reads it back.
// grep over redisconfig_test.go finds assertions on Username, Password, DB,
// SentinelUsername and SentinelPassword, and none on TLSConfig, so any one of
// the five lines can be deleted and the whole suite stays green.
//
// It is reachable: cmd/server/main.go:317-319 sets it from the --redis-tls
// flag. Dropping it does not fail to connect — Redis without TLS on the same
// port simply answers, so the connection succeeds and every command, including
// AUTH with the password this same config carries, goes out in cleartext. That
// is the failure mode a TLS flag exists to prevent, and it is silent.
//
// The nil control below matters as much as the non-nil one: without it a
// constructor that always attaches some TLS config satisfies every assertion
// here, which would make every plaintext deployment fail to connect instead.

func tlsProbe() *tls.Config {
	return &tls.Config{ServerName: "redis.probe.invalid", MinVersion: tls.VersionTLS12}
}

func TestAsynqConnOptCarriesTLSConfig(t *testing.T) {
	probe := tlsProbe()

	for _, tc := range []struct {
		name string
		cfg  RedisConfig
		get  func(asynqlib.RedisConnOpt) *tls.Config
	}{
		{
			name: "single",
			cfg:  RedisConfig{Mode: RedisModeSingle, Addrs: []string{"127.0.0.1:6379"}},
			get: func(o asynqlib.RedisConnOpt) *tls.Config {
				return o.(asynqlib.RedisClientOpt).TLSConfig
			},
		},
		{
			name: "sentinel",
			cfg:  RedisConfig{Mode: RedisModeSentinel, MasterName: "mymaster", Addrs: []string{"127.0.0.1:26379"}},
			get: func(o asynqlib.RedisConnOpt) *tls.Config {
				return o.(asynqlib.RedisFailoverClientOpt).TLSConfig
			},
		},
		{
			name: "cluster",
			cfg:  RedisConfig{Mode: RedisModeCluster, Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"}},
			get: func(o asynqlib.RedisConnOpt) *tls.Config {
				return o.(asynqlib.RedisClusterClientOpt).TLSConfig
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Negative control first: an unconfigured RedisConfig must not grow
			// a TLS config out of nowhere, or every plaintext deployment breaks.
			plainOpt, err := tc.cfg.AsAsynqConnOpt()
			if err != nil {
				t.Fatalf("AsAsynqConnOpt() error = %v", err)
			}
			if got := tc.get(plainOpt); got != nil {
				t.Fatalf("TLSConfig = %v for a config that set none: the queue "+
					"would attempt a TLS handshake against a plaintext Redis", got)
			}

			cfg := tc.cfg
			cfg.TLSConfig = probe
			opt, err := cfg.AsAsynqConnOpt()
			if err != nil {
				t.Fatalf("AsAsynqConnOpt() error = %v", err)
			}
			got := tc.get(opt)
			if got == nil {
				t.Fatal("TLSConfig was dropped: the queue plane connects in " +
					"cleartext and sends AUTH with the configured password over it, " +
					"while the operator asked for TLS and nothing reports otherwise")
			}
			if got.ServerName != probe.ServerName {
				t.Fatalf("TLSConfig.ServerName = %q, want %q: a config was attached "+
					"but it is not the one the operator supplied, so certificate "+
					"verification is checking the wrong name", got.ServerName, probe.ServerName)
			}
		})
	}
}

func TestNewRedisClientCarriesTLSConfig(t *testing.T) {
	probe := tlsProbe()

	for _, tc := range []struct {
		name string
		cfg  RedisConfig
	}{
		{"single", RedisConfig{Mode: RedisModeSingle, Addrs: []string{"127.0.0.1:6379"}}},
		{"sentinel", RedisConfig{Mode: RedisModeSentinel, MasterName: "mymaster", Addrs: []string{"127.0.0.1:26379"}}},
		{"cluster", RedisConfig{Mode: RedisModeCluster, Addrs: []string{"127.0.0.1:6379"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plain, err := newRedisClient(tc.cfg)
			if err != nil {
				t.Fatalf("newRedisClient() error = %v", err)
			}
			defer func() { _ = plain.Close() }()
			if got := clientTLSConfig(t, plain); got != nil {
				t.Fatalf("TLSConfig = %v for a config that set none", got)
			}

			cfg := tc.cfg
			cfg.TLSConfig = probe
			client, err := newRedisClient(cfg)
			if err != nil {
				t.Fatalf("newRedisClient() error = %v", err)
			}
			defer func() { _ = client.Close() }()

			got := clientTLSConfig(t, client)
			if got == nil {
				t.Fatal("TLSConfig was dropped: the state plane holds the leases " +
					"and every execution's node state, and would carry all of it, " +
					"plus AUTH, over an unencrypted connection")
			}
			if got.ServerName != probe.ServerName {
				t.Fatalf("TLSConfig.ServerName = %q, want %q", got.ServerName, probe.ServerName)
			}
		})
	}
}

// clientTLSConfig reads the TLS config back off whichever concrete client
// newRedisClient built. Both go-redis client types expose their options, so
// this does not depend on knowing which one was chosen — which is the point:
// the mode-to-type mapping is asserted elsewhere.
func clientTLSConfig(t *testing.T, c redis.UniversalClient) *tls.Config {
	t.Helper()
	switch v := c.(type) {
	case *redis.ClusterClient:
		return v.Options().TLSConfig
	case *redis.Client:
		return v.Options().TLSConfig
	default:
		t.Fatalf("unexpected client type %T", c)
		return nil
	}
}
