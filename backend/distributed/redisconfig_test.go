package distributed

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func TestRedisConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     RedisConfig
		wantErr string
	}{
		{
			name: "single ok empty addrs",
			cfg:  RedisConfig{Mode: RedisModeSingle},
		},
		{
			name: "single ok with addr",
			cfg:  RedisConfig{Mode: RedisModeSingle, Addrs: []string{"127.0.0.1:6379"}},
		},
		{
			name:    "single with MasterName rejected",
			cfg:     RedisConfig{Mode: RedisModeSingle, MasterName: "mymaster"},
			wantErr: "single mode does not use MasterName",
		},
		{
			name:    "single with addr and MasterName rejected",
			cfg:     RedisConfig{Mode: RedisModeSingle, Addrs: []string{"127.0.0.1:6379"}, MasterName: "mymaster"},
			wantErr: "single mode does not use MasterName",
		},
		{
			name: "sentinel ok",
			cfg: RedisConfig{
				Mode:       RedisModeSentinel,
				MasterName: "mymaster",
				Addrs:      []string{"127.0.0.1:26379"},
			},
		},
		{
			name:    "sentinel missing MasterName",
			cfg:     RedisConfig{Mode: RedisModeSentinel, Addrs: []string{"127.0.0.1:26379"}},
			wantErr: "sentinel mode requires MasterName",
		},
		{
			name:    "sentinel missing addrs",
			cfg:     RedisConfig{Mode: RedisModeSentinel, MasterName: "mymaster"},
			wantErr: "sentinel mode requires at least one sentinel address",
		},
		{
			name: "cluster ok",
			cfg: RedisConfig{
				Mode:  RedisModeCluster,
				Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"},
			},
		},
		{
			name:    "cluster empty addrs",
			cfg:     RedisConfig{Mode: RedisModeCluster},
			wantErr: "cluster mode requires at least one cluster address",
		},
		{
			name:    "cluster with MasterName rejected",
			cfg:     RedisConfig{Mode: RedisModeCluster, MasterName: "mymaster", Addrs: []string{"127.0.0.1:6379"}},
			wantErr: "cluster mode does not use MasterName",
		},
		{
			name:    "unknown mode",
			cfg:     RedisConfig{Mode: "replicated"},
			wantErr: "unsupported redis mode",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate() error = nil, want %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validate() error = %q, want containing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestNewRedisClientSingle(t *testing.T) {
	cfg := RedisConfig{
		Mode:  RedisModeSingle,
		Addrs: []string{"127.0.0.1:6379"},
	}
	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	defer client.Close()

	if _, ok := client.(*redis.Client); !ok {
		t.Fatalf("newRedisClient(single) type = %T, want *redis.Client", client)
	}
}

func TestNewRedisClientSentinel(t *testing.T) {
	cfg := RedisConfig{
		Mode:       RedisModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379", "127.0.0.1:26380"},
	}
	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	defer client.Close()

	// go-redis NewUniversalClient with MasterName returns a failover *redis.Client.
	if _, ok := client.(*redis.Client); !ok {
		t.Fatalf("newRedisClient(sentinel) type = %T, want *redis.Client", client)
	}
}

func TestNewRedisClientCluster(t *testing.T) {
	// Use two addresses so NewUniversalClient selects the cluster path and
	// returns a *redis.ClusterClient. A single address would fall back to a
	// plain *redis.Client, which is still valid for construction but not what
	// we want to assert here.
	cfg := RedisConfig{
		Mode:  RedisModeCluster,
		Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"},
	}
	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	defer client.Close()

	if _, ok := client.(*redis.ClusterClient); !ok {
		t.Fatalf("newRedisClient(cluster) type = %T, want *redis.ClusterClient", client)
	}
}

func TestNewRedisClientInvalid(t *testing.T) {
	cfg := RedisConfig{Mode: RedisModeSentinel}
	if _, err := newRedisClient(cfg); err == nil {
		t.Fatal("newRedisClient(invalid) error = nil, want non-nil")
	}
}

func TestWithRedisConfig(t *testing.T) {
	cfg := RedisConfig{
		Mode:  RedisModeSingle,
		Addrs: []string{"127.0.0.1:6379"},
	}
	c := &config{}
	WithRedisConfig(cfg)(c)

	if c.redisConfig == nil {
		t.Fatal("WithRedisConfig did not inject redisConfig")
	}
	if c.redisConfig.Mode != RedisModeSingle {
		t.Fatalf("WithRedisConfig Mode = %q, want %q", c.redisConfig.Mode, RedisModeSingle)
	}
	if len(c.redisConfig.Addrs) != 1 || c.redisConfig.Addrs[0] != "127.0.0.1:6379" {
		t.Fatalf("WithRedisConfig Addrs = %v, want [127.0.0.1:6379]", c.redisConfig.Addrs)
	}
}

func TestNewWithRedisConfigSingle(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	defer mr.Close()

	b, err := New("", nil,
		WithConsumer(false),
		WithRedisConfig(RedisConfig{
			Mode:  RedisModeSingle,
			Addrs: []string{mr.Addr()},
		}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer b.nonConsumerStop()()

	if b.RedisClient() == nil {
		t.Fatal("Backend.RedisClient() = nil, want non-nil")
	}
	if err := b.RedisClient().Ping(context.Background()).Err(); err != nil {
		t.Fatalf("RedisClient().Ping() error = %v", err)
	}
}

func TestRedisConfigAsAsynqConnOptSingle(t *testing.T) {
	cfg := RedisConfig{
		Mode:     RedisModeSingle,
		Addrs:    []string{"127.0.0.1:6379"},
		Username: "u",
		Password: "p",
		DB:       3,
	}
	opt, err := cfg.AsAsynqConnOpt()
	if err != nil {
		t.Fatalf("AsAsynqConnOpt() error = %v", err)
	}
	got, ok := opt.(asynqlib.RedisClientOpt)
	if !ok {
		t.Fatalf("opt type = %T, want RedisClientOpt", opt)
	}
	if got.Addr != "127.0.0.1:6379" {
		t.Fatalf("Addr = %q, want 127.0.0.1:6379", got.Addr)
	}
	if got.Username != "u" || got.Password != "p" || got.DB != 3 {
		t.Fatalf("unexpected fields: username=%q password=%q db=%d", got.Username, got.Password, got.DB)
	}
}

func TestRedisConfigAsAsynqConnOptSentinel(t *testing.T) {
	cfg := RedisConfig{
		Mode:             RedisModeSentinel,
		MasterName:       "mymaster",
		Addrs:            []string{"127.0.0.1:26379", "127.0.0.1:26380"},
		Username:         "u",
		Password:         "p",
		SentinelUsername: "su",
		SentinelPassword: "sp",
		DB:               2,
	}
	opt, err := cfg.AsAsynqConnOpt()
	if err != nil {
		t.Fatalf("AsAsynqConnOpt() error = %v", err)
	}
	got, ok := opt.(asynqlib.RedisFailoverClientOpt)
	if !ok {
		t.Fatalf("opt type = %T, want RedisFailoverClientOpt", opt)
	}
	if got.MasterName != "mymaster" {
		t.Fatalf("MasterName = %q, want mymaster", got.MasterName)
	}
	if len(got.SentinelAddrs) != 2 || got.SentinelAddrs[0] != "127.0.0.1:26379" || got.SentinelAddrs[1] != "127.0.0.1:26380" {
		t.Fatalf("SentinelAddrs = %v, want [127.0.0.1:26379 127.0.0.1:26380]", got.SentinelAddrs)
	}
	if got.Username != "u" || got.Password != "p" || got.DB != 2 {
		t.Fatalf("unexpected fields: username=%q password=%q db=%d", got.Username, got.Password, got.DB)
	}
	if got.SentinelUsername != "su" || got.SentinelPassword != "sp" {
		t.Fatalf("unexpected sentinel fields: sentinelUsername=%q sentinelPassword=%q", got.SentinelUsername, got.SentinelPassword)
	}
}

func TestRedisConfigAsAsynqConnOptSentinelNoAuth(t *testing.T) {
	// Sentinel auth is optional; empty sentinel credentials must still validate
	// and map cleanly.
	cfg := RedisConfig{
		Mode:       RedisModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379"},
		Username:   "u",
		Password:   "p",
		DB:         1,
	}
	opt, err := cfg.AsAsynqConnOpt()
	if err != nil {
		t.Fatalf("AsAsynqConnOpt() error = %v", err)
	}
	got, ok := opt.(asynqlib.RedisFailoverClientOpt)
	if !ok {
		t.Fatalf("opt type = %T, want RedisFailoverClientOpt", opt)
	}
	if got.SentinelUsername != "" || got.SentinelPassword != "" {
		t.Fatalf("sentinel auth fields = %q/%q, want empty", got.SentinelUsername, got.SentinelPassword)
	}
}

func TestRedisConfigAsAsynqConnOptCluster(t *testing.T) {
	cfg := RedisConfig{
		Mode:     RedisModeCluster,
		Addrs:    []string{"127.0.0.1:6379", "127.0.0.1:6380"},
		Username: "u",
		Password: "p",
	}
	opt, err := cfg.AsAsynqConnOpt()
	if err != nil {
		t.Fatalf("AsAsynqConnOpt() error = %v", err)
	}
	got, ok := opt.(asynqlib.RedisClusterClientOpt)
	if !ok {
		t.Fatalf("opt type = %T, want RedisClusterClientOpt", opt)
	}
	if len(got.Addrs) != 2 || got.Addrs[0] != "127.0.0.1:6379" || got.Addrs[1] != "127.0.0.1:6380" {
		t.Fatalf("Addrs = %v, want [127.0.0.1:6379 127.0.0.1:6380]", got.Addrs)
	}
	if got.Username != "u" || got.Password != "p" {
		t.Fatalf("unexpected fields: username=%q password=%q", got.Username, got.Password)
	}
}

func TestRedisConfigAsAsynqConnOptInvalid(t *testing.T) {
	cfg := RedisConfig{Mode: RedisModeSentinel}
	if _, err := cfg.AsAsynqConnOpt(); err == nil {
		t.Fatal("AsAsynqConnOpt(invalid) error = nil, want non-nil")
	}
}
