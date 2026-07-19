package distributed

import (
	"strings"
	"testing"

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
