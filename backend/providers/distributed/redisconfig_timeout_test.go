package distributed

import (
	"testing"
	"time"

	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// TestRedisTimeoutsReachBothClients pins that a configured timeout reaches the
// go-redis client AND the asynq connection.
//
// The two planes are built from one RedisConfig through two separate code
// paths (newRedisClient and AsAsynqConnOpt), and they are easy to update
// independently. Leaving one at go-redis's 3s default while the deployment
// needs ~20s means half the pipeline keeps failing with "i/o timeout" and the
// symptom points at Redis rather than at the half that was missed.
func TestRedisTimeoutsReachBothClients(t *testing.T) {
	cfg := RedisConfig{
		Mode:         RedisModeSingle,
		Addrs:        []string{"127.0.0.1:6379"},
		DB:           3,
		DialTimeout:  7 * time.Second,
		ReadTimeout:  21 * time.Second,
		WriteTimeout: 22 * time.Second,
		PoolTimeout:  23 * time.Second,
	}

	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	plain, ok := client.(*redis.Client)
	if !ok {
		t.Fatalf("newRedisClient(single) = %T, want *redis.Client", client)
	}
	got := plain.Options()
	if got.DialTimeout != cfg.DialTimeout {
		t.Errorf("DialTimeout = %s, want %s", got.DialTimeout, cfg.DialTimeout)
	}
	if got.ReadTimeout != cfg.ReadTimeout {
		t.Errorf("ReadTimeout = %s, want %s", got.ReadTimeout, cfg.ReadTimeout)
	}
	if got.WriteTimeout != cfg.WriteTimeout {
		t.Errorf("WriteTimeout = %s, want %s", got.WriteTimeout, cfg.WriteTimeout)
	}
	if got.PoolTimeout != cfg.PoolTimeout {
		t.Errorf("PoolTimeout = %s, want %s", got.PoolTimeout, cfg.PoolTimeout)
	}

	connOpt, err := cfg.AsAsynqConnOpt()
	if err != nil {
		t.Fatalf("AsAsynqConnOpt() error = %v", err)
	}
	asynqOpt, ok := connOpt.(asynqlib.RedisClientOpt)
	if !ok {
		t.Fatalf("AsAsynqConnOpt(single) = %T, want asynqlib.RedisClientOpt", connOpt)
	}
	if asynqOpt.DialTimeout != cfg.DialTimeout {
		t.Errorf("asynq DialTimeout = %s, want %s", asynqOpt.DialTimeout, cfg.DialTimeout)
	}
	if asynqOpt.ReadTimeout != cfg.ReadTimeout {
		t.Errorf("asynq ReadTimeout = %s, want %s", asynqOpt.ReadTimeout, cfg.ReadTimeout)
	}
	if asynqOpt.WriteTimeout != cfg.WriteTimeout {
		t.Errorf("asynq WriteTimeout = %s, want %s", asynqOpt.WriteTimeout, cfg.WriteTimeout)
	}
}

// TestRedisTimeoutsZeroKeepsLibraryDefaults is the compatibility half: an
// unconfigured deployment must behave exactly as it did before these fields
// existed, so a zero value has to fall through to go-redis's own defaults
// rather than to a fallback this code picks.
//
// The defaults are spelled out rather than read back from the library: the
// assertion that matters is that xflow adds nothing of its own, and comparing
// against Options().init() values would keep passing if xflow started
// overriding them.
func TestRedisTimeoutsZeroKeepsLibraryDefaults(t *testing.T) {
	cfg := RedisConfig{Mode: RedisModeSingle, Addrs: []string{"127.0.0.1:6379"}}

	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	got := client.(*redis.Client).Options()
	if got.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %s, want go-redis's default 5s", got.DialTimeout)
	}
	if got.ReadTimeout != 3*time.Second {
		t.Errorf("ReadTimeout = %s, want go-redis's default 3s", got.ReadTimeout)
	}
	if got.WriteTimeout != 3*time.Second {
		t.Errorf("WriteTimeout = %s, want go-redis's default 3s", got.WriteTimeout)
	}
}
