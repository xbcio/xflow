package distributed

import (
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Redis deployment modes supported by RedisConfig.
const (
	RedisModeSingle   = "single"
	RedisModeSentinel = "sentinel"
	RedisModeCluster  = "cluster"
)

// RedisConfig describes how to connect to a single-node, sentinel or cluster
// Redis deployment. It is consumed by distributed.New via WithRedisConfig.
// When a RedisConfig is injected, New builds the appropriate go-redis client
// with redis.NewUniversalClient; otherwise New keeps the legacy single-address
// behavior.
type RedisConfig struct {
	Mode       string
	Addrs      []string
	MasterName string
	Username   string
	Password   string
	TLSConfig  *tls.Config
	DB         int
}

// validate checks that the configuration is self-consistent. It is fail-closed:
// any invalid combination returns an error.
func (c RedisConfig) validate() error {
	switch c.Mode {
	case RedisModeSingle:
		if c.MasterName != "" {
			return errors.New("redis single mode does not use MasterName")
		}
	case RedisModeSentinel:
		if c.MasterName == "" {
			return errors.New("redis sentinel mode requires MasterName")
		}
		if len(c.Addrs) == 0 {
			return errors.New("redis sentinel mode requires at least one sentinel address")
		}
	case RedisModeCluster:
		if c.MasterName != "" {
			return errors.New("redis cluster mode does not use MasterName")
		}
		if len(c.Addrs) == 0 {
			return errors.New("redis cluster mode requires at least one cluster address")
		}
	default:
		return fmt.Errorf("unsupported redis mode %q", c.Mode)
	}
	return nil
}

// firstAddr returns the first configured address, or an empty string if none is
// configured. It is used to keep the legacy asynq transport address string alive
// until Task 3.1 replaces it with HA-specific connection options.
func (c RedisConfig) firstAddr() string {
	if len(c.Addrs) == 0 {
		return ""
	}
	return c.Addrs[0]
}

// newRedisClient constructs the go-redis client appropriate for cfg.Mode.
// The caller owns the returned client and must Close it.
func newRedisClient(cfg RedisConfig) (redis.UniversalClient, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	switch cfg.Mode {
	case RedisModeSingle:
		addr := cfg.firstAddr()
		return redis.NewClient(&redis.Options{
			Addr:      addr,
			Username:  cfg.Username,
			Password:  cfg.Password,
			TLSConfig: cfg.TLSConfig,
			DB:        cfg.DB,
		}), nil
	case RedisModeSentinel, RedisModeCluster:
		// redis.NewUniversalClient returns a *redis.Client failover client for
		// sentinel and a *redis.ClusterClient for cluster when Addrs has more
		// than one entry (or even one entry without MasterName).
		return redis.NewUniversalClient(&redis.UniversalOptions{
			MasterName: cfg.MasterName,
			Addrs:      cfg.Addrs,
			Username:   cfg.Username,
			Password:   cfg.Password,
			TLSConfig:  cfg.TLSConfig,
			DB:         cfg.DB,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported redis mode %q", cfg.Mode)
	}
}
