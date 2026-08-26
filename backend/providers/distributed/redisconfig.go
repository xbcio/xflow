package distributed

import (
	"crypto/tls"
	"errors"
	"fmt"

	asynqlib "github.com/hibiken/asynq"
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
	Mode             string
	Addrs            []string
	MasterName       string
	Username         string
	Password         string
	SentinelUsername string
	SentinelPassword string
	TLSConfig        *tls.Config
	DB               int
}

// validate checks that the configuration is self-consistent. It is fail-closed:
// any invalid combination returns an error.
func (c RedisConfig) validate() error {
	switch c.Mode {
	case RedisModeSingle:
		if c.MasterName != "" {
			return errors.New("redis single mode does not use MasterName")
		}
		if len(c.Addrs) == 0 {
			return errors.New("redis single mode requires at least one address")
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

// Validate is the exported entry point for RedisConfig validation. It returns
// the same result as the unexported validate method and is exposed so callers
// outside the distributed package can reuse the fail-closed checks.
func (c RedisConfig) Validate() error {
	return c.validate()
}

// firstAddr returns the first configured address, or an empty string if none is
// configured. It is used for single-mode client construction.
func (c RedisConfig) firstAddr() string {
	if len(c.Addrs) == 0 {
		return ""
	}
	return c.Addrs[0]
}

// AsAsynqConnOpt maps the RedisConfig to the equivalent asynq RedisConnOpt.
// It returns RedisClientOpt for single-node, RedisFailoverClientOpt for
// sentinel, and RedisClusterClientOpt for cluster deployments.
func (c RedisConfig) AsAsynqConnOpt() (asynqlib.RedisConnOpt, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	switch c.Mode {
	case RedisModeSingle:
		return asynqlib.RedisClientOpt{
			Addr:      c.firstAddr(),
			Username:  c.Username,
			Password:  c.Password,
			TLSConfig: c.TLSConfig,
			DB:        c.DB,
		}, nil
	case RedisModeSentinel:
		return asynqlib.RedisFailoverClientOpt{
			MasterName:       c.MasterName,
			SentinelAddrs:    c.Addrs,
			Username:         c.Username,
			Password:         c.Password,
			SentinelUsername: c.SentinelUsername,
			SentinelPassword: c.SentinelPassword,
			TLSConfig:        c.TLSConfig,
			DB:               c.DB,
		}, nil
	case RedisModeCluster:
		return asynqlib.RedisClusterClientOpt{
			Addrs:     c.Addrs,
			Username:  c.Username,
			Password:  c.Password,
			TLSConfig: c.TLSConfig,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported redis mode %q", c.Mode)
	}
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
		// redis.NewUniversalClient does not dispatch on any explicit mode: with
		// MasterName set it builds a failover *redis.Client, and otherwise it
		// only reaches the cluster constructor when len(Addrs) > 1 or
		// IsClusterMode is set (universal.go:385-394). A cluster given a single
		// seed address would therefore fall through to a plain *redis.Client,
		// which does not follow MOVED — and a single seed is the normal way to
		// bootstrap cluster discovery, as well as the only shape an Elasticache
		// configuration endpoint has. The asynq half of the same RedisConfig
		// builds a real cluster client from the same address list, so the two
		// planes would disagree about the topology of one deployment.
		//
		// IsClusterMode pins the choice to our own mode instead of to the
		// address count. It must stay false for sentinel: combined with
		// MasterName it selects NewFailoverClusterClient, which is a third
		// thing again.
		return redis.NewUniversalClient(&redis.UniversalOptions{
			MasterName:       cfg.MasterName,
			Addrs:            cfg.Addrs,
			IsClusterMode:    cfg.Mode == RedisModeCluster,
			Username:         cfg.Username,
			Password:         cfg.Password,
			SentinelUsername: cfg.SentinelUsername,
			SentinelPassword: cfg.SentinelPassword,
			TLSConfig:        cfg.TLSConfig,
			DB:               cfg.DB,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported redis mode %q", cfg.Mode)
	}
}
