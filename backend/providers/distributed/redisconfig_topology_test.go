package distributed

import (
	"testing"

	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// The client type newRedisClient returns is the whole of the topology
// contract: a *redis.Client does not follow MOVED, so pointing one at a real
// cluster works for whatever keys happen to live on the seed node and fails
// for every other slot.
//
// go-redis picks that type from the options alone — there is no "mode" field
// it reads. NewUniversalClient reaches the cluster constructor only when
// len(Addrs) > 1 or IsClusterMode is set (universal.go:385-394). Our own
// RedisConfig.validate accepts a cluster with a single address, and a single
// address is the normal way to configure one: a bootstrap seed, a DNS name
// that resolves to the cluster, or an Elasticache configuration endpoint,
// which has no other shape.
//
// The existing TestNewRedisClientCluster passes two addresses and says so in a
// comment — the fixture was chosen so the assertion would hold, which means the
// single-address case was never covered by anything.
//
// The failure is silent and it splits one deployment in two: distributed.New
// builds the state plane through newRedisClient (backend.go:318) and the queue
// plane through AsAsynqConnOpt (backend.go:348), and AsAsynqConnOpt returns
// asynq's RedisClusterClientOpt for every cluster config regardless of address
// count. So with one seed the queue speaks cluster and the state plane does
// not, from a single config block, with no error at startup.
func TestNewRedisClientClusterWithOneSeedIsStillAClusterClient(t *testing.T) {
	cfg := RedisConfig{
		Mode:  RedisModeCluster,
		Addrs: []string{"127.0.0.1:6379"},
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() rejected a single-seed cluster config: %v — if this "+
			"ever becomes an error the test below is moot, but then cmd/server "+
			"must reject it too", err)
	}

	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, ok := client.(*redis.ClusterClient); !ok {
		t.Fatalf("newRedisClient(cluster, 1 addr) type = %T, want *redis.ClusterClient: "+
			"a plain client does not follow MOVED, so every key outside the seed "+
			"node's slot range fails at runtime — while the asynq half of the same "+
			"RedisConfig still builds a real cluster client", client)
	}
}

// TestNewRedisClientSentinelWithOneAddrStaysAFailoverClient is the positive
// control for the fix above. IsClusterMode is not a blanket "always cluster"
// switch: go-redis combines it with MasterName to select
// NewFailoverClusterClient, a third client type that talks to sentinels *and*
// shards. Setting it unconditionally would silently convert every sentinel
// deployment into that.
//
// One sentinel address is also the case the address-count dispatch would get
// wrong in the other direction, so it is the fixture that pins both halves.
func TestNewRedisClientSentinelWithOneAddrStaysAFailoverClient(t *testing.T) {
	cfg := RedisConfig{
		Mode:       RedisModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379"},
	}
	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, ok := client.(*redis.ClusterClient); ok {
		t.Fatal("newRedisClient(sentinel) returned a *redis.ClusterClient: " +
			"MasterName together with IsClusterMode selects a failover *cluster* " +
			"client, which is not what a sentinel deployment is")
	}
	if _, ok := client.(*redis.Client); !ok {
		t.Fatalf("newRedisClient(sentinel, 1 addr) type = %T, want a failover *redis.Client", client)
	}
}

// TestNewRedisClientSentinelWithManyAddrsStaysAFailoverClient covers the other
// side of the count dispatch: with MasterName set, more than one address must
// still not turn the client into a cluster. This is the case the address-count
// rule happens to get right, kept so a future change that keys off the count
// again cannot pass by only handling one of the two.
func TestNewRedisClientSentinelWithManyAddrsStaysAFailoverClient(t *testing.T) {
	cfg := RedisConfig{
		Mode:       RedisModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"},
	}
	client, err := newRedisClient(cfg)
	if err != nil {
		t.Fatalf("newRedisClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, ok := client.(*redis.Client); !ok {
		t.Fatalf("newRedisClient(sentinel, 3 addrs) type = %T, want a failover *redis.Client", client)
	}
}

// TestAsynqConnOptAgreesWithTheClientTopology pins the two planes together.
// They are built from one RedisConfig by two different functions, and nothing
// else in the repo compares them: distributed.New calls newRedisClient for the
// state plane and AsAsynqConnOpt for the queue plane, and a disagreement about
// what the deployment *is* shows up only as runtime MOVED errors on one of
// them.
func TestAsynqConnOptAgreesWithTheClientTopology(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  RedisConfig
	}{
		{"cluster one seed", RedisConfig{Mode: RedisModeCluster, Addrs: []string{"127.0.0.1:6379"}}},
		{"cluster many", RedisConfig{Mode: RedisModeCluster, Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := newRedisClient(tc.cfg)
			if err != nil {
				t.Fatalf("newRedisClient() error = %v", err)
			}
			defer func() { _ = client.Close() }()
			_, stateIsCluster := client.(*redis.ClusterClient)

			opt, err := tc.cfg.AsAsynqConnOpt()
			if err != nil {
				t.Fatalf("AsAsynqConnOpt() error = %v", err)
			}
			_, queueIsCluster := opt.(asynqlib.RedisClusterClientOpt)

			if stateIsCluster != queueIsCluster {
				t.Fatalf("state plane cluster=%v but queue plane cluster=%v for the "+
					"same RedisConfig: one half of the backend would not follow MOVED",
					stateIsCluster, queueIsCluster)
			}
		})
	}
}
