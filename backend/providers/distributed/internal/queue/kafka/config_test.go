package kafka

import (
	"strings"
	"testing"
	"time"
)

func TestConfigValidate_Accepts(t *testing.T) {
	for name, cfg := range map[string]Config{
		"minimal":         {Brokers: []string{"b:9092"}},
		"ipv6 broker":     {Brokers: []string{"[::1]:9092"}},
		"several brokers": {Brokers: []string{"a:9092", "10.0.0.2:9093"}},
		"non-positive consumer fields fall back": {
			Brokers: []string{"b:9092"}, SessionTimeout: -1, HeartbeatInterval: -1, RebalanceTimeout: -1,
			FetchMaxBytes: -1, MaxRetries: -1, MaxPending: -1, HandlerTimeout: -1, ShutdownTimeout: -1,
			MaxRetryBackoff: -1, BatchBytes: -1, WriteTimeout: -1, MaxAttempts: -1,
		},
		"heartbeat exactly a third of the session": {
			Brokers: []string{"b:9092"}, SessionTimeout: 30 * time.Second, HeartbeatInterval: 10 * time.Second,
		},
		"fetch limit equal to batch limit": {
			Brokers: []string{"b:9092"}, BatchBytes: 2 << 20, FetchMaxBytes: 2 << 20,
		},
		"fetch limit follows the batch limit": {Brokers: []string{"b:9092"}, BatchBytes: 2 << 20},
		"valid sasl": {
			Brokers: []string{"b:9092"}, SASLMechanism: "scram-sha-512", SASLUsername: "u", SASLPassword: "p",
		},
		"create topics with counts": {
			Brokers: []string{"b:9092"}, CreateTopics: true, Partitions: 3, ReplicationFactor: 1,
		},
		"max enqueue delay at the floor": {
			Brokers: []string{"b:9092"}, MaxEnqueueDelay: minMaxEnqueueDelay,
		},
		"max enqueue delay at the ceiling": {
			Brokers: []string{"b:9092"}, MaxEnqueueDelay: maxMaxEnqueueDelay,
		},
		"max enqueue delay above the engine's longest delay": {
			Brokers: []string{"b:9092"}, MaxEnqueueDelay: defaultMaxEnqueueDelay,
		},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: Validate = %v, want nil", name, err)
		}
	}
}

// Each rejected Config names the field and shows the offending value.
func TestConfigValidate_Rejects(t *testing.T) {
	ok := func() Config { return Config{Brokers: []string{"b:9092"}} }
	with := func(mutate func(*Config)) Config {
		c := ok()
		mutate(&c)
		return c
	}
	cases := []struct {
		name string
		cfg  Config
		want []string // substrings the error must contain
	}{
		{"no brokers", Config{}, []string{"Brokers"}},
		{"empty broker list", Config{Brokers: []string{}}, []string{"Brokers"}},
		{"broker without port", Config{Brokers: []string{"b"}}, []string{"Brokers[0]", `"b"`}},
		{"broker with empty host", Config{Brokers: []string{":9092"}}, []string{"Brokers[0]", `":9092"`, "host"}},
		{"broker with empty port", Config{Brokers: []string{"b:"}}, []string{"Brokers[0]", `"b:"`, "port"}},
		{"bad broker after a good one", Config{Brokers: []string{"a:9092", "oops"}}, []string{"Brokers[1]", `"oops"`}},
		{"sasl without credentials", with(func(c *Config) { c.SASLMechanism = "plain" }), []string{"sasl", "plain"}},
		{"unsupported sasl mechanism", with(func(c *Config) {
			c.SASLMechanism, c.SASLUsername, c.SASLPassword = "gssapi", "u", "p"
		}), []string{"gssapi"}},
		{"heartbeat above a third of the default session", with(func(c *Config) { c.HeartbeatInterval = 11 * time.Second }),
			[]string{"HeartbeatInterval 11s", "SessionTimeout", "30s"}},
		{"heartbeat above a third of an explicit session", with(func(c *Config) {
			c.SessionTimeout, c.HeartbeatInterval = 12*time.Second, 5*time.Second
		}), []string{"HeartbeatInterval 5s", "12s"}},
		{"default heartbeat against a short session", with(func(c *Config) { c.SessionTimeout = 6 * time.Second }),
			[]string{"HeartbeatInterval 3s", "6s"}},
		{"fetch limit below batch limit", with(func(c *Config) { c.BatchBytes, c.FetchMaxBytes = 2<<20, 1<<20 }),
			[]string{"FetchMaxBytes 1048576", "BatchBytes 2097152"}},
		{"fetch limit below the default batch limit", with(func(c *Config) { c.FetchMaxBytes = 1 << 20 }),
			[]string{"FetchMaxBytes 1048576", "BatchBytes 8388608"}},
		{"counts without create topics: partitions", with(func(c *Config) { c.Partitions = 3 }),
			[]string{"Partitions 3", "CreateTopics"}},
		{"counts without create topics: replication", with(func(c *Config) { c.ReplicationFactor = 2 }),
			[]string{"ReplicationFactor 2", "CreateTopics"}},
		{"negative count without create topics", with(func(c *Config) { c.Partitions = -1 }),
			[]string{"Partitions -1", "CreateTopics"}},
		{"create topics without partitions", with(func(c *Config) { c.CreateTopics, c.ReplicationFactor = true, 1 }),
			[]string{"Partitions 0", "CreateTopics"}},
		{"create topics without replication", with(func(c *Config) { c.CreateTopics, c.Partitions = true, 3 }),
			[]string{"ReplicationFactor 0", "CreateTopics"}},
		{"create topics with negative partitions", with(func(c *Config) {
			c.CreateTopics, c.Partitions, c.ReplicationFactor = true, -3, 1
		}), []string{"Partitions -3"}},
		{"create topics with replication beyond the wire field", with(func(c *Config) {
			c.CreateTopics, c.Partitions, c.ReplicationFactor = true, 3, 1<<16
		}), []string{"ReplicationFactor 65536"}},
		// Mutation note: deleting any branch of validateMaxEnqueueDelay turns its
		// row red; so does widening minMaxEnqueueDelay past 6m or narrowing the
		// accepted range, which would reject or accept a config the comment on
		// the field promises the other way.
		{"max enqueue delay below the engine's longest retry delay", with(func(c *Config) {
			c.MaxEnqueueDelay = 30 * time.Second
		}), []string{"MaxEnqueueDelay 30s", "6m0s"}},
		{"max enqueue delay just under the floor", with(func(c *Config) {
			c.MaxEnqueueDelay = minMaxEnqueueDelay - time.Nanosecond
		}), []string{"5m59.999999999s", "6m0s"}},
		{"max enqueue delay above the ceiling", with(func(c *Config) {
			c.MaxEnqueueDelay = maxMaxEnqueueDelay + time.Second
		}), []string{"MaxEnqueueDelay 1h0m1s", "1h0m0s"}},
		{"negative max enqueue delay names the default", with(func(c *Config) {
			c.MaxEnqueueDelay = -time.Minute
		}), []string{"MaxEnqueueDelay -1m0s", "15m0s"}},
	}
	for _, tc := range cases {
		err := tc.cfg.Validate()
		if err == nil {
			t.Errorf("%s: Validate = nil, want an error", tc.name)
			continue
		}
		for _, w := range tc.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: error %q does not contain %q", tc.name, err, w)
			}
		}
	}
}

func TestNew_ReturnsValidateError(t *testing.T) {
	cfg := Config{Brokers: []string{"no-port"}}
	want := cfg.Validate()
	if want == nil {
		t.Fatal("setup: Validate accepted a broker without a port")
	}
	tr, err := New(cfg, "", withWriter(&fakeWriter{}))
	if tr != nil || err == nil || err.Error() != want.Error() {
		t.Errorf("New = %v, %v; want no transport and the Validate error %q", tr, err, want)
	}
}
