package main

import (
	"reflect"
	"strings"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// kafkaRedisArgs is a Redis backend plus brokers, the smallest valid Kafka
// selection. Callers append the flag under test.
func kafkaRedisArgs(extra ...string) []string {
	return append([]string{"-redis", "127.0.0.1:1", "-kafka-brokers", "b1:9092"}, extra...)
}

// TestParseServerConfigKafkaDefaultsToAsynq pins the default: with no --kafka-*
// flag the transport stays Asynq, whether the backend is Redis or memory.
func TestParseServerConfigKafkaDefaultsToAsynq(t *testing.T) {
	t.Setenv(envKafkaSASLPassword, "")
	for _, args := range [][]string{{"-memory"}, {"-redis", "127.0.0.1:1"}} {
		cfg, err := parseServerConfig(args)
		if err != nil {
			t.Fatalf("parseServerConfig(%v): %v", args, err)
		}
		if cfg.kafka != nil || cfg.kafkaRequested() {
			t.Fatalf("parseServerConfig(%v): kafka = %+v, want nil (Asynq)", args, cfg.kafka)
		}
	}
}

func TestParseServerConfigKafkaFlags(t *testing.T) {
	t.Setenv(envKafkaSASLPassword, "")
	cfg, err := parseServerConfig([]string{
		"-redis", "127.0.0.1:1",
		"-kafka-brokers", "b1:9092, b2:9092",
		"-kafka-tls",
		"-kafka-sasl-mechanism", "scram-sha-512",
		"-kafka-sasl-username", "svc",
		"-kafka-sasl-password", "flag-pw",
		"-kafka-create-topics", "-kafka-partitions", "6", "-kafka-replication-factor", "3",
	})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	kc := cfg.kafka
	if kc == nil {
		t.Fatal("kafka config is nil; want it built from the flags")
	}
	if !reflect.DeepEqual(kc.Brokers, []string{"b1:9092", "b2:9092"}) {
		t.Errorf("Brokers = %v, want the comma list split and trimmed", kc.Brokers)
	}
	if kc.TLS == nil || kc.TLS.RootCAs != nil || kc.TLS.InsecureSkipVerify {
		t.Errorf("TLS = %+v, want a config that verifies against the system roots", kc.TLS)
	}
	if kc.SASLMechanism != "scram-sha-512" || kc.SASLUsername != "svc" || kc.SASLPassword != "flag-pw" {
		t.Errorf("SASL = %q/%q/<redacted>, want scram-sha-512/svc/flag value", kc.SASLMechanism, kc.SASLUsername)
	}
	if !kc.CreateTopics || kc.Partitions != 6 || kc.ReplicationFactor != 3 {
		t.Errorf("provisioning = %v/%d/%d, want true/6/3", kc.CreateTopics, kc.Partitions, kc.ReplicationFactor)
	}

	// Without --kafka-tls and SASL the transport is plaintext and unauthenticated,
	// and no tuning field is invented.
	cfg, err = parseServerConfig(kafkaRedisArgs())
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if kc = cfg.kafka; kc == nil || kc.TLS != nil || kc.SASLMechanism != "" || kc.CreateTopics ||
		kc.Partitions != 0 || kc.ReplicationFactor != 0 || kc.BatchBytes != 0 || kc.MaxRetries != 0 {
		t.Fatalf("minimal kafka config = %+v, want only the brokers set", kc)
	}
}

// TestParseServerConfigKafkaPasswordEnvOverridesFlag pins the precedence: a set
// XFLOW_KAFKA_SASL_PASSWORD wins over the flag, an empty one leaves it alone,
// and the environment alone is enough.
func TestParseServerConfigKafkaPasswordEnvOverridesFlag(t *testing.T) {
	sasl := []string{"-kafka-sasl-mechanism", "plain", "-kafka-sasl-username", "svc"}
	cases := []struct {
		name, env string
		extra     []string
		want      string
	}{
		{"env wins over flag", "env-pw", append([]string{"-kafka-sasl-password", "flag-pw"}, sasl...), "env-pw"},
		{"empty env keeps flag", "", append([]string{"-kafka-sasl-password", "flag-pw"}, sasl...), "flag-pw"},
		{"env alone", "env-pw", sasl, "env-pw"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(envKafkaSASLPassword, c.env)
			cfg, err := parseServerConfig(kafkaRedisArgs(c.extra...))
			if err != nil {
				t.Fatalf("parseServerConfig: %v", err)
			}
			if cfg.kafka == nil || cfg.kafka.SASLPassword != c.want {
				t.Fatalf("password precedence wrong (want the %q source)", c.want)
			}
		})
	}
}

// TestParseServerConfigRejectsInvalidKafka lists the settings that must fail at
// flag parsing, before any service is contacted.
func TestParseServerConfigRejectsInvalidKafka(t *testing.T) {
	cases := []struct {
		name string
		env  string
		args []string
		want string
	}{
		{"tls without brokers", "", []string{"-redis", "127.0.0.1:1", "-kafka-tls"}, "--kafka-brokers is required"},
		{"partitions without brokers", "", []string{"-redis", "127.0.0.1:1", "-kafka-partitions", "3"}, "--kafka-brokers is required"},
		{"sasl without brokers", "", []string{"-redis", "127.0.0.1:1", "-kafka-sasl-mechanism", "plain"}, "--kafka-brokers is required"},
		{"password env without brokers", "x", []string{"-redis", "127.0.0.1:1"}, "--kafka-brokers is required"},
		{"memory backend", "", []string{"-memory", "-kafka-brokers", "b:9092"}, "Redis backend"},
		{"no redis at all", "", []string{"-kafka-brokers", "b:9092"}, "Redis backend"},
		{"only separators in brokers", "", []string{"-redis", "127.0.0.1:1", "-kafka-brokers", ","}, "at least one broker"},
		{"broker without port", "", []string{"-redis", "127.0.0.1:1", "-kafka-brokers", "nohostport"}, "Brokers[0]"},
		{"sasl without credentials", "", kafkaRedisArgs("-kafka-sasl-mechanism", "plain"), "requires a username and a password"},
		{"unsupported mechanism", "pw", kafkaRedisArgs("-kafka-sasl-mechanism", "gssapi", "-kafka-sasl-username", "u"), "unsupported"},
		{"create-topics without counts", "", kafkaRedisArgs("-kafka-create-topics"), "Partitions"},
		{"partitions without create-topics", "", kafkaRedisArgs("-kafka-partitions", "3"), "only takes effect when CreateTopics"},
		{"replication without create-topics", "", kafkaRedisArgs("-kafka-replication-factor", "2"), "only takes effect when CreateTopics"},
		{"negative partitions", "", kafkaRedisArgs("-kafka-create-topics", "-kafka-partitions", "-1", "-kafka-replication-factor", "1"), "Partitions -1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(envKafkaSASLPassword, c.env)
			_, err := parseServerConfig(c.args)
			if err == nil {
				t.Fatalf("parseServerConfig(%v) accepted an unusable Kafka setting", c.args)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %q, want it to mention %q", err, c.want)
			}
		})
	}
}

// TestParseServerConfigKafkaErrorsNeverEchoThePassword covers each path that
// fails with a password in play, including the SCRAM builder whose own error
// quotes the password it could not normalise (an ASCII control character is
// prohibited by SASLprep).
func TestParseServerConfigKafkaErrorsNeverEchoThePassword(t *testing.T) {
	const secret = "s3cret-pw"
	cases := []struct {
		name, password string
		args           []string
	}{
		{"scram normalisation failure via env", secret + "\x07",
			kafkaRedisArgs("-kafka-sasl-mechanism", "scram-sha-256", "-kafka-sasl-username", "u")},
		{"scram normalisation failure via flag", "",
			kafkaRedisArgs("-kafka-sasl-mechanism", "scram-sha-512", "-kafka-sasl-username", "u", "-kafka-sasl-password", secret+"\x07")},
		{"unsupported mechanism", secret,
			kafkaRedisArgs("-kafka-sasl-mechanism", "gssapi", "-kafka-sasl-username", "u")},
		{"missing username", secret,
			kafkaRedisArgs("-kafka-sasl-mechanism", "plain")},
		{"memory backend", secret,
			[]string{"-memory", "-kafka-brokers", "b:9092", "-kafka-sasl-mechanism", "plain", "-kafka-sasl-username", "u"}},
		{"no brokers", secret,
			[]string{"-redis", "127.0.0.1:1", "-kafka-sasl-mechanism", "plain", "-kafka-sasl-username", "u"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(envKafkaSASLPassword, c.password)
			_, err := parseServerConfig(c.args)
			if err == nil {
				t.Fatalf("parseServerConfig(%v) succeeded; want a failure to inspect", c.args)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error echoes the SASL password: %q", err)
			}
		})
	}
}

// TestBuildServerOptionsReachesKafkaQueue proves the middle hop: the parsed
// Kafka configuration is attached to the server options and arrives at the
// apiserver. NewServer gets no Redis here, which the apiserver refuses for a
// Kafka selection, so an error naming KafkaQueue can only come from the option
// having travelled flag -> serverConfig -> ServerOption -> apiserver.Config.
func TestBuildServerOptionsReachesKafkaQueue(t *testing.T) {
	t.Setenv(envKafkaSASLPassword, "")
	cfg, err := parseServerConfig(append([]string{"-mode", "dev"}, kafkaRedisArgs()...))
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	_, err = xflowsdk.NewServer(xflowsdk.ServerConfig{}, buildServerOptions(cfg, serverDeps{})...)
	if err == nil || !strings.Contains(err.Error(), "KafkaQueue") {
		t.Fatalf("NewServer without Redis = %v, want the apiserver's KafkaQueue-requires-Redis error", err)
	}

	// The same wiring without a Kafka flag must not mention Kafka at all.
	cfg, err = parseServerConfig([]string{"-mode", "dev", "-memory"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if _, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, buildServerOptions(cfg, serverDeps{})...); err != nil {
		t.Fatalf("NewServer with the default transport: %v", err)
	}
}
