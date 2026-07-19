package main

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/distributed"
	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/service/apiserver"
)

func TestParseServerConfigSupportsMemoryMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":9090", "-memory", "-concurrency", "3"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.addr != ":9090" || !cfg.memory || cfg.concurrency != 3 {
		t.Fatalf("config = %+v, want addr :9090 memory concurrency 3", cfg)
	}
}

func TestParseServerConfigSupportsRedisMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":8080", "-redis", "localhost:6379"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redis != "localhost:6379" || cfg.memory {
		t.Fatalf("config = %+v, want redis localhost:6379", cfg)
	}
}

func TestParseServerConfigSupportsObservabilityFlags(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-memory",
		"-log-format", "json",
		"-metrics-addr", "127.0.0.1:0",
		"-metrics-path", "/custom-metrics",
		"-trace", "disabled",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.logFormat != "json" || cfg.metricsAddr != "127.0.0.1:0" || cfg.metricsPath != "/custom-metrics" || cfg.traceMode != "disabled" {
		t.Fatalf("config = %+v, want observability flags parsed", cfg)
	}
}

func TestParseServerConfigRejectsUnsupportedTraceMode(t *testing.T) {
	if _, err := parseServerConfig([]string{"-trace", "bogus"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for unsupported trace mode")
	}
}

func TestParseServerConfigSupportsTraceModes(t *testing.T) {
	for _, mode := range []string{"disabled", "stdout", "otlp"} {
		cfg, err := parseServerConfig([]string{"-memory", "-trace", mode})
		if err != nil {
			t.Fatalf("parseServerConfig(-trace %s) error = %v", mode, err)
		}
		if cfg.traceMode != mode {
			t.Fatalf("traceMode = %q, want %q", cfg.traceMode, mode)
		}
	}
}

func TestBuildLoggerRejectsUnknownFormat(t *testing.T) {
	if _, err := buildLogger(serverConfig{logFormat: "xml"}); err == nil {
		t.Fatal("buildLogger() error = nil, want error for unknown format")
	}
}

func TestBuildLoggerUsesZap(t *testing.T) {
	log, err := buildLogger(serverConfig{logFormat: "json"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := log.(obslogger.ZapLogger); !ok {
		t.Fatalf("buildLogger() = %T, want observability/logger.ZapLogger", log)
	}
}

func TestRunServerBuildsFromMemoryConfig(t *testing.T) {
	srv, err := apiserver.New(apiserver.Config{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if srv == nil {
		t.Fatal("apiserver.New returned nil")
	}
}

func TestParseServerConfigSupportsAPIAuthToken(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-api-auth-token", "mysecrettoken"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.apiAuthToken != "mysecrettoken" {
		t.Fatalf("apiAuthToken = %q, want mysecrettoken", cfg.apiAuthToken)
	}
}

func TestParseServerConfigRequireAPIAuthFlag(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-require-api-auth"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.requireAPIAuth {
		t.Fatal("requireAPIAuth = false, want true")
	}
}

func TestParseServerConfigSupportsManagementFlag(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-management"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.management {
		t.Fatal("management = false, want true")
	}
}

func TestParseServerConfigRedisModeFlag(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-redis-mode", "sentinel",
		"-redis-sentinel-master", "mymaster",
		"-redis-sentinel-addrs", "s1:26379,s2:26379",
		"-redis-password", "secret",
		"-redis-db", "3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redisMode != "sentinel" {
		t.Fatalf("redisMode = %q, want sentinel", cfg.redisMode)
	}
	if cfg.memory {
		t.Fatal("memory = true, want false when HA topology is configured")
	}
	if cfg.redisSentinelMaster != "mymaster" || cfg.redisSentinelAddrs != "s1:26379,s2:26379" || cfg.redisPassword != "secret" || cfg.redisDB != 3 {
		t.Fatalf("unexpected HA flags: %+v", cfg)
	}
}

func TestParseServerConfigRejectsInvalidRedisMode(t *testing.T) {
	if _, err := parseServerConfig([]string{"-redis-mode", "bogus"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for invalid --redis-mode")
	}
}

func TestBuildRedisConfigLegacySingleReturnsNil(t *testing.T) {
	cfg := serverConfig{redis: "localhost:6379"}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc != nil {
		t.Fatalf("buildRedisConfig() = %+v, want nil for legacy --redis", rc)
	}
}

func TestBuildRedisConfigDefaultRedisEmptyGoesInMemory(t *testing.T) {
	cfg := serverConfig{}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc != nil {
		t.Fatalf("buildRedisConfig() = %+v, want nil for empty redis", rc)
	}
}

func TestBuildRedisConfigSingleMode(t *testing.T) {
	cfg := serverConfig{redis: "localhost:6379", redisMode: "single", redisPassword: "secret", redisDB: 2, redisTLS: true}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("buildRedisConfig() = nil, want RedisConfig")
	}
	if rc.Mode != distributed.RedisModeSingle || len(rc.Addrs) != 1 || rc.Addrs[0] != "localhost:6379" || rc.Password != "secret" || rc.DB != 2 || rc.TLSConfig == nil {
		t.Fatalf("unexpected RedisConfig: %+v", rc)
	}
}

func TestBuildRedisConfigSentinelRequiresMaster(t *testing.T) {
	cfg := serverConfig{redisMode: "sentinel", redisSentinelAddrs: "s1:26379"}
	if _, err := buildRedisConfig(cfg); err == nil {
		t.Fatal("buildRedisConfig() error = nil, want error for missing master")
	}
}

func TestBuildRedisConfigSentinelRequiresAddrs(t *testing.T) {
	cfg := serverConfig{redisMode: "sentinel", redisSentinelMaster: "mymaster"}
	if _, err := buildRedisConfig(cfg); err == nil {
		t.Fatal("buildRedisConfig() error = nil, want error for missing sentinel addrs")
	}
}

func TestBuildRedisConfigSentinelMode(t *testing.T) {
	cfg := serverConfig{
		redisMode:           "sentinel",
		redisSentinelMaster: "mymaster",
		redisSentinelAddrs:  "s1:26379, s2:26379 ,s3:26379",
		redisUsername:       "u",
		redisPassword:       "p",
		redisDB:             1,
		redisTLS:            true,
	}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("buildRedisConfig() = nil, want RedisConfig")
	}
	want := []string{"s1:26379", "s2:26379", "s3:26379"}
	if rc.Mode != distributed.RedisModeSentinel || rc.MasterName != "mymaster" || len(rc.Addrs) != 3 || strings.Join(rc.Addrs, ",") != strings.Join(want, ",") || rc.Username != "u" || rc.Password != "p" || rc.SentinelUsername != "u" || rc.SentinelPassword != "p" || rc.DB != 1 || rc.TLSConfig == nil {
		t.Fatalf("unexpected RedisConfig: %+v", rc)
	}
}

func TestBuildRedisConfigSentinelUsesDedicatedCreds(t *testing.T) {
	cfg := serverConfig{
		redisMode:             "sentinel",
		redisSentinelMaster:   "mymaster",
		redisSentinelAddrs:    "s1:26379,s2:26379",
		redisUsername:         "master-user",
		redisPassword:         "master-pass",
		redisSentinelUsername: "sentinel-user",
		redisSentinelPassword: "sentinel-pass",
	}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("buildRedisConfig() = nil, want RedisConfig")
	}
	if rc.Username != "master-user" || rc.Password != "master-pass" {
		t.Fatalf("unexpected master creds: username=%q password=%q", rc.Username, rc.Password)
	}
	if rc.SentinelUsername != "sentinel-user" || rc.SentinelPassword != "sentinel-pass" {
		t.Fatalf("unexpected sentinel creds: username=%q password=%q", rc.SentinelUsername, rc.SentinelPassword)
	}
}

func TestBuildRedisConfigSentinelFallsBackToMasterCreds(t *testing.T) {
	cfg := serverConfig{
		redisMode:           "sentinel",
		redisSentinelMaster: "mymaster",
		redisSentinelAddrs:  "s1:26379",
		redisUsername:       "shared-user",
		redisPassword:       "shared-pass",
	}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("buildRedisConfig() = nil, want RedisConfig")
	}
	if rc.SentinelUsername != "shared-user" || rc.SentinelPassword != "shared-pass" {
		t.Fatalf("sentinel creds did not fall back to master creds: username=%q password=%q", rc.SentinelUsername, rc.SentinelPassword)
	}
}

func TestParseServerConfigSupportsSentinelAuthFlags(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-redis-mode", "sentinel",
		"-redis-sentinel-master", "mymaster",
		"-redis-sentinel-addrs", "s1:26379",
		"-redis-username", "master-user",
		"-redis-password", "master-pass",
		"-redis-sentinel-username", "sentinel-user",
		"-redis-sentinel-password", "sentinel-pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redisUsername != "master-user" || cfg.redisPassword != "master-pass" {
		t.Fatalf("master creds = %q/%q, want master-user/master-pass", cfg.redisUsername, cfg.redisPassword)
	}
	if cfg.redisSentinelUsername != "sentinel-user" || cfg.redisSentinelPassword != "sentinel-pass" {
		t.Fatalf("sentinel creds = %q/%q, want sentinel-user/sentinel-pass", cfg.redisSentinelUsername, cfg.redisSentinelPassword)
	}
}

func TestBuildRedisConfigClusterRequiresAddrs(t *testing.T) {
	cfg := serverConfig{redisMode: "cluster"}
	if _, err := buildRedisConfig(cfg); err == nil {
		t.Fatal("buildRedisConfig() error = nil, want error for missing cluster addrs")
	}
}

func TestBuildRedisConfigClusterMode(t *testing.T) {
	cfg := serverConfig{
		redisMode:         "cluster",
		redisClusterAddrs: "c1:6379,c2:6379",
		redisUsername:     "u",
		redisPassword:     "p",
		redisTLS:          true,
	}
	rc, err := buildRedisConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("buildRedisConfig() = nil, want RedisConfig")
	}
	want := []string{"c1:6379", "c2:6379"}
	if rc.Mode != distributed.RedisModeCluster || len(rc.Addrs) != 2 || strings.Join(rc.Addrs, ",") != strings.Join(want, ",") || rc.Username != "u" || rc.Password != "p" || rc.TLSConfig == nil {
		t.Fatalf("unexpected RedisConfig: %+v", rc)
	}
}
