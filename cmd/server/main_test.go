package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/providers/distributed"
	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
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

func TestParseServerConfigSupportsAuthTokensFile(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-auth-tokens-file", "/etc/xflow/tokens.json"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.authTokensFile != "/etc/xflow/tokens.json" {
		t.Fatalf("authTokensFile = %q, want /etc/xflow/tokens.json", cfg.authTokensFile)
	}
}

// writeTokenFile writes a JSON token mapping file with the given mode for
// loadAuthTokenMappings tests.
func writeTokenFile(t *testing.T, name, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func TestLoadAuthTokenMappingsParsesMultiNamespaceRegistry(t *testing.T) {
	path := writeTokenFile(t, "tokens.json",
		`[{"token":"tok-a","subject":"op-a","namespace":"namespaceA","scopes":["workflow","management.read"]},{"token":"tok-b","subject":"op-b","namespace":"namespaceB","scopes":["workflow"]}]`,
		0600)
	mappings, err := loadAuthTokenMappings(serverConfig{authTokensFile: path})
	if err != nil {
		t.Fatalf("loadAuthTokenMappings: %v", err)
	}
	if len(mappings) != 2 {
		t.Fatalf("mappings = %d, want 2", len(mappings))
	}
	if mappings[0].Namespace != "namespaceA" || mappings[0].Subject != "op-a" || mappings[0].Token != "tok-a" {
		t.Fatalf("mappings[0] = %+v, want op-a/namespaceA/tok-a", mappings[0])
	}
	if mappings[1].Namespace != "namespaceB" {
		t.Fatalf("mappings[1].Namespace = %q, want namespaceB", mappings[1].Namespace)
	}
}

func TestLoadAuthTokenMappingsRejectsWorldReadableFile(t *testing.T) {
	// 0644 is group/world readable → must be rejected to avoid leaking tokens.
	path := writeTokenFile(t, "tokens.json", `[]`, 0644)
	if _, err := loadAuthTokenMappings(serverConfig{authTokensFile: path}); err == nil {
		t.Fatal("loadAuthTokenMappings() error = nil, want error for group/world-readable token file")
	}
}

func TestLoadAuthTokenMappingsRejectsMissingFields(t *testing.T) {
	// namespace is required (empty normalized to default is only for the
	// single-token constructor path, not the file).
	path := writeTokenFile(t, "tokens.json", `[{"token":"t","subject":"s"}]`, 0600)
	if _, err := loadAuthTokenMappings(serverConfig{authTokensFile: path}); err == nil {
		t.Fatal("loadAuthTokenMappings() error = nil, want error for missing namespace")
	}
}

func TestLoadAuthTokenMappingsNilWhenUnset(t *testing.T) {
	mappings, err := loadAuthTokenMappings(serverConfig{})
	if err != nil {
		t.Fatalf("loadAuthTokenMappings: %v", err)
	}
	if mappings != nil {
		t.Fatalf("mappings = %v, want nil when no file configured", mappings)
	}
}

func TestRunServerMultiNamespaceTokenFileBuildsPrincipalAuth(t *testing.T) {
	// Verify the --auth-tokens-file path end-to-end at the builder layer (which
	// runServer calls): the file is parsed into a multi-namespace token registry
	// and each token resolves to its bound namespace via the authenticator. Tokens
	// are hashed in-process; the file is 0600.
	path := writeTokenFile(t, "tokens.json",
		`[{"token":"tok-a","subject":"op-a","namespace":"namespaceA","scopes":["workflow","execution","management.read","management.write","deadletter.list","deadletter.replay"]},{"token":"tok-b","subject":"op-b","namespace":"namespaceB","scopes":["workflow"]}]`,
		0600)
	cfg, err := parseServerConfig([]string{"-memory", "-auth-tokens-file", path, "-management"})
	if err != nil {
		t.Fatal(err)
	}
	mappings, err := loadAuthTokenMappings(cfg)
	if err != nil {
		t.Fatalf("loadAuthTokenMappings: %v", err)
	}
	auth := apiserver.NewBearerPrincipalAuthMulti(mappings)

	reqA, _ := http.NewRequest(http.MethodGet, "/", nil)
	reqA.Header.Set("Authorization", "Bearer tok-a")
	pA, err := auth.Authenticate(reqA)
	if err != nil {
		t.Fatalf("Authenticate tok-a: %v", err)
	}
	if pA.Namespace != "namespaceA" || pA.Subject != "op-a" {
		t.Fatalf("tok-a principal = %+v, want op-a/namespaceA", pA)
	}

	reqB, _ := http.NewRequest(http.MethodGet, "/", nil)
	reqB.Header.Set("Authorization", "Bearer tok-b")
	pB, err := auth.Authenticate(reqB)
	if err != nil {
		t.Fatalf("Authenticate tok-b: %v", err)
	}
	if pB.Namespace != "namespaceB" {
		t.Fatalf("tok-b namespace = %q, want namespaceB", pB.Namespace)
	}

	// --auth-tokens-file takes precedence over --api-auth-token; verify the
	// flag was parsed and cfg.apiAuthToken is unset (we did not pass it).
	if cfg.apiAuthToken != "" {
		t.Fatalf("apiAuthToken = %q, want empty (file takes precedence)", cfg.apiAuthToken)
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
	want := []string{"c1:6379", "c2:6379"}
	if rc.Mode != distributed.RedisModeCluster || len(rc.Addrs) != 2 || strings.Join(rc.Addrs, ",") != strings.Join(want, ",") || rc.Username != "u" || rc.Password != "p" || rc.TLSConfig == nil {
		t.Fatalf("unexpected RedisConfig: %+v", rc)
	}
}

// TestRunServerMultiNamespaceManagementHTTPAuth is a cmd/server-level end-to-end
// regression test for the Task 7.3 review finding: the outer
// ManagementAuthMiddleware must accept any token in the multi-namespace registry,
// not just the first one. namespaceB (tok-b) must reach the route-level authz
// wrapper and successfully inspect its own execution; unknown tokens are still
// rejected; cross-namespace access remains 404.
func TestRunServerMultiNamespaceManagementHTTPAuth(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	// Build the same backend/control-plane stack runServer would use, but with
	// the queue consumer disabled so queued tasks do not race with the test.
	backend, err := distributed.New(redisServer.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	path := writeTokenFile(t, "tokens.json",
		`[{"token":"tok-a","subject":"op-a","namespace":"namespaceA","scopes":["workflow","execution","management.read"]},`+
			`{"token":"tok-b","subject":"op-b","namespace":"namespaceB","scopes":["workflow","execution","management.read"]}]`,
		0600)
	cfg, err := parseServerConfig([]string{"-redis", redisServer.Addr(), "-auth-tokens-file", path, "-management"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	mappings, err := loadAuthTokenMappings(cfg)
	if err != nil {
		t.Fatalf("loadAuthTokenMappings: %v", err)
	}
	principalAuth := apiserver.NewBearerPrincipalAuthMulti(mappings)

	// Replicate runServer's production wiring: PrincipalAuth drives the B3
	// authz wrapper, and the same registry also gates /v1/management/* via the
	// outer ManagementAuthMiddleware.
	srv, err := apiserver.New(apiserver.Config{
		RedisAddr:     redisServer.Addr(),
		Concurrency:   1,
		PrincipalAuth: principalAuth,
		Authorizer:    apiserver.NamespaceAwareAuthorizer{},
		AuditSink:     apiserver.NewInMemoryAuditSink(),
	}, apiserver.WithControlPlane(cp), apiserver.WithManagement(),
		apiserver.WithHTTPMiddleware(apiserver.ManagementAuthMiddleware(principalAuth)))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	submitWorkflow := func(token string) string {
		body := map[string]any{
			"workflow": map[string]any{
				"name":  "mgmt-auth-wf",
				"nodes": []map[string]any{{"name": "start", "type": "test.echo"}},
			},
		}
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(body)
		req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/workflows", &buf)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("submit %s: %v", token, err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("submit %s status = %d, want 200", token, resp.StatusCode)
		}
		var out struct {
			ExecutionID string `json:"execution_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if out.ExecutionID == "" {
			t.Fatalf("submit %s returned empty execution_id", token)
		}
		return out.ExecutionID
	}

	managementExecStatus := func(token, execID string) int {
		req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/management/executions/"+execID, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("management exec %s/%s: %v", token, execID, err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	execB := submitWorkflow("tok-b")
	execA := submitWorkflow("tok-a")

	if code := managementExecStatus("tok-b", execB); code != http.StatusOK {
		t.Fatalf("namespaceB inspect own exec = %d, want 200 (outer middleware must accept tok-b)", code)
	}
	if code := managementExecStatus("tok-unknown", execB); code != http.StatusUnauthorized {
		t.Fatalf("unknown token = %d, want 401", code)
	}
	if code := managementExecStatus("tok-b", execA); code != http.StatusNotFound {
		t.Fatalf("namespaceB inspect namespaceA exec = %d, want 404 (IDOR, no existence leak)", code)
	}
}

func TestParseServerConfigEnableRunnerMetricsProxyFlag(t *testing.T) {
	// Positive: flag explicitly set → cfg field must be true.
	// This test fails if the flag is bound on the global flag.CommandLine instead
	// of the local FlagSet (the exact defect the brief warns about).
	cfg, err := parseServerConfig([]string{"-memory", "-enable-runner-metrics-proxy"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.enableRunnerMetricsProxy {
		t.Fatal("enableRunnerMetricsProxy = false, want true when flag is set")
	}

	// Negative: flag absent → cfg field must be false (not accidentally defaulted
	// to true by a mis-declaration).
	cfg2, err := parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.enableRunnerMetricsProxy {
		t.Fatal("enableRunnerMetricsProxy = true, want false when flag is absent")
	}
}

func TestParseServerConfigRunnerMetricsIntervalFlag(t *testing.T) {
	// Positive: flag set to 30s → cfg field must be 30s.
	cfg, err := parseServerConfig([]string{"-memory", "-runner-metrics-interval", "30s"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.runnerMetricsInterval != 30*time.Second {
		t.Fatalf("runnerMetricsInterval = %v, want 30s", cfg.runnerMetricsInterval)
	}

	// Negative: flag absent → cfg field must be 0 (no opinion).
	cfg2, err := parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.runnerMetricsInterval != 0 {
		t.Fatalf("runnerMetricsInterval = %v, want 0 when flag is absent", cfg2.runnerMetricsInterval)
	}
}
