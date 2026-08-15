package xflow

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/types"
)

// TestNewServerRedisConfigCarriesCredentials pins that an embedded host can
// reach a Redis that requires AUTH.
//
// ServerConfig.RedisAddr is a bare address with nowhere to put a password, so
// on every managed Redis — and on the SAS host this SDK path exists for — it
// fails at the connection ping. The two halves are asserted together because
// the failing half is what gives the passing half its meaning: without the
// RedisAddr control, a RedisConfig that was silently dropped would still look
// green whenever the Redis under test happens to accept unauthenticated
// clients.
func TestNewServerRedisConfigCarriesCredentials(t *testing.T) {
	const password = "s3cret-embedded-redis"
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)
	srv.RequireAuth(password)

	// Control: the credential-less field cannot reach this Redis. NewServer
	// builds (and pings) the backend eagerly, so the failure surfaces here.
	if _, err := NewServer(ServerConfig{RedisAddr: srv.Addr()}); err == nil {
		t.Fatal("NewServer with RedisAddr alone succeeded against a password-protected Redis; " +
			"this test can no longer tell a wired RedisConfig from a dropped one")
	} else if !strings.Contains(err.Error(), "redis ping") {
		t.Fatalf("RedisAddr error = %v, want a redis ping failure", err)
	}

	s, err := NewServer(ServerConfig{RedisConfig: &distributed.RedisConfig{
		Mode:     distributed.RedisModeSingle,
		Addrs:    []string{srv.Addr()},
		Password: password,
	}})
	if err != nil {
		t.Fatalf("NewServer with RedisConfig: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Shutdown(context.Background()) }()

	// A dropped RedisConfig leaves BOTH redis fields empty, and apiserver reads
	// that as "no Redis configured" and silently builds the in-memory backend —
	// which starts, serves and submits perfectly happily. Reaching this Redis is
	// the only observation that separates the two.
	httpSrv := httptest.NewServer(s.Handler())
	defer httpSrv.Close()
	submitSDKWorkflow(t, httpSrv.URL, &types.WorkflowDef{
		Name:  "redis-auth-passthrough",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.sdk-e2e"}},
	}, nil)

	if keys := srv.Keys(); len(keys) == 0 {
		t.Fatal("a submitted execution left no key in this Redis; RedisConfig was " +
			"dropped and the in-memory backend was used instead")
	}
}
