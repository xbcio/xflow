package xflow

import (
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/memstore"
)

// blobSDKConfig is a structurally complete blob selection backed by in-memory
// stores, so nothing outside the process is contacted.
func blobSDKConfig() distributed.BlobConfig {
	return distributed.BlobConfig{
		Store: blob.NewMemory(),
		Index: blob.NewMemoryIndex(nil),
		Grace: 7654321 * time.Nanosecond,
		Batch: 10,
	}
}

// TestNewClusterBlobRequiresStore: the blob layer is part of the durable mode,
// so a cluster that selects it without a store is refused, and the error names
// the requirement without echoing any configured value.
func TestNewClusterBlobRequiresStore(t *testing.T) {
	mr := miniredis.RunT(t)
	bc := blobSDKConfig()
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr(), Blob: &bc})
	if err == nil {
		eng.Stop()
		t.Fatal("NewCluster accepted a blob selection with no store")
	}
	if !strings.Contains(err.Error(), "WithBlobStore") {
		t.Fatalf("error = %q, want it to name WithBlobStore", err)
	}
	if strings.Contains(err.Error(), "7654321") || strings.Contains(err.Error(), "7.654321ms") {
		t.Fatalf("error = %q leaks a configured value", err)
	}
}

// TestNewClusterRejectsInvalidBlob: with a store present the selection reaches
// the backend, which validates it inside NewCluster and names the field.
func TestNewClusterRejectsInvalidBlob(t *testing.T) {
	mr := miniredis.RunT(t)
	bc := blobSDKConfig()
	bc.Grace = 0
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr(), Store: memstore.New(), Blob: &bc})
	if err == nil {
		eng.Stop()
		t.Fatal("NewCluster accepted a blob configuration with no Grace")
	}
	if !strings.Contains(err.Error(), "Grace") {
		t.Fatalf("error = %q, want it to name the Grace field", err)
	}
}

// TestNewClusterWithoutBlobChangesNothing: a nil Blob field builds as before.
func TestNewClusterWithoutBlobChangesNothing(t *testing.T) {
	mr := miniredis.RunT(t)
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr(), Store: memstore.New()})
	if err != nil {
		t.Fatalf("NewCluster without Blob: %v", err)
	}
	eng.Stop()
}

// TestWithServerBlobReachesTheAPIConfig pins both SDK-side hops: the option into
// serverConfig, and serverConfig into apiserver.Config through
// buildServerAPIConfig. The option must also copy its argument, so a later edit
// of the caller's struct cannot change the server.
func TestWithServerBlobReachesTheAPIConfig(t *testing.T) {
	bc := blobSDKConfig()
	sc := &serverConfig{}
	WithServerBlob(bc)(sc)
	bc.Batch = 99

	if sc.blob == nil {
		t.Fatal("serverConfig.blob is nil after WithServerBlob")
	}
	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.Blob == nil {
		t.Fatal("apiserver Config.Blob is nil: the option stopped at serverConfig")
	}
	if cfg.Blob.Batch != 10 || cfg.Blob.Store == nil || cfg.Blob.Index == nil {
		t.Fatalf("apiserver Config.Blob = %+v, want the configuration passed to WithServerBlob", *cfg.Blob)
	}
}

// TestServerWithoutBlobLeavesAPIConfigNil: no option, no blob selection.
func TestServerWithoutBlobLeavesAPIConfigNil(t *testing.T) {
	if cfg := buildServerAPIConfig(ServerConfig{}, &serverConfig{}); cfg.Blob != nil {
		t.Fatalf("apiserver Config.Blob = %+v without WithServerBlob, want nil", *cfg.Blob)
	}
}

// TestNewServerBlobRequiresRedis: the server must not fall back to the
// in-memory backend and ignore the selection.
func TestNewServerBlobRequiresRedis(t *testing.T) {
	_, err := NewServer(ServerConfig{},
		WithServerInsecureNoRunnerAuth(),
		WithServerBlob(blobSDKConfig()))
	if err == nil || !strings.Contains(err.Error(), "Blob requires a Redis") {
		t.Fatalf("NewServer(Blob, no Redis) = %v, want the Blob-requires-Redis error", err)
	}
}
