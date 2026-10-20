package control

import (
	"strings"
	"testing"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
)

func TestSelectRunnerDirectoryScopesKeysByOwnedNamespaces(t *testing.T) {
	scopedPrefix, err := RunnerDirectoryKeyPrefix("local-jack")
	if err != nil {
		t.Fatalf("RunnerDirectoryKeyPrefix: %v", err)
	}
	tests := []struct {
		name  string
		owned []namespace.Namespace
		want  string
	}{
		// Mutation: scoping the default deployment strands live directory state
		// during a rolling upgrade from v0.0.47.
		{"unscoped keeps the legacy prefix", nil, legacyRunnerDirectoryPrefix},
		{"default-only keeps the legacy prefix", nsList("default"), legacyRunnerDirectoryPrefix},
		{"default plus another keeps the legacy prefix", nsList("default", "x"), legacyRunnerDirectoryPrefix},
		// Mutation: not passing the option leaves a dedicated deployment sharing
		// the default deployment's runner directory.
		{"dedicated namespace gets its own prefix", nsList("local-jack"), scopedPrefix},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := redisBackendStub{Provider: backendlocal.New(), rdb: newMiniRedis(t)}
			directory := selectRunnerDirectory(Config{Backend: provider, OwnedNamespaces: tc.owned}, nil)
			redisDirectory, ok := directory.(*RedisRunnerDirectory)
			if !ok {
				t.Fatalf("selectRunnerDirectory() = %T, want *RedisRunnerDirectory", directory)
			}
			if redisDirectory.keys.prefix != tc.want {
				t.Fatalf("keys.prefix = %q, want %q", redisDirectory.keys.prefix, tc.want)
			}
		})
	}
}

func TestSelectRunnerDirectoryExplicitOverrideBypassesScope(t *testing.T) {
	override := NewMemoryRunnerDirectory()
	provider := redisBackendStub{Provider: backendlocal.New(), rdb: newMiniRedis(t)}
	// An ambiguous set would panic if the scope were consulted at all.
	got := selectRunnerDirectory(Config{
		Backend: provider, RunnerDirectory: override, OwnedNamespaces: nsList("a", "b"),
	}, nil)
	if got != RunnerDirectory(override) {
		t.Fatalf("selectRunnerDirectory() = %T, want the configured override", got)
	}
}

func TestSelectRunnerDirectoryMemoryFallbackIgnoresScope(t *testing.T) {
	directory := selectRunnerDirectory(Config{Backend: backendlocal.New(), OwnedNamespaces: nsList("local-jack")}, nil)
	if _, ok := directory.(*MemoryRunnerDirectory); !ok {
		t.Fatalf("selectRunnerDirectory() = %T, want *MemoryRunnerDirectory", directory)
	}
}

// Mutation: swallowing the suffix error would fall back to the shared legacy
// keys, silently defeating the isolation the owned set asked for.
func TestSelectRunnerDirectoryPanicsOnAmbiguousScope(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("selectRunnerDirectory with an ambiguous owned set did not panic")
		}
	}()
	provider := redisBackendStub{Provider: backendlocal.New(), rdb: newMiniRedis(t)}
	selectRunnerDirectory(Config{Backend: provider, OwnedNamespaces: nsList("a", "b")}, nil)
}

// The scope must be resolved (and written back into cfg) before the directory
// is selected: a backend-inherited set that reached only the workers would
// leave the directory on the shared legacy keys.
func TestNewControlPlaneAppliesInheritedScope(t *testing.T) {
	scopedPrefix, _ := RunnerDirectoryKeyPrefix("local-jack")
	provider := ownedRedisBackend{
		redisBackendStub: redisBackendStub{Provider: backendlocal.New(), rdb: newMiniRedis(t)},
		owned:            nsList("local-jack"),
	}
	cp, err := NewControlPlane(Config{Backend: provider, EntryActivationStore: NewMemoryEntryActivationStore()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	directory, ok := cp.RunnerDirectory().(*RedisRunnerDirectory)
	if !ok {
		t.Fatalf("RunnerDirectory() = %T, want *RedisRunnerDirectory", cp.RunnerDirectory())
	}
	if directory.keys.prefix != scopedPrefix {
		t.Fatalf("directory prefix = %q, want %q", directory.keys.prefix, scopedPrefix)
	}
	got := cp.EntryActivationReconciler().Namespaces()
	if len(got) != 1 || got[0] != "local-jack" {
		t.Fatalf("reconciler namespaces = %v, want [local-jack]", got)
	}
}

// The resolved owned set must reach the two workers that take it from
// NewControlPlane. Each is checked through its own state: a worker built
// without the scope would still construct and still run, just unscoped.
func TestNewControlPlaneScopesLeaderGatedWorkers(t *testing.T) {
	cp, err := NewControlPlane(Config{
		Backend:              ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("local-jack")},
		EntryActivationStore: NewMemoryEntryActivationStore(),
		RunnerPools:          NewMemoryRunnerPoolStore(),
		IssuedIdentities:     NewMemoryIssuedIdentityStore(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.instancePruner == nil {
		t.Fatal("instance pruner was not constructed")
	}
	if len(cp.instancePruner.owned) != 1 || cp.instancePruner.owned[0] != "local-jack" {
		t.Fatalf("pruner owned = %v, want [local-jack]", cp.instancePruner.owned)
	}
	worker := cp.WorkflowActivationProjectionWorker()
	if worker == nil {
		t.Fatal("projection worker was not constructed")
	}
	if _, ok := worker.scope["local-jack"]; !ok || len(worker.scope) != 1 {
		t.Fatalf("projection worker scope = %v, want {local-jack}", worker.scope)
	}
}

func TestNewControlPlaneLegacyWorkersStayUnscoped(t *testing.T) {
	cp, err := NewControlPlane(Config{
		Backend:              backendlocal.New(),
		EntryActivationStore: NewMemoryEntryActivationStore(),
		RunnerPools:          NewMemoryRunnerPoolStore(),
		IssuedIdentities:     NewMemoryIssuedIdentityStore(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.instancePruner == nil || cp.instancePruner.owned != nil {
		t.Fatalf("legacy pruner owned = %v, want nil", cp.instancePruner)
	}
	if worker := cp.WorkflowActivationProjectionWorker(); worker == nil || worker.scope != nil {
		t.Fatalf("legacy projection worker = %+v, want unscoped", worker)
	}
}

func TestNewControlPlaneLegacyEntryNamespacesUnchanged(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), EntryActivationStore: NewMemoryEntryActivationStore()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	got := cp.EntryActivationReconciler().Namespaces()
	if len(got) != 1 || got[0] != namespace.Default {
		t.Fatalf("reconciler namespaces = %v, want [default]", got)
	}
}

func TestNewControlPlaneRejectsInconsistentScope(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		part string
	}{
		{"entry outside owned", Config{
			Backend: backendlocal.New(), OwnedNamespaces: nsList("local-jack"), EntryNamespaces: nsList("default"),
		}, "outside the owned namespaces"},
		{"config differs from backend", Config{
			Backend:         ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("default")},
			OwnedNamespaces: nsList("local-jack"),
		}, "differs from the backend"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewControlPlane(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.part) {
				t.Fatalf("NewControlPlane error = %v, want it to contain %q", err, tc.part)
			}
		})
	}
}
