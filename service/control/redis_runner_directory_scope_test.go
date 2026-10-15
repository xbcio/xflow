package control

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

const legacyRunnerDirectoryPrefix = "xflow:runner-directory:{control}"

// hashTag returns the Redis Cluster hash tag of a key: the text between the
// first '{' and the next '}', which is all the cluster hashes.
func hashTag(key string) string {
	open := strings.Index(key, "{")
	if open < 0 {
		return ""
	}
	end := strings.Index(key[open+1:], "}")
	if end < 0 {
		return ""
	}
	return key[open+1 : open+1+end]
}

func TestRunnerDirectoryKeyPrefix(t *testing.T) {
	tests := []struct {
		name    string
		suffix  string
		want    string
		wantErr bool
	}{
		// Mutation: returning anything but the original literal for "" moves every
		// default-owning deployment onto new keys and strands live directory state
		// during a rolling upgrade.
		{"empty is the legacy prefix", "", legacyRunnerDirectoryPrefix, false},
		{"dedicated namespace", "local-jack", "xflow:runner-directory:ns:local-jack:{control}", false},
		{"suffix with plus", "team+a", "xflow:runner-directory:ns:team+a:{control}", false},
		// Mutation: skipping validation lets a ':' or a brace rewrite the key
		// schema or the hash tag.
		{"colon rejected", "a:b", "", true},
		{"open brace rejected", "a{b", "", true},
		{"close brace rejected", "a}b", "", true},
		{"glob rejected", "a*", "", true},
		{"too long rejected", strings.Repeat("x", 65), "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RunnerDirectoryKeyPrefix(tc.suffix)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RunnerDirectoryKeyPrefix(%q) error = %v, wantErr %v", tc.suffix, err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("RunnerDirectoryKeyPrefix(%q) = %q, want %q", tc.suffix, got, tc.want)
			}
		})
	}
}

// Every key of one directory must hash to one cluster slot, and the scoped and
// legacy keyspaces must not be reachable from each other by a prefix scan.
func TestRunnerDirectoryKeyPrefixShape(t *testing.T) {
	scoped, err := RunnerDirectoryKeyPrefix("local-jack")
	if err != nil {
		t.Fatalf("RunnerDirectoryKeyPrefix: %v", err)
	}
	for _, p := range []string{legacyRunnerDirectoryPrefix, scoped} {
		if got := hashTag(newRedisRunnerDirectoryKeys(p).queue); got != "control" {
			t.Fatalf("hash tag of %q keys = %q, want %q", p, got, "control")
		}
	}
	// Mutation: a scoped prefix that starts with the legacy one would put the
	// dedicated deployment's keys inside the legacy deployment's scans.
	if strings.HasPrefix(scoped, legacyRunnerDirectoryPrefix) || strings.HasPrefix(legacyRunnerDirectoryPrefix, scoped) {
		t.Fatalf("scoped prefix %q and legacy prefix %q overlap", scoped, legacyRunnerDirectoryPrefix)
	}
}

func TestWithRedisRunnerDirectoryScopeRejectsInvalidSuffix(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithRedisRunnerDirectoryScope(\"a:b\") did not panic; a bad scope must not fall back to the legacy keys")
		}
	}()
	WithRedisRunnerDirectoryScope("a:b")
}

// Mutation: constructing the keys from the legacy constant instead of the
// configured prefix leaves the option inert — this test is the one that turns
// red, because every behavior below depends on the keys actually moving.
func TestRedisRunnerDirectoryScopeMovesEveryKey(t *testing.T) {
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	scoped := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryScope("local-jack"))
	prefix, err := RunnerDirectoryKeyPrefix("local-jack")
	if err != nil {
		t.Fatalf("RunnerDirectoryKeyPrefix: %v", err)
	}
	if scoped.keys.prefix != prefix {
		t.Fatalf("keys.prefix = %q, want %q", scoped.keys.prefix, prefix)
	}
	v := reflect.ValueOf(scoped.keys)
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		if field.Type.Kind() != reflect.String {
			continue
		}
		if value := v.Field(i).String(); !strings.HasPrefix(value, prefix) {
			t.Errorf("keys.%s = %q does not start with the scoped prefix %q", field.Name, value, prefix)
		}
	}

	unscoped := NewRedisRunnerDirectory(rdb)
	if unscoped.keys.prefix != legacyRunnerDirectoryPrefix {
		t.Fatalf("unscoped keys.prefix = %q, want the legacy %q", unscoped.keys.prefix, legacyRunnerDirectoryPrefix)
	}
	explicitLegacy := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryScope(""))
	if explicitLegacy.keys.prefix != legacyRunnerDirectoryPrefix {
		t.Fatalf(`scope "" keys.prefix = %q, want the legacy %q`, explicitLegacy.keys.prefix, legacyRunnerDirectoryPrefix)
	}
}

// Two deployments on one Redis: a runner registered in one directory is
// invisible to the other, the same runner id can live in both, and an
// assignment queued in one is never claimable through the other.
func TestRedisRunnerDirectoryScopeIsolatesDeployments(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	legacy := NewRedisRunnerDirectory(rdb)
	local := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryScope("local-jack"))

	legacySession := registerRedisDirectoryRunner(t, ctx, legacy, "runner-shared", 1)
	if _, found := local.Runner(ctx, "runner-shared"); found {
		t.Fatal("scoped directory sees a runner registered in the legacy directory")
	}
	// The live-instance guard (ErrRunnerIDConflict) is per keyspace, so the same
	// id registers in the other deployment without conflict.
	localSession := registerRedisDirectoryRunner(t, ctx, local, "runner-shared", 1)
	if _, found := legacy.Runner(ctx, "runner-shared"); !found {
		t.Fatal("legacy directory lost its own runner")
	}

	mustEnqueueRedisDirectoryAssignment(t, ctx, local, redisDirectoryTestAssignment("local-only"))
	if _, ok, err := legacy.ClaimForRunner(ctx, redisDirectoryClaimRequest(legacySession, 1)); err != nil || ok {
		t.Fatalf("legacy runner claimed from the scoped queue: ok=%v err=%v", ok, err)
	}
	claim, ok, err := local.ClaimForRunner(ctx, redisDirectoryClaimRequest(localSession, 1))
	if err != nil || !ok || claim.Assignment.AssignmentID != "local-only" {
		t.Fatalf("scoped runner claim = (%+v, ok=%v, err=%v), want local-only", claim, ok, err)
	}

	// Every key the scoped directory wrote lives under its prefix; none leaked
	// into the legacy keyspace beyond what the legacy runner itself wrote.
	scopedPrefix, _ := RunnerDirectoryKeyPrefix("local-jack")
	var scopedKeys int
	for _, key := range server.Keys() {
		if strings.HasPrefix(key, scopedPrefix) {
			scopedKeys++
			continue
		}
		if !strings.HasPrefix(key, legacyRunnerDirectoryPrefix) {
			t.Errorf("key %q is under neither prefix", key)
		}
		if strings.Contains(key, "local-only") {
			t.Errorf("scoped assignment leaked into legacy key %q", key)
		}
	}
	if scopedKeys == 0 {
		t.Fatal("scoped directory wrote no keys under its prefix")
	}
}
