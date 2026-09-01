package architecture

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestKafkaClusterIDsAreValidAndConsistent(t *testing.T) {
	repoRoot := findRepositoryRoot(t)
	paths := []string{
		".github/workflows/ci.yml",
		".github/workflows/perf-sample.yml",
		"test/env/docker-compose.yml",
	}
	clusterIDPattern := regexp.MustCompile(`(?m)^\s+CLUSTER_ID:\s+([A-Za-z0-9_-]+)\s*$`)

	var expected string
	for _, relativePath := range paths {
		data, err := os.ReadFile(filepath.Join(repoRoot, relativePath))
		if err != nil {
			t.Fatalf("read %s: %v", relativePath, err)
		}
		matches := clusterIDPattern.FindAllSubmatch(data, -1)
		if len(matches) != 1 {
			t.Fatalf("%s contains %d CLUSTER_ID values, want exactly one", relativePath, len(matches))
		}
		clusterID := string(matches[0][1])
		raw, err := base64.RawURLEncoding.DecodeString(clusterID)
		if err != nil {
			t.Fatalf("%s CLUSTER_ID %q is not unpadded URL-safe base64: %v", relativePath, clusterID, err)
		}
		if len(raw) != 16 {
			t.Fatalf("%s CLUSTER_ID %q decodes to %d bytes, want Kafka's 16-byte UUID", relativePath, clusterID, len(raw))
		}
		if expected == "" {
			expected = clusterID
		} else if clusterID != expected {
			t.Fatalf("%s CLUSTER_ID = %q, want shared value %q", relativePath, clusterID, expected)
		}
	}
}

func TestLocalMySQLImageMatchesCI80(t *testing.T) {
	repoRoot := findRepositoryRoot(t)
	checks := []struct {
		path string
		want string
	}{
		{path: ".github/workflows/ci.yml", want: "image: mysql:8.0"},
		{path: "test/env/docker-compose.yml", want: "image: docker.io/library/mysql:8.0"},
	}
	for _, check := range checks {
		data, err := os.ReadFile(filepath.Join(repoRoot, check.path))
		if err != nil {
			t.Fatalf("read %s: %v", check.path, err)
		}
		if !strings.Contains(string(data), check.want) {
			t.Fatalf("%s does not contain %q", check.path, check.want)
		}
	}
}
