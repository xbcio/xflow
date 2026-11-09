package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// fakeSupplyReader records the arguments of the last FetchSupply call.
type fakeSupplyReader struct {
	runnerID string
	ns       string
	name     string
	calls    int
	content  []byte
	hash     string
	revision uint64
	err      error
}

func (f *fakeSupplyReader) FetchSupply(_ context.Context, runnerID, ns, name string) ([]byte, string, uint64, error) {
	f.runnerID, f.ns, f.name = runnerID, ns, name
	f.calls++
	return f.content, f.hash, f.revision, f.err
}

// TestInProcessSupplyFetcherPassesIdentityAndNamespace: the namespace is read
// from ctx, never from a per-call parameter, and the runner identity is carried
// so Core can authenticate the read.
func TestInProcessSupplyFetcherPassesIdentityAndNamespace(t *testing.T) {
	reader := &fakeSupplyReader{content: []byte(`{"rules":[]}`), hash: "sha256:abc", revision: 3}
	f := &InProcessSupplyFetcher{Reader: reader, RunnerID: "runner-1"}
	ctx := namespace.WithNamespace(context.Background(), "ns-9")

	content, hash, revision, err := f.Fetch(ctx, "rules")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(content) != `{"rules":[]}` || hash != "sha256:abc" || revision != 3 {
		t.Fatalf("got (%q, %q, %d), want the reader's content/hash/revision", content, hash, revision)
	}
	if reader.runnerID != "runner-1" || reader.ns != "ns-9" || reader.name != "rules" {
		t.Fatalf("reader called with (%q, %q, %q), want (runner-1, ns-9, rules)", reader.runnerID, reader.ns, reader.name)
	}
}

func TestInProcessSupplyFetcherMapsNotFound(t *testing.T) {
	f := &InProcessSupplyFetcher{Reader: &fakeSupplyReader{err: store.ErrNotFound}, RunnerID: "runner-1"}
	if _, _, _, err := f.Fetch(context.Background(), "rules"); !errors.Is(err, ErrSupplyNotFound) {
		t.Fatalf("error = %v, want ErrSupplyNotFound", err)
	}
}

func TestInProcessSupplyFetcherRejectsOversizeContent(t *testing.T) {
	reader := &fakeSupplyReader{content: make([]byte, maxSupplyResponseBytes+1)}
	f := &InProcessSupplyFetcher{Reader: reader, RunnerID: "runner-1"}
	if _, _, _, err := f.Fetch(context.Background(), "rules"); err == nil {
		t.Fatal("an oversized supply was accepted; the HTTP fetcher refuses the same size")
	}
}

func TestInProcessSupplyFetcherRejectsEmptyNameAndMissingReader(t *testing.T) {
	if _, _, _, err := (&InProcessSupplyFetcher{Reader: &fakeSupplyReader{}}).Fetch(context.Background(), ""); err == nil {
		t.Fatal("empty supply name was accepted")
	}
	if _, _, _, err := (&InProcessSupplyFetcher{}).Fetch(context.Background(), "rules"); err == nil {
		t.Fatal("a fetcher with no reader was accepted")
	}
}
