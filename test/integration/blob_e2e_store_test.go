//go:build integration

package integration

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
)

// errBlobE2ETransient is what the fault-injecting store returns for a read that
// is meant to fail once and succeed later. It is none of the blob sentinels, so
// the blob layer classifies it as transient.
var errBlobE2ETransient = errors.New("blob e2e: injected transient read failure")

// blobE2EStore wraps the blob store the backend writes to. It counts what
// reaches the store, so a scenario can tell that a payload really went through
// the blob layer, and it can fail the next reads on demand.
type blobE2EStore struct {
	blob.Store

	mu       sync.Mutex
	puts     int
	gets     int
	getFails int // reads that returned an error, injected or real
	refs     []blob.Ref
	failNext int    // how many of the next reads fail with errBlobE2ETransient
	vanish   string // when set, a read first removes every object file under this dir
	removed  int    // object files that removal actually deleted
	// beforeRead, when set, runs once ahead of the next read, on the reader's
	// goroutine, and is then cleared.
	beforeRead func()
}

// onFirstRead arranges for fn to run ahead of the next read of the store.
func (s *blobE2EStore) onFirstRead(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beforeRead = fn
}

// removedObjects returns how many object files reads have deleted so far.
func (s *blobE2EStore) removedObjects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removed
}

func (s *blobE2EStore) Put(ctx context.Context, ns namespace.Namespace, data []byte) (blob.Ref, error) {
	ref, err := s.Store.Put(ctx, ns, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if err == nil {
		s.refs = append(s.refs, ref)
	}
	return ref, err
}

func (s *blobE2EStore) Get(ctx context.Context, ns namespace.Namespace, ref blob.Ref) ([]byte, error) {
	s.mu.Lock()
	hook := s.beforeRead
	s.beforeRead = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	s.gets++
	if s.failNext > 0 {
		s.failNext--
		s.getFails++
		s.mu.Unlock()
		return nil, errBlobE2ETransient
	}
	vanish := s.vanish
	s.mu.Unlock()
	if vanish != "" {
		for _, path := range blobE2EObjects(vanish) {
			if os.Remove(path) == nil {
				s.mu.Lock()
				s.removed++
				s.mu.Unlock()
			}
		}
	}
	data, err := s.Store.Get(ctx, ns, ref)
	if err != nil {
		s.mu.Lock()
		s.getFails++
		s.mu.Unlock()
	}
	return data, err
}

// failReads makes the next n reads fail with errBlobE2ETransient.
func (s *blobE2EStore) failReads(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = n
}

// vanishOnRead makes every read from now on find the object files gone: the
// real store then answers with its own not-found, which is what a blob lost from
// the backend looks like. dir is the directory the object store is rooted at.
func (s *blobE2EStore) vanishOnRead(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vanish = dir
}

// counts returns the number of writes, reads and failed reads seen so far.
func (s *blobE2EStore) counts() (puts, gets, getFails int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts, s.gets, s.getFails
}

// written returns the refs of every blob the store accepted.
func (s *blobE2EStore) written() []blob.Ref {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]blob.Ref(nil), s.refs...)
}

// blobE2EObjects lists the blob object files under dir. A temp file of an
// in-flight write does not count as an object.
func blobE2EObjects(dir string) []string {
	var out []string
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(d.Name(), ".tmp-") {
			out = append(out, path)
		}
		return nil
	})
	return out
}
