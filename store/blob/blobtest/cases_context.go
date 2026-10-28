package blobtest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

// caseContextCancelled allows a Store to either honour or ignore a cancelled
// context, but not to panic, to lose a write it reported as successful, or to
// return an error that is not a context error.
func caseContextCancelled(t *testing.T, s blob.Store) {
	live, err := s.Put(context.Background(), nsA, []byte("exists before cancel"))
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	written := []byte("written under a cancelled context")
	ref, err := s.Put(ctx, nsA, written)
	switch {
	case err == nil:
		got, getErr := s.Get(context.Background(), nsA, ref)
		if getErr != nil || !bytes.Equal(got, written) {
			t.Errorf("Put under a cancelled context returned nil, but Get = (%q, %v), want (%q, nil)", got, getErr, written)
		}
	case !errors.Is(err, context.Canceled):
		t.Errorf("Put under a cancelled context error = %v, want nil or context.Canceled", err)
	}
	if _, err := s.Get(ctx, nsA, live); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Get under a cancelled context error = %v, want nil or context.Canceled", err)
	}
	if err := s.Delete(ctx, nsA, live); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Delete under a cancelled context error = %v, want nil or context.Canceled", err)
	}
}

func caseConcurrentPut(t *testing.T, s blob.Store) {
	const workers = 16
	ctx := context.Background()
	data := randomBytes(t, 64<<10)
	refs := make([]blob.Ref, workers)
	errs := make([]error, workers)

	// Every goroutine is parked on start until all are running, so the Puts
	// genuinely overlap instead of finishing one after another.
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	for i := range workers {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			refs[i], errs[i] = s.Put(ctx, nsA, data)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("concurrent Put #%d error = %v", i, errs[i])
		}
		if refs[i].Digest != refs[0].Digest {
			t.Fatalf("concurrent Put #%d digest = %q, want %q", i, refs[i].Digest, refs[0].Digest)
		}
	}
	got, err := s.Get(ctx, nsA, refs[0])
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get after concurrent Puts error = %v, equal = %t", err, bytes.Equal(got, data))
	}
}
