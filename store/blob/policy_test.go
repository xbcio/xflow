package blob_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/blob/blobtest"
	"github.com/xbcio/xflow/store/objectstore"
)

const (
	policyNS    namespace.Namespace = "policy"
	policyExec                      = "exec-1"
	policyGrace                     = time.Hour
	policyTTL                       = 24 * time.Hour
)

// policyEvents is an ordered log shared by the fake store and the fake index, so
// a test can assert the order of Put and Link across both.
type policyEvents struct {
	mu   sync.Mutex
	list []string
}

func (e *policyEvents) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, s)
}

func (e *policyEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.list...)
}

// policyRecorder is a PolicyObserver that remembers every result it is told.
type policyRecorder struct {
	mu   sync.Mutex
	puts []string
	gets []string
}

func (r *policyRecorder) OnPut(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.puts = append(r.puts, result)
}

func (r *policyRecorder) OnGet(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gets = append(r.gets, result)
}

func (r *policyRecorder) snapshot() (puts, gets []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.puts...), append([]string(nil), r.gets...)
}

// recordingStore wraps a Store and logs every call to the shared event list.
type recordingStore struct {
	blob.Store
	events *policyEvents
	puts   atomic.Int32
	gets   atomic.Int32
}

func (s *recordingStore) Put(ctx context.Context, ns namespace.Namespace, data []byte) (blob.Ref, error) {
	s.puts.Add(1)
	s.events.add("put")
	return s.Store.Put(ctx, ns, data)
}

func (s *recordingStore) Get(ctx context.Context, ns namespace.Namespace, ref blob.Ref) ([]byte, error) {
	s.gets.Add(1)
	s.events.add("get")
	return s.Store.Get(ctx, ns, ref)
}

// linkCall is one Index.Link call as scriptedIndex saw it.
type linkCall struct {
	execID   string
	ref      blob.Ref
	expireAt time.Time
}

// linkRunawayLimit stops a Policy that retries without bound: past this many
// Link calls the scripted index fails with errLinkRunaway, which ends the loop
// and fails the test's call-count assertion instead of hanging the package.
const linkRunawayLimit = 20

var errLinkRunaway = errors.New("test: Link called more often than any bounded retry would")

// scriptedIndex wraps an Index and lets a test decide what Link does.
type scriptedIndex struct {
	blob.Index
	events *policyEvents
	// beforeLink, when set, runs before each Link with the 1-based call number and
	// the call's Ref. A non-nil error is returned in place of the wrapped Link.
	beforeLink func(call int, ref blob.Ref) error

	mu    sync.Mutex
	calls []linkCall
}

func (x *scriptedIndex) Link(ctx context.Context, execID string, ref blob.Ref, expireAt time.Time) error {
	x.mu.Lock()
	x.calls = append(x.calls, linkCall{execID: execID, ref: ref, expireAt: expireAt})
	n := len(x.calls)
	x.mu.Unlock()
	x.events.add("link")
	if x.beforeLink != nil {
		// The bound applies to scripted tests only; an unscripted index serves
		// as many Links as the test makes.
		if n > linkRunawayLimit {
			return errLinkRunaway
		}
		if err := x.beforeLink(n, ref); err != nil {
			return err
		}
	}
	return x.Index.Link(ctx, execID, ref, expireAt)
}

func (x *scriptedIndex) linkCalls() []linkCall {
	x.mu.Lock()
	defer x.mu.Unlock()
	return append([]linkCall(nil), x.calls...)
}

// policyRig is a Policy over a recording ObjectBacked store on a temporary
// directory and a scripted MemoryIndex, with the clock and observer a test needs.
type policyRig struct {
	policy *blob.Policy
	store  *recordingStore
	backed *blob.ObjectBacked
	index  *scriptedIndex
	mem    *blob.MemoryIndex
	events *policyEvents
	clock  *blobtest.Clock
	obs    *policyRecorder
	dir    string
}

// faultStore returns putErr or getErr in place of the wrapped Store's answer
// when one is set.
type faultStore struct {
	blob.Store
	putErr error
	getErr error
}

func (s *faultStore) Put(ctx context.Context, ns namespace.Namespace, data []byte) (blob.Ref, error) {
	if s.putErr != nil {
		return blob.Ref{}, s.putErr
	}
	return s.Store.Put(ctx, ns, data)
}

func (s *faultStore) Get(ctx context.Context, ns namespace.Namespace, ref blob.Ref) ([]byte, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.Store.Get(ctx, ns, ref)
}

// newPolicyRig builds a rig whose Policy externalizes payloads over inlineMax
// bytes. Store and index are wired the way production wires them: the
// ObjectBacked store registers with the same index the Policy links in.
func newPolicyRig(t *testing.T, inlineMax int) *policyRig {
	t.Helper()
	return newPolicyRigWith(t, inlineMax, false, func(s blob.Store) blob.Store { return s })
}

// newPolicyRigWith is newPolicyRig with a compression switch and a wrap applied
// to the ObjectBacked store before the recorder, so a test can inject faults
// beneath the Policy.
func newPolicyRigWith(t *testing.T, inlineMax int, compress bool, wrap func(blob.Store) blob.Store) *policyRig {
	t.Helper()
	clock := blobtest.NewClock()
	events := &policyEvents{}
	mem := blob.NewMemoryIndex(clock.Now)
	index := &scriptedIndex{Index: mem, events: events}
	dir := t.TempDir()
	backed := blob.NewObjectBacked(objectstore.NewFSStore(dir), blob.ObjectBackedOptions{Index: mem, Compress: compress})
	store := &recordingStore{Store: wrap(backed), events: events}
	obs := &policyRecorder{}
	policy, err := blob.NewPolicy(store, index, blob.PolicyOptions{InlineMax: inlineMax, Observer: obs})
	if err != nil {
		t.Fatalf("NewPolicy error = %v", err)
	}
	return &policyRig{
		policy: policy, store: store, backed: backed, index: index, mem: mem,
		events: events, clock: clock, obs: obs, dir: dir,
	}
}

// expireAt returns a link expiry comfortably after the rig clock.
func (r *policyRig) expireAt() time.Time {
	return r.clock.Now().Add(policyTTL + policyGrace)
}

// externalize calls Externalize with the rig's standard execution and expiry
// under a deadline, so a runaway retry fails the test instead of hanging it.
func (r *policyRig) externalize(t *testing.T, data []byte) (blob.Ref, bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return r.policy.Externalize(ctx, policyNS, policyExec, data, r.expireAt())
}

// payload returns n bytes of a repeating pattern.
func payload(n int) []byte {
	return bytes.Repeat([]byte("x"), n)
}

func TestNewPolicyValidatesArguments(t *testing.T) {
	mem := blob.NewMemoryIndex(nil)
	store := blob.NewMemory()
	cases := []struct {
		name  string
		store blob.Store
		index blob.Index
		opts  blob.PolicyOptions
	}{
		{"NilStore", nil, mem, blob.PolicyOptions{}},
		{"NilIndex", store, nil, blob.PolicyOptions{}},
		{"NegativeInlineMax", store, mem, blob.PolicyOptions{InlineMax: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if p, err := blob.NewPolicy(tc.store, tc.index, tc.opts); err == nil || p != nil {
				t.Fatalf("NewPolicy = (%v, %v), want (nil, error)", p, err)
			}
		})
	}
}

func TestPolicyInlineMaxBoundary(t *testing.T) {
	const inlineMax = 10
	cases := []struct {
		size           int
		wantExternaled bool
	}{
		{0, false},
		{inlineMax - 1, false},
		{inlineMax, false},
		{inlineMax + 1, true},
		{inlineMax * 100, true},
	}
	for _, tc := range cases {
		rig := newPolicyRig(t, inlineMax)
		ref, externalized, err := rig.externalize(t, payload(tc.size))
		if err != nil {
			t.Fatalf("Externalize(%d bytes, InlineMax %d) error = %v", tc.size, inlineMax, err)
		}
		if externalized != tc.wantExternaled {
			t.Errorf("Externalize(%d bytes, InlineMax %d) externalized = %v, want %v", tc.size, inlineMax, externalized, tc.wantExternaled)
		}
		if !tc.wantExternaled {
			if ref != (blob.Ref{}) {
				t.Errorf("Externalize(%d bytes) inline ref = %+v, want zero", tc.size, ref)
			}
			if ev := rig.events.snapshot(); len(ev) != 0 {
				t.Errorf("Externalize(%d bytes) touched store or index: %v", tc.size, ev)
			}
			if puts, _ := rig.obs.snapshot(); len(puts) != 0 {
				t.Errorf("Externalize(%d bytes) told the observer %v, want nothing for an inline payload", tc.size, puts)
			}
		}
	}
}

func TestPolicyZeroInlineMaxExternalizesEveryNonEmptyPayload(t *testing.T) {
	rig := newPolicyRig(t, 0)
	if _, externalized, err := rig.externalize(t, nil); err != nil || externalized {
		t.Fatalf("Externalize(empty) = (%v, %v), want inline", externalized, err)
	}
	if _, externalized, err := rig.externalize(t, payload(1)); err != nil || !externalized {
		t.Fatalf("Externalize(1 byte) = (%v, %v), want externalized", externalized, err)
	}
}

func TestPolicyLinksBeforeReturningRef(t *testing.T) {
	rig := newPolicyRig(t, 8)
	data := payload(100)
	ref, externalized, err := rig.externalize(t, data)
	if err != nil || !externalized {
		t.Fatalf("Externalize = (%v, %v), want externalized", externalized, err)
	}
	if got, want := rig.events.snapshot(), []string{"put", "link"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
	calls := rig.index.linkCalls()
	if len(calls) != 1 {
		t.Fatalf("Link calls = %d, want 1", len(calls))
	}
	if calls[0].execID != policyExec || calls[0].ref != ref || !calls[0].expireAt.Equal(rig.expireAt()) {
		t.Fatalf("Link(%q, %+v, %v), want (%q, %+v, %v)", calls[0].execID, calls[0].ref, calls[0].expireAt, policyExec, ref, rig.expireAt())
	}
	// The link is real, not just a call: well past the grace period the unlinked
	// object would be garbage, and the linked one must not be.
	rig.clock.Advance(2 * policyGrace)
	candidates, err := rig.mem.Candidates(context.Background(), policyGrace, 10)
	if err != nil {
		t.Fatalf("Candidates error = %v", err)
	}
	for _, c := range candidates {
		if c.Digest == ref.Digest {
			t.Fatalf("object %s is a collection candidate although Externalize returned its Ref", ref.Digest)
		}
	}
}

func TestPolicyRoundTrip(t *testing.T) {
	for _, compress := range []bool{false, true} {
		rig := newPolicyRigWith(t, 8, compress, func(s blob.Store) blob.Store { return s })
		data := bytes.Repeat([]byte("round trip "), 500)
		want := append([]byte(nil), data...)
		ref, externalized, err := rig.externalize(t, data)
		if err != nil || !externalized {
			t.Fatalf("compress=%v: Externalize = (%v, %v), want externalized", compress, externalized, err)
		}
		// Neither the Ref nor the stored blob may alias the caller's slice.
		for i := range data {
			data[i] = 0
		}
		got, err := rig.policy.Resolve(context.Background(), policyNS, ref)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("compress=%v: Resolve = (%d bytes, %v), want the original %d bytes", compress, len(got), err, len(want))
		}
		got[0] ^= 0xff
		again, err := rig.policy.Resolve(context.Background(), policyNS, ref)
		if err != nil || !bytes.Equal(again, want) {
			t.Fatalf("compress=%v: Resolve after the caller changed a result = (%d bytes, %v), want the original", compress, len(again), err)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// failLinks makes the first n Link calls return err, then lets the real Index
// answer.
func failLinks(n int, err error) func(int, blob.Ref) error {
	return func(call int, _ blob.Ref) error {
		if call <= n {
			return err
		}
		return nil
	}
}

func TestPolicyRetriesAfterFenceOrNotFound(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"Fenced", blob.ErrFenced},
		{"NotFound", blob.ErrNotFound},
	} {
		for _, failures := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s_x%d", tc.name, failures), func(t *testing.T) {
				rig := newPolicyRig(t, 8)
				rig.index.beforeLink = failLinks(failures, tc.err)
				ref, externalized, err := rig.externalize(t, payload(100))
				if err != nil || !externalized {
					t.Fatalf("Externalize after %d %s = (%v, %v), want externalized", failures, tc.name, externalized, err)
				}
				if got, want := int(rig.store.puts.Load()), failures+1; got != want {
					t.Errorf("Put calls = %d, want %d (one per attempt)", got, want)
				}
				if got, want := len(rig.index.linkCalls()), failures+1; got != want {
					t.Errorf("Link calls = %d, want %d", got, want)
				}
				if _, err := rig.policy.Resolve(context.Background(), policyNS, ref); err != nil {
					t.Errorf("Resolve of the final ref error = %v", err)
				}
				if puts, _ := rig.obs.snapshot(); !equalStrings(puts, []string{blob.PutResultOK}) {
					t.Errorf("observer puts = %v, want one %q for the whole call", puts, blob.PutResultOK)
				}
			})
		}
	}
}

func TestPolicyFallsBackInlineAfterThreeFences(t *testing.T) {
	for _, linkErr := range []error{blob.ErrFenced, blob.ErrNotFound} {
		rig := newPolicyRig(t, 8)
		rig.index.beforeLink = failLinks(linkRunawayLimit, linkErr)
		ref, externalized, err := rig.externalize(t, payload(100))
		if err != nil || externalized || ref != (blob.Ref{}) {
			t.Fatalf("Externalize with Link always %v = (%+v, %v, %v), want (zero ref, false, nil) so the caller stays inline", linkErr, ref, externalized, err)
		}
		if got := int(rig.store.puts.Load()); got != 3 {
			t.Errorf("Put calls with Link always %v = %d, want exactly 3", linkErr, got)
		}
		if got := len(rig.index.linkCalls()); got != 3 {
			t.Errorf("Link calls with Link always %v = %d, want exactly 3", linkErr, got)
		}
		if puts, _ := rig.obs.snapshot(); !equalStrings(puts, []string{blob.PutResultError}) {
			t.Errorf("observer puts = %v, want one %q", puts, blob.PutResultError)
		}
	}
}

func TestPolicyDoesNotRetryOtherLinkErrors(t *testing.T) {
	rig := newPolicyRig(t, 8)
	rig.index.beforeLink = failLinks(linkRunawayLimit, errors.New("index unreachable"))
	ref, externalized, err := rig.externalize(t, payload(100))
	if err == nil || externalized || ref != (blob.Ref{}) {
		t.Fatalf("Externalize = (%+v, %v, %v), want an error and no ref", ref, externalized, err)
	}
	if !errors.Is(err, blob.ErrTransient) {
		t.Errorf("error = %v, want errors.Is ErrTransient", err)
	}
	if got := len(rig.index.linkCalls()); got != 1 {
		t.Errorf("Link calls = %d, want 1: only a fence or a missing object is retried", got)
	}
}

func TestPolicyDoesNotRetryFailedPut(t *testing.T) {
	boom := errors.New("disk on fire")
	fault := &faultStore{putErr: boom}
	rig := newPolicyRigWith(t, 8, false, func(s blob.Store) blob.Store { fault.Store = s; return fault })
	ref, externalized, err := rig.externalize(t, payload(100))
	if err == nil || externalized || ref != (blob.Ref{}) {
		t.Fatalf("Externalize = (%+v, %v, %v), want an error and no ref", ref, externalized, err)
	}
	if !errors.Is(err, blob.ErrTransient) || !errors.Is(err, boom) {
		t.Errorf("error = %v, want ErrTransient that still matches the cause", err)
	}
	if got := int(rig.store.puts.Load()); got != 1 {
		t.Errorf("Put calls = %d, want 1", got)
	}
	if got := len(rig.index.linkCalls()); got != 0 {
		t.Errorf("Link calls = %d, want 0 after a failed Put", got)
	}
}

func TestPolicyTooLargeIsPermanent(t *testing.T) {
	mem := blob.NewMemoryIndex(nil)
	small := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{Index: mem, MaxBytes: 16})
	obs := &policyRecorder{}
	policy, err := blob.NewPolicy(small, mem, blob.PolicyOptions{InlineMax: 8, Observer: obs})
	if err != nil {
		t.Fatalf("NewPolicy error = %v", err)
	}
	_, externalized, err := policy.Externalize(context.Background(), policyNS, policyExec, payload(17), time.Now().Add(time.Hour))
	if !errors.Is(err, blob.ErrTooLarge) || errors.Is(err, blob.ErrTransient) || externalized {
		t.Fatalf("Externalize(17 bytes, limit 16) = (%v, %v), want ErrTooLarge and not transient", externalized, err)
	}
	if puts, _ := obs.snapshot(); !equalStrings(puts, []string{blob.PutResultTooLarge}) {
		t.Errorf("observer puts = %v, want [%q]", puts, blob.PutResultTooLarge)
	}
}

func TestPolicyRejectsBadExecIDBeforeTouchingStore(t *testing.T) {
	rig := newPolicyRig(t, 8)
	for _, id := range []string{"", "has space", string(bytes.Repeat([]byte("e"), blob.MaxExecIDLen+1))} {
		_, externalized, err := rig.policy.Externalize(context.Background(), policyNS, id, payload(100), rig.expireAt())
		if !errors.Is(err, blob.ErrInvalidRef) || externalized {
			t.Errorf("Externalize(execID %q) = (%v, %v), want ErrInvalidRef", id, externalized, err)
		}
	}
	if ev := rig.events.snapshot(); len(ev) != 0 {
		t.Errorf("a bad execution id reached the store or index: %v", ev)
	}
}

func TestPolicyHonoursCancelledContext(t *testing.T) {
	rig := newPolicyRig(t, 8)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, externalized, err := rig.policy.Externalize(ctx, policyNS, policyExec, payload(100), rig.expireAt())
	if !errors.Is(err, context.Canceled) || !errors.Is(err, blob.ErrTransient) || externalized {
		t.Fatalf("Externalize with a cancelled context = (%v, %v), want a transient context.Canceled", externalized, err)
	}
	if ev := rig.events.snapshot(); len(ev) != 0 {
		t.Errorf("a cancelled context still reached the store or index: %v", ev)
	}
}

// TestPolicySurvivesRealCollectorFence drives the genuine race instead of a
// scripted error: a collector claims the object between Put and Link, so the
// real Index answers ErrFenced, and the retry must revive it and link.
func TestPolicySurvivesRealCollectorFence(t *testing.T) {
	rig := newPolicyRig(t, 8)
	claimed := false
	rig.index.beforeLink = func(call int, ref blob.Ref) error {
		if call != 1 {
			return nil
		}
		rig.clock.Advance(2 * policyGrace)
		if _, err := rig.mem.MarkDeleting(context.Background(), ref, policyGrace); err != nil {
			t.Errorf("MarkDeleting error = %v, want the collector to win the race", err)
			return nil
		}
		claimed = true
		return nil
	}
	data := payload(100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ref, externalized, err := rig.policy.Externalize(ctx, policyNS, policyExec, data, rig.clock.Now().Add(policyTTL+3*policyGrace))
	if err != nil || !externalized {
		t.Fatalf("Externalize = (%v, %v), want externalized after the revive", externalized, err)
	}
	if !claimed {
		t.Fatal("the collector never claimed the object, so the fence path was not exercised")
	}
	if got, err := rig.policy.Resolve(ctx, policyNS, ref); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Resolve after the revive = (%d bytes, %v), want the original bytes", len(got), err)
	}
}

func TestPolicyIsSafeForConcurrentUse(t *testing.T) {
	rig := newPolicyRig(t, 8)
	var failures atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for i := 0; i < 20; i++ {
				data := []byte(fmt.Sprintf("goroutine %d item %d %s", g, i, payload(50)))
				ref, externalized, err := rig.policy.Externalize(ctx, policyNS, policyExec, data, rig.expireAt())
				if err != nil || !externalized {
					failures.Add(1)
					t.Errorf("goroutine %d item %d: Externalize = (%v, %v)", g, i, externalized, err)
					continue
				}
				if got, err := rig.policy.Resolve(ctx, policyNS, ref); err != nil || !bytes.Equal(got, data) {
					failures.Add(1)
					t.Errorf("goroutine %d item %d: Resolve = (%d bytes, %v)", g, i, len(got), err)
				}
			}
		}()
	}
	wg.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d concurrent Externalize/Resolve round trips failed", n)
	}
	puts, gets := rig.obs.snapshot()
	if len(puts) != 160 || len(gets) != 160 {
		t.Fatalf("observer saw %d puts and %d gets, want 160 of each", len(puts), len(gets))
	}
}

func TestPolicyResultVocabulariesAreClosedSets(t *testing.T) {
	for name, list := range map[string][]string{"PutResults": blob.PutResults(), "GetResults": blob.GetResults()} {
		seen := map[string]bool{}
		for _, v := range list {
			if v == "" || seen[v] {
				t.Errorf("%s() has empty or duplicate value %q in %v", name, v, list)
			}
			seen[v] = true
		}
	}
	// The lists are copies: a caller that sorts or edits one must not change the
	// vocabulary the next caller sees.
	blob.PutResults()[0] = "mutated"
	blob.GetResults()[0] = "mutated"
	if blob.PutResults()[0] == "mutated" || blob.GetResults()[0] == "mutated" {
		t.Error("PutResults or GetResults returns its backing array")
	}
}

// TestPolicyObserverEmitsExactlyTheVocabulary provokes every outcome and checks
// two things: nothing outside PutResults/GetResults is ever emitted, and every
// value in them is emitted by some path, so the lists carry no dead entries.
func TestPolicyObserverEmitsExactlyTheVocabulary(t *testing.T) {
	obs := &policyRecorder{}
	ctx := context.Background()
	exp := time.Now().Add(time.Hour)

	mem := blob.NewMemoryIndex(nil)
	backed := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{Index: mem, MaxBytes: 64})
	fault := &faultStore{Store: backed}
	policy, err := blob.NewPolicy(fault, mem, blob.PolicyOptions{InlineMax: 4, Observer: obs})
	if err != nil {
		t.Fatalf("NewPolicy error = %v", err)
	}

	// put: ok, too_large, error (store failure), error (bad execution id).
	ref, ok, err := policy.Externalize(ctx, policyNS, policyExec, payload(10), exp)
	if err != nil || !ok {
		t.Fatalf("Externalize(10 bytes) = (%v, %v), want externalized", ok, err)
	}
	_, _, _ = policy.Externalize(ctx, policyNS, policyExec, payload(65), exp)
	fault.putErr = errors.New("store down")
	_, _, _ = policy.Externalize(ctx, policyNS, policyExec, payload(10), exp)
	fault.putErr = nil
	_, _, _ = policy.Externalize(ctx, policyNS, "", payload(10), exp)

	// get: ok, namespace, missing, corrupt (reported by the store), error.
	if _, err := policy.Resolve(ctx, policyNS, ref); err != nil {
		t.Fatalf("Resolve error = %v", err)
	}
	_, _ = policy.Resolve(ctx, "another-ns", ref)
	missing := ref
	missing.Digest = string(bytes.Repeat([]byte("a"), 64))
	_, _ = policy.Resolve(ctx, policyNS, missing)
	fault.getErr = fmt.Errorf("%w: planted", blob.ErrCorrupt)
	_, _ = policy.Resolve(ctx, policyNS, ref)
	fault.getErr = errors.New("store down")
	_, _ = policy.Resolve(ctx, policyNS, ref)

	puts, gets := obs.snapshot()
	checkExhaustive(t, "OnPut", puts, blob.PutResults())
	checkExhaustive(t, "OnGet", gets, blob.GetResults())
}

// checkExhaustive fails when emitted holds a value outside vocab or vocab holds a
// value that was never emitted.
func checkExhaustive(t *testing.T, what string, emitted, vocab []string) {
	t.Helper()
	inVocab := map[string]bool{}
	for _, v := range vocab {
		inVocab[v] = true
	}
	seen := map[string]bool{}
	for _, v := range emitted {
		if !inVocab[v] {
			t.Errorf("%s emitted %q, which is not in the vocabulary %v", what, v, vocab)
		}
		seen[v] = true
	}
	for _, v := range vocab {
		if !seen[v] {
			t.Errorf("%s never emitted %q although it is in the vocabulary; emitted %v", what, v, emitted)
		}
	}
}

func TestPolicyNilObserverIsSafe(t *testing.T) {
	mem := blob.NewMemoryIndex(nil)
	backed := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{Index: mem})
	policy, err := blob.NewPolicy(backed, mem, blob.PolicyOptions{InlineMax: 4})
	if err != nil {
		t.Fatalf("NewPolicy error = %v", err)
	}
	ctx := context.Background()
	ref, ok, err := policy.Externalize(ctx, policyNS, policyExec, payload(10), time.Now().Add(time.Hour))
	if err != nil || !ok {
		t.Fatalf("Externalize with a nil observer = (%v, %v)", ok, err)
	}
	if _, err := policy.Resolve(ctx, policyNS, ref); err != nil {
		t.Fatalf("Resolve with a nil observer error = %v", err)
	}
	if _, err := policy.Resolve(ctx, "another-ns", ref); err == nil {
		t.Fatal("Resolve in another namespace succeeded")
	}
}
