package triggertest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/types"
)

// errLegacyPathUnsupported is returned by FakeEntrySeedRuntime's legacy
// TriggerRuntime methods. A trigger that is supposed to route through the
// entry-seed admission must not be able to fall back onto Emit/Dedup without a
// test noticing: the production runtime it stands in for
// (protocol.HTTPEntrySeedRuntime) fails closed on those same paths, so the fake
// fails closed too rather than silently accepting an event the control plane
// would never have seen.
var errLegacyPathUnsupported = errors.New("triggertest: fake entry-seed runtime supports only the entry-seed path")

// SeedCall is one SeedExecutionFromEntry invocation. Every attempt is recorded,
// retries included: a test asserting "Refused is not retried" and a test
// asserting "a transport error is retried at most N times" are both counting
// calls, so the record cannot be deduplicated per admission key.
type SeedCall struct {
	Request types.EntrySeedRequest
	// Attempt is this admission key's 1-based attempt number within the fake.
	Attempt int
}

// FakeEntrySeedRuntime is a types.EntrySeedRuntime fake that models the control
// plane's admission semantics on the runner side of the wire.
//
// Its point is the deduplication: it admits each AdmissionKey exactly once and
// answers every later attempt for that key with Duplicate, which is what lets
// two trigger instances share one fake and observe the "N replicas, one
// execution per tick" property without a control plane. It records every
// attempt, so the §5 response table can be driven row by row.
//
// It implements types.TriggerRuntime as well, so it can be assigned straight to
// TriggerActivateInput.Runtime, but its legacy methods fail closed — see
// errLegacyPathUnsupported. State returns an in-memory TriggerState, because a
// trigger that persists something across activations (a watermark) must be able
// to do it against this fake.
type FakeEntrySeedRuntime struct {
	mu       sync.Mutex
	calls    []SeedCall
	attempts map[string]int
	accepted map[string]types.ExecutionID
	state    map[string][]byte
	nextID   int

	respondMu sync.Mutex
	respond   func(req types.EntrySeedRequest, attempt int) (types.EntrySeedResponse, error)

	seedSignal chan struct{}
}

// NewFakeEntrySeedRuntime returns an empty entry-seed fake.
func NewFakeEntrySeedRuntime() *FakeEntrySeedRuntime {
	return &FakeEntrySeedRuntime{
		attempts:   map[string]int{},
		accepted:   map[string]types.ExecutionID{},
		state:      map[string][]byte{},
		seedSignal: make(chan struct{}, 64),
	}
}

var (
	_ types.EntrySeedRuntime = (*FakeEntrySeedRuntime)(nil)
	_ types.TriggerRuntime   = (*FakeEntrySeedRuntime)(nil)
)

// SeedExecutionFromEntry admits req: the first attempt for an AdmissionKey is
// Accepted with a fresh execution id, every later attempt for the same key is
// Duplicate with that same id.
//
// With no responder installed, a trigger's retry loop is therefore its own
// termination: a trigger that retries a refused admission forever will keep
// getting Accepted here (the fake never refuses on its own), so a test that
// wants a refusal installs one via SetResponder.
func (r *FakeEntrySeedRuntime) SeedExecutionFromEntry(_ context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	r.mu.Lock()
	attempt := r.attempts[req.AdmissionKey] + 1
	r.attempts[req.AdmissionKey] = attempt
	r.calls = append(r.calls, SeedCall{Request: req, Attempt: attempt})
	r.mu.Unlock()

	select {
	case r.seedSignal <- struct{}{}:
	default:
	}

	r.respondMu.Lock()
	respond := r.respond
	r.respondMu.Unlock()
	if respond != nil {
		resp, err := respond(req, attempt)
		// A responder answers only the attempts it has an opinion about; the
		// zero response with no error is that "no opinion" and falls through to
		// the fake's own dedup below.
		if err != nil || resp != (types.EntrySeedResponse{}) {
			return resp, err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.accepted[req.AdmissionKey]; ok {
		return types.EntrySeedResponse{Accepted: true, Duplicate: true, ExecutionID: id}, nil
	}
	r.nextID++
	id := types.ExecutionID(fmt.Sprintf("exec-%d", r.nextID))
	r.accepted[req.AdmissionKey] = id
	return types.EntrySeedResponse{Accepted: true, ExecutionID: id}, nil
}

// SetResponder installs a callback that decides each attempt. It runs before
// the fake's own dedup, so a responder can model a refusal (Refused=true) or a
// transport error, and can return a successful admission that the fake then
// records in its accepted set. attempt is the 1-based attempt number for the
// request's admission key.
//
// A responder that returns the zero EntrySeedResponse and a nil error defers to
// the fake's own admission — accepted on the key's first attempt, duplicate
// after — which is how a responder refuses only the attempts it means to.
func (r *FakeEntrySeedRuntime) SetResponder(fn func(req types.EntrySeedRequest, attempt int) (types.EntrySeedResponse, error)) {
	r.respondMu.Lock()
	defer r.respondMu.Unlock()
	r.respond = fn
}

// Seeds returns a copy of every recorded attempt, in call order.
func (r *FakeEntrySeedRuntime) Seeds() []SeedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]SeedCall(nil), r.calls...)
}

// SeedCount returns how many admission attempts have been made.
func (r *FakeEntrySeedRuntime) SeedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// AcceptedCount returns how many distinct admission keys were accepted.
func (r *FakeEntrySeedRuntime) AcceptedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.accepted)
}

// AcceptedKeys returns the admitted admission keys, in no particular order.
func (r *FakeEntrySeedRuntime) AcceptedKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.accepted))
	for key := range r.accepted {
		keys = append(keys, key)
	}
	return keys
}

// WaitForSeedCount blocks until at least want admission attempts have been
// recorded, and reports whether that happened before the timeout.
func (r *FakeEntrySeedRuntime) WaitForSeedCount(want int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if r.SeedCount() >= want {
			return true
		}
		select {
		case <-r.seedSignal:
		case <-deadline.C:
			return r.SeedCount() >= want
		}
	}
}

// Emit is not supported: this fake models the entry-seed runtime, whose
// production counterpart fails closed on the legacy path.
func (r *FakeEntrySeedRuntime) Emit(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
	return "", errLegacyPathUnsupported
}

// Dedup is not supported; admission deduplication is server-side on this path.
func (r *FakeEntrySeedRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return false, errLegacyPathUnsupported
}

// TryLock is not supported: the seed path needs no runner-side lock.
func (r *FakeEntrySeedRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return nil, false, errLegacyPathUnsupported
}

// State returns the fake's in-memory TriggerState for scope. Unlike the
// production entry-seed runtime — which has no state yet and returns nil — the
// fake keeps state, so a trigger persisting a watermark across activations can
// be exercised here.
func (r *FakeEntrySeedRuntime) State(_ context.Context, scope string) types.TriggerState {
	return &fakeTriggerState{rt: r, scope: scope}
}

// StateKeys returns the scopes that hold at least one key, for tests asserting
// what a trigger persisted.
func (r *FakeEntrySeedRuntime) StateKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	scopes := make([]string, 0, len(r.state))
	for key := range r.state {
		scopes = append(scopes, key)
	}
	return scopes
}

type fakeTriggerState struct {
	rt    *FakeEntrySeedRuntime
	scope string
}

func (s *fakeTriggerState) Get(_ context.Context, key string) ([]byte, error) {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	value, ok := s.rt.state[s.scope+"\x00"+key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), value...), nil
}

func (s *fakeTriggerState) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	s.rt.state[s.scope+"\x00"+key] = append([]byte(nil), value...)
	return nil
}

func (s *fakeTriggerState) Delete(_ context.Context, key string) error {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	delete(s.rt.state, s.scope+"\x00"+key)
	return nil
}
