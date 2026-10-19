package local

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type triggerPrimitives struct {
	mu       sync.Mutex
	dedup    map[string]time.Time
	locks    map[string]memoryTriggerLockRecord
	state    map[string]map[string][]byte
	lockSeed uint64
	// status is the store the engine runs on, and the source of the optional
	// types.ExecutionStatusReader capability: a trigger asking whether the
	// execution it last admitted is still running reads exactly the status the
	// engine wrote, from the same place the engine wrote it to.
	status engine.ExecutionStatusReader
}

type memoryTriggerLockRecord struct {
	token   string
	expires time.Time
}

// newTriggerPrimitives builds the local primitives over the state store they
// must answer execution-status questions from. status may be nil only in tests
// that do not exercise that capability; the method then says it cannot answer.
func newTriggerPrimitives(status engine.ExecutionStatusReader) *triggerPrimitives {
	return &triggerPrimitives{
		dedup:  make(map[string]time.Time),
		locks:  make(map[string]memoryTriggerLockRecord),
		state:  make(map[string]map[string][]byte),
		status: status,
	}
}

func (p *triggerPrimitives) Dedup(_ context.Context, key string, ttl time.Duration) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if expires, ok := p.dedup[key]; ok && now.Before(expires) {
		return false, nil
	}
	p.dedup[key] = now.Add(ttl)
	return true, nil
}

// ReleaseDedup removes a claim whose emission failed, so a redelivery is
// admitted instead of being answered as a duplicate for the rest of the TTL.
func (p *triggerPrimitives) ReleaseDedup(_ context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.dedup, key)
	return nil
}

func (p *triggerPrimitives) TryLock(_ context.Context, key string, ttl time.Duration) (types.TriggerLock, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ttl <= 0 {
		ttl = time.Minute
	}
	now := time.Now()
	if existing, ok := p.locks[key]; ok && now.Before(existing.expires) {
		return nil, false, nil
	}
	p.lockSeed++
	token := strconv.FormatUint(p.lockSeed, 10)
	p.locks[key] = memoryTriggerLockRecord{
		token:   token,
		expires: now.Add(ttl),
	}
	return &triggerLock{p: p, key: key, token: token}, true, nil
}

func (p *triggerPrimitives) State(_ context.Context, scope string) types.TriggerState {
	return &triggerState{p: p, scope: scope}
}

// GetExecutionStatus implements the optional types.ExecutionStatusReader
// capability by forwarding to the store. Without a store the honest answer is
// ErrExecutionStatusUnavailable — not a status, and not found=false, which means
// the execution is gone.
func (p *triggerPrimitives) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	if p.status == nil {
		return "", false, types.ErrExecutionStatusUnavailable
	}
	return p.status.GetExecutionStatus(ctx, id)
}

type triggerLock struct {
	p     *triggerPrimitives
	key   string
	token string
}

func (l *triggerLock) Renew(_ context.Context, ttl time.Duration) (bool, error) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()

	if ttl <= 0 {
		ttl = time.Minute
	}
	record, ok := l.p.locks[l.key]
	now := time.Now()
	if !ok || record.token != l.token || !now.Before(record.expires) {
		return false, nil
	}
	record.expires = now.Add(ttl)
	l.p.locks[l.key] = record
	return true, nil
}

func (l *triggerLock) Release(_ context.Context) error {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	record, ok := l.p.locks[l.key]
	if ok && record.token == l.token {
		delete(l.p.locks, l.key)
	}
	return nil
}

type triggerState struct {
	p     *triggerPrimitives
	scope string
}

func (s *triggerState) Get(_ context.Context, key string) ([]byte, error) {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	values := s.p.state[s.scope]
	if values == nil {
		return nil, nil
	}
	value := values[key]
	if value == nil {
		return nil, nil
	}
	cp := append([]byte(nil), value...)
	return cp, nil
}

func (s *triggerState) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	if s.p.state[s.scope] == nil {
		s.p.state[s.scope] = make(map[string][]byte)
	}
	s.p.state[s.scope][key] = append([]byte(nil), value...)
	return nil
}

func (s *triggerState) Delete(_ context.Context, key string) error {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	if s.p.state[s.scope] != nil {
		delete(s.p.state[s.scope], key)
	}
	return nil
}
