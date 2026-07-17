# Testing Strategy

## Commands

```bash
# All tests
go test ./... -race -count=1

# Engine core only (pure unit tests, no IO)
go test ./engine/... -race -count=1

# SDK + backend integration
go test ./backend/... ./sdk/... -race -count=1
```

## Engine Core Unit Tests (zero IO deps)

Uses fake StateStore + fake TaskQueue:
- `scheduler_test.go`: linear chain / fan-out / fan-in / port routing / skip cascade / multiple nodes ready simultaneously
- `errorpolicy_test.go`: four strategies
- `suspend_test.go`: signal early/late arrival, timer, timeout, multi-signal quorum
- `graph_test.go`: Compile validation, cycle detection

Fake StateStore uses mutex (~100 lines) to simulate concurrent contention.

## Backend / IO Binding Integration Tests

- `backend/local/` — real memoryState + memoryQueue, end-to-end
- `backend/distributed/` — Redis state + Asynq queue, full scenario coverage
- Shared `compat_test.go` test cases (same workflows run on both local/cluster)

## Testing Conventions

- Use `t.Run` for sub-cases; do not create separate `Test*_Case` functions
- Use table-driven tests for parametric cases
- Include inputs in failure messages: `t.Errorf("Wait(%q) status = %q, want %q", id, got, want)`
- Test helpers must call `t.Helper()`
- No `time.Sleep` — use buffered channels / sync.WaitGroup / context.WithTimeout
- Test-only handler types use `test.` prefix, registered in `init()` inside `_test.go` files
