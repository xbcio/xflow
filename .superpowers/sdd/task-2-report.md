# Task 2 Report: Structured Engine Commit Outcomes

## Scope

- Modified `engine/types.go`
- Modified `engine/engine.go`
- Modified `engine/runner_commit_test.go`

No SDK files were changed.

## TDD Record

### RED

Command:

```bash
go test ./engine -run TestEngine_CommitTaskResultWithOutcomeClassifiesAcceptedDuplicateAndStale -count=1
```

Observed failure:

- `eng.CommitTaskResultWithOutcome` undefined
- `CommitOutcomeAccepted` undefined
- `CommitOutcomeDuplicateTerminal` undefined
- `CommitOutcomeStaleToken` undefined

This matched the expected missing outcome API surface from the brief.

### GREEN

Implemented:

- `CommitOutcome` type and constants in `engine/types.go`
- `CommitOutcome.ReleasesLeasedCapacity()`
- `Engine.CommitTaskResultWithOutcome(...) (CommitOutcome, error)` in `engine/engine.go`
- `Engine.CommitTaskResult(...) error` as a delegating wrapper preserving existing callers

Focused verification:

```bash
go test ./engine -run TestEngine_CommitTaskResultWithOutcomeClassifiesAcceptedDuplicateAndStale -count=1
```

Result:

- Pass (`3 passed`)

Package verification:

```bash
go test ./engine -count=1
```

Result:

- Pass (`49 passed`)

## Brief Adaptation

I made one small adaptation to the example test because task 1 changed surrounding lease semantics:

- The duplicate-terminal assertion now uses a two-node workflow so the execution remains active after the first accepted commit. With the current task-1 behavior, repeating a commit on a single-node workflow after completion classifies as `execution_inactive`.
- The stale-token assertion now uses a reclaimed-and-reissued lease. Once a node is already terminal, `ClaimTaskLease` intentionally treats repeat commits as idempotent terminal duplicates rather than stale-token failures, so a stale token must be exercised against a superseded running lease instead.

These adaptations preserve the intended behavior under the current engine rules:

- accepted commit => `CommitOutcomeAccepted`
- duplicate terminal commit => `CommitOutcomeDuplicateTerminal`
- stale superseded lease => `CommitOutcomeStaleToken` with `ErrInvalidLeaseToken`

## Commit

Planned commit:

```bash
git add engine/types.go engine/engine.go engine/runner_commit_test.go .superpowers/sdd/task-2-report.md
git commit -m "feat(engine): classify task commit outcomes"
```

## Re-review Fix: Remaining Outcome Coverage

### Scope

- Modified `engine/runner_commit_test.go`
- Modified `.superpowers/sdd/task-2-report.md`

No production files changed in this fix pass.

### TDD Record

#### Test-first additions

Added focused coverage for the remaining public outcome semantics:

- `CommitOutcomeExecutionInactive`
- `CommitOutcomeTransientError`
- `CommitOutcome.ReleasesLeasedCapacity()` truth table

#### Focused verification

Command:

```bash
go test ./engine -run 'TestEngine_CommitTaskResultWithOutcomeClassifiesExecutionInactive|TestEngine_CommitTaskResultWithOutcomeClassifiesTransientError|TestCommitOutcome_ReleasesLeasedCapacity' -count=1
```

Result:

- Pass (`9 passed`)

This was not a RED failure: the newly added tests passed immediately, which indicates the current production implementation already handled these semantics correctly and the gap was missing coverage rather than missing behavior.

#### Package verification

Command:

```bash
go test ./engine -count=1
```

Result:

- Pass (`58 passed`)

### Coverage Added

- `execution_inactive` is returned with `nil` error when a runner re-commits against an execution that has already completed.
- `transient_error` is returned when `ClaimTaskLease` fails with a backend/storage error.
- `ReleasesLeasedCapacity()` returns:
  - `true` for `accepted`, `duplicate_terminal`, `stale_token`, `execution_inactive`
  - `false` for `transient_error`
  - `false` for an unknown outcome value
