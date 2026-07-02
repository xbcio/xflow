# Task 4 Report: Assignment-First MemoryRunnerDirectory

## Status

DONE

## Scope Delivered

- Added `service/control/runner_directory.go` with the new assignment-first directory DTOs, interface, and `ErrRunnerSessionStale`.
- Added `service/control/memory_runner_directory.go` with a mutex-backed in-memory implementation:
  - runner registration with session fencing
  - FIFO assignment queue with seen de-duplication
  - claim/finalize/release bookkeeping
  - leased-capacity accounting based on `capacity - inFlight - finalized - activeClaims`
  - stable requeue ordering for active claims on runner re-registration
- Preserved `RunnerPool` compatibility:
  - existing API unchanged
  - matching logic now reuses route-aware capability checks
  - added a regression test for versioned routing with version-omitted capabilities

## TDD Notes

### RED

Added `service/control/memory_runner_directory_test.go` first, then ran:

```bash
go test ./service/control -run 'TestMemoryRunnerDirectory' -count=1
```

Observed expected build failures for undefined directory types and constructor.

### GREEN

Implemented the new directory/types and reran:

```bash
go test ./service/control -run 'TestMemoryRunnerDirectory|TestRunnerPool' -count=1
```

Passed.

## Verification

Ran after formatting:

```bash
gofmt -w service/control/runner_directory.go service/control/memory_runner_directory.go service/control/memory_runner_directory_test.go service/control/runner_pool.go service/control/runner_pool_test.go
go test ./service/control -run 'TestMemoryRunnerDirectory|TestRunnerPool' -count=1
go test ./service/control -count=1
```

Results:

- Focused suite: `8 passed`
- Full `service/control` package: `42 passed`

## Files Changed

- `service/control/runner_directory.go`
- `service/control/memory_runner_directory.go`
- `service/control/memory_runner_directory_test.go`
- `service/control/runner_pool.go`
- `service/control/runner_pool_test.go`

## Concerns

- None for this task boundary. `RunnerPool` remains the compatibility path; dispatcher/core migration to `RunnerDirectory` is intentionally left for later tasks.

## Review Fix Follow-Up

### Findings Addressed

- Preserved finalized lease accounting across `MemoryRunnerDirectory.Register` re-registration so runner headroom cannot reopen until `ReleaseLeased`.
- Added focused regression coverage for:
  - headroom math with `InFlight`, active claims, and finalized leases
  - `ReleaseClaimRequeue`, `ReleaseClaimDrop`, and `ReleaseClaimKeepSeen`
  - seen-marker retention/removal in `ReleaseLeased`
  - re-registration keeping finalized lease accounting while requeueing active claims

### TDD / Verification

- RED:
  - `go test ./service/control -run 'TestMemoryRunnerDirectory' -count=1`
  - failed on `TestMemoryRunnerDirectoryReregisterPreservesFinalizedLeasesAndRequeuesActiveClaims`, confirming the leased-accounting reset bug
- GREEN:
  - `go test ./service/control -run 'TestMemoryRunnerDirectory' -count=1`
  - `9 passed`
- Required verification:
  - `go test ./service/control -run 'TestMemoryRunnerDirectory|TestRunnerPool' -count=1`
  - `15 passed`
  - `go test ./service/control -count=1`
  - `49 passed`

## Re-review Fix Follow-Up

### Finding Addressed

- Preserved the last known `RunnerSnapshot.InFlight` across `MemoryRunnerDirectory.Register` re-registration so `headroom()` cannot over-assign capacity before the replacement session sends its first heartbeat.

### TDD / Verification

- RED:
  - `go test ./service/control -run 'TestMemoryRunnerDirectoryReregisterPreservesInflightUntilHeartbeat' -count=1`
  - failed on the new regression test, confirming re-registration reset `InFlight` and allowed an extra claim
- GREEN:
  - `go test ./service/control -run 'TestMemoryRunnerDirectoryReregisterPreservesInflightUntilHeartbeat' -count=1`
  - `1 passed`
- Required verification:
  - `go test ./service/control -run 'TestMemoryRunnerDirectory|TestRunnerPool' -count=1`
  - `16 passed`
  - `go test ./service/control -count=1`
  - `50 passed`

### Files Updated

- `service/control/memory_runner_directory.go`
- `service/control/memory_runner_directory_test.go`
