# Task 1 Report: Cobra Root and Run Command Compatibility

## Summary

Implemented Task 1 in the isolated worktree by converting `cmd/runner` from the previous `flag`-based entrypoint to a Cobra root command with a `run` subcommand, while preserving the existing runner process wiring and flag behavior.

Commit created:

- `d34edba` `feat(runner): add cobra run command`

## What Changed

1. Added Cobra-based command construction:
   - `newRootCommand(opts commandOptions) *cobra.Command`
   - `newRunCommand(opts commandOptions, cfg *runnerConfig) *cobra.Command`
   - `executeRoot(args ...string) error`
2. Moved runner process wiring out of `main.go` into `run.go`:
   - `runnerConfig`
   - `bindRunnerFlags`
   - `recordChangedFlags`
   - `parseCapabilities`
   - `runRunner`
   - `runWithSignals`
3. Added temporary config defaults/resolution in `config.go`:
   - `defaultRunnerConfig`
   - `resolveRunnerConfig`
4. Preserved existing flag compatibility:
   - `--server`
   - `--id`
   - `--concurrency`
   - `--cap`
5. Added `capRaw` so `--cap` is parsed at command resolution time and converted into `[]protocol.Capability`.
6. Simplified `main.go` so it just executes the Cobra root and exits on error.
7. Added Cobra dependencies to `go.mod` / `go.sum`.

## TDD Evidence

### RED

Replaced `cmd/runner/main_test.go` with the required compatibility tests from the brief, then ran:

```bash
GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestRunCommandParsesExistingFlags|TestRootDefaultsToRunCommand'
```

Observed failure before implementation:

```text
cmd/runner/main_test.go:10:9: undefined: newRootCommand
cmd/runner/main_test.go:10:24: undefined: commandOptions
cmd/runner/main_test.go:38:9: undefined: newRootCommand
cmd/runner/main_test.go:38:24: undefined: commandOptions
FAIL github.com/xbcio/xflow/cmd/runner [build failed]
```

This matched the brief’s expected failure mode.

### GREEN

After implementing the Cobra root/run split and config resolver, reran:

```bash
GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestRunCommandParsesExistingFlags|TestRootDefaultsToRunCommand'
```

Observed pass:

```text
ok  	github.com/xbcio/xflow/cmd/runner	0.559s
```

Fresh verification before completion:

```text
ok  	github.com/xbcio/xflow/cmd/runner	(cached)
```

## Tests Run

- `GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestRunCommandParsesExistingFlags|TestRootDefaultsToRunCommand'` (RED, expected failure)
- `GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestRunCommandParsesExistingFlags|TestRootDefaultsToRunCommand'` (GREEN, pass)
- `GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestRunCommandParsesExistingFlags|TestRootDefaultsToRunCommand'` (final verification, pass)

## Files Changed

- `go.mod`
- `go.sum`
- `cmd/runner/main.go`
- `cmd/runner/main_test.go`
- `cmd/runner/command.go`
- `cmd/runner/config.go`
- `cmd/runner/run.go`

## Self-Review

- Verified the task stayed within `cmd/runner` and module metadata, without modifying `engine/`, `execution/`, `backend/`, `sdk/`, or `service/`.
- Confirmed root command compatibility for both:
  - explicit `run` usage
  - root invocation without subcommand
- Confirmed `--cap` is preserved and converted into the expected capability slice.
- Normalized module requirements with `go mod tidy` so `github.com/spf13/cobra` and `github.com/spf13/pflag` are direct dependencies in `go.mod`.

## Concerns

- None for Task 1. `resolveRunnerConfig` is intentionally temporary and minimal per the brief; future tasks will likely extend config loading and validation behavior.

## Fix After Review: Single-Dash Flag Compatibility

Addressed the Cobra regression where legacy single-dash long-form flags such as `-server` and `-id` were being interpreted as shorthand clusters. The fix stays inside `cmd/runner` by normalizing the known legacy runner flags before Cobra parses arguments, so both `run -server ...` and root-default `-id runner-root` follow the same execution path as before.

### Changed Files

- `cmd/runner/command.go`
- `cmd/runner/main_test.go`

### Focused Test Output

```text
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestExecuteRootRunCommandParsesLegacySingleDashFlags|TestExecuteRootDefaultsToRunCommandWithLegacySingleDashFlag'
ok  	github.com/xbcio/xflow/cmd/runner	0.583s
```

## Fix After Re-Review: newRootCommand Legacy Args Coverage

Addressed the remaining gap from re-review: `newRootCommand(...).SetArgs(...)` now gets the same legacy single-dash normalization as `executeRoot*`, and the acceptance tests once again cover both direct `newRootCommand` execution and `executeRoot` compatibility paths.

### Changed Files

- `cmd/runner/command.go`
- `cmd/runner/main_test.go`

### TDD Evidence

RED with restored and extended `newRootCommand` coverage:

```text
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
--- FAIL: TestNewRootCommandRunCommandParsesLegacySingleDashFlags (0.00s)
    main_test.go:54: unknown shorthand flag: 's' in -server
--- FAIL: TestNewRootCommandDefaultsToRunCommandWithLegacySingleDashFlag (0.00s)
    main_test.go:76: unknown command "runner-root" for "xflow-runner"
FAIL
FAIL	github.com/xbcio/xflow/cmd/runner	0.556s
FAIL
```

GREEN after moving normalization into the `newRootCommand` execution path:

```text
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
ok  	github.com/xbcio/xflow/cmd/runner	0.558s
```

### Output Summary

- Exact test command: `GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner`
- Result: pass
- Coverage restored/extended for:
  - `newRootCommand` with explicit `run` and `--server`
  - `newRootCommand` with explicit `run` and legacy `-server`, `-id`, `-concurrency`, `-cap`
  - `newRootCommand` root-default execution with legacy `-id`
  - `executeRoot` explicit `run` and root-default legacy single-dash compatibility
