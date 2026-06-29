# Final Fix Report

## Changes

- Fixed Cobra help handling in `cmd/runner` by switching back to normal Cobra parsing, normalizing legacy single-dash runner flags before `Execute()`, and removing the custom parse wrapper that was calling `runFunc` during `--help`.
- Made `--config` a root persistent flag so `xflow-runner --config <file> config validate` and other subcommands resolve the same config file while keeping root/default and `run` flag compatibility.
- Added `cobra.NoArgs` validation to the root/default runner path, `run`, `verify`, and `config validate` so unexpected positional arguments are rejected before side effects.
- Parsed `heartbeat-interval` and `poll-wait` into `time.Duration` values and passed them into `service/runner.Config` via a small `runnerServiceConfig` helper plus an injectable `newRunnerService` factory seam for testing.
- Removed dead MVP logging surface from the runner CLI/config flow: dropped `--log-level` and `--log-format`, removed their config/env handling and validation, and removed the logging block from the sample config output.
- Updated `cmd/runner` tests to cover help behavior, global `--config`, positional arg rejection, and duration propagation while preserving existing single-dash compatibility coverage.

## Tests

- `GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner`
  - Result: `ok  	github.com/xbcio/xflow/cmd/runner	0.287s`
- Post-commit rerun: `GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner`
  - Result: `ok  	github.com/xbcio/xflow/cmd/runner	0.355s`
