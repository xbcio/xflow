# Task 2 Report: YAML, Environment, Flag Precedence, and Validation

## Status

Completed in `<repo-root>/.worktrees/codex-runner-cobra-cli`.

## Scope Delivered

- Added YAML-backed runner config loading in `cmd/runner/config.go`.
- Added `XFLOW_RUNNER_*` environment overrides for:
  - `SERVER`
  - `ID`
  - `CONCURRENCY`
  - `CAP`
  - `HEARTBEAT_INTERVAL`
  - `POLL_WAIT`
  - `LOG_LEVEL`
  - `LOG_FORMAT`
- Implemented config precedence as required:
  - CLI flags
  - environment
  - config file
  - defaults
- Preserved Task 1 contracts:
  - `newRootCommand(opts commandOptions) *cobra.Command`
  - legacy single-dash flag compatibility
  - no `verify` or `config` commands added
- Added validation for:
  - absolute `http`/`https` server URL
  - non-empty runner ID
  - positive concurrency
  - non-empty parsed capabilities
  - positive duration values for heartbeat and poll wait
  - allowed log level and format values
- Added `sampleRunnerConfigYAML()` helper.

## TDD Record

1. Added failing tests in `cmd/runner/config_test.go`.
2. Ran the focused Task 2 test command and confirmed red state:
   - missing `loadRunnerConfig`
   - missing `applyEnvOverrides`
   - missing `validateRunnerConfig`
3. Implemented the minimum production code to satisfy the brief.
4. Added an extra precedence test to verify CLI > env > file > defaults.
5. Re-ran focused and package tests successfully.

## Files Changed

- `go.mod`
- `go.sum`
- `cmd/runner/config.go`
- `cmd/runner/config_test.go`

## Notes

- `gopkg.in/yaml.v3 v3.0.1` was added as a direct dependency.
- `go mod tidy` added the expected checksum entries in `go.sum`.
- The config path continues to come from Task 1 behavior:
  - default from `XFLOW_RUNNER_CONFIG`
  - overridden by `--config` / `-c`

## Verification

Focused Task 2 tests:

```bash
GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test ./cmd/runner -run 'TestLoadRunnerConfigFromYAML|TestRunCommandUsesConfigFile|TestEnvOverridesFileConfig|TestValidateRunnerConfigRejectsInvalidValues'
```

Package test:

```bash
GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
```

Both passed on the final tree.

## Concerns

- The brief did not require validation behavior for invalid environment integer parsing, so `XFLOW_RUNNER_CONCURRENCY` parse failures are ignored until final validation runs against the resolved config.
- The sample YAML helper is implemented but not yet surfaced through a CLI command; Task 3 owns that surface area.

## Review Fix: Explicit Invalid YAML Values

### Finding Addressed

- `loadRunnerConfig` was treating explicit YAML values as "missing" when they decoded to Go zero values:
  - `runner.concurrency: 0`
  - `runner.capabilities: []`
- That caused invalid file input to fall back to defaults instead of reaching validation.

### Change Made

- Updated `cmd/runner/config.go` so `runnerConfigFile.Runner.Concurrency` is `*int`.
- Updated `cmd/runner/config.go` so `runnerConfigFile.Runner.Capabilities` is `*[]string`.
- `loadRunnerConfig` now distinguishes:
  - omitted field: keep default
  - explicit zero / empty field: preserve the explicit value and let `resolveRunnerConfig` validation fail

### Regression Tests Added

- `TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile`
- `TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile`

These exercise file-backed config through `resolveRunnerConfig` and the Cobra CLI path instead of only calling `validateRunnerConfig` directly.

### Test Output

Red run before the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile|TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile'
--- FAIL: TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile (0.00s)
    config_test.go:201: error = <nil>, want containing "concurrency"
--- FAIL: TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile (0.00s)
    config_test.go:217: run should not execute, got {configPath:/var/.../runner.yaml serverURL:http://localhost:8080 runnerID:runner-61326 concurrency:1 changed:map[config:true] capRaw:xflow.function capabilities:[{NodeType:xflow.function NodeVersion:0}] heartbeatInterval:5s pollWait:1s logLevel:info logFormat:text}
FAIL
FAIL	github.com/xbcio/xflow/cmd/runner	0.589s
FAIL
```

Green run after the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile|TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile|TestLoadRunnerConfigFromYAML|TestRunCommandUsesConfigFile|TestEnvOverridesFileConfig|TestResolveRunnerConfigPrecedence|TestValidateRunnerConfigRejectsInvalidValues'
ok  	github.com/xbcio/xflow/cmd/runner	0.514s
```

Required package test:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
ok  	github.com/xbcio/xflow/cmd/runner	0.274s
```

## Re-review Fix: Preserve Explicit Empty String YAML Values

### Finding Addressed

- Explicit empty-string YAML values for string-backed config fields were still being treated as omitted and replaced by defaults before validation.
- This affected the file-loading path for values such as:
  - `runner.id: ""`
  - `server.url: ""`
  - `poll.wait: ""`
  - `heartbeat.interval: ""`
  - `logging.level: ""`
  - `logging.format: ""`

### Root Cause

- `runnerConfigFile` used plain `string` fields for those YAML keys.
- `yaml.Unmarshal` decodes both omitted and explicit `""` values to Go's zero-value string.
- `loadRunnerConfig` then used `!= ""` checks, so explicit empty strings were indistinguishable from missing fields and silently fell back to defaults.

### Change Made

- Updated the YAML file struct in `cmd/runner/config.go` to use `*string` for string-backed YAML fields that need omission vs. explicit-empty distinction:
  - `runner.id`
  - `server.url`
  - `poll.wait`
  - `heartbeat.interval`
  - `logging.level`
  - `logging.format`
- Updated `loadRunnerConfig` to apply file values when the pointer is non-nil, even if the pointed value is `""`.
- Omitted fields still preserve default values because their pointers remain `nil`.

### Regression Tests Added

- `TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile`
  - `runner.id: ""`
  - `server.url: ""`
  - `poll.wait: ""`
  - `logging.level: ""`

These are file-backed tests that exercise `resolveRunnerConfig`, so the invalid values now survive loading and are rejected by validation instead of being defaulted away.

### Test Output

Red run before the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile|TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile|TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile'
--- FAIL: TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile (0.00s)
    --- FAIL: TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile/runner_id (0.00s)
        config_test.go:257: error = <nil>, want containing "runner id"
    --- FAIL: TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile/server_url (0.00s)
        config_test.go:257: error = <nil>, want containing "server URL"
    --- FAIL: TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile/poll_wait (0.00s)
        config_test.go:257: error = <nil>, want containing "poll wait"
    --- FAIL: TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile/log_level (0.00s)
        config_test.go:257: error = <nil>, want containing "log level"
FAIL
FAIL	github.com/xbcio/xflow/cmd/runner	0.570s
FAIL
```

Focused green run after the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsExplicitZeroConcurrencyFromFile|TestResolveRunnerConfigRejectsExplicitEmptyStringsFromFile|TestRunCommandRejectsExplicitEmptyCapabilitiesFromFile'
ok  	github.com/xbcio/xflow/cmd/runner	0.486s
```

Required package test:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
ok  	github.com/xbcio/xflow/cmd/runner	0.272s
```

## Re-review Fix: Invalid Env Concurrency Must Fail Resolution

### Finding Addressed

- Invalid `XFLOW_RUNNER_CONCURRENCY` values were silently ignored.
- That let lower-precedence file/default concurrency survive, which violated the intended precedence rules.

### Root Cause

- `applyEnvOverrides` called `strconv.Atoi` for `XFLOW_RUNNER_CONCURRENCY` and discarded parse errors.
- `resolveRunnerConfig` therefore kept the earlier file/default concurrency instead of failing.

### Change Made

- Updated `cmd/runner/config.go` so `applyEnvOverrides` returns `(runnerConfig, error)`.
- Invalid `XFLOW_RUNNER_CONCURRENCY` now returns an error that explicitly mentions concurrency.
- `resolveRunnerConfig` now masks `XFLOW_RUNNER_CONCURRENCY` only when the CLI explicitly changed `--concurrency`, preserving flag precedence over env.

### Regression Tests Added

- `TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig`
- `TestResolveRunnerConfigUsesCLIConcurrencyWhenEnvConcurrencyIsInvalid`

### Test Output

Red run before the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig|TestResolveRunnerConfigUsesCLIConcurrencyWhenEnvConcurrencyIsInvalid'
--- FAIL: TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig (0.00s)
    config_test.go:222: error = <nil>, want containing "concurrency"
FAIL
FAIL	github.com/xbcio/xflow/cmd/runner	0.567s
FAIL
```

Focused green run after the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig|TestResolveRunnerConfigUsesCLIConcurrencyWhenEnvConcurrencyIsInvalid|TestEnvOverridesFileConfig'
ok  	github.com/xbcio/xflow/cmd/runner	0.489s
```

Required package test:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
ok  	github.com/xbcio/xflow/cmd/runner	0.287s
```

## Final Review Fix: Explicit Empty Env Values Must Override Then Fail

### Findings Addressed

- Explicit empty `XFLOW_RUNNER_*` values were being treated as unset.
- `applyEnvOverrides` had drifted from the brief signature and env parsing errors were returned too early instead of remaining in config resolution state.

### Change Made

- Restored `applyEnvOverrides(cfg runnerConfig, getenv func(string) string) runnerConfig`.
- Added a separate lookup-based env helper in `cmd/runner/config.go` so `resolveRunnerConfig` can distinguish:
  - unset env
  - env explicitly set to `""`
- Added `resolutionIssues` tracking on `runnerConfig` so invalid env parsing, especially `XFLOW_RUNNER_CONCURRENCY`, remains attached to config state until final resolution.
- Explicit CLI flags now clear the matching resolution issue and override env as intended.

### Regression Tests Added

- `TestResolveRunnerConfigRejectsExplicitEmptyEnvValuesOverValidFileConfig`
  - `XFLOW_RUNNER_SERVER=`
  - `XFLOW_RUNNER_ID=`
  - `XFLOW_RUNNER_CAP=`
  - `XFLOW_RUNNER_POLL_WAIT=`
  - `XFLOW_RUNNER_LOG_LEVEL=`
- `TestResolveRunnerConfigUsesCLIFlagWhenEnvOverrideIsEmptyOrInvalid`
  - CLI `--server` beats `XFLOW_RUNNER_SERVER=`
  - CLI `--id` beats `XFLOW_RUNNER_ID=`
  - CLI `--concurrency` beats `XFLOW_RUNNER_CONCURRENCY=` and `XFLOW_RUNNER_CONCURRENCY=bad`
  - CLI `--cap` beats `XFLOW_RUNNER_CAP=`
  - CLI `--poll-wait` beats `XFLOW_RUNNER_POLL_WAIT=`
  - CLI `--log-level` beats `XFLOW_RUNNER_LOG_LEVEL=`

### Test Output

Red run before the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsExplicitEmptyEnvValuesOverValidFileConfig|TestResolveRunnerConfigUsesCLIFlagWhenEnvOverrideIsEmptyOrInvalid|TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig|TestEnvOverridesFileConfig'
# github.com/xbcio/xflow/cmd/runner [github.com/xbcio/xflow/cmd/runner.test]
cmd/runner/config_test.go:104:9: assignment mismatch: 1 variable but applyEnvOverrides returns 2 values
FAIL	github.com/xbcio/xflow/cmd/runner [build failed]
FAIL
```

Focused green run after the fix:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner -run 'TestResolveRunnerConfigRejectsExplicitEmptyEnvValuesOverValidFileConfig|TestResolveRunnerConfigUsesCLIFlagWhenEnvOverrideIsEmptyOrInvalid|TestResolveRunnerConfigRejectsInvalidEnvConcurrencyOverFileConfig|TestEnvOverridesFileConfig'
ok  	github.com/xbcio/xflow/cmd/runner	0.483s
```

Required package test:

```bash
$ GOCACHE=$PWD/.tmp/gocache GOMODCACHE=$PWD/.tmp/gomodcache go test -count=1 ./cmd/runner
ok  	github.com/xbcio/xflow/cmd/runner	0.288s
```

### Concerns

- `applyEnvOverrides` retains the brief signature for string-only callers, so explicit empty env handling lives in the lookup-based helper used by `resolveRunnerConfig`. That keeps the public Task 2 surface intact while fixing precedence semantics where they actually matter.
