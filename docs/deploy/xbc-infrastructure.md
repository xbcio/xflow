# xbc infrastructure

`--xbc-config` lets the server and runner source logging, metrics, tracing, and
the durable database from [xbc](https://github.com/xbcio/xbc) instead of the
binary's own constructors. It is opt-in: without the flag the binaries behave
exactly as before.

The integration stays in `cmd/`. xflow's SDK, engine, store, and backend
packages do not import xbc, so an embedder that uses `sdk/xflow` never inherits
the dependency; `test/architecture` enforces that boundary.

## Requirements

The seam uses xbc APIs first released in v0.2.0: `gorm.DefaultConfig`/`Open`/`Close`,
`metrics.Gatherer` and `metrics.PrometheusRegisterer`, and `log.CtxWith`. The
`require` entries in `go.mod` name v0.2.0 of `xbc`, `extensions/observability/metrics`,
and `extensions/storage/gorm`; an older release does not export them.

xbc ships no slog package: its `log` facade is the only logging contract. The
runner logs through `log/slog`, so `cmd/internal/xbcinfra` carries the host-side
handler that routes those records through the facade.

## Configuration file

The file is an xbc configuration tree, not an xflow configuration file. Only
these sections are owned; any other key fails startup by name.

```yaml
log:
  level: info
  console:
    enabled: true
    format: json
  # xbc also supports file rotation, sampling, and mask_fields here.

plugins:
  metrics:
    namespace: xflow
    runtime:
      go_collector: true
      process_collector: true
  tracing:
    service_name: xflow-server
    sample_ratio: 0.1
    exporter:
      protocol: grpc
      endpoint: otel-collector:4317
      insecure: false
  gorm:
    default:
      driver: mysql
      dsn: "xflow:change-me@tcp(mysql:3306)/xflow?parseTime=true"
      max_open_conn: 25
      max_idle_conn: 5
      conn_max_lifetime: 5m
      translate_error: true
```

Layering follows xbc: built-in defaults, then the file, then the profile
overlay named by `XFLOW_XBC_PROFILE`, then the environment. Environment
variables spell the full path under `XFLOW_XBC_`, for example
`XFLOW_XBC_LOG_LEVEL`, `XFLOW_XBC_PLUGINS_GORM_DEFAULT_DSN`, or
`XFLOW_XBC_PLUGINS_TRACING_EXPORTER_ENDPOINT`. The separate prefix is
deliberate: xbc rejects any variable under its own prefix that names no
declared section, so sharing xflow's `XFLOW_` namespace would turn an
unrelated variable such as `XFLOW_MASTER_KEY` into a startup error. YAML does
not expand `${...}`; use the environment layer for secrets.

Each section is independent. A file with only `log` leaves metrics, tracing,
and the database on their existing flags.

## Server

```sh
xflow-server --xbc-config /etc/xflow/xbc.yml \
  --metrics-addr :9090 \
  --auth-tokens-file /etc/xflow/tokens.json \
  --mode production
```

- **Logging** is initialized through xbc's facade, so xflow records inherit its
  level filtering, sensitive-field masking, sampling, and file rotation. The
  engine receives an adapter over the same backend, and the facade's sinks are
  flushed and closed on the way out, including after a failed startup. Records
  the server writes through the standard `log` package (bootstrap and apiserver
  lifecycle lines) still go to stderr.
- **Metrics** use xbc's private Prometheus registry. xflow's collector registers
  into it through the standard `prometheus.Registerer` adapter, and `--metrics-addr`
  still owns the scrape listener; `plugins.metrics.http` is rejected because
  this binary has no xbc Web adapter to serve it.
- **Tracing** uses xbc's private OpenTelemetry provider. No process-global
  provider is installed; xflow's `TraceIDFromContext` keeps reading the span
  from the request context.
- **Database** is opened through xbc's GORM plugin, pinged at startup, and fed
  to `sqlstore`. It replaces `--mysql-dsn` and satisfies the production gate's
  durable-audit requirement. Only `plugins.gorm.default` is read.

A flag that would contradict a configured section is rejected rather than
silently ignored: `--log-format` with `log`, any `--trace-*` with
`plugins.tracing`, and `--mysql-dsn` with `plugins.gorm.default`. Flags that
choose a listener (`--metrics-addr`, `--metrics-path`, TLS, authentication)
remain xflow-owned.

## Runner

```sh
xflow-runner --xbc-config /etc/xflow/xbc.yml --server https://xflow:8443
```

The runner consumes `--xbc-config` before delegating to `sdk/runner` and
installs xbc's slog bridge as the process default, so every runner record
inherits the same masking and rotation; the sinks are flushed before the
process exits. The runner currently reads only the `log` section; its registry
and tracer remain xflow-owned.
