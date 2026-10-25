// Package xbcinfra wires xbc's infrastructure capabilities into xflow's
// composition roots.
//
// It is deliberately internal to cmd. xflow's SDK, engine, store, and backend
// packages never import xbc; only the server and runner binaries do, and only
// when the deployment opts in with --xbc-config. That keeps an embedder's
// dependency graph unchanged while giving this repository's binaries one
// owner for logging, metrics, tracing, and the durable database connection.
//
// The adapter is intentionally thin:
//
//   - logging: xbc's log facade, including its sensitive-field masking, file
//     rotation, and sampling, exposed as engine.Logger, plus a host-side slog
//     handler for the runner;
//   - metrics: xbc's private Prometheus registry, exposed to xflow's collector
//     through NewWithRegisterer;
//   - tracing: xbc's private OpenTelemetry provider, exposed as xflow's Tracer;
//   - database: xbc's GORM plugin opened directly, feeding xflow's sqlstore.
//
// The configuration file is an xbc configuration tree with only the sections
// this package owns: log, plugins.metrics, plugins.tracing, and
// plugins.gorm.default. It is layered exactly as xbc defines it -- file, then
// profile overlay, then environment (XFLOW_XBC_ prefix) -- and a key outside
// those sections is rejected instead of ignored.
package xbcinfra
