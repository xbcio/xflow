package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// reloadFunc performs one hot-reload action and reports whether it changed
// anything worth logging about. It returns an error to keep the previous
// state untouched — every reloadFunc registered by this binary (policy file,
// TLS material) already guarantees that itself; reloader only decides when to
// call them and how to report the outcome, never how to recover from a
// partial failure.
type reloadFunc func() error

// reloader listens for reload signals on sig and, on each one, runs every
// registered reloadFunc in order, logging each outcome. It is a plain value
// rather than a goroutine wired directly to signal.Notify so a test can drive
// it by sending on an ordinary channel instead of delivering a real OS signal
// to the test process — sending SIGHUP to `go test` would affect the whole
// binary, not just the code under test.
//
// run blocks until ctx is cancelled or sig is closed, so cmd/server starts it
// in its own goroutine before calling srv.Run and relies on ctx cancellation
// (the same context Run's caller cancels on SIGINT/SIGTERM) to stop it.
type reloader struct {
	funcs []namedReload
}

// namedReload pairs a reloadFunc with the name used in its log lines, so a
// multi-entry reloader's log output says which reload succeeded or failed
// rather than only "reload succeeded" with no indication of which one.
type namedReload struct {
	name string
	fn   reloadFunc
}

// newReloader builds a reloader with no funcs registered; use add to register
// one or more before calling run.
func newReloader() *reloader { return &reloader{} }

// add registers a reload action under name. Registration order is the order
// funcs run on each signal.
func (r *reloader) add(name string, fn reloadFunc) {
	r.funcs = append(r.funcs, namedReload{name: name, fn: fn})
}

// run blocks, invoking every registered reloadFunc each time sig fires, until
// ctx is cancelled or sig is closed. It is meant to run in its own goroutine;
// the caller stops it by cancelling ctx (or closing sig, for a test that owns
// the channel directly).
func (r *reloader) run(ctx context.Context, sig <-chan os.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sig:
			if !ok {
				return
			}
			r.reloadAll(ctx)
		}
	}
}

// reloadAll runs every registered func once, in registration order, logging
// each outcome. A failure in one func does not skip the rest — the policy
// reload and the TLS reload are independent axes and an operator who signalled
// to rotate one should not lose the other because it happened to be listed
// second.
func (r *reloader) reloadAll(ctx context.Context) {
	for _, nr := range r.funcs {
		if err := nr.fn(); err != nil {
			log.Printf("xflow-server: %s reload failed, keeping previous configuration: %v", nr.name, err)
			continue
		}
		log.Printf("xflow-server: %s reload succeeded", nr.name)
	}
	_ = ctx // reserved for a future per-reload timeout budget; funcs today are synchronous file reads.
}

// policyReloadMetrics is the narrow slice of the metrics facade a policy
// reload failure increments. It is satisfied by *metrics.Metrics (nil-safe:
// Metrics.Inc no-ops on a nil receiver), so passing a nil *metrics.Metrics
// when --metrics-addr is unset costs nothing and adds no branching here.
type policyReloadMetrics interface {
	Inc(name string, labels map[string]string)
}

const metricPolicyReloadFailures = "xflow_auth_policy_reload_failures_total"

// newPolicyReloadFunc builds the reloadFunc that re-reads path into store. On
// success it logs the resulting entry count plus, per entry, its name/
// id_prefix and TokenFingerprint of the bound token — never the token itself
// or file contents. On failure it increments metricPolicyReloadFailures (when
// m is non-nil) and returns the error so reloader logs it at error level and
// leaves the previous policy in force; FilePolicyStore.Reload never mutates
// its snapshot unless every step (file permission check, YAML parse,
// resolveConfig, including the token_file 0600 check) succeeds.
func newPolicyReloadFunc(store *control.FilePolicyStore, path string, m policyReloadMetrics) reloadFunc {
	return func() error {
		if err := store.Reload(path); err != nil {
			if m != nil {
				m.Inc(metricPolicyReloadFailures, nil)
			}
			return err
		}
		for _, summary := range store.DescribeEntries() {
			log.Printf("xflow-server: auth policy entry name=%q id_prefix=%q token=%s",
				summary.Name, summary.IDPrefix, summary.TokenFingerprint)
		}
		log.Printf("xflow-server: auth policy reloaded from %q (%d entries)", path, len(store.DescribeEntries()))
		return nil
	}
}

// notifyReloadSignal installs the OS-level SIGHUP → channel wiring used by
// production. Tests exercise reloader.run directly against a channel they
// control instead of calling this, so SIGHUP is never delivered to a test
// binary.
func notifyReloadSignal() (chan os.Signal, func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	return ch, func() { signal.Stop(ch) }
}

// tlsReloaderFor is the narrow signature newTLSReloadFunc needs to reach the
// live TLS holder: *xflowsdk.Server.TLSReloader (and *apiserver.APIServer.
// TLSReloader, used directly by the analogous test) both satisfy it. Taking a
// func rather than the reloader value itself is what lets registration happen
// before srv.Run has started the transports and populated the holder —
// srv.Run runs in the same goroutine that later blocks in main, so the getter
// is what defers "read the holder" until a signal actually arrives, by which
// time Run has long since populated it.
type tlsReloaderFor func() *apiserver.TLSReloader

// newTLSReloadFunc builds the reloadFunc that re-reads the server's
// certificate/key/client-CA files. getReloader is called fresh on every
// invocation (see tlsReloaderFor's doc) and a nil result — TLS not configured,
// or Run has not started the transports yet — is treated as "nothing to
// reload", not an error: a plaintext deployment sending SIGHUP for its policy
// file must not see a spurious TLS reload failure logged against it.
func newTLSReloadFunc(getReloader tlsReloaderFor) reloadFunc {
	return func() error {
		r := getReloader()
		if r == nil {
			return nil
		}
		return r.Reload()
	}
}
