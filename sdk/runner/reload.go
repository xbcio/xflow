package runner

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// credentialReloadFunc performs one SIGHUP-triggered credential reload
// attempt and reports the outcome. It mirrors cmd/server/reload.go's
// reloadFunc shape (a plain func() error, logged by the caller) rather than
// introducing a second reload abstraction for the runner side.
type credentialReloadFunc func() error

// newCredentialReloadFunc builds the reloadFunc runWithSignals registers for
// SIGHUP. On each call it re-resolves cfg from the same config file, process
// environment, and already-parsed flags runRunner started with — exactly the
// precedence resolveRunnerConfig already applies at startup — and hands the
// live token and TLS material to runner.Reload.
//
// The token has two distinct sources, and this function must not conflate
// them:
//
//   - A statically configured token (--token / XFLOW_RUNNER_TOKEN / YAML
//     token) is re-read from cfg on every call, so rotating it in the config
//     file and sending SIGHUP picks up the new value — the runner-side half of
//     the credential-key-rotation-runbook's axis 3a procedure.
//   - An enrollment-issued identity's token is NOT re-read from cfg here: per
//     the runbook's axis 3b, that token cannot be rotated in place (renewal
//     only extends its expiry), so re-reading cfg.token for an enrolled runner
//     would either send the same token again (harmless) or, worse, send a
//     stale static token left over in the config file from before enrollment
//     (would break authentication). store.Load's second return value is what
//     distinguishes the two cases: an issued identity in the store always wins
//     over whatever cfg carries, mirroring resolveRunnerIdentity's own
//     precedence.
//
// TLS material (server CA, client cert/key) has no enrollment-issued
// counterpart, so it is always re-read from cfg.
func newCredentialReloadFunc(cfg runnerConfig, store identityStore, runner credentialReloadingRunner) credentialReloadFunc {
	return func() error {
		resolved, err := resolveRunnerConfig(cfg)
		if err != nil {
			return err
		}
		token := resolved.token
		if stored, ok, loadErr := store.Load(); loadErr == nil && ok {
			token = stored.Token
		}
		return runner.Reload(xflowsdk.CredentialReloaderSource{
			Token:         token,
			TLSServerCA:   resolved.tlsServerCA,
			TLSClientCert: resolved.tlsClientCert,
			TLSClientKey:  resolved.tlsClientKey,
		})
	}
}

// installCredentialReloadSignal wires fn to fire on every SIGHUP delivered to
// this process, logging the outcome the same way cmd/server/reload.go's
// reloader does: a log line naming success or failure, and on failure, the
// error — never the token or key material, since credentialReloadFunc's own
// errors (CredentialReloader.Reload's) never carry either (see
// xflow.CredentialReloader.Reload's doc). It returns a stop func that undoes
// the signal.Notify registration; the caller is responsible for calling it on
// shutdown, matching notifyReloadSignal's contract in cmd/server.
//
// Production always goes through this. A test drives credentialReloadFunc
// directly instead of sending a real SIGHUP to the test binary, for the same
// reason cmd/server/reload_test.go avoids it: SIGHUP would affect the whole
// `go test` process, not just the code under test.
func installCredentialReloadSignal(ctx context.Context, fn credentialReloadFunc) func() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-sig:
				if !ok {
					return
				}
				if err := fn(); err != nil {
					slog.Error("runner: credential reload failed, keeping previous credentials", "error", err)
					continue
				}
				slog.Info("runner: credential reload succeeded")
			}
		}
	}()
	return func() {
		signal.Stop(sig)
		close(sig)
		<-done
	}
}
