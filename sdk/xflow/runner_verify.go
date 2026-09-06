package xflow

import (
	"context"
	"time"

	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

// VerifyRunner performs a one-shot preflight against the control plane: it
// connects exactly as NewRunner would, registers, and heartbeats once. It
// starts no lease loop and hosts no triggers, so it is safe to run against a
// production control plane; the registration it sends is the same one a real
// session would send, which is the entire point.
//
// The fidelity requirement is not decorative. cmd/runner's verify command used
// to build its own client and its own registration, and every field it forgot
// turned into a preflight whose verdict did not describe the runner that would
// actually start:
//
//   - No TLS material: an mTLS control plane refuses the handshake, so verify
//     fails on a configuration that is correct — a false negative in the one
//     deployment that most needs a preflight.
//   - No token: an authenticating server answers 401, again on a correct
//     configuration.
//   - No transport: --transport grpc still probed HTTP. Against a server that
//     serves both, that is a false *positive* — verify passes over a path the
//     runner will never take.
//   - No namespaces, no group-exec feature, no SupportsEncryption: the server
//     matches assignments on exactly these, so a registration missing them can
//     be accepted while the real one is rejected, or routed differently.
//   - No SessionID on the heartbeat: a server that ties heartbeats to the
//     session it just issued rejects an unbound one.
//
// Every one of those came from the preflight owning a second, hand-maintained
// copy of the registration. This function has no copy: it shares
// newRunnerProtocolClient with NewRunner, and builds the payload from the same
// translation helpers buildRunnerServiceConfig uses. Fields added to the
// registration reach both paths or neither.
func VerifyRunner(ctx context.Context, cfg RunnerConfig) error {
	client, cleanup, err := newRunnerProtocolClient(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	// Deliberately not buildRunnerServiceConfig: that assembles artifact
	// resolvers, a node registry and the group/subgraph runtimes, and may
	// create the artifact cache directory. A preflight must not leave those
	// behind on a host that is only checking its configuration.
	registered, err := client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     cfg.RunnerID,
		Concurrency:  cfg.Concurrency,
		Capabilities: runnerCapabilities(cfg.Capabilities),
		Labels:       cloneStringMap(cfg.Labels),
		Namespaces:   runnersvc.NamespaceStrings(cfg.Namespaces),
		// Constant in buildRunnerServiceConfig, so constant here. Activations
		// is left empty: a preflight has no prior session to renew.
		SupportsEncryption: true,
	})
	if err != nil {
		return err
	}

	_, err = client.Heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID:  cfg.RunnerID,
		SessionID: registered.SessionID,
		Capacity:  cfg.Concurrency,
		InFlight:  0,
		Timestamp: time.Now().Unix(),
	})
	return err
}
