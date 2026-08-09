package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

func TestRunCommandPropagatesResolvedDurationsToRunnerService(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
server:
  url: http://file-server:8080
heartbeat:
  interval: 7s
poll:
  wait: 2s
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("XFLOW_RUNNER_HEARTBEAT_INTERVAL", "9s")
	t.Setenv("XFLOW_RUNNER_POLL_WAIT", "3s")

	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.PollWait != 4*time.Second {
			t.Fatalf("PollWait = %s, want 4s", cfg.PollWait)
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--config", path, "--heartbeat-interval", "11s", "--poll-wait", "4s")
	if err != nil {
		t.Fatal(err)
	}
}

type runnerServiceFunc func(context.Context) error

func (f runnerServiceFunc) Run(ctx context.Context) error {
	return f(ctx)
}

func stubRunnerServiceFactory(check func(runnersvc.Config) error) func() {
	previous := newRunnerService
	newRunnerService = func(_ runnersvc.ProtocolClient, _ engine.HandlerRegistry, cfg runnersvc.Config) runnerService {
		return runnerServiceFunc(func(context.Context) error {
			return check(cfg)
		})
	}
	return func() {
		newRunnerService = previous
	}
}

// A batch lease fails outright when the runner has no SubgraphRuntime: the
// batch names a synthetic node ("m/_batch/0") with no Input and no registered
// handler, so the ordinary node path has nothing to run. Only the e2e tests
// used to wire the runtime themselves — the production binary never did, which
// made every map node undeployable the moment its batches escaped to a runner.
func TestRunCommandWiresTheSubgraphRuntime(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.SubgraphRuntime == nil {
			t.Error("runner service got no SubgraphRuntime; every batch lease this " +
				"runner claims will fail with 'no SubgraphRuntime configured'")
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080", "--cap", "xflow.map,xflow.function")
	if err != nil {
		t.Fatal(err)
	}
}

// The group counterpart of the SubgraphRuntime wiring above, and the reason it
// went unnoticed far longer: a runner with no GroupRuntime does not FAIL a group
// lease, it never receives one. A group unit's routing demands the
// group.exec.v1 feature, so a runner that does not advertise it is filtered out
// during assignment and the task sits queued — no error, no log, no lease.
//
// Both halves are required and neither is sufficient. Advertising without the
// runtime means runner.go:359 falls through to the handler path, where the
// group's synthetic node name resolves to nothing. Wiring the runtime without
// advertising leaves the runner invisible to the selector. So both are asserted
// here, in one test, on the production command path.
func TestRunCommandWiresAndAdvertisesGroupExecution(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.GroupRuntime == nil {
			t.Error("runner service got no GroupRuntime; a group lease reaching this " +
				"runner falls through to the handler path, which has no handler " +
				"registered for the group's synthetic node name")
		}
		// parseCapabilities cannot produce a Features list — the --cap flag has no
		// syntax for one — so this capability can only come from the binary itself.
		var advertised bool
		for _, c := range cfg.Capabilities {
			if c.NodeType != "xflow.group" {
				continue
			}
			for _, f := range c.Features {
				if f == engine.FeatureGroupExecV1 {
					advertised = true
				}
			}
		}
		if !advertised {
			t.Errorf("capabilities %+v carry no {xflow.group, %s}; MatchCapabilities "+
				"rejects this runner for every group task, so the task stays queued "+
				"forever with no error anywhere", cfg.Capabilities, engine.FeatureGroupExecV1)
		}
		return nil
	})
	defer restore()

	// Deliberately no xflow.group in --cap: the operator is not expected to know
	// it exists, and could not spell the feature even if they did.
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080", "--cap", "xflow.function")
	if err != nil {
		t.Fatal(err)
	}
}

// An operator who has heard of group execution reaches for the tool they have:
// `--cap xflow.group`. That produces a bare {NodeType: "xflow.group"} with no
// Features, because --cap cannot express one — and a featureless entry is worse
// than no entry at all. It satisfies canRunRouting (which ignores Features)
// while failing MatchCapabilities (which does not), so the runner looks
// correctly configured from the command line and still receives nothing.
//
// So a declared group capability must be completed, not deferred to.
func TestDeclaringTheGroupCapabilityByHandStillGetsTheFeature(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		var groupCaps int
		var advertised bool
		for _, c := range cfg.Capabilities {
			if c.NodeType != engine.GroupNodeType {
				continue
			}
			groupCaps++
			for _, f := range c.Features {
				if f == engine.FeatureGroupExecV1 {
					advertised = true
				}
			}
		}
		if !advertised {
			t.Errorf("capabilities %+v: an operator-declared xflow.group was left "+
				"without %s, which is the one shape that passes canRunRouting and "+
				"fails MatchCapabilities — the runner looks configured and gets nothing",
				cfg.Capabilities, engine.FeatureGroupExecV1)
		}
		// hasCapabilityForRequirement stops at the first NodeType match, so a
		// featureless duplicate sitting ahead of the real one would mask it.
		if groupCaps != 1 {
			t.Errorf("got %d xflow.group capabilities, want exactly 1: a duplicate can "+
				"shadow the feature-bearing entry during requirement matching", groupCaps)
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080", "--cap", "xflow.function,xflow.group")
	if err != nil {
		t.Fatal(err)
	}
}

// TestRunCommandWiresGroupRuntimeIntoTriggerActivationHandler pins the
// construction-order fix. Before it, runRunner built the registry and
// GroupRuntime AFTER runnerServiceConfig had already returned, so the
// TriggerActivationHandler that config wires could never be given one — and
// every trigger-group activation on the production binary failed closed at
// activateGroup's first guard (spec 2026-08-07 §3, the gap this feature
// closes).
//
// A group directive is pushed through the real ActivationTracker the config
// assembled. Package is nil on purpose: the post-fix path must still fail,
// but at the SECOND guard. Which guard fires is the whole signal.
func TestRunCommandWiresGroupRuntimeIntoTriggerActivationHandler(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.GroupRuntime == nil {
			t.Fatal("cfg.GroupRuntime is nil")
		}
		if cfg.ActivationTracker == nil {
			t.Fatal("cfg.ActivationTracker is nil — hostsTriggers(xflow.trigger.kafka) should have wired it")
		}

		var activateErr error
		cfg.ActivationTracker.SetOnActivateFailed(func(_ protocol.ActivateDirective, err error) {
			activateErr = err
		})
		if err := cfg.ActivationTracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
			Activate: []protocol.ActivateDirective{{
				Namespace: "default", WorkflowID: "wf-1", WorkflowVersion: "v1",
				EntryUnitID: "g", NodeType: engine.GroupNodeType, Generation: 1,
				PackageHash: "pkg-sha256:v1:x",
				// Package deliberately omitted — see the doc comment.
			}},
		}); err != nil {
			t.Fatalf("ProcessDirectives: %v", err)
		}

		if activateErr == nil {
			t.Fatal("group activation with a nil Package unexpectedly succeeded")
		}
		if strings.Contains(activateErr.Error(), "no GroupRuntime configured") {
			t.Errorf("group activation failed at the missing-runtime guard: %v\n"+
				"the TriggerActivationHandler was built before the GroupRuntime existed; "+
				"every trigger-group activation on this runner fails closed", activateErr)
		}
		if !strings.Contains(activateErr.Error(), "carries no package") {
			t.Errorf("activation error = %v, want the nil-package guard", activateErr)
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080", "--cap", "xflow.trigger.kafka")
	if err != nil {
		t.Fatal(err)
	}
}
