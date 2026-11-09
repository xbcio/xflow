package xflow

import (
	"strings"
	"testing"

	runnersvc "github.com/xbcio/xflow/service/runner"
)

// TestWireRunnerTriggerHostingInProcUsesTheInProcessChannels proves the
// in-process branch: an embedded runner that declares a trigger capability gets
// an activation tracker and a supply gate whose fetcher reads from the control
// plane in process, not over HTTP.
func TestWireRunnerTriggerHostingInProcUsesTheInProcessChannels(t *testing.T) {
	cp := newInProcTestControlServer(t)
	svcCfg := runnersvc.Config{}
	o := &runnerOptions{controlPlane: cp}
	cfg := RunnerConfig{
		Transport:    RunnerTransportInProc,
		Capabilities: []string{"xflow.trigger.cron"},
	}
	if err := wireRunnerTriggerHosting(&svcCfg, cfg, o, nil); err != nil {
		t.Fatalf("wireRunnerTriggerHosting: %v", err)
	}
	if svcCfg.ActivationTracker == nil {
		t.Fatal("no ActivationTracker was installed; the embedded runner hosts triggers with nothing to activate them")
	}
	if svcCfg.SupplyGate == nil {
		t.Fatal("no SupplyGate was installed")
	}
	if _, ok := svcCfg.SupplyGate.Fetcher().(*runnersvc.InProcessSupplyFetcher); !ok {
		t.Fatalf("gate fetcher = %T, want *runnersvc.InProcessSupplyFetcher", svcCfg.SupplyGate.Fetcher())
	}
	if svcCfg.SupplyRegistry == nil {
		t.Fatal("no supply registry was installed")
	}
}

// TestWireRunnerTriggerHostingInProcWithoutAControlPlaneIsAnError: the old
// behaviour silently disabled trigger hosting, which is indistinguishable from
// a quiet trigger. Under inproc the control plane is the only channel, so its
// absence is a configuration error.
func TestWireRunnerTriggerHostingInProcWithoutAControlPlaneIsAnError(t *testing.T) {
	svcCfg := runnersvc.Config{}
	cfg := RunnerConfig{
		Transport:    RunnerTransportInProc,
		Capabilities: []string{"xflow.trigger.cron"},
	}
	err := wireRunnerTriggerHosting(&svcCfg, cfg, &runnerOptions{}, nil)
	if err == nil {
		t.Fatal("wireRunnerTriggerHosting accepted an in-process runner hosting triggers with no control plane")
	}
	if !strings.Contains(err.Error(), "WithRunnerControlPlane") {
		t.Errorf("error %q does not name the option that fixes it", err)
	}
	if svcCfg.ActivationTracker != nil {
		t.Fatal("a tracker was installed despite the error")
	}
}

// TestNewRunnerInProcTriggerHostingNeedsNoServerURL is the end-to-end
// configuration shape the feature exists for: an embedded host declaring a
// trigger capability, with no origin and no token, built against its own control
// plane.
func TestNewRunnerInProcTriggerHostingNeedsNoServerURL(t *testing.T) {
	cp := newInProcTestControlServer(t)
	r, err := NewRunner(RunnerConfig{
		Transport:    RunnerTransportInProc,
		Capabilities: []string{"xflow.trigger.cron"},
	}, WithRunnerControlPlane(cp))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
}
