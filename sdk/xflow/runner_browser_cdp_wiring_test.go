package xflow

import (
	"strings"
	"testing"
	"time"

	xnode "github.com/xbcio/xflow/node"
	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/observability/metrics"
)

func TestResolveRunnerBrowserCDPConfigUsesDefaultsAndCopiesAllowlist(t *testing.T) {
	defaults := xnode.BrowserCDPConfigDefaults()
	input := xnode.BrowserCDPConfig{EndpointAllowlist: []string{"chrome.test.internal"}}

	got := resolveRunnerBrowserCDPConfig(input)
	if got.MaxContexts != defaults.MaxContexts {
		t.Fatalf("MaxContexts = %d, want node default %d", got.MaxContexts, defaults.MaxContexts)
	}
	if got.QueueTimeout != defaults.QueueTimeout {
		t.Fatalf("QueueTimeout = %s, want node default %s", got.QueueTimeout, defaults.QueueTimeout)
	}
	if got.ConnectTimeout != defaults.ConnectTimeout {
		t.Fatalf("ConnectTimeout = %s, want node default %s", got.ConnectTimeout, defaults.ConnectTimeout)
	}
	input.EndpointAllowlist[0] = "mutated.test.internal"
	if got.EndpointAllowlist[0] != "chrome.test.internal" {
		t.Fatalf("EndpointAllowlist = %v, want an independent copy", got.EndpointAllowlist)
	}
}

func TestNewRunnerOrdinaryAndBrowserCDPRunnersCoexist(t *testing.T) {
	browserConfig := xnode.BrowserCDPConfigDefaults()
	browserConfig.EndpointAllowlist = []string{"chrome-a.test.internal"}
	browser := newBrowserCDPTestRunner(t, browserConfig)
	t.Cleanup(func() {
		if err := browser.Close(); err != nil {
			t.Errorf("Close Browser runner: %v", err)
		}
	})

	// Deliberately conflict with the live Browser configuration. A runner that
	// does not advertise the exact Browser node type must neither validate nor
	// acquire this process-global configuration.
	ordinaryConfig := browserConfig
	ordinaryConfig.EndpointAllowlist = []string{"chrome-b.test.internal"}
	ordinaryConfig.MaxContexts++
	ordinary, err := NewRunner(RunnerConfig{
		ServerURL:    "http://127.0.0.1:1",
		RunnerID:     "ordinary-with-unused-browser-config",
		Capabilities: []string{"xflow.function", "xflow.browser.cdp.extra"},
		BrowserCDP:   ordinaryConfig,
	})
	if err != nil {
		t.Fatalf("NewRunner ordinary: %v", err)
	}
	if err := ordinary.Close(); err != nil {
		t.Fatalf("Close ordinary runner: %v", err)
	}
}

func TestNewRunnerBrowserCDPConfigLeaseLifecycle(t *testing.T) {
	firstConfig := xnode.BrowserCDPConfigDefaults()
	firstConfig.EndpointAllowlist = []string{"chrome-a.test.internal"}
	firstConfig.MaxContexts = 2
	firstConfig.QueueTimeout = 3 * time.Second
	firstConfig.ConnectTimeout = 4 * time.Second

	first := newBrowserCDPTestRunner(t, firstConfig)
	second := newBrowserCDPTestRunner(t, firstConfig)
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Errorf("Close first runner: %v", err)
		}
		if err := second.Close(); err != nil {
			t.Errorf("Close second runner: %v", err)
		}
	})

	conflictingConfig := firstConfig
	conflictingConfig.EndpointAllowlist = append([]string(nil), firstConfig.EndpointAllowlist...)
	conflictingConfig.MaxContexts = firstConfig.MaxContexts + 1
	if _, err := NewRunner(browserCDPTestRunnerConfig(conflictingConfig)); err == nil {
		t.Fatal("NewRunner accepted a Browser CDP configuration conflicting with live runners")
	} else if !strings.Contains(strings.ToLower(err.Error()), "browser") {
		t.Fatalf("conflict error = %v, want it to identify Browser CDP configuration", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("Close first runner: %v", err)
	}
	if _, err := NewRunner(browserCDPTestRunnerConfig(conflictingConfig)); err == nil {
		t.Fatal("NewRunner accepted a conflicting configuration while the second lease was live")
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close second runner: %v", err)
	}

	third := newBrowserCDPTestRunner(t, conflictingConfig)
	t.Cleanup(func() {
		if err := third.Close(); err != nil {
			t.Errorf("Close third runner: %v", err)
		}
	})
}

func TestNewRunnerObserverPanicRollsBackBrowserCDPLeaseAndObservers(t *testing.T) {
	kafkatrigger.SetObserver(nil)
	kafkatrigger.SetObserver(probeKafkaObserver{})
	defer kafkatrigger.SetObserver(nil)

	firstConfig := xnode.BrowserCDPConfigDefaults()
	firstConfig.EndpointAllowlist = []string{"chrome-a.test.internal"}
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("NewRunner did not preserve the observer conflict panic")
			}
		}()
		_, _ = NewRunner(browserCDPTestRunnerConfig(firstConfig), WithRunnerMetrics(metrics.New()))
	}()

	// The failed constructor reached the final Kafka observer setter. Clearing
	// the deliberately occupied slot must now allow all earlier observer slots
	// and the Browser CDP lease to be acquired with a different configuration.
	kafkatrigger.SetObserver(nil)
	secondConfig := firstConfig
	secondConfig.EndpointAllowlist = []string{"chrome-b.test.internal"}
	secondConfig.MaxContexts++
	runner, err := NewRunner(browserCDPTestRunnerConfig(secondConfig), WithRunnerMetrics(metrics.New()))
	if err != nil {
		t.Fatalf("NewRunner after observer panic: %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("Close runner: %v", err)
	}
}

func TestNewRunnerBrowserCDPConfigErrorDoesNotAcquireALease(t *testing.T) {
	invalidConfig := xnode.BrowserCDPConfigDefaults()
	invalidConfig.MaxContexts = -1
	if _, err := NewRunner(browserCDPTestRunnerConfig(invalidConfig)); err == nil {
		t.Fatal("NewRunner accepted an invalid Browser CDP configuration")
	}

	validConfig := xnode.BrowserCDPConfigDefaults()
	validConfig.EndpointAllowlist = []string{"chrome.test.internal"}
	runner := newBrowserCDPTestRunner(t, validConfig)
	t.Cleanup(func() {
		if err := runner.Close(); err != nil {
			t.Errorf("Close runner: %v", err)
		}
	})
}

func newBrowserCDPTestRunner(t *testing.T, browserCDP xnode.BrowserCDPConfig) *Runner {
	t.Helper()
	runner, err := NewRunner(browserCDPTestRunnerConfig(browserCDP))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return runner
}

func browserCDPTestRunnerConfig(browserCDP xnode.BrowserCDPConfig) RunnerConfig {
	return RunnerConfig{
		ServerURL:    "http://127.0.0.1:1",
		RunnerID:     "browser-cdp-config-test",
		Capabilities: []string{"xflow.browser.cdp"},
		BrowserCDP:   browserCDP,
	}
}
