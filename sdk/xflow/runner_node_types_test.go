package xflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// runnerOnlyNodeType is known only to the runner's descriptor set: it is
// never registered in the process-global node/registry the co-hosted server
// projects, so /v1/node-types can only learn it from the runner.
const runnerOnlyNodeType = "acme.analyse"

// withRunnerOnlyDescriptor makes NewRunner report runnerOnlyNodeType v1 on
// top of the real registry, for the duration of the test.
func withRunnerOnlyDescriptor(t *testing.T) {
	t.Helper()
	if _, ok := registry.Lookup(runnerOnlyNodeType); ok {
		t.Fatalf("%s is in the server registry; the test needs a runner-only type", runnerOnlyNodeType)
	}
	prev := registeredNodeDescriptors
	registeredNodeDescriptors = func() []registry.RegisteredDescriptor {
		return append(prev(), registry.RegisteredDescriptor{
			Type:    runnerOnlyNodeType,
			Version: 1,
			Descriptor: types.Descriptor{
				Type:        runnerOnlyNodeType,
				Kind:        types.NodeKindAction,
				DisplayName: "Analyse",
				Params:      []types.ParamSpec{{Name: "depth", Type: types.ParamNumber, Default: 3}},
			},
		})
	}
	t.Cleanup(func() { registeredNodeDescriptors = prev })
}

// nodeTypeEntry is the raw wire object of one /v1/node-types entry, so the
// test can see which keys are present, not only their values.
type nodeTypeEntry map[string]json.RawMessage

func fetchNodeType(t *testing.T, baseURL, query, nodeType string) (nodeTypeEntry, bool) {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/node-types" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/node-types%s: status %d", query, resp.StatusCode)
	}
	var env struct {
		Data struct {
			NodeTypes []nodeTypeEntry `json:"node_types"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	for _, entry := range env.Data.NodeTypes {
		var typ string
		if err := json.Unmarshal(entry["node_type"], &typ); err == nil && typ == nodeType {
			return entry, true
		}
	}
	return nil, false
}

func waitForNodeType(t *testing.T, baseURL, query, nodeType string, present bool) nodeTypeEntry {
	t.Helper()
	// Control caches the fleet descriptor read for ~2s, so a change can take
	// that long to surface on top of registration latency. The budget is
	// generous because under the full race gate registration alone has been
	// observed to exceed 10s on a loaded host.
	deadline := time.Now().Add(30 * time.Second)
	for {
		entry, ok := fetchNodeType(t, baseURL, query, nodeType)
		if ok == present {
			return entry
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s present=%v on /v1/node-types%s, want present=%v", nodeType, ok, query, present)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startNodeTypesRunner runs a runner declaring runnerOnlyNodeType against srv
// and returns a func that stops it and waits for Run to return.
func startNodeTypesRunner(t *testing.T, cfg RunnerConfig, opts ...RunnerOption) (stop func()) {
	t.Helper()
	cfg.RunnerID = "acme-runner"
	cfg.Capabilities = []string{runnerOnlyNodeType}
	cfg.PollWait = 10 * time.Millisecond
	r, err := NewRunner(cfg, opts...)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var stopped bool
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("runner Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("runner did not stop")
		}
		_ = r.Close()
	}
	t.Cleanup(stop)
	return stop
}

func startNodeTypesServer(t *testing.T, cfg ServerConfig) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(cfg, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		httpSrv.Close()
		cancel()
		_ = srv.Shutdown(context.Background())
	})
	return srv, httpSrv
}

// A type registered only on a runner reaches a namespace-scoped node-types
// list as a runner-sourced entry, never the unscoped one, and leaves it once
// the runner's session ends.
func TestRunnerNodeTypesReachNodeTypes(t *testing.T) {
	backends := []struct {
		name  string
		setup func(t *testing.T) ServerConfig
	}{
		{name: "memory", setup: func(*testing.T) ServerConfig { return ServerConfig{} }},
		{name: "redis", setup: func(t *testing.T) ServerConfig {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(mr.Close)
			return ServerConfig{RedisAddr: mr.Addr()}
		}},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			withRunnerOnlyDescriptor(t)
			_, httpSrv := startNodeTypesServer(t, backend.setup(t))
			stop := startNodeTypesRunner(t, RunnerConfig{ServerURL: httpSrv.URL})

			entry := waitForNodeType(t, httpSrv.URL, "?namespace=default", runnerOnlyNodeType, true)
			if got := string(entry["source"]); got != `"runner"` {
				t.Errorf("source = %s, want \"runner\"", got)
			}
			if raw, ok := entry["runner_pools"]; ok {
				t.Errorf("pool-less runner entry carries runner_pools %s", raw)
			}
			var display string
			if err := json.Unmarshal(entry["display_name"], &display); err != nil || display != "Analyse" {
				t.Errorf("display_name = %s, want the runner's Analyse", entry["display_name"])
			}
			if _, ok := fetchNodeType(t, httpSrv.URL, "", runnerOnlyNodeType); ok {
				t.Error("unscoped /v1/node-types served a runner type")
			}

			// The HTTP runner deregisters on a graceful stop, which ends its
			// live window at once instead of after the TTL.
			stop()
			waitForNodeType(t, httpSrv.URL, "?namespace=default", runnerOnlyNodeType, false)
		})
	}
}

// Without a deregister (the in-process transport has none) a stopped runner's
// types stay listed for the live-TTL grace window that covers a rolling
// restart; expiry after DefaultRunnerLiveTTL is covered through the same
// handler, with a controllable clock, in service/apiserver.
func TestRunnerNodeTypesOutliveAStopWithinTheLiveTTL(t *testing.T) {
	withRunnerOnlyDescriptor(t)
	srv, httpSrv := startNodeTypesServer(t, ServerConfig{})
	stop := startNodeTypesRunner(t, RunnerConfig{Transport: RunnerTransportInProc}, WithRunnerControlPlane(srv.ControlServer()))

	waitForNodeType(t, httpSrv.URL, "?namespace=default", runnerOnlyNodeType, true)
	stop()
	if _, ok := fetchNodeType(t, httpSrv.URL, "?namespace=default", runnerOnlyNodeType); !ok {
		t.Fatalf("%s left /v1/node-types as soon as the runner stopped; want it kept until the live TTL", runnerOnlyNodeType)
	}
}
