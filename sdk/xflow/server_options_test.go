package xflow

import (
	"net/http"
	"testing"
	"time"

	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
)

// These four settings exist on apiserver.Config and are set by cmd/server. The
// SDK left every one of them at its zero value, so an embedded server ran with
// no tracing, the backend's default concurrency, no management API and no
// runner metrics proxy — each of which reads as "the feature is off" rather
// than "the feature was never wired", and none of which surfaces at startup.

// A nil Tracer skips the OTel HTTP middleware entirely, so an embedded server
// is a hole in the trace: spans enter at the caller and resume at the runner
// with no parent linking them. The host program already builds a provider for
// its own code; not being able to hand it over is what makes the gap silent.
func TestWithServerTracerReachesTheAPIConfig(t *testing.T) {
	tracer := tracing.NoopTracer{}
	sc := &serverConfig{}
	WithServerTracer(tracer)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.Tracer == nil {
		t.Error("Tracer = nil: the HTTP tracing middleware never registers, so the " +
			"embedded server drops out of every distributed trace")
	}
}

// Concurrency bounds how many tasks the backend dispatches at once. At zero the
// backend applies its own default, which is not the host's choice — a host
// sizing the server to its runner fleet had no way to express it.
func TestWithServerConcurrencyReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerConcurrency(32)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.Concurrency != 32 {
		t.Errorf("Concurrency = %d, want 32", cfg.Concurrency)
	}
}

// The runner metrics proxy is what lets a runner in another network domain —
// the one that cannot be scraped, which is the whole reason the proxy exists —
// report into the server's /metrics. Off by default is correct; unreachable is
// not.
func TestWithServerRunnerMetricsProxyReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerRunnerMetricsProxy()(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if !cfg.EnableRunnerMetricsProxy {
		t.Error("EnableRunnerMetricsProxy = false: runners that cannot be scraped " +
			"have no way to report, so their metrics are simply absent")
	}
}

// The reporting cadence is annotated onto every heartbeat response whether or
// not the inbox is enabled, so it is its own option rather than a parameter of
// the proxy one. Binding them would make the case an operator most needs —
// a negative value, which suspends reporting fleet-wide without a restart —
// reachable only by also turning on an inbox they do not want.
func TestRunnerMetricsIntervalIsIndependentOfTheProxy(t *testing.T) {
	sc := &serverConfig{}
	WithServerRunnerMetricsInterval(-1)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.RunnerMetricsInterval != -1 {
		t.Errorf("RunnerMetricsInterval = %v, want -1: the fleet-wide suspend "+
			"cadence never reaches the heartbeat response without the proxy",
			cfg.RunnerMetricsInterval)
	}
	if cfg.EnableRunnerMetricsProxy {
		t.Error("EnableRunnerMetricsProxy = true: setting a cadence must not " +
			"also open the inbox endpoint")
	}
}

func TestWithServerRunnerMetricsIntervalReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerRunnerMetricsProxy()(sc)
	WithServerRunnerMetricsInterval(30 * time.Second)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.RunnerMetricsInterval != 30*time.Second {
		t.Errorf("RunnerMetricsInterval = %v, want 30s", cfg.RunnerMetricsInterval)
	}
}

// The management API is opt-in via an apiserver Option rather than a Config
// field, so it is the one of the four that needs the option to be forwarded
// rather than a field copied. Asserted by the route responding at all: a
// module that never registered gives 404 from the mux, not from the handler.
func TestWithServerManagementRegistersTheRoute(t *testing.T) {
	ts := startTestServer(t, WithServerManagement())

	resp, err := http.Get(ts.URL + "/v1/management/leader")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("/v1/management/leader = 404: the management module never " +
			"registered, so the ops read-only API is absent from an embedded server")
	}
}

// The supply transport key rotates on a period; a caller that needs a shorter
// one than the default (a shared-tenancy deployment, say) had no way to say so,
// and a negative value — "do not rotate" — was equally unreachable. Zero adopts
// the apiserver default, so the option's job is to make non-default reachable.
func TestWithServerSupplyKeyRotationReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerSupplyKeyRotation(90 * time.Minute)(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if cfg.SupplyKeyRotationPeriod != 90*time.Minute {
		t.Errorf("SupplyKeyRotationPeriod = %v, want 90m", cfg.SupplyKeyRotationPeriod)
	}
}

// The management API is an operator surface and needs its own guard: the
// workflow authenticator gates /v1/workflows, but /v1/management/* is
// registered by a separate module that does not consult it. cmd/server wraps it
// with ManagementAuthMiddleware for exactly this reason; without the same hook
// an embedded server that enables management exposes leader identity, runner
// status and execution lookup to anyone who can reach the port.
func TestServerManagementCanBeGatedByMiddleware(t *testing.T) {
	auth := apiserver.NewBearerTokenAuth("mgmt-token")
	ts := startTestServer(t,
		WithServerManagement(),
		WithServerHTTPMiddleware(apiserver.ManagementAuthMiddleware(auth)))

	resp, err := http.Get(ts.URL + "/v1/management/leader")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /v1/management/leader with no token = %d, want 401: the "+
			"management surface is ungated", resp.StatusCode)
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/management/leader", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer mgmt-token")
	authed, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer authed.Body.Close()
	if authed.StatusCode == http.StatusUnauthorized {
		t.Error("the configured management token was rejected")
	}
}

// And it stays off unless asked for, so the default posture does not quietly
// grow an ops surface.
func TestManagementRouteAbsentByDefault(t *testing.T) {
	ts := startTestServer(t)

	resp, err := http.Get(ts.URL + "/v1/management/leader")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/v1/management/leader = %d without WithServerManagement, want 404",
			resp.StatusCode)
	}
}
