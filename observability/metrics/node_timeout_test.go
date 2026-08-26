package metrics_test

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// blockingMetricHandler ignores ctx and blocks until released is closed. It is
// the production worst case for the timeout path: a handler that does not
// cooperate with context cancellation, so the runner must abandon its goroutine.
type blockingMetricHandler struct {
	blocked  chan struct{}
	released chan struct{}
}

func (blockingMetricHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.blocking-metric"}
}

func (h blockingMetricHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	close(h.blocked)
	<-h.released
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// metricHandlerRegistry is a minimal engine.HandlerRegistry for the metrics
// test: it returns one fixed handler regardless of node type/version/name.
type metricHandlerRegistry struct {
	handler types.ActionHandler
}

func (r metricHandlerRegistry) Get(types.ExecutionID, string, string, int) (types.ActionHandler, error) {
	return r.handler, nil
}

// metricsBody scrapes the registry through the same Prometheus text handler the
// server exposes, so the test reads exactly what an operator would scrape.
func metricsBody(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

// TestNodeTimeoutMetricSourceLabelRunner proves the runner production path
// (execution.Runner.Execute) EMITS xflow_node_timeouts_total with source=runner
// when a non-cooperative handler outlives its deadline. It must go red if the
// emission call site in execution/runner.go is deleted.
func TestNodeTimeoutMetricSourceLabelRunner(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)
	runner := execution.NewRunner(metricHandlerRegistry{handler: blockingMetricHandler{
		blocked:  make(chan struct{}),
		released: make(chan struct{}),
	}}, execution.WithTimeoutObserver(obs))

	budget := 60 * time.Millisecond
	lease := &engine.TaskLease{
		Task: engine.Task{ExecutionID: "exec-metric-runner", NodeName: "probe"},
		Input: &types.Input{
			ExecutionID: "exec-metric-runner",
			NodeName:    "probe",
			Timeout:     budget,
			// Sensitive-looking content that must never appear in metric labels.
			Data: map[string]any{"token": "should-not-leak", "output": "secret-http-body"},
		},
		NodeType: "test.blocking-metric",
		// ExecutionDeadline in the past: triggers the early-expired branch
		// (handler never runs), which is the simplest non-racy emission path.
		ExecutionDeadline: time.Now().Add(-time.Second),
	}

	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want permanent node.timeout")
	}

	body := metricsBody(t, m)
	want := `xflow_node_timeouts_total{node_type="test.blocking-metric",source="runner"} 1`
	if !strings.Contains(body, want) {
		t.Fatalf("metrics body missing %q:\n%s", want, body)
	}
}

// TestNodeTimeoutMetricAbandonedGaugeFallsBackToZero proves the abandoned gauge
// rises when a handler is abandoned (deadline fires, handler still running) and
// falls back to zero once that handler finally returns. A gauge that only ever
// increments is a counter with a misleading type; this test pins the decrement.
func TestNodeTimeoutMetricAbandonedGaugeFallsBackToZero(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)

	released := make(chan struct{})
	blocked := make(chan struct{})
	h := blockingMetricHandler{blocked: blocked, released: released}
	runner := execution.NewRunner(metricHandlerRegistry{handler: h}, execution.WithTimeoutObserver(obs))

	budget := 60 * time.Millisecond
	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-abandoned", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-abandoned", NodeName: "probe", Timeout: budget},
		NodeType:          "test.blocking-metric",
		ExecutionDeadline: time.Now().Add(budget),
	}

	// Safety backstop: never let the test hang.
	go func() {
		time.Sleep(5 * time.Second)
		select {
		case <-released:
		default:
			close(released)
		}
	}()

	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want permanent node.timeout")
	}

	// Wait for the handler to have entered (it closed blocked) so the abandon
	// branch definitely ran and the gauge incremented.
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never entered; blocking handler blocked channel not closed")
	}

	// After Execute returns with a deadline-abandon, the gauge must be 1.
	if got := gaugeValue(t, m, "xflow_node_timeout_abandoned", "test.blocking-metric"); got != 1 {
		t.Fatalf("abandoned gauge = %v after abandon, want 1 (the handler outlived its deadline)", got)
	}

	// Release the abandoned handler goroutine and let its watcher observe the
	// return so the gauge decrements back to zero.
	close(released)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := gaugeValue(t, m, "xflow_node_timeout_abandoned", "test.blocking-metric"); got == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("abandoned gauge never fell back to 0 after handler returned; last value = %v",
		gaugeValue(t, m, "xflow_node_timeout_abandoned", "test.blocking-metric"))
}

// TestNodeTimeoutMetricExecutionDuration proves the duration histogram is emitted
// from the runner production path on a normal (non-timed-out) bounded
// invocation. It must go red if the OnHandlerDuration call site is deleted.
func TestNodeTimeoutMetricExecutionDuration(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)

	h := cooperativeQuickHandler{}
	runner := execution.NewRunner(metricHandlerRegistry{handler: h}, execution.WithTimeoutObserver(obs))

	budget := 5 * time.Second
	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-duration", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-duration", NodeName: "probe", Timeout: budget},
		NodeType:          "test.cooperative-quick",
		ExecutionDeadline: time.Now().Add(budget),
	}

	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if res.Error != nil {
		t.Fatalf("TaskResult.Error = %v, want nil (handler completed within budget)", res.Error)
	}

	body := metricsBody(t, m)
	if !strings.Contains(body, `xflow_node_execution_duration_seconds_count{node_type="test.cooperative-quick"} 1`) {
		t.Fatalf("metrics body missing duration sample:\n%s", body)
	}
}

// TestNodeTimeoutMetricHelpTextsRegistered pins that the three new metrics
// carry real Help text in the registry (so an operator scraping /metrics can
// tell them apart from the suspend-timeout metric xflow_node_timed_out_total).
// Help text is attached at registration, so each metric is emitted once first.
func TestNodeTimeoutMetricHelpTextsRegistered(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)
	ctx := context.Background()
	// Emit one sample of each so the underlying family registers with its Help.
	obs.OnNodeExecutionTimeout(ctx, "test.help", "runner")
	obs.OnHandlerAbandoned(ctx, "test.help", 1)
	obs.OnHandlerDuration(ctx, "test.help", time.Millisecond)
	// Also register the distinct suspend-timeout metric so its Help is present
	// for the comparison assertion below.
	metrics.NewMetricsHooks(m).OnNodeTimeout(ctx, "exec-x", "node-x")

	body := metricsBody(t, m)
	// Assert the documented sentence, not merely that a HELP line exists.
	// helpText falls back to "xflow metric <name>" for any metric missing from
	// metricHelp and Prometheus emits that fallback as a HELP line like any
	// other, so `Contains(body, "# HELP "+name+" ")` was true no matter what —
	// including in exactly the case the failure message named, a metric that
	// "would fall back to the generic description". Deleting all four entries
	// from metricHelp left it green.
	for name, wantHelp := range map[string]string{
		"xflow_node_timeouts_total":             "Node executions terminated for exceeding their deadline.",
		"xflow_node_timeout_abandoned":          "Handlers whose deadline passed but which have not returned.",
		"xflow_node_execution_duration_seconds": "Wall-clock duration of one handler invocation.",
		// The distinct suspend-timeout metric must still be documented
		// separately — same name prefix, different meaning.
		"xflow_node_timed_out_total": "Nodes that exceeded their timeout.",
	} {
		if !strings.Contains(body, "# HELP "+name+" "+wantHelp) {
			t.Errorf("metric %q is not documented as %q; it fell back to the generic "+
				"description or was reworded", name, wantHelp)
		}
	}
}

// TestNodeTimeoutMetricDoesNotLeakSensitiveLabels asserts the new metrics carry
// only node_type/source labels — never node name, execution ID, params, or any
// node output. This is the sharp edge from the security constraints.
func TestNodeTimeoutMetricDoesNotLeakSensitiveLabels(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)
	runner := execution.NewRunner(metricHandlerRegistry{handler: blockingMetricHandler{
		blocked:  make(chan struct{}),
		released: make(chan struct{}),
	}}, execution.WithTimeoutObserver(obs))

	lease := &engine.TaskLease{
		// Sensitive-looking values that must NEVER appear as labels.
		Task: engine.Task{ExecutionID: "exec-secret-exec-id-xyz", NodeName: "secret-node-name"},
		Input: &types.Input{
			ExecutionID: "exec-secret-exec-id-xyz",
			NodeName:    "secret-node-name",
			Timeout:     60 * time.Millisecond,
			Data:        map[string]any{"token": "should-not-leak", "output": "secret-http-body"},
		},
		NodeType: "test.blocking-metric",
		// Past deadline: triggers the early-expired emission path with no race.
		ExecutionDeadline: time.Now().Add(-time.Second),
	}

	if _, err := runner.Execute(context.Background(), lease); err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}

	body := metricsBody(t, m)
	for _, forbidden := range []string{"secret-node-name", "exec-secret-exec-id-xyz", "should-not-leak", "secret-http-body"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("metrics body leaked sensitive value %q:\n%s", forbidden, body)
		}
	}
}

// TestNodeTimeoutMetricsConcurrentAbandonedGauge exercises the gauge under
// concurrent abandon+return pairs to catch a race in the delta-tracking state.
// Run with -race.
func TestNodeTimeoutMetricsConcurrentAbandonedGauge(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)

	const n = 8
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.OnHandlerAbandoned(context.Background(), "test.concurrent", 1)
			obs.OnHandlerAbandoned(context.Background(), "test.concurrent", -1)
		}()
	}
	wg.Wait()

	if got := gaugeValue(t, m, "xflow_node_timeout_abandoned", "test.concurrent"); got != 0 {
		t.Fatalf("abandoned gauge = %v after balanced abandon/return, want 0", got)
	}
}

// TestNodeTimeoutMetricNotIncrementedOnParentCancel proves the runner does NOT
// emit xflow_node_timeouts_total when a handler's context is cancelled by its
// parent before the deadline elapsed (lease fenced/lost, renewal failed
// MaxRetries times, execution cancelled, runner shutdown). Metric labels are
// permanent time series: counting a cancellation as a timeout would inflate the
// counter for an event that was never a timeout. It must go red if
// observeTimeout is gated on ctx.Err() != nil instead of on a genuine deadline
// expiry.
func TestNodeTimeoutMetricNotIncrementedOnParentCancel(t *testing.T) {
	m := metrics.New()
	obs := metrics.NewNodeTimeoutMetrics(m)
	runner := execution.NewRunner(metricHandlerRegistry{handler: cancelWaitMetricHandler{}}, execution.WithTimeoutObserver(obs))

	budget := 10 * time.Minute
	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-cancel-metric", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-cancel-metric", NodeName: "probe", Timeout: budget},
		NodeType:          "test.cancel-metric",
		ExecutionDeadline: time.Now().Add(budget),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	res, err := runner.Execute(ctx, lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a non-nil cancellation error")
	}
	if types.IsPermanent(res.Error) {
		t.Fatalf("TaskResult.Error = %v, want NOT permanent (a cancellation must not be a terminal timeout)", res.Error)
	}

	body := metricsBody(t, m)
	if strings.Contains(body, `xflow_node_timeouts_total{node_type="test.cancel-metric",source="runner"}`) {
		t.Fatalf("metrics body incremented xflow_node_timeouts_total for a parent cancellation (not a timeout):\n%s", body)
	}
}

// cancelWaitMetricHandler waits for ctx cancellation then returns ctx.Err(). It
// is the cooperative handler for the parent-cancel metric test: when the parent
// cancels (deadline 10 minutes away) it surfaces context.Canceled.
type cancelWaitMetricHandler struct{}

func (cancelWaitMetricHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cancel-metric"}
}

func (cancelWaitMetricHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// --- helpers and cooperative handlers ---

// cooperativeQuickHandler returns immediately without touching ctx, exercising
// the bounded-path duration recording on a successful non-timeout invocation.
type cooperativeQuickHandler struct{}

func (cooperativeQuickHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cooperative-quick"}
}

func (cooperativeQuickHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"done": true}}, nil
}

// gaugeValue parses the Prometheus text exposition for a gauge line of the form
// `name{node_type="..."} <value>`. Returns -1 if the line is absent.
func gaugeValue(t *testing.T, m *metrics.Metrics, name, nodeType string) float64 {
	t.Helper()
	body := metricsBody(t, m)
	needle := name + `{node_type="` + nodeType + `"}`
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, needle) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("malformed gauge line %q", line)
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("gauge value %q: %v", fields[len(fields)-1], err)
		}
		return v
	}
	return -1
}
