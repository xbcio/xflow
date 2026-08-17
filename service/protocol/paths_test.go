package protocol

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// stubRunnerHandler is a no-op RunnerHTTPHandler so the dead-constant guard can
// mount the real RegisterRunnerRoutes without depending on a control plane.
// Its methods never run in this test — the guard probes mux.Handler (path
// matching only), so the handler body is irrelevant.
type stubRunnerHandler struct{}

func (stubRunnerHandler) HandleRegisterRunner(http.ResponseWriter, *http.Request) {}
func (stubRunnerHandler) HandleHeartbeat(http.ResponseWriter, *http.Request)       {}
func (stubRunnerHandler) HandlePollTask(http.ResponseWriter, *http.Request)         {}
func (stubRunnerHandler) HandleReportResult(http.ResponseWriter, *http.Request)     {}
func (stubRunnerHandler) HandleRenewLease(http.ResponseWriter, *http.Request)      {}
func (stubRunnerHandler) HandleActivationAck(http.ResponseWriter, *http.Request)   {}
func (stubRunnerHandler) HandleReportMetrics(http.ResponseWriter, *http.Request)   {}

// runnerPathSamples maps every exported runner-protocol path constant to a
// concrete sample path (placeholders are not used in this protocol face — every
// route is an exact path). The table is hand-written and must cover every
// constant; step 2 of the guard asserts that lockstep so a constant added
// without a row here cannot silently escape coverage.
var runnerPathSamples = []struct {
	constName string
	pathConst string
	method    string
	sample    string
}{
	{"RegisterRunnerPath", RegisterRunnerPath, http.MethodPost, RegisterRunnerPath},
	{"HeartbeatPath", HeartbeatPath, http.MethodPost, HeartbeatPath},
	{"PollTaskPath", PollTaskPath, http.MethodPost, PollTaskPath},
	{"ReportResultPath", ReportResultPath, http.MethodPost, ReportResultPath},
	{"RenewLeasePath", RenewLeasePath, http.MethodPost, RenewLeasePath},
	{"ActivationAckPath", ActivationAckPath, http.MethodPost, ActivationAckPath},
	{"ReportMetricsPath", ReportMetricsPath, http.MethodPost, ReportMetricsPath},
}

// allRunnerPathConsts is the enumerable form of the runner-protocol path
// vocabulary. Go cannot reflect over constants, so this list is the source the
// guard reads to prove the sample table covers every constant. Adding a path
// constant without adding it here (and to runnerPathSamples) is the drift
// defect the guard exists to catch.
var allRunnerPathConsts = []string{
	RegisterRunnerPath,
	HeartbeatPath,
	PollTaskPath,
	ReportResultPath,
	RenewLeasePath,
	ActivationAckPath,
	ReportMetricsPath,
}

// TestRunnerPathsHaveMuxRegistration is the protocol-face twin of the apiserver
// dead-constant guard: every exported runner-protocol path constant must
// resolve to a route registered by RegisterRunnerRoutes. A constant with no
// registration is a dead contract — a client coded against it gets a 404 and
// nothing flagged the gap. The three dead constants this guard would have
// caught (ActivatePath/DeactivatePath/ActivationListPath) were already removed
// in T8, so this guard was born green. It exists to prevent the next dead
// constant, not to fix a present defect.
func TestRunnerPathsHaveMuxRegistration(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRunnerRoutes(mux, stubRunnerHandler{})

	// Step 2: the hand-written sample table must cover every constant in
	// allRunnerPathConsts, or adding a constant without a row would silently
	// escape the guard (field-by-field-copy-drops-new-fields shape).
	tablePaths := make(map[string]string, len(runnerPathSamples))
	for _, s := range runnerPathSamples {
		if prev, dup := tablePaths[s.pathConst]; dup {
			t.Fatalf("duplicate sample entry for %q (also %s)", s.pathConst, prev)
		}
		tablePaths[s.pathConst] = s.constName
	}
	var missing []string
	for _, p := range allRunnerPathConsts {
		if _, ok := tablePaths[p]; !ok {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("runnerPathSamples is missing path constants (add them or the guard silently skips them): %v", missing)
	}

	for _, s := range runnerPathSamples {
		req := httptest.NewRequest(s.method, s.sample, nil)
		_, pattern := mux.Handler(req)
		if pattern == "" {
			t.Errorf("%s = %q: %s %s matched no registered route (dead constant or wrong method/sample)",
				s.constName, s.pathConst, s.method, s.sample)
		}
	}
}
