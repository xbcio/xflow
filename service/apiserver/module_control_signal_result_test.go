package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// signalResultFacade adds the optional DeliverSignalResult capability to the
// shared fake. It is a wrapper rather than a method on fakeControlFacade so the
// existing signal tests keep exercising the facade-without-capability path.
type signalResultFacade struct {
	*fakeControlFacade
	delivery engine.SignalDelivery
	err      error
	// called records that the capability, not the plain DeliverSignal, ran.
	called bool
}

func (f *signalResultFacade) DeliverSignalResult(_ context.Context, _ types.ExecutionID, name string, data map[string]any) (engine.SignalDelivery, error) {
	f.called = true
	f.signalName = name
	f.signalData = data
	return f.delivery, f.err
}

// newSignalResultMux serves the control routes over a facade that carries the
// DeliverSignalResult capability.
func newSignalResultMux(f *signalResultFacade) http.Handler {
	m := &workflowControlModule{eng: f}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux
}

func postSignal(t *testing.T, mux http.Handler) *http.Response {
	t.Helper()
	return doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/signals", signalRequest{
		Name: "approve",
		Data: map[string]any{"ok": true},
	})
}

// rawSignalData returns the envelope's data object exactly as sent, so a test
// can assert on the keys that are present and not only on decoded values.
func rawSignalData(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var env struct {
		Success bool           `json:"success"`
		Code    string         `json:"code"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode envelope: %v (body=%s)", err, body)
	}
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success/code=200 (body=%s)", env, body)
	}
	return env.Data
}

func TestWorkflowControlSignalReportsDeliveryOutcome(t *testing.T) {
	tests := []struct {
		name         string
		outcome      engine.SignalOutcome
		wantOutcome  string
		wantResumed  bool
		wantBuffered bool
	}{
		{"resumed a waiting node", engine.SignalOutcomeResumed, "resumed", true, false},
		{"stored without waking anything", engine.SignalOutcomeBuffered, "buffered", false, true},
		{"execution already finished", engine.SignalOutcomeInactive, "inactive", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &signalResultFacade{
				fakeControlFacade: &fakeControlFacade{},
				delivery:          engine.SignalDelivery{Outcome: tt.outcome},
			}
			resp := postSignal(t, newSignalResultMux(f))
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("signal status = %d, want 200", resp.StatusCode)
			}
			data := rawSignalData(t, resp)

			// accepted keeps its meaning in every case: the request was taken.
			if data["accepted"] != true {
				t.Errorf("accepted = %v, want true for outcome %q", data["accepted"], tt.outcome)
			}
			if data["outcome"] != tt.wantOutcome {
				t.Errorf("outcome = %v, want %q", data["outcome"], tt.wantOutcome)
			}
			if data["resumed"] != tt.wantResumed {
				t.Errorf("resumed = %v, want %v for outcome %q", data["resumed"], tt.wantResumed, tt.outcome)
			}
			if data["buffered"] != tt.wantBuffered {
				t.Errorf("buffered = %v, want %v for outcome %q", data["buffered"], tt.wantBuffered, tt.outcome)
			}
			if !f.called {
				t.Error("DeliverSignalResult was not called")
			}
			if f.signalName != "approve" || f.signalData["ok"] != true {
				t.Errorf("signal = %q %v, want approve ok=true", f.signalName, f.signalData)
			}
		})
	}
}

// TestWorkflowControlSignalWithoutResultCapabilityKeepsAcceptedOnlyBody pins
// the fallback: a facade that cannot report an outcome still delivers the
// signal and answers with exactly the body older servers sent.
func TestWorkflowControlSignalWithoutResultCapabilityKeepsAcceptedOnlyBody(t *testing.T) {
	f := &fakeControlFacade{}
	resp := postSignal(t, newControlMux(f))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signal status = %d, want 200", resp.StatusCode)
	}
	data := rawSignalData(t, resp)
	if len(data) != 1 || data["accepted"] != true {
		t.Fatalf("data = %v, want exactly {accepted:true}", data)
	}
	if f.signalName != "approve" {
		t.Fatalf("signalName = %q, want approve", f.signalName)
	}
}

func TestWorkflowControlSignalResultErrorMapsLikeDeliverSignal(t *testing.T) {
	f := &signalResultFacade{
		fakeControlFacade: &fakeControlFacade{},
		err:               engine.ErrExecutionNotFound,
	}
	resp := postSignal(t, newSignalResultMux(f))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("signal status = %d, want 404 for a missing execution", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Success || env.Code != "execution_not_found" {
		t.Fatalf("envelope = %+v, want failure/code=execution_not_found", env)
	}

	// An unclassified error collapses to 500 internal_error, as before.
	f.err = errors.New("redis: connection refused")
	resp2 := postSignal(t, newSignalResultMux(f))
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusInternalServerError {
		t.Fatalf("signal status = %d, want 500 for an unclassified error", resp2.StatusCode)
	}
}

// TestSignalRouteReportsOutcomeOnTheRealEngine drives the route over the real
// engine and the distributed backend (miniredis), so the two outcomes that
// need no suspended node are checked end to end: a signal to a live execution
// that nothing waits on is buffered, and the same signal after the execution
// was canceled is inactive. The resumed outcome needs a parked node and is
// covered against the engine in the engine and local-backend tests.
func TestSignalRouteReportsOutcomeOnTheRealEngine(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID) + "/signals"

	data := rawSignalData(t, f.do("tok-full", http.MethodPost, path, signalRequest{Name: "approval"}))
	if data["accepted"] != true || data["outcome"] != "buffered" || data["buffered"] != true || data["resumed"] != false {
		t.Fatalf("signal to a live execution with no waiter: data = %v, want accepted+buffered, not resumed", data)
	}

	resp := f.do("tok-full", http.MethodPost, "/v1/executions/"+string(f.execID)+"/cancel", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}

	data = rawSignalData(t, f.do("tok-full", http.MethodPost, path, signalRequest{Name: "approval"}))
	if data["accepted"] != true || data["outcome"] != "inactive" || data["buffered"] != false || data["resumed"] != false {
		t.Fatalf("signal to a canceled execution: data = %v, want accepted+inactive, neither buffered nor resumed", data)
	}
}
