package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestAnUnmappedRunnerErrorBecomesA500ThatLeaksNothing drives writeRunnerError's
// default: branch (server.go:384-396), which nothing in this package has ever
// reached.
//
// Getting there is not a matter of picking the right test name — it needs an
// error that is none of the eight sentinels the switch names. Every existing
// test that sets an engine error sets a sentinel: fakeControlEngine.commitErr is
// only ever engine.ErrInvalidLeaseToken (server_test.go:262, :322, :387, :435,
// grpc_server_test.go:159), which HandleReportResult special-cases at
// server.go:238 before writeRunnerError is called at all, and buildErr is only
// ever engine.ErrLeaseAlreadyActive (server_test.go:712), which pollTask handles
// in its own switch at core.go:502. Grepping the package for the one thing that
// would prove otherwise — http.StatusInternalServerError or ErrInternalServer in
// any _test.go — returns nothing.
//
// The branch is worth a test rather than a shrug for the reason its own comment
// gives: the response cannot carry the cause, so the log line is the only record
// that will ever exist, and a runner's pollLoop treats a poll error as fatal. An
// unmapped error therefore idles a runner permanently.
//
// Two things are pinned, and the second is the one that matters. The status has
// to be 500, because a 4xx would tell the runner its own request was at fault.
// And the body must carry the generic text only: org rule §7 forbids returning
// internals to the caller, and this is precisely the branch where an arbitrary
// unrecognised error — which may have a DSN, a path or a query in it — is in
// hand. The body assertion is a substring check against the raw error text
// rather than an equality check against the generic message, so wrapping the
// cause into the response at any layer fails it.
//
// Measured, so the claim stays bounded to what was shown. Two mutations of
// server.go's default arm, each run against all ten packages that depend on
// service/control, and each also run against the tree with this file removed:
//
//   - ErrInternalServer.Error() -> err.Error(): the leak assertion goes red;
//     without this file the whole scope stays green.
//   - StatusInternalServerError -> StatusBadRequest: the status assertion goes
//     red; without this file the whole scope stays green.
//
// So both halves were uncovered, not just relocated from somewhere else.
func TestAnUnmappedRunnerErrorBecomesA500ThatLeaksNothing(t *testing.T) {
	// Shaped like something that would really be embarrassing in a response
	// body, so the leak assertion below is testing what it claims to test.
	const secretish = "dial tcp 10.0.0.7:3306: user=xflow password=hunter2"
	fake := &fakeControlEngine{buildErr: errors.New(secretish)}
	dir := NewMemoryRunnerDirectory()
	srv := httptest.NewServer(NewServer(fake, dir).Handler())
	defer srv.Close()

	var reg protocol.RegisterRunnerResponse
	status, body := postForStatus(t, srv.URL+protocol.RegisterRunnerPath, protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if status != http.StatusOK {
		t.Fatalf("register status = %d body = %s, want 200", status, body)
	}
	if err := json.Unmarshal(body, &reg); err != nil {
		t.Fatalf("decode register response: %v", err)
	}

	task := engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"}
	enqueued, err := dir.EnqueueAssignment(context.Background(), Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function", NodeVersion: 1},
	})
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatal("EnqueueAssignment() enqueued = false, want true")
	}

	status, body = postForStatus(t, srv.URL+protocol.PollTaskPath, protocol.PollTaskRequest{
		RunnerID:     "runner-1",
		SessionID:    reg.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})

	if status != http.StatusInternalServerError {
		t.Fatalf("poll status for an unmapped engine error = %d, want %d: a 4xx "+
			"would tell the runner its own request was malformed, and a 2xx "+
			"would hide a control-plane fault entirely; body = %s",
			status, http.StatusInternalServerError, body)
	}
	if strings.Contains(string(body), secretish) {
		t.Fatalf("response body echoed the raw engine error: %s\n"+
			"this branch is exactly where an arbitrary unrecognised error is in "+
			"hand, so it is the one place a DSN or a path is most likely to "+
			"reach a caller; the cause belongs in the server's own log only",
			body)
	}
	// Both halves of the leak check: the generic text must actually be there,
	// so an empty body cannot pass the substring assertion above by default.
	var decoded errorResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode error response %s: %v", body, err)
	}
	if decoded.Error != ErrInternalServer.Error() {
		t.Fatalf("error message = %q, want %q", decoded.Error, ErrInternalServer.Error())
	}
}

func postForStatus(t *testing.T, url string, payload any) (int, []byte) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, body
}
