package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// This file covers the queue_position column of a GET /v1/executions row
// (§6.8: a queued row shows 排队 · 第 n 位). The position is not stored with the
// row — it belongs to the live queue — so the list handler reads it back for
// the queued rows of a page. The properties pinned here are the ones a mistake
// would turn into a wrong number rather than a compile error:
//
//   - a queued row carries a 1-based position, read from the queue owner;
//   - a row that is not queued is never read and never carries the field
//     (absent, not 0 — 0 would read as a position a client could display);
//   - a failed read leaves the row in the page without a position and does not
//     change the page's status, and the failure is logged once per page, not
//     once per row;
//   - end to end, the number comes from the REAL queue: an execution parked by
//     queue mode reports the same place through the list that the detail route
//     reports.

// queuePositionFacade answers InspectStatus per execution id and records every
// read, so a test can assert both the value projected and that no other row was
// read at all. The rest of the EngineFacade surface comes from the shared fake.
type queuePositionFacade struct {
	*fakeControlFacade
	details map[types.ExecutionID]engine.ExecutionDetail
	failFor map[types.ExecutionID]error
	reads   []types.ExecutionID
}

func (f *queuePositionFacade) InspectStatus(_ context.Context, id types.ExecutionID) (engine.ExecutionDetail, error) {
	f.reads = append(f.reads, id)
	if err := f.failFor[id]; err != nil {
		return engine.ExecutionDetail{}, err
	}
	return f.details[id], nil
}

// countMentions reports how many logged lines contain needle. It lives here
// because the per-page throttle is the only assertion that needs a count;
// recordingLogger's own helper answers presence.
func (l *recordingLogger) countMentions(needle string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, needle) {
			n++
		}
	}
	return n
}

// execListRowsByID decodes the raw rows of a list body keyed by execution id,
// so a test can tell an absent field from a zero one per row.
func execListRowsByID(t *testing.T, body []byte) map[string]map[string]json.RawMessage {
	t.Helper()
	var raw struct {
		Data struct {
			List []map[string]json.RawMessage `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw list: %v (body=%s)", err, body)
	}
	rows := make(map[string]map[string]json.RawMessage, len(raw.Data.List))
	for _, row := range raw.Data.List {
		var id string
		if err := json.Unmarshal(row["execution_id"], &id); err != nil {
			t.Fatalf("decode execution_id: %v (row=%v)", err, row)
		}
		rows[id] = row
	}
	return rows
}

// queuePositionOf returns the row's queue_position and whether the field is
// present at all. Presence is the contract: an omitted field means unknown.
func queuePositionOf(t *testing.T, row map[string]json.RawMessage) (int, bool) {
	t.Helper()
	raw, ok := row["queue_position"]
	if !ok {
		return 0, false
	}
	var pos int
	if err := json.Unmarshal(raw, &pos); err != nil {
		t.Fatalf("decode queue_position: %v (raw=%s)", err, raw)
	}
	return pos, true
}

// TestListExecutionsReadsQueuePositionOnlyForQueuedRows pins the read policy:
// one status read per QUEUED row and none for any other row, the position
// omitted when the read fails, and one summary log line for the page instead of
// one per failure.
func TestListExecutionsReadsQueuePositionOnlyForQueuedRows(t *testing.T) {
	st := memstore.New()
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	seedExecution(t, st, string(namespace.Default), "wf-q", "exec-q-1", types.ExecutionStatusQueued, base.Add(3*time.Second))
	seedExecution(t, st, string(namespace.Default), "wf-q", "exec-q-2", types.ExecutionStatusQueued, base.Add(2*time.Second))
	seedExecution(t, st, string(namespace.Default), "wf-r", "exec-running", types.ExecutionStatusRunning, base.Add(time.Second))

	facade := &queuePositionFacade{
		fakeControlFacade: &fakeControlFacade{},
		details: map[types.ExecutionID]engine.ExecutionDetail{
			"exec-q-1": {ExecutionID: "exec-q-1", Status: types.ExecutionStatusQueued, QueuePosition: 2},
			// A row the listing called queued that has since started: it must
			// not report a position. The position is pinned non-zero here so
			// the assertion refuses a stale number rather than passing because
			// the engine happened to return 0 for a non-queued status.
			"exec-q-2": {ExecutionID: "exec-q-2", Status: types.ExecutionStatusRunning, QueuePosition: 5},
		},
		failFor: map[types.ExecutionID]error{},
	}
	log := &recordingLogger{}
	m := &workflowControlModule{executions: st, eng: facade, log: log}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	status, body := doRaw(t, mux, http.MethodGet, PathExecutions)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}

	if got, want := facade.reads, []types.ExecutionID{"exec-q-1", "exec-q-2"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("status reads = %v, want %v: only queued rows are read, in page order", got, want)
	}

	rows := execListRowsByID(t, body)
	if pos, ok := queuePositionOf(t, rows["exec-q-1"]); !ok || pos != 2 {
		t.Fatalf("exec-q-1 queue_position = (%d, present=%v), want (2, true)", pos, ok)
	}
	if pos, ok := queuePositionOf(t, rows["exec-q-2"]); ok {
		t.Fatalf("exec-q-2 queue_position = %d, want the field absent: the execution is no longer queued", pos)
	}
	if pos, ok := queuePositionOf(t, rows["exec-running"]); ok {
		t.Fatalf("exec-running queue_position = %d, want the field absent: a running execution is not waiting", pos)
	}

	// The failure exit is next: BOTH queued rows fail to read. Each row must
	// keep its place in the page with the field omitted, and the two failures
	// must produce ONE log line — the per-page summary, not one per row.
	facade.failFor["exec-q-1"] = errors.New("queue owner unavailable")
	facade.failFor["exec-q-2"] = errors.New("queue owner unavailable")
	status, body = doRaw(t, mux, http.MethodGet, PathExecutions)
	if status != http.StatusOK {
		t.Fatalf("status with a failed queue read = %d, want 200 (body=%s)", status, body)
	}
	rows = execListRowsByID(t, body)
	if len(rows) != 3 {
		t.Fatalf("rows with a failed queue read = %d, want 3: a missing position is not a missing row (body=%s)", len(rows), body)
	}
	for _, id := range []string{"exec-q-1", "exec-q-2"} {
		if pos, ok := queuePositionOf(t, rows[id]); ok {
			t.Fatalf("%s queue_position = %d, want the field absent after a failed read", id, pos)
		}
	}
	if got := log.countMentions("list_executions_queue_position_unavailable"); got != 1 {
		t.Fatalf("queue-read failure log lines = %d, want 1 for the page: %v", got, log.lines)
	}
}

// TestExecutionListCarriesQueuePositionEndToEnd is the visibility half §6.8
// needs from the server: a parked submission's place in line reaches the list
// from the real queue, not from a fixture. The in-memory engine keeps queue
// state in its own state store and the list pages over the durable execution
// projection, so the two rows are seeded from the executions the engine itself
// created — the ids, statuses and position are all real.
func TestExecutionListCarriesQueuePositionEndToEnd(t *testing.T) {
	st := memstore.New()
	srv, err := New(Config{Concurrency: 1, Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	wf := queueModeWorkflow()
	holder := submitWorkflowE2E(t, httpSrv.URL, wf, nil)
	holderDetail := inspectExecutionE2E(t, httpSrv.URL, holder)
	waiting, _ := submitWorkflowRawE2E(t, httpSrv.URL, wf)
	if waiting.Status != types.ExecutionStatusQueued {
		t.Fatalf("parked status = %q, want %q", waiting.Status, types.ExecutionStatusQueued)
	}

	now := time.Now().UTC()
	for _, rec := range []*store.ExecutionRecord{
		{
			ExecutionID: holder, Namespace: string(namespace.Default),
			WorkflowName: wf.Name, Status: holderDetail.Status, CreatedAt: now, UpdatedAt: now,
		},
		{
			ExecutionID: waiting.ExecutionID, Namespace: string(namespace.Default),
			WorkflowName: wf.Name, Status: waiting.Status, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
		},
	} {
		if err := st.CreateExecution(context.Background(), rec); err != nil {
			t.Fatalf("seed list row %s: %v", rec.ExecutionID, err)
		}
	}

	status, body := getExecutionList(t, httpSrv.URL, "", "")
	if status != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body=%s)", status, body)
	}
	rows := execListRowsByID(t, body)
	position, ok := queuePositionOf(t, rows[string(waiting.ExecutionID)])
	if !ok || position < 1 {
		t.Fatalf("queued row queue_position = (%d, present=%v), want present and >= 1 (body=%s)",
			position, ok, body)
	}
	// The list must report the SAME place the detail route reports: both read
	// the one queue owner, so a different number would mean one of them is
	// answering from a stale copy.
	if detail := inspectExecutionE2E(t, httpSrv.URL, waiting.ExecutionID); detail.QueuePosition != position {
		t.Fatalf("list queue_position = %d, detail queue_position = %d, want the same live position",
			position, detail.QueuePosition)
	}
	if pos, ok := queuePositionOf(t, rows[string(holder)]); ok {
		t.Fatalf("running row queue_position = %d, want the field absent (body=%s)", pos, body)
	}
	if holderDetail.Status != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want %q: the fixture needs a non-queued row",
			holderDetail.Status, types.ExecutionStatusRunning)
	}
}
