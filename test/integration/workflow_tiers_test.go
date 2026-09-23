//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The three tier tests run the same definitions the coverage suite measures,
// through the production server+runner topology: an HTTP submit into the control
// plane, tasks drawn from Redis by a real runner over the gRPC protocol, and
// state written back to Redis. The embedded engine is not used here — a
// definition proven only against in-memory state says nothing about the
// serialization, lease, and signal paths the distributed topology adds.

// TestWorkflowTierLowDistributed runs the low tier on both arms. The two arms
// differ only in which branch node executes, which is the property the tier
// exists to exercise: a mutually exclusive branch released by a wait-any merge.
func TestWorkflowTierLowDistributed(t *testing.T) {
	addr := requireRedis(t)

	for _, tc := range []struct {
		name   string
		input  map[string]any
		ranNot string
	}{
		{name: "bulk_arm", input: workflows.AboveThresholdInput(), ranNot: "small"},
		{name: "single_arm", input: workflows.BelowThresholdInput(), ranNot: "big"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def, err := workflows.LowWorkflow().Definition()
			if err != nil {
				t.Fatalf("Definition: %v", err)
			}
			h := newServerRunnerHarness(t, addr, 1)
			startWorkflowRunner(t, h, "runner-low-"+tc.name, def, nil, nil)

			execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, tc.input)
			result := waitDistributedCompletion(t, h, execID)
			if result.Status != types.ExecutionStatusSuccess {
				t.Fatalf("status = %s, want success", result.Status)
			}

			// The arm the condition rejected must not have executed. Asserting
			// only the terminal status would pass even if both arms ran, which is
			// exactly the branch semantics this tier pins.
			skipped := nodeStatus(t, h, execID, tc.ranNot)
			if skipped != types.NodeStatusSkipped {
				t.Fatalf("node %q status = %s, want skipped", tc.ranNot, skipped)
			}
		})
	}
}

// TestWorkflowTierMediumDistributed runs the medium tier on both arms through a
// live HTTP endpoint.
//
// The enrich endpoint is a real httptest server rather than a stubbed transport:
// the HTTP node's response envelope (`status`, and the body it returns) is what
// the following rehydrate node reads, so stubbing the handler would remove the
// exact behaviour under test.
func TestWorkflowTierMediumDistributed(t *testing.T) {
	addr := requireRedis(t)

	enrich := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/enrich" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"enriched": true}`))
	}))
	defer enrich.Close()

	for _, tc := range []struct {
		name  string
		input map[string]any
	}{
		{name: "bulk_arm", input: workflows.AboveThresholdInput()},
		{name: "single_arm", input: workflows.BelowThresholdInput()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def, err := workflows.MediumWorkflow().Definition()
			if err != nil {
				t.Fatalf("Definition: %v", err)
			}
			workflows.WithVars(def, workflows.MediumVars(enrich.URL, "qa@example.test"))

			h := newServerRunnerHarness(t, addr, 1)
			startWorkflowRunner(t, h, "runner-medium-"+tc.name, def, nil, nil)

			execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, tc.input)
			result := waitDistributedCompletion(t, h, execID, "renamed", "script")
			if result.Status != types.ExecutionStatusSuccess {
				dumpNodes(t, h, execID, mediumNodeNames()...)
				t.Fatalf("status = %s, want success (error=%s)", result.Status, result.Error)
			}

			// The transform chain is only meaningful if it actually narrowed the
			// seeded collection: four distinct ids minus the zero-qty row leaves
			// three, which is what the limit and the counts downstream assume.
			count := numericField(t, result.Output["renamed"], "count")
			if count != 3 {
				t.Fatalf("renamed count = %v, want 3", count)
			}
			if ok, _ := result.Output["script"].(map[string]any)["script_ok"].(bool); !ok {
				t.Fatalf("script output = %#v, want script_ok true", result.Output["script"])
			}
		})
	}
}

// TestWorkflowTierHighDistributed runs the high tier against a real MySQL table,
// a live gRPC endpoint, and real signal delivery.
//
// Two environment facts shape it. The database node takes its connection from a
// runner-scoped credential resolver and a process-scoped resource pool, so this
// test installs both — the same wiring a production runner uses. The gRPC node
// answers only on its error path (see defs_high.go), so the endpoint returns
// NotFound, which the node classifies as permanent and routes to its error port.
func TestWorkflowTierHighDistributed(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	table := uniqueTableName(t, "qa_high_tier")
	createHighTierTable(t, dsn, table)
	t.Cleanup(func() { dropTable(t, dsn, table) })

	grpcHost := startNotFoundGRPCServer(t)

	pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Close(ctx)
	})
	// The resolver returns the DSN to the node. It is never logged: the value
	// carries the MySQL password, and the node reports only classified errors.
	resolver := func(_ namespace.Namespace, name string) map[string]any {
		if name != workflows.CredentialDB {
			return nil
		}
		return map[string]any{"driver": "mysql", "dsn": dsn}
	}

	rowID := "row-" + uniqueSuffix(t)
	def, err := workflows.HighWorkflow().Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	workflows.WithVars(def, workflows.HighVars(grpcHost, table, rowID))

	h := newServerRunnerHarness(t, addr, 1)
	startWorkflowRunner(t, h, "runner-high", def, pool, resolver)

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, map[string]any{})

	// The approval node parks the execution; the decision arrives over the same
	// HTTP route an operator would use.
	waitNodeSuspended(t, h, execID, "gate")
	postSignal(t, h, execID, workflows.HighApprovalSignal, map[string]any{
		"approver": workflows.HighApprover,
		"action":   "approve",
	})

	// The wait node parks a second time; releasing it exercises the signal-wait
	// resume path with a payload the node did not know at compile time.
	waitNodeSuspended(t, h, execID, "hold")
	postSignal(t, h, execID, workflows.HighReleaseSignal, map[string]any{"release": true})

	result := waitDistributedCompletionDumping(t, h, execID, highNodeNames(), "summarize", "approved")
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", result.Status)
	}

	if decision := stringField(t, result.Output["approved"], "decision"); decision != "approved" {
		t.Fatalf("approved decision = %q, want approved", decision)
	}

	// rows_loaded carries the select result through the node boundary: it is the
	// end-to-end proof that insert and select both reached MySQL.
	summarize := result.Output["summarize"]
	rows, ok := summarizeField(t, summarize, "rows_loaded").([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("summarize rows_loaded = %#v, want one row", summarizeField(t, summarize, "rows_loaded"))
	}

	// The gRPC node reached its error port, so the recovery node set
	// risk_scored=false and that value is what the summary carries.
	if scored, ok := summarizeField(t, summarize, "risk_ok").(bool); !ok || scored {
		t.Fatalf("summarize risk_ok = %#v, want false (gRPC must take the error port)", summarizeField(t, summarize, "risk_ok"))
	}
}

// --- helpers ---

// mediumNodeNames lists the medium tier's nodes in execution order. It exists so
// a failed run can name the node that stuck instead of reporting only the
// terminal status, which on this topology is all the result carries.
func mediumNodeNames() []string {
	return []string{
		"start", "seed", "tag", "enrich", "rehydrate", "normalize", "dedupe",
		"ordered", "capped", "rollup", "renamed", "script", "route",
		"fan", "dq", "single_lane", "join", "notify", "done",
	}
}

// highNodeNames lists the high tier's nodes for the same reason as
// mediumNodeNames.
func highNodeNames() []string {
	return []string{
		"start", "db_insert", "db_load", "risk", "risk_gap", "gate", "hold",
		"tick", "approved", "timed_out", "rejected", "join", "summarize", "done",
	}
}

// dumpNodes logs each node's status and error. Only for failure paths.
func dumpNodes(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, names ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if snap, err := h.state.GetExecution(ctx, id); err == nil && snap != nil {
		t.Logf("execution %s status=%s error=%s", id, snap.Status, snap.Error)
	} else {
		t.Logf("execution %s <no snapshot: %v>", id, err)
	}
	for _, name := range names {
		snap, err := h.state.GetNode(ctx, id, name)
		if err != nil || snap == nil {
			// A body member such as the map's "dq" only gets a snapshot once a
			// batch creates one, so an absent snapshot is itself informative.
			t.Logf("  node %-12s <no snapshot: %v>", name, err)
			continue
		}
		t.Logf("  node %-12s status=%-10s attempt=%d port=%-6s error=%s",
			name, snap.Status, snap.Attempt, snap.Port, snap.Error)
	}
}

// nodeStatus reads one node's status from the control plane's state store.
func nodeStatus(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, name string) types.NodeStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snap, err := h.state.GetNode(ctx, id, name)
	if err != nil {
		t.Fatalf("GetNode(%s): %v", name, err)
	}
	return snap.Status
}

// uniqueSuffix returns a short random token for names that must not collide
// across runs.
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// uniqueTableName returns a table name safe to splat into DDL.
func uniqueTableName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%s", prefix, uniqueSuffix(t))
}

// createHighTierTable creates the table the high tier inserts into. The database
// node never issues DDL, so the caller owns the schema — the same division a
// deployment has.
func createHighTierTable(t *testing.T, dsn, table string) {
	t.Helper()
	db := openTestMySQL(t, dsn)
	defer db.Close()
	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (`id` VARCHAR(64) NOT NULL PRIMARY KEY, `qty` INT NOT NULL)",
		table,
	)
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("create table %s: %v", table, err)
	}
}

func dropTable(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("drop table %s: open: %v", table, err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)); err != nil {
		t.Logf("drop table %s: %v", table, err)
	}
}

func openTestMySQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		// Never echo the DSN: it embeds the database password.
		t.Fatalf("open mysql: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}
	return db
}

// startNotFoundGRPCServer runs a gRPC server that answers every unknown method
// with NotFound, and returns its host:port.
//
// The unknown-service handler never unmarshals a request: the server has no
// descriptor for /qa.RiskService/Score, which is also why it can answer a client
// whose response handling is broken — the status arrives without a body to
// decode.
func startNotFoundGRPCServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, _ grpc.ServerStream) error {
		return status.Error(codes.NotFound, "qa: subject not registered")
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// numericField reads a numeric field out of a node output map, tolerating the
// float64 JSON round trip.
func numericField(t *testing.T, output any, field string) float64 {
	t.Helper()
	m, ok := output.(map[string]any)
	if !ok {
		t.Fatalf("output = %#v, want an object", output)
	}
	switch v := m[field].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		t.Fatalf("field %q = %#v, want a number", field, m[field])
		return 0
	}
}

func stringField(t *testing.T, output any, field string) string {
	t.Helper()
	m, ok := output.(map[string]any)
	if !ok {
		t.Fatalf("output = %#v, want an object", output)
	}
	s, _ := m[field].(string)
	return s
}

func summarizeField(t *testing.T, output any, field string) any {
	t.Helper()
	m, ok := output.(map[string]any)
	if !ok {
		t.Fatalf("summarize output = %#v, want an object", output)
	}
	return m[field]
}
