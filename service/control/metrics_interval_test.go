package control

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// The clamp lives on the server so a runner can adopt whatever arrives without
// re-validating. A misconfigured 1s would otherwise have every runner in the
// fleet hammering the endpoint five times per report window.
func TestHeartbeatClampsMetricsReportInterval(t *testing.T) {
	cases := []struct {
		name string
		cfg  time.Duration
		want int
	}{
		{"unset means no opinion", 0, 0},
		{"below the floor is raised", time.Second, 5},
		{"in range is passed through", 30 * time.Second, 30},
		{"above the ceiling is capped", 10 * time.Minute, 300},
		{"negative suspends", -time.Second, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestMetricsIntervalServer(t, tc.cfg)
			resp := heartbeatOnce(t, srv)
			if resp.MetricsReportIntervalSeconds != tc.want {
				t.Fatalf("MetricsReportIntervalSeconds = %d, want %d",
					resp.MetricsReportIntervalSeconds, tc.want)
			}
		})
	}
}

// omitempty must keep the field off the wire when the server has no opinion, so
// an old runner's JSON decode is byte-identical to before this feature.
func TestHeartbeatOmitsIntervalWhenUnset(t *testing.T) {
	srv := newTestMetricsIntervalServer(t, 0)
	resp := heartbeatOnce(t, srv)
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("metrics_report_interval_seconds")) {
		t.Fatalf("unset interval still serialized: %s", data)
	}
}

func newTestMetricsIntervalServer(t *testing.T, interval time.Duration) *Server {
	t.Helper()
	dir := NewMemoryRunnerDirectory()
	srv := NewServer(&fakeControlEngine{}, dir)
	srv.core.metricsReportInterval = interval
	return srv
}

func heartbeatOnce(t *testing.T, srv *Server) protocol.HeartbeatResponse {
	t.Helper()
	ctx := context.Background()
	reg, err := srv.core.register(ctx, protocol.RegisterRunnerRequest{
		RunnerID: "runner-a", Concurrency: 1,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp, err := srv.core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: "runner-a", SessionID: reg.SessionID, Capacity: 1,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return resp
}
