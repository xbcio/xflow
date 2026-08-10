package protocol

import (
	"encoding/binary"
	"time"
)

// --- Path and header constants ---
const (
	// ReportMetricsPath is the runner metrics proxy endpoint. A runner deployed
	// in another network domain cannot be scraped by Prometheus, so it pushes
	// its whole registry here and the server merges it into its own /metrics.
	ReportMetricsPath = "/v1/runners/metrics"

	// RunnerIDHeader and SessionIDHeader carry the reporter's identity. They are
	// headers rather than body fields because the body is a raw protobuf stream
	// with nowhere to put them. The server authenticates the identity in
	// RunnerIDHeader; it never trusts a runner_id found inside the payload.
	RunnerIDHeader  = "X-Xflow-Runner-Id"
	SessionIDHeader = "X-Xflow-Session-Id"

	// MaxRunnerMetricsBytes caps a single metrics report body. Measured payloads
	// are ~11 KB at 200 rules, so 1 MiB is a hundredfold margin; the cap exists
	// to bound a malicious or runaway reporter, not to constrain normal use.
	// Over-limit requests are rejected with 413 and never decoded.
	MaxRunnerMetricsBytes = 1 << 20

	// ReportMetricsContentType is the delimited-protobuf MetricFamily stream
	// format produced by expfmt.NewFormat(expfmt.TypeProtoDelim).
	ReportMetricsContentType = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"
)

// MetricsPayloadStamp prefixes a stored metrics payload with the server-side
// receive time as 8 big-endian bytes of Unix nanoseconds.
//
// The stamp exists because xflow_runner_metrics_last_report_age_seconds needs
// the report time and nothing else in the system carries it: LastHeartbeat is
// the heartbeat clock (5s) not the report clock (15s, and skipped rounds are
// expected), and Redis cannot return a key's remaining TTL in the same MGET
// round trip that fetches its value. Prefixing a fixed-width header is not
// "decoding the payload" — the protobuf bytes are still passed through
// untouched, which is what the write path's zero-decode rule protects.
func MetricsPayloadStamp(at time.Time, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(out[:8], uint64(at.UnixNano()))
	copy(out[8:], payload)
	return out
}

// MetricsPayloadUnstamp splits a stored payload back into receive time and the
// original body. ok is false for anything too short to hold a stamp plus at
// least one payload byte, so a truncated or foreign value is skipped rather
// than mis-decoded.
func MetricsPayloadUnstamp(stored []byte) (time.Time, []byte, bool) {
	if len(stored) <= 8 {
		return time.Time{}, nil, false
	}
	nanos := int64(binary.BigEndian.Uint64(stored[:8]))
	return time.Unix(0, nanos), stored[8:], true
}
