package protocol

import (
	"testing"
	"time"
)

func TestReportMetricsWireConstants(t *testing.T) {
	if ReportMetricsPath != "/v1/runners/metrics" {
		t.Fatalf("ReportMetricsPath = %q, want /v1/runners/metrics", ReportMetricsPath)
	}
	if RunnerIDHeader != "X-Xflow-Runner-Id" {
		t.Fatalf("RunnerIDHeader = %q", RunnerIDHeader)
	}
	if SessionIDHeader != "X-Xflow-Session-Id" {
		t.Fatalf("SessionIDHeader = %q", SessionIDHeader)
	}
	if MaxRunnerMetricsBytes != 1<<20 {
		t.Fatalf("MaxRunnerMetricsBytes = %d, want %d", MaxRunnerMetricsBytes, 1<<20)
	}
	want := "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"
	if ReportMetricsContentType != want {
		t.Fatalf("ReportMetricsContentType = %q, want %q", ReportMetricsContentType, want)
	}
}

func TestMetricsPayloadStampRoundTrip(t *testing.T) {
	at := time.Unix(1754000000, 123456789)
	stamped := MetricsPayloadStamp(at, []byte("gzipbytes"))
	if len(stamped) != 8+len("gzipbytes") {
		t.Fatalf("stamped len = %d, want %d", len(stamped), 8+len("gzipbytes"))
	}
	gotAt, body, ok := MetricsPayloadUnstamp(stamped)
	if !ok {
		t.Fatal("MetricsPayloadUnstamp: ok = false, want true")
	}
	if !gotAt.Equal(at) {
		t.Fatalf("timestamp = %v, want %v", gotAt, at)
	}
	if string(body) != "gzipbytes" {
		t.Fatalf("body = %q, want gzipbytes", body)
	}
}

func TestMetricsPayloadUnstampRejectsShortInput(t *testing.T) {
	for _, in := range [][]byte{nil, {}, make([]byte, 7), make([]byte, 8)} {
		if _, _, ok := MetricsPayloadUnstamp(in); ok {
			t.Fatalf("MetricsPayloadUnstamp(%d bytes): ok = true, want false", len(in))
		}
	}
}
