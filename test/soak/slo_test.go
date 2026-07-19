//go:build soak

package soak

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSLOReportEvaluatePass(t *testing.T) {
	r := NewSLOReport()
	for i := 0; i < 9995; i++ {
		r.RecordAPI(true)
	}
	for i := 0; i < 5; i++ {
		r.RecordAPI(false)
	}
	r.LeaderSwitchTime(30 * time.Second)
	r.RecoveryTime(20 * time.Second)

	eval := r.Evaluate()
	if !eval.Passed {
		t.Fatalf("expected PASS, got failures: %v", eval.Failures)
	}
	if len(eval.Failures) != 0 {
		t.Fatalf("expected no failures, got %v", eval.Failures)
	}
}

func TestSLOReportEvaluateFailures(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*SLOReport)
		wantSub string
	}{
		{
			name: "API availability below target",
			setup: func(r *SLOReport) {
				for i := 0; i < 998; i++ {
					r.RecordAPI(true)
				}
				for i := 0; i < 2; i++ {
					r.RecordAPI(false)
				}
			},
			wantSub: "API availability",
		},
		{
			name: "leader switch time too slow",
			setup: func(r *SLOReport) {
				r.LeaderSwitchTime(46 * time.Second)
			},
			wantSub: "leader switch time",
		},
		{
			name: "recovery time too slow",
			setup: func(r *SLOReport) {
				r.RecoveryTime(31 * time.Second)
			},
			wantSub: "recovery time",
		},
		{
			name: "error rate at boundary fails",
			setup: func(r *SLOReport) {
				for i := 0; i < 99; i++ {
					r.RecordAPI(true)
				}
				r.RecordAPI(false)
			},
			wantSub: "error rate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewSLOReport()
			tt.setup(r)
			eval := r.Evaluate()
			if eval.Passed {
				t.Fatalf("expected FAIL for %s", tt.name)
			}
			found := false
			for _, f := range eval.Failures {
				if strings.Contains(f, tt.wantSub) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected failure containing %q, got %v", tt.wantSub, eval.Failures)
			}
		})
	}
}

func TestSLOReportEvaluateBoundaries(t *testing.T) {
	r := NewSLOReport()
	// Exactly at target should pass (error rate strict <, the others ≤).
	for i := 0; i < 9990; i++ {
		r.RecordAPI(true)
	}
	for i := 0; i < 10; i++ {
		r.RecordAPI(false)
	}
	r.LeaderSwitchTime(45 * time.Second)
	r.RecoveryTime(30 * time.Second)

	eval := r.Evaluate()
	if !eval.Passed {
		t.Fatalf("expected boundary values to PASS, got %v", eval.Failures)
	}
}

func TestSLOReportAsRecorder(t *testing.T) {
	// Verify *SLOReport can be wired as Options.SLORecorder in a real cluster.
	rec := NewSLOReport()
	c, err := NewCluster(t, Options{ReplicaCount: 2, SLORecorder: rec})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Cluster.Start: %v", err)
	}

	// The cluster starts with a nil context in this test; the real lifetime is
	// bounded by t.Cleanup. We just need to prove the recorder compiles and is
	// accepted by the harness.
	if c.SLO() != rec {
		t.Fatalf("cluster SLO recorder was not the SLOReport instance")
	}
}

func TestSLOReportRender(t *testing.T) {
	r := NewSLOReport()
	r.RecordAPI(true)
	r.RecordAPI(false)
	r.LeaderSwitchTime(5 * time.Second)
	r.RecoveryTime(3 * time.Second)
	r.RecordFaultOutcome(FaultOutcome{
		Fault:              "leader kill",
		InjectedAt:         time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
		ExpectedInvariant:  "non-leader takes over within ≤ TTL",
		ActualRecoveryTime: 3 * time.Second,
		DuplicateDelivery:  0,
		CommitOutcome:      "once",
		Passed:             true,
		Notes:              "ENV-GATED: real crash-kill not exercised in-process",
	})

	out := r.Render()
	for _, want := range []string{
		"xflow HA Soak SLO Report",
		"leader kill",
		"SLO Verdict",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered report missing %q; got:\n%s", want, out)
		}
	}
}
