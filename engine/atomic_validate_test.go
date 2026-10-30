package engine

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestCommitNodeRequestValidateArrivalsAgainstAdvanceTask pins the mutual
// exclusion between the two acyclic scheduling carriers. They consume the same
// destination in-degree, so a request carrying both would count every arrival
// twice: once folded into this commit and once again when the advance task
// runs. Validate is the only place that can reject the combination while it is
// still a caller error; otherwise the backend applies both halves and the
// second decrement can drive the in-degree to zero early.
func TestCommitNodeRequestValidateArrivalsAgainstAdvanceTask(t *testing.T) {
	arrival := DownstreamArrival{
		NodeName:     "target",
		NodeIdx:      1,
		UnitIdx:      0,
		ArrivalCount: 1,
		ActiveCount:  1,
	}
	advance := &Task{
		ExecutionID: "validate-arrivals",
		NodeName:    "target",
		Type:        TaskTypeNodeAdvance,
	}
	base := CommitNodeRequest{
		ExecutionID: "validate-arrivals",
		NodeName:    "source",
		Status:      types.NodeStatusSuccess,
	}

	t.Run("both carriers are rejected", func(t *testing.T) {
		req := base
		req.Arrivals = []DownstreamArrival{arrival}
		req.AdvanceTask = advance
		err := req.Validate()
		if err == nil {
			t.Fatal("Validate() = nil, want an error: Arrivals and AdvanceTask both consume " +
				"the same downstream in-degree")
		}
		if !strings.Contains(err.Error(), "alternative acyclic scheduling paths") {
			t.Fatalf("Validate() error = %v, want the Arrivals/AdvanceTask conflict", err)
		}
	})

	t.Run("either carrier alone is accepted", func(t *testing.T) {
		onlyArrivals := base
		onlyArrivals.Arrivals = []DownstreamArrival{arrival}
		if err := onlyArrivals.Validate(); err != nil {
			t.Fatalf("Validate(Arrivals only) error = %v, want nil", err)
		}

		onlyAdvance := base
		onlyAdvance.AdvanceTask = advance
		if err := onlyAdvance.Validate(); err != nil {
			t.Fatalf("Validate(AdvanceTask only) error = %v, want nil", err)
		}

		if err := base.Validate(); err != nil {
			t.Fatalf("Validate(neither) error = %v, want nil", err)
		}
	})
}

// TestCommitNodeRequestValidateMergeCancel pins where the merge-cancel intent
// may ride. It is a companion to Arrivals (it consumes no downstream in-degree)
// but an alternative to AdvanceTask (both fill the commit's single advance
// intent slot), and it only exists on a successful, non-fatal, non-system
// acyclic commit: that is the one commit a wait_any merge makes when it wins.
func TestCommitNodeRequestValidateMergeCancel(t *testing.T) {
	cancel := &Task{
		ExecutionID: "validate-merge-cancel",
		NodeName:    "merge",
		Type:        TaskTypeMergeCancel,
	}
	arrival := DownstreamArrival{NodeName: "target", NodeIdx: 1, ArrivalCount: 1, ActiveCount: 1}
	base := CommitNodeRequest{
		ExecutionID: "validate-merge-cancel",
		NodeName:    "merge",
		Status:      types.NodeStatusSuccess,
		MergeCancel: cancel,
	}

	t.Run("a successful acyclic commit accepts it", func(t *testing.T) {
		if err := base.Validate(); err != nil {
			t.Fatalf("Validate(MergeCancel only) error = %v, want nil", err)
		}
	})

	t.Run("it coexists with Arrivals", func(t *testing.T) {
		req := base
		req.Arrivals = []DownstreamArrival{arrival}
		if err := req.Validate(); err != nil {
			t.Fatalf("Validate(MergeCancel + Arrivals) error = %v, want nil: the intent "+
				"consumes no downstream in-degree", err)
		}
	})

	t.Run("it is exclusive with AdvanceTask", func(t *testing.T) {
		req := base
		req.AdvanceTask = &Task{ExecutionID: "validate-merge-cancel", NodeName: "merge", Type: TaskTypeNodeAdvance}
		err := req.Validate()
		if err == nil {
			t.Fatal("Validate(MergeCancel + AdvanceTask) = nil, want an error: both fill the " +
				"single advance intent slot")
		}
		if !strings.Contains(err.Error(), "MergeCancel and AdvanceTask") {
			t.Fatalf("Validate() error = %v, want the MergeCancel/AdvanceTask conflict", err)
		}
	})

	rejected := []struct {
		name   string
		mutate func(*CommitNodeRequest)
	}{
		{"cyclic graph", func(r *CommitNodeRequest) { r.AllowCycles = true }},
		{"fatal commit", func(r *CommitNodeRequest) { r.Fatal = true }},
		{"system commit", func(r *CommitNodeRequest) { r.System = true }},
		{"failed status", func(r *CommitNodeRequest) { r.Status = types.NodeStatusFailed }},
		{"skipped status", func(r *CommitNodeRequest) { r.Status = types.NodeStatusSkipped }},
		{"canceled status", func(r *CommitNodeRequest) { r.Status = types.NodeStatusCanceled }},
	}
	for _, tt := range rejected {
		t.Run("rejected on "+tt.name, func(t *testing.T) {
			req := base
			tt.mutate(&req)
			err := req.Validate()
			if err == nil {
				t.Fatalf("Validate(MergeCancel on %s) = nil, want an error", tt.name)
			}
			if !strings.Contains(err.Error(), "MergeCancel") {
				t.Fatalf("Validate() error = %v, want it to name MergeCancel", err)
			}
		})
	}
}
