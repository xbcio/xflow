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
