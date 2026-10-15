package metrics

import (
	"strings"
	"testing"
)

// The queue and assignment-queue families are per deployment, not per namespace:
// a process owning one non-default namespace reads suffixed broker queues and a
// prefixed runner directory, so these series describe that deployment only and
// carry no namespace label. The help text is where an operator learns that, so
// it must keep saying which names to expect and that fleet-wide totals are a
// query-time sum.
// Mutation: dropping the sentence from any of the four entries turns this red.
func TestQueueFamilyHelpDescribesPerDeploymentScope(t *testing.T) {
	tests := []struct {
		family string
		want   []string
	}{
		{"xflow_queue_depth", []string{"xflow:default:ns:<namespace>", "xflow:batch:ns:<namespace>", "no namespace label"}},
		{"xflow_queue_oldest_pending_age_seconds", []string{"suffixed per deployment", "no namespace label"}},
		{"xflow_assignment_queue_depth", []string{"xflow:runner-directory:ns:<namespace>:{control}", "no namespace label"}},
		{"xflow_assignment_queue_oldest_age_seconds", []string{"xflow:runner-directory:ns:<namespace>:{control}", "no namespace label"}},
	}
	for _, tc := range tests {
		t.Run(tc.family, func(t *testing.T) {
			help, ok := metricHelp[tc.family]
			if !ok {
				t.Fatalf("no metricHelp entry for %s", tc.family)
			}
			for _, sub := range tc.want {
				if !strings.Contains(help, sub) {
					t.Errorf("%s help is missing %q", tc.family, sub)
				}
			}
		})
	}
}
