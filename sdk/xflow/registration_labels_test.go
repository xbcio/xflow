package xflow

import (
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

// The version label is the only channel through which a control plane can tell
// two one-release-apart runners apart: a registration's declared capabilities
// name node TYPES, and a node type's parameter set is not part of them, so a
// runner whose xflow predates mode=raw advertises exactly what a current one
// does. Absence of this label is therefore the signal, not a missing nicety —
// which is why it must survive an empty caller label set.
func TestRunnerRegistrationLabelsStampsTheVersionOntoAnEmptyLabelSet(t *testing.T) {
	got := runnerRegistrationLabels(nil, "v0.0.31")
	if got[protocol.RunnerXflowVersionLabel] != "v0.0.31" {
		t.Fatalf("Labels[%s] = %q, want %q: a runner that declares no labels of its own "+
			"still links an xflow version, and the absent label is what a control plane "+
			"reads as a mismatched runner",
			protocol.RunnerXflowVersionLabel, got[protocol.RunnerXflowVersionLabel], "v0.0.31")
	}
}

func TestRunnerRegistrationLabelsKeepsTheCallersLabels(t *testing.T) {
	got := runnerRegistrationLabels(map[string]string{"workload": "sas-runner", "env": "test"}, "v0.0.31")
	for key, want := range map[string]string{
		"workload":                       "sas-runner",
		"env":                            "test",
		protocol.RunnerXflowVersionLabel: "v0.0.31",
	} {
		if got[key] != want {
			t.Fatalf("Labels[%q] = %q, want %q (all: %v)", key, got[key], want, got)
		}
	}
}

// A caller-supplied value for the reserved key must lose. Deferring to it would
// let a runner claim a version it does not link, which is worse than the
// mismatch the label exists to expose: the control plane would conclude the two
// sides agree and hand over requests the runner cannot honour.
func TestRunnerRegistrationLabelsOverwritesACallerSuppliedVersion(t *testing.T) {
	got := runnerRegistrationLabels(
		map[string]string{protocol.RunnerXflowVersionLabel: "v0.0.30"},
		"v0.0.31",
	)
	if got[protocol.RunnerXflowVersionLabel] != "v0.0.31" {
		t.Fatalf("Labels[%s] = %q, want the linked version %q",
			protocol.RunnerXflowVersionLabel, got[protocol.RunnerXflowVersionLabel], "v0.0.31")
	}
}

// An unknown version stamps nothing. A placeholder would be compared as a
// version by a control plane that has no way to know it means "unknown", so two
// runners of which one is unverifiable would read as matching.
func TestRunnerRegistrationLabelsOmitsAnUnknownVersion(t *testing.T) {
	for name, labels := range map[string]map[string]string{
		"no caller labels": nil,
		"caller labels":    {"env": "test"},
	} {
		t.Run(name, func(t *testing.T) {
			got := runnerRegistrationLabels(labels, "")
			if _, present := got[protocol.RunnerXflowVersionLabel]; present {
				t.Fatalf("Labels[%s] is present (%q) for an unknown version; want the key absent so "+
					"not-reported stays distinguishable from equal",
					protocol.RunnerXflowVersionLabel, got[protocol.RunnerXflowVersionLabel])
			}
		})
	}
}

func TestRunnerRegistrationLabelsDoesNotMutateTheCaller(t *testing.T) {
	labels := map[string]string{"env": "test"}
	runnerRegistrationLabels(labels, "v0.0.31")
	if len(labels) != 1 || labels["env"] != "test" {
		t.Fatalf("the caller's label map was mutated: %v", labels)
	}
	if _, present := labels[protocol.RunnerXflowVersionLabel]; present {
		t.Fatalf("the version label was written into the caller's map: %v", labels)
	}
}
