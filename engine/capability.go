package engine

import (
	"sort"

	"github.com/xbcio/xflow/engine/graph"
)

const FeatureGroupExecV1 = "group.exec.v1"

// CapabilityRequirement describes what a task (node or group) requires from a
// runner in order to be executed. For group tasks, the requirements are the
// union of all member node requirements plus the group execution feature.
type CapabilityRequirement struct {
	NodeType    string   `json:"node_type"`
	NodeVersion int      `json:"node_version,omitempty"`
	Runtime     string   `json:"runtime,omitempty"`
	Resource    string   `json:"resource,omitempty"`
	Credentials []string `json:"credentials,omitempty"`
	Feature     string   `json:"feature,omitempty"`
}

// GroupRouteIdentity is the stable routing identity for a group task. It does
// NOT include attempt or lease generation — those belong to the lease layer,
// not the route identity (see F4 in the execution plan).
type GroupRouteIdentity struct {
	GroupID      string `json:"group_id"`
	UnitIdx      int    `json:"unit_idx"`
	PackageHash  string `json:"package_hash"`
	ActivationID int    `json:"activation_id"`
}

// NormalizeRequirements deduplicates and deterministically sorts a slice of
// CapabilityRequirements so that equivalent requirement sets always produce the
// same output regardless of input order.
func NormalizeRequirements(reqs []CapabilityRequirement) []CapabilityRequirement {
	if len(reqs) == 0 {
		return nil
	}

	type key struct {
		nodeType    string
		nodeVersion int
		runtime     string
		resource    string
		feature     string
	}

	seen := map[key]int{}
	deduped := make([]CapabilityRequirement, 0, len(reqs))

	for _, r := range reqs {
		k := key{
			nodeType:    r.NodeType,
			nodeVersion: r.NodeVersion,
			runtime:     r.Runtime,
			resource:    r.Resource,
			feature:     r.Feature,
		}
		if idx, exists := seen[k]; exists {
			// Merge credentials.
			deduped[idx].Credentials = mergeCredentials(deduped[idx].Credentials, r.Credentials)
			continue
		}
		seen[k] = len(deduped)
		cp := r
		if len(r.Credentials) > 0 {
			cp.Credentials = make([]string, len(r.Credentials))
			copy(cp.Credentials, r.Credentials)
			sort.Strings(cp.Credentials)
		}
		deduped = append(deduped, cp)
	}

	sort.Slice(deduped, func(i, j int) bool {
		a, b := deduped[i], deduped[j]
		if a.NodeType != b.NodeType {
			return a.NodeType < b.NodeType
		}
		if a.NodeVersion != b.NodeVersion {
			return a.NodeVersion < b.NodeVersion
		}
		if a.Runtime != b.Runtime {
			return a.Runtime < b.Runtime
		}
		if a.Resource != b.Resource {
			return a.Resource < b.Resource
		}
		return a.Feature < b.Feature
	})
	return deduped
}

func mergeCredentials(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	set := make(map[string]bool, len(a)+len(b))
	for _, c := range a {
		set[c] = true
	}
	for _, c := range b {
		set[c] = true
	}
	merged := make([]string, 0, len(set))
	for c := range set {
		merged = append(merged, c)
	}
	sort.Strings(merged)
	return merged
}

// RequirementsFromGraphPackage converts graph-level requirements from a
// compiled SubgraphPackage into engine-level CapabilityRequirements, adding the
// mandatory group execution feature requirement.
func RequirementsFromGraphPackage(graphReqs []graph.Requirement) []CapabilityRequirement {
	reqs := make([]CapabilityRequirement, 0, len(graphReqs)+1)
	for _, gr := range graphReqs {
		reqs = append(reqs, CapabilityRequirement{
			NodeType:    gr.NodeType,
			NodeVersion: gr.NodeVersion,
			Runtime:     gr.Runtime,
			Resource:    gr.Resource,
			Credentials: gr.Credentials,
		})
	}
	reqs = append(reqs, CapabilityRequirement{
		NodeType: "xflow.group",
		Feature:  FeatureGroupExecV1,
	})
	return NormalizeRequirements(reqs)
}
