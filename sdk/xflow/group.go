package xflow

import (
	"time"

	"github.com/xbcio/xflow/types"
)

type groupEntry struct {
	name               string
	members            []string
	runnerSelector     *types.RunnerSelector
	onError            types.OnError
	errorOutputs       []types.Connection
	retry              *types.RetrySettings
	timeout            time.Duration
	mode               string
	activationReplicas uint32
}

// GroupRef is the builder handle for a co-location group, symmetric with
// NodeRef.RunnerSelector usage patterns.
type GroupRef struct{ entry *groupEntry }

// Group declares a co-location group and returns its reference.
func (w *WorkflowBuilder) Group(name string) *GroupRef {
	e := &groupEntry{name: name}
	w.groups = append(w.groups, e)
	return &GroupRef{entry: e}
}

func (g *GroupRef) RunnerSelector(s types.RunnerSelector) *GroupRef {
	g.entry.runnerSelector = cloneRunnerSelector(&s)
	return g
}

// ActivationReplicas sets how many distinct runners should host this
// trigger-entry group. Zero or one means one activation.
func (g *GroupRef) ActivationReplicas(replicas uint32) *GroupRef {
	g.entry.activationReplicas = replicas
	return g
}
func (g *GroupRef) OnError(oe types.OnError) *GroupRef { g.entry.onError = oe; return g }

// ErrorOutputs declares the downstream targets for the group's synthesized
// "error" output port. Only meaningful when OnError is OnErrorOutput;
// graph.Compile rejects a non-empty ErrorOutputs under any other policy, and
// rejects OnErrorOutput with no targets (see NODE-GROUP-COLOCATION.md §12.2).
// build() stays permissive the same way OnError's type-legal-but-unchecked
// values do — validation runs at graph.Compile, not here.
func (g *GroupRef) ErrorOutputs(targets ...types.Connection) *GroupRef {
	g.entry.errorOutputs = append([]types.Connection(nil), targets...)
	return g
}
func (g *GroupRef) Retry(r types.RetrySettings) *GroupRef { g.entry.retry = &r; return g }
func (g *GroupRef) Timeout(d time.Duration) *GroupRef     { g.entry.timeout = d; return g }
func (g *GroupRef) Transient() *GroupRef {
	g.entry.mode = types.GroupModeTransient
	return g
}

// Group appends this node to the group (member order = call order).
func (n *NodeRef) Group(g *GroupRef) *NodeRef {
	g.entry.members = append(g.entry.members, n.name)
	return n
}

func (w *WorkflowBuilder) assembleGroups(def *types.WorkflowDef) {
	for _, e := range w.groups {
		def.Groups = append(def.Groups, types.GroupDef{
			Name:               e.name,
			Members:            append([]string(nil), e.members...),
			RunnerSelector:     cloneRunnerSelector(e.runnerSelector),
			OnError:            string(e.onError),
			ErrorOutputs:       append([]types.Connection(nil), e.errorOutputs...),
			Retry:              e.retry,
			Timeout:            e.timeout,
			Mode:               e.mode,
			ActivationReplicas: e.activationReplicas,
		})
	}
}
