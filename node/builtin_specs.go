package node

import (
	"sync"

	"github.com/xbcio/xflow/node/internal/action"
	codepkg "github.com/xbcio/xflow/node/internal/code"
	scriptpkg "github.com/xbcio/xflow/node/internal/code/script"
	"github.com/xbcio/xflow/node/internal/flow"
	"github.com/xbcio/xflow/node/internal/group"
	supplypkg "github.com/xbcio/xflow/node/internal/supply"
	"github.com/xbcio/xflow/node/internal/transform"
	"github.com/xbcio/xflow/node/trigger/cron"
	"github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/node/trigger/redis"
	"github.com/xbcio/xflow/node/trigger/timer"
	"github.com/xbcio/xflow/node/trigger/webhook"
	"github.com/xbcio/xflow/types"
)

// BuiltinParamSpecs returns the ParamSpecs of the builtin node type
// (kind, nodeType, version), or false when it is not a builtin. An empty kind
// means action, as it does to the compiler; version 0 resolves the latest
// builtin version, as it does at dispatch. The returned slice is a copy.
//
// It exists for workflow hash canonicalization (backend/workflowhash), which
// treats an omitted builtin param as equal to its Default. It deliberately
// reads a fixed table of the builtins this package ships rather than the live
// node registry:
//   - the hash must not depend on what else a process registered, so two
//     processes always agree on it;
//   - "omitted equals Default" is proven only for builtins, whose Defaults
//     equal their handler fallbacks (default_contract_*_test.go).
//
// A builtin's Default is therefore part of hash identity: changing one is a
// hash-format change (TestBuiltinDefaultsGolden).
func BuiltinParamSpecs(kind types.NodeKind, nodeType string, version int) ([]types.ParamSpec, bool) {
	if kind == "" {
		kind = types.NodeKindAction
	}
	table := builtinTable()
	if version == 0 {
		version = table.latest[builtinTypeKey{kind: kind, typ: nodeType}]
	}
	d, ok := table.specs[builtinKey{kind: kind, typ: nodeType, version: version}]
	if !ok {
		return nil, false
	}
	return d.Clone().Params, true
}

type builtinTypeKey struct {
	kind types.NodeKind
	typ  string
}

type builtinKey struct {
	kind    types.NodeKind
	typ     string
	version int
}

type builtinSpecTable struct {
	specs  map[builtinKey]types.Descriptor
	latest map[builtinTypeKey]int
}

// builtinDescribers lists one value of every builtin node type: the same
// values each package's init() registers, plus the two supply declarations,
// which have no registered handler. node/builtin_specs_test.go fails if a
// registered "xflow." builtin is missing here.
func builtinDescribers() []types.DescriptorProvider {
	return []types.DescriptorProvider{
		&action.HTTPNode{},
		&action.DatabaseNode{},
		&action.GRPCNode{},
		&action.CDPNode{},
		&codepkg.FunctionNode{},
		&scriptpkg.ScriptNode{},
		&transform.SetNode{},
		&transform.PickNode{},
		&transform.RenameNode{},
		&transform.FilterNode{},
		&transform.SortNode{},
		&transform.LimitNode{},
		&transform.RemoveDuplicatesNode{},
		&transform.AggregateNode{},
		&flow.StartNode{},
		&flow.EndNode{},
		&flow.IfNode{},
		&flow.SwitchNode{},
		&flow.MergeNode{},
		&flow.SplitNode{},
		&flow.MapNode{},
		&flow.WaitNode{},
		&group.ApprovalNode{},
		&group.NotificationNode{},
		&timer.Node{},
		&cron.Node{},
		&webhook.Node{},
		&kafka.Node{},
		&redis.Node{},
		supplypkg.External(""),
		supplypkg.Static(nil),
	}
}

var builtinTable = sync.OnceValue(func() builtinSpecTable {
	t := builtinSpecTable{
		specs:  make(map[builtinKey]types.Descriptor),
		latest: make(map[builtinTypeKey]int),
	}
	for _, p := range builtinDescribers() {
		d := p.Descriptor()
		kind := d.Kind
		if kind == "" {
			kind = types.NodeKindAction
		}
		version := 1
		if v, ok := p.(interface{ NodeVersion() int }); ok {
			version = v.NodeVersion()
		}
		t.specs[builtinKey{kind: kind, typ: d.Type, version: version}] = d
		tk := builtinTypeKey{kind: kind, typ: d.Type}
		if version > t.latest[tk] {
			t.latest[tk] = version
		}
	}
	return t
})
