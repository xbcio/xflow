package graph

import (
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

// assignPinData resolves the workflow's pin_data block onto NodeMeta.PinOutput
// and records the effective mode on the graph (DSL-SPECIFICATION §7).
//
// It runs after compileGroups because co-location membership decides whether a
// node can be pinned at all: a group member is dispatched inside its group's
// TaskTypeGroupExec unit, which the engine's pin hook never sees. Such entries
// are reported and ignored rather than silently running for real.
//
// Disabled takes precedence over a pin: a disabled node never receives a
// PinOutput. pin_data_mode=disabled assigns nothing at all, so a graph compiled
// in that mode is byte-identical (and hash-identical) to one without pin_data.
func assignPinData(def *types.WorkflowDef, g *Graph) error {
	mode := types.PinDataModeTestOnly
	if def.Settings != nil && def.Settings.PinDataMode != "" {
		mode = def.Settings.PinDataMode
	}
	switch mode {
	case types.PinDataModeTestOnly, types.PinDataModeAlways, types.PinDataModeDisabled:
	default:
		return fmt.Errorf("settings.pin_data_mode %q is not one of %q, %q, %q",
			mode, types.PinDataModeTestOnly, types.PinDataModeAlways, types.PinDataModeDisabled)
	}
	if len(def.PinData) == 0 || mode == types.PinDataModeDisabled {
		return nil
	}

	disabled := make(map[string]bool, len(def.Nodes))
	schemas := make(map[string]map[string]any, len(def.Nodes))
	// bodyMembers names map-body members: a body runs as its own sub-execution
	// compiled from a projected package that carries no pin_data, so a pin on a
	// member could never take effect.
	bodyMembers := make(map[string]string)
	for _, nd := range def.Nodes {
		disabled[nd.Name] = nd.Disabled
		schemas[nd.Name] = nd.OutputSchema
		if members, ok := SubgraphBodyMembers(nd.Parameters); ok {
			for _, m := range members {
				bodyMembers[m.Name] = nd.Name
			}
		}
	}

	names := make([]string, 0, len(def.PinData))
	for name := range def.PinData {
		names = append(names, name)
	}
	sort.Strings(names)

	pinned := 0
	for _, name := range names {
		idx, ok := g.index[name]
		if !ok {
			if parent, inBody := bodyMembers[name]; inBody {
				g.addWarning(fmt.Sprintf("pin_data: 节点 %q 是 %q 的 body 成员，当前版本不支持钉住 body 成员，该条目被忽略", name, parent))
				continue
			}
			g.addWarning(fmt.Sprintf("pin_data: 节点 %q 不存在于 nodes，该条目被忽略", name))
			continue
		}
		meta := &g.nodes[idx]
		switch {
		case disabled[name]:
			g.addWarning(fmt.Sprintf("pin_data: 节点 %q 已 disabled 且配了 pin_data，pin 被忽略（disabled 优先：节点按 skipped 提交，不执行也不使用 mock）", name))
			continue
		case meta.Kind == types.NodeKindSupply:
			g.addWarning(fmt.Sprintf("pin_data: 节点 %q 是 supply 节点，不参与调度，该条目被忽略", name))
			continue
		case meta.GroupIdx >= 0:
			g.addWarning(fmt.Sprintf("pin_data: 节点 %q 属于 co-location 组，当前版本不支持钉住组成员，该条目被忽略", name))
			continue
		case g.allowCycles:
			g.addWarning(fmt.Sprintf("pin_data: 循环工作流（allow_cycles）当前不支持钉住，节点 %q 的条目被忽略", name))
			continue
		case g.faf:
			g.addWarning(fmt.Sprintf("pin_data: faf 工作流直接派发、不经调度，节点 %q 的条目被忽略", name))
			continue
		}
		mock, ok := def.PinData[name].(map[string]any)
		if !ok {
			g.addWarning(fmt.Sprintf("pin_data: 节点 %q 的 mock 数据必须是对象（得到 %T），该条目被忽略", name, def.PinData[name]))
			continue
		}
		if missing := missingRequiredOutputFields(schemas[name], mock); len(missing) > 0 {
			g.addWarning(fmt.Sprintf("pin_data: 节点 %q 的 mock 数据缺少 output_schema 必填字段 %v，下游可能拿到 nil", name, missing))
		}
		meta.PinOutput = cloneStringAnyMap(mock)
		if meta.PinOutput == nil {
			meta.PinOutput = map[string]any{}
		}
		pinned++
	}
	if pinned == 0 {
		return nil
	}
	g.pinDataMode = mode
	if mode == types.PinDataModeAlways {
		g.addWarning(fmt.Sprintf("pin_data_mode: always 让 %d 个钉住节点在所有执行中都跳过真实调用，仅适用于调试场景", pinned))
	}
	return nil
}

// missingRequiredOutputFields returns the output_schema "required" names absent
// from mock, sorted. A schema without a usable required list yields nil.
func missingRequiredOutputFields(schema map[string]any, mock map[string]any) []string {
	if schema == nil {
		return nil
	}
	var required []string
	switch v := schema["required"].(type) {
	case []string:
		required = v
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				required = append(required, s)
			}
		}
	}
	var missing []string
	for _, field := range required {
		if _, ok := mock[field]; !ok {
			missing = append(missing, field)
		}
	}
	sort.Strings(missing)
	return missing
}
