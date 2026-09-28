package apiserver

import (
	"strings"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// NodeFormSchema v1 wire DTOs (GET /v1/node-types). They are a hand-mapped
// projection of types.Descriptor — types/ carries no json tags on purpose —
// and their consumer is the TypeScript NodeFormSchema in
// web/packages/xflow-core/src/nodeForm.ts. The cross-language fixture
// web/packages/xflow-editor/src/node-form/testdata/node-types.generated.json
// (TestNodeFormFixtureIsCurrent) pins the two together.
//
// Wire rules:
//   - optional keys are omitted, never written as null;
//   - Condition.eq is omitted when the Go Eq is nil: on the wire `eq: null` is
//     a real clause ("param is unset") that Go cannot express, so writing it
//     for "no Eq clause" would change the meaning;
//   - Condition.in / any_of are written whenever the Go slice is non-nil, even
//     when empty: a non-nil empty In or AnyOf is a clause that never holds;
//   - types.ParamBool ("bool") is spelled "boolean".

// nodeFormSpecVersion is NodeFormSchema.spec.
const nodeFormSpecVersion = "node-form/v1"

// nodeTypesResponse is the data payload of GET /v1/node-types.
type nodeTypesResponse struct {
	ParamValidationMode types.ParamValidationMode `json:"param_validation_mode"`
	NodeTypes           []nodeFormSchema          `json:"node_types"`
}

type nodeFormSchema struct {
	Spec         string          `json:"spec"`
	NodeType     string          `json:"node_type"`
	NodeVersion  int             `json:"node_version"`
	Kind         types.NodeKind  `json:"kind"`
	DisplayName  string          `json:"display_name,omitempty"`
	Docs         string          `json:"docs,omitempty"`
	Capabilities []string        `json:"capabilities,omitempty"`
	Credentials  []string        `json:"credentials,omitempty"`
	Ports        *nodeFormPorts  `json:"ports,omitempty"`
	Groups       []nodeFormGroup `json:"groups,omitempty"`
	OneOf        []nodeFormOneOf `json:"one_of,omitempty"`
	Fields       []nodeFormField `json:"fields"`
}

type nodeFormPort struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name,omitempty"`
}

// nodeFormPorts has no dynamic_outputs member: types.Descriptor has no
// declarative source for it, and an absent key is the wire's "not dynamic".
type nodeFormPorts struct {
	Inputs  []nodeFormPort `json:"inputs,omitempty"`
	Outputs []nodeFormPort `json:"outputs,omitempty"`
}

type nodeFormGroup struct {
	Key         string `json:"key"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
	Collapsed   bool   `json:"collapsed,omitempty"`
}

type nodeFormOneOf struct {
	Params []string `json:"params"`
	Mode   string   `json:"mode,omitempty"`
}

// nodeFormField is one param. Name and Path identify it; everything else is
// shared with an array element schema (nodeFormItem), which has no identity.
type nodeFormField struct {
	Name string `json:"name"`
	Path string `json:"path"`
	nodeFormItem
}

type nodeFormItem struct {
	Label        string                    `json:"label,omitempty"`
	Type         string                    `json:"type"`
	Widget       string                    `json:"widget,omitempty"`
	Required     bool                      `json:"required,omitempty"`
	RequiredWhen *nodeFormCondition        `json:"required_when,omitempty"`
	Default      any                       `json:"default,omitempty"`
	Fallback     any                       `json:"fallback,omitempty"`
	Help         string                    `json:"help,omitempty"`
	Group        string                    `json:"group,omitempty"`
	Order        int                       `json:"order,omitempty"`
	Secret       bool                      `json:"secret,omitempty"`
	Deprecated   string                    `json:"deprecated,omitempty"`
	Expression   *nodeFormExpression       `json:"expression,omitempty"`
	Rules        []nodeFormRule            `json:"rules,omitempty"`
	VisibleWhen  *nodeFormCondition        `json:"visible_when,omitempty"`
	Options      []nodeFormEnumOption      `json:"options,omitempty"`
	OptionsWhen  []nodeFormConditionalEnum `json:"options_when,omitempty"`
	Item         *nodeFormItem             `json:"item,omitempty"`
	Fields       []nodeFormField           `json:"fields,omitempty"`
}

type nodeFormExpression struct {
	Mode graph.ExpressionModeValue `json:"mode"`
}

// nodeFormRule is one projected Constraints entry. Value is a pointer so a
// zero bound (min 0) is still written.
type nodeFormRule struct {
	Type     string   `json:"type"`
	Value    *float64 `json:"value,omitempty"`
	Pattern  string   `json:"pattern,omitempty"`
	Format   string   `json:"format,omitempty"`
	Advisory bool     `json:"advisory,omitempty"`
}

type nodeFormEnumOption struct {
	Value       any    `json:"value"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

type nodeFormConditionalEnum struct {
	When    nodeFormCondition    `json:"when"`
	Options []nodeFormEnumOption `json:"options"`
}

// nodeFormCondition mirrors types.Condition. In and AnyOf are pointers so a
// non-nil empty Go slice (a clause that never holds) survives omitempty.
type nodeFormCondition struct {
	Param  string               `json:"param,omitempty"`
	Eq     any                  `json:"eq,omitempty"`
	In     *[]any               `json:"in,omitempty"`
	Truthy *bool                `json:"truthy,omitempty"`
	AllOf  []nodeFormCondition  `json:"all_of,omitempty"`
	AnyOf  *[]nodeFormCondition `json:"any_of,omitempty"`
	Not    *nodeFormCondition   `json:"not,omitempty"`
}

// advisoryFormats are the Constraints.Format names the backend does not
// enforce (descriptor contract §2.2); their rules carry advisory: true.
var advisoryFormats = map[string]bool{"url": true, "host-port": true, "code": true}

// nodeFormFallbacks indexes node.BuiltinFallbacks by type@version and dotted
// param path. Derived fallbacks have no constant value and are left out: the
// wire's fallback is a value, and there is no member for a description.
type nodeFormFallbacks map[nodeFormFallbackKey]any

type nodeFormFallbackKey struct {
	typ     string
	version int
	param   string
}

func builtinNodeFormFallbacks() nodeFormFallbacks {
	out := nodeFormFallbacks{}
	for _, f := range node.BuiltinFallbacks() {
		if f.Derived != "" || f.Value == nil {
			continue
		}
		out[nodeFormFallbackKey{f.Type, f.Version, f.Param}] = f.Value
	}
	return out
}

// projectNodeTypes projects every registered descriptor, in registry order
// (type, then version).
func projectNodeTypes(descs []registry.RegisteredDescriptor, fallbacks nodeFormFallbacks) []nodeFormSchema {
	out := make([]nodeFormSchema, 0, len(descs))
	for _, rd := range descs {
		out = append(out, projectNodeForm(rd, fallbacks))
	}
	return out
}

// projectNodeForm maps one registered descriptor to its NodeFormSchema.
func projectNodeForm(rd registry.RegisteredDescriptor, fallbacks nodeFormFallbacks) nodeFormSchema {
	d := rd.Descriptor
	kind := d.Kind
	if kind == "" {
		kind = types.NodeKindAction
	}
	p := nodeFormProjector{kind: kind, typ: rd.Type, version: rd.Version, fallbacks: fallbacks}
	s := nodeFormSchema{
		Spec:         nodeFormSpecVersion,
		NodeType:     rd.Type,
		NodeVersion:  rd.Version,
		Kind:         kind,
		DisplayName:  d.DisplayName,
		Docs:         d.Docs,
		Capabilities: d.Capabilities,
		Credentials:  d.Credentials,
		Fields:       make([]nodeFormField, 0, len(d.Params)),
	}
	if len(d.Inputs) > 0 || len(d.Outputs) > 0 {
		s.Ports = &nodeFormPorts{Inputs: projectPorts(d.Inputs), Outputs: projectPorts(d.Outputs)}
	}
	for _, g := range d.Groups {
		s.Groups = append(s.Groups, nodeFormGroup{Key: g.Key, DisplayName: g.DisplayName, Description: g.Description, Collapsed: g.Collapsed})
	}
	for _, g := range d.OneOf {
		s.OneOf = append(s.OneOf, nodeFormOneOf{Params: g.Params, Mode: g.Mode})
	}
	for i := range d.Params {
		spec := &d.Params[i]
		s.Fields = append(s.Fields, p.field(spec, "/parameters/"+escapeNodeFormPointer(spec.Name), spec.Name, spec.Name))
	}
	return s
}

func projectPorts(ports []types.PortSpec) []nodeFormPort {
	if len(ports) == 0 {
		return nil
	}
	out := make([]nodeFormPort, 0, len(ports))
	for _, p := range ports {
		out = append(out, nodeFormPort{Name: p.Name, DisplayName: p.DisplayName})
	}
	return out
}

type nodeFormProjector struct {
	kind      types.NodeKind
	typ       string
	version   int
	fallbacks nodeFormFallbacks
}

// field projects a param or sub-field.
//
//   - pointer is the field's wire path. Top-level params and object sub-fields
//     get the JSON Pointer from the WorkflowNode root ("/parameters/a/b").
//     Inside an array element the path is relative to that element
//     ("condition", "cfg/x"): elements have no fixed index, and the editor
//     compiler binds element fields by name, not by path.
//   - exprPath is the graph.ExpressionMode path: relative to parameters,
//     "/"-separated, array levels dropped ("rules/condition").
//   - fallbackKey is the dotted Fallback.Param path ("tuning.start_id").
func (p nodeFormProjector) field(spec *types.ParamSpec, pointer, exprPath, fallbackKey string) nodeFormField {
	f := nodeFormField{Name: spec.Name, Path: pointer}
	f.nodeFormItem = p.item(spec, pointer, exprPath, fallbackKey)
	if f.Label == "" {
		f.Label = spec.Name
	}
	return f
}

// item projects everything but a field's identity. elementBase is the pointer
// the spec's sub-fields hang off: the field's own pointer for an object, and
// for an element schema the element-relative base ("" at the element root).
func (p nodeFormProjector) item(spec *types.ParamSpec, elementBase, exprPath, fallbackKey string) nodeFormItem {
	it := nodeFormItem{
		Label:        spec.DisplayName,
		Type:         nodeFormFieldType(spec.Type),
		Widget:       spec.Widget,
		Required:     spec.Required,
		RequiredWhen: projectCondition(spec.RequiredWhen),
		Default:      spec.Default,
		Help:         spec.Description,
		Group:        spec.Group,
		Order:        spec.Order,
		Secret:       spec.Secret,
		Deprecated:   spec.Deprecated,
		Expression:   &nodeFormExpression{Mode: graph.ExpressionMode(p.kind, p.typ, exprPath)},
		Rules:        projectRules(spec.Constraints),
		VisibleWhen:  projectCondition(spec.VisibleWhen),
		Options:      projectEnum(spec.Enum),
	}
	if spec.Default == nil {
		if v, ok := p.fallbacks[nodeFormFallbackKey{p.typ, p.version, fallbackKey}]; ok {
			it.Fallback = v
		}
	}
	for _, ce := range spec.EnumWhen {
		it.OptionsWhen = append(it.OptionsWhen, nodeFormConditionalEnum{
			When:    *projectCondition(&ce.When),
			Options: projectEnum(ce.Enum),
		})
	}
	if spec.Item != nil {
		// The element shares the array's ExpressionMode path and fallback key:
		// ExpressionMode has no index segments and Fallback.Param no item level.
		el := p.item(spec.Item, "", exprPath, fallbackKey)
		if el.Label == "" {
			el.Label = spec.Item.Name
		}
		it.Item = &el
	}
	for i := range spec.Fields {
		sub := &spec.Fields[i]
		it.Fields = append(it.Fields, p.field(sub,
			joinNodeFormPointer(elementBase, sub.Name),
			exprPath+"/"+sub.Name,
			fallbackKey+"."+sub.Name))
	}
	return it
}

// nodeFormFieldType maps a ParamType to the wire's field type; "bool" is
// spelled "boolean" there. Any other value passes through unchanged (the
// editor degrades an unknown type to string, with a compile warning).
func nodeFormFieldType(t types.ParamType) string {
	if t == types.ParamBool {
		return "boolean"
	}
	return string(t)
}

func projectRules(c *types.Constraints) []nodeFormRule {
	if c == nil {
		return nil
	}
	var rules []nodeFormRule
	bound := func(typ string, v *float64) {
		if v != nil {
			value := *v
			rules = append(rules, nodeFormRule{Type: typ, Value: &value})
		}
	}
	count := func(typ string, v *int) {
		if v != nil {
			value := float64(*v)
			rules = append(rules, nodeFormRule{Type: typ, Value: &value})
		}
	}
	bound("min", c.Min)
	bound("max", c.Max)
	count("min_length", c.MinLength)
	count("max_length", c.MaxLength)
	if c.Pattern != "" {
		rules = append(rules, nodeFormRule{Type: "pattern", Pattern: c.Pattern})
	}
	if c.Format != "" {
		rules = append(rules, nodeFormRule{Type: "format", Format: c.Format, Advisory: advisoryFormats[c.Format]})
	}
	count("min_items", c.MinItems)
	count("max_items", c.MaxItems)
	if c.UniqueItems {
		rules = append(rules, nodeFormRule{Type: "unique_items"})
	}
	return rules
}

func projectEnum(options []types.EnumOption) []nodeFormEnumOption {
	if len(options) == 0 {
		return nil
	}
	out := make([]nodeFormEnumOption, 0, len(options))
	for _, o := range options {
		out = append(out, nodeFormEnumOption{Value: o.Value, Label: o.DisplayName, Description: o.Description})
	}
	return out
}

func projectCondition(c *types.Condition) *nodeFormCondition {
	if c == nil {
		return nil
	}
	out := &nodeFormCondition{Param: c.Param, Eq: c.Eq, Truthy: c.Truthy, Not: projectCondition(c.Not)}
	if c.In != nil {
		in := append([]any{}, c.In...)
		out.In = &in
	}
	for i := range c.AllOf {
		out.AllOf = append(out.AllOf, *projectCondition(&c.AllOf[i]))
	}
	if c.AnyOf != nil {
		anyOf := make([]nodeFormCondition, 0, len(c.AnyOf))
		for i := range c.AnyOf {
			anyOf = append(anyOf, *projectCondition(&c.AnyOf[i]))
		}
		out.AnyOf = &anyOf
	}
	return out
}

// escapeNodeFormPointer escapes one JSON Pointer reference token (RFC 6901).
func escapeNodeFormPointer(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

func joinNodeFormPointer(base, name string) string {
	if base == "" {
		return escapeNodeFormPointer(name)
	}
	return base + "/" + escapeNodeFormPointer(name)
}
