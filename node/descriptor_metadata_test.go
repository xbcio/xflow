package node_test

// Contract tests over the editor metadata every builtin Descriptor carries
// (Enum, EnumWhen, VisibleWhen, RequiredWhen, OneOf, Item, Fields,
// Constraints, Group, Widget). They pin what the node-form projection reads,
// so a change to a builtin descriptor that the editor would render
// differently -- or that would move an SDK workflow hash through Default --
// has to be made on purpose.

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

type versionedDescriptor struct {
	version int
	desc    types.Descriptor
}

// builtinDescriptors returns every builtin descriptor: every registered
// version of every "xflow." action and trigger type, plus the two supply
// declarations, which have no registered handler.
func builtinDescriptors(t *testing.T) []versionedDescriptor {
	t.Helper()
	seen := map[string]bool{}
	var out []versionedDescriptor
	add := func(version int, d types.Descriptor) {
		key := fmt.Sprintf("%s@%d", d.Type, version)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, versionedDescriptor{version: version, desc: d})
	}
	for _, typ := range registry.Types() {
		if !strings.HasPrefix(typ, "xflow.") {
			continue
		}
		for _, v := range registry.Versions(typ) {
			h, ok := registry.LookupVersion(typ, v)
			if !ok {
				t.Fatalf("LookupVersion(%q, %d) failed", typ, v)
			}
			add(v, h.Descriptor())
		}
	}
	for _, typ := range registry.TriggerTypes() {
		if !strings.HasPrefix(typ, "xflow.") {
			continue
		}
		for _, v := range registry.TriggerVersions(typ) {
			h, ok := registry.LookupTriggerVersion(typ, v)
			if !ok {
				t.Fatalf("LookupTriggerVersion(%q, %d) failed", typ, v)
			}
			add(v, h.Descriptor())
		}
	}
	add(0, node.SupplyExternal("").Descriptor())
	add(0, node.SupplyStatic(nil).Descriptor())
	sort.Slice(out, func(i, j int) bool {
		if out[i].desc.Type != out[j].desc.Type {
			return out[i].desc.Type < out[j].desc.Type
		}
		return out[i].version < out[j].version
	})
	if len(out) < 20 {
		t.Fatalf("only %d builtin descriptors found; the builtin packages are not linked", len(out))
	}
	return out
}

// walkParams visits every ParamSpec reachable from params. path is the
// dotted path ("aggregate.dead_letter_topic", "rules[].condition"); siblings
// is the level the spec lives at, which is what Condition.Param refers to.
func walkParams(params []types.ParamSpec, prefix string, visit func(path string, spec types.ParamSpec, siblings []types.ParamSpec)) {
	for _, p := range params {
		path := prefix + p.Name
		visit(path, p, params)
		if len(p.Fields) > 0 {
			walkParams(p.Fields, path+".", visit)
		}
		if p.Item != nil {
			visit(path+"[]", *p.Item, nil)
			if len(p.Item.Fields) > 0 {
				walkParams(p.Item.Fields, path+"[].", visit)
			}
		}
	}
}

// findParam resolves a dotted path produced by walkParams.
func findParam(d types.Descriptor, path string) (types.ParamSpec, bool) {
	var found types.ParamSpec
	ok := false
	walkParams(d.Params, "", func(p string, spec types.ParamSpec, _ []types.ParamSpec) {
		if p == path {
			found, ok = spec, true
		}
	})
	return found, ok
}

// TestBuiltinDefaultsGolden pins every builtin ParamSpec.Default, value and Go
// type. The SDK writes Default into a workflow's params, so changing one --
// or adding one -- moves the definition hash of every SDK workflow using that
// node, and the next redeploy of an unchanged workflow conflicts. See the
// descriptor-contract design, "Default 不可改".
func TestBuiltinDefaultsGolden(t *testing.T) {
	want := map[string]string{
		"xflow.approval@1/mode":                  "string:any",
		"xflow.approval@1/timeout_action":        "string:route",
		"xflow.browser.cdp@1/timeout_ms":         "int:30000",
		"xflow.browser.cdp@1/total_timeout_ms":   "int:45000",
		"xflow.http@1/method":                    "string:GET",
		"xflow.map@1/batch_size":                 "int:1",
		"xflow.map@1/body_concurrency":           "int:1",
		"xflow.map@1/continue_on_error":          "bool:false",
		"xflow.merge@1/on_others":                "string:cancel",
		"xflow.split@1/continue_on_error":        "bool:false",
		"xflow.supply.external@0/require_ready":  "bool:true",
		"xflow.supply.static@0/require_ready":    "bool:true",
		"xflow.trigger.cron@1/timezone":          "string:UTC",
		"xflow.trigger.kafka@1/max_inflight":     "float64:64",
		"xflow.trigger.kafka@1/start_offset":     "string:latest",
		"xflow.trigger.redis@1/max_inflight":     "float64:64",
		"xflow.trigger.redis@1/mode":             "string:stream",
		"xflow.trigger.webhook@1/max_body_bytes": "float64:1.048576e+06",
		"xflow.wait@1/mode":                      "string:signal",
	}

	got := map[string]string{}
	for _, vd := range builtinDescriptors(t) {
		walkParams(vd.desc.Params, "", func(path string, spec types.ParamSpec, _ []types.ParamSpec) {
			if spec.Default == nil {
				return
			}
			key := fmt.Sprintf("%s@%d/%s", vd.desc.Type, vd.version, path)
			got[key] = fmt.Sprintf("%T:%v", spec.Default, spec.Default)
		})
	}

	for key, w := range want {
		if g, ok := got[key]; !ok {
			t.Errorf("Default %s removed (was %s)", key, w)
		} else if g != w {
			t.Errorf("Default %s = %s, want %s", key, g, w)
		}
	}
	for key, g := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("new Default %s = %s; adding a Default changes SDK workflow hashes", key, g)
		}
	}
}

func condIn(param string, vs ...any) *types.Condition {
	return &types.Condition{Param: param, In: vs}
}

func condEq(param string, v any) *types.Condition {
	return &types.Condition{Param: param, Eq: v}
}

// TestBuiltinLinkagesDeclared is the coverage table for the node-form design
// §4.4 linkages. Where the handler
// disagrees with the table, the row encodes the handler and says why.
func TestBuiltinLinkagesDeclared(t *testing.T) {
	truthy := true
	type visible struct {
		typ, path string
		want      *types.Condition // nil means "always visible"
	}
	visibles := []visible{
		// wait: mode -> signal_name, signals, duration, until. An unset mode
		// behaves as signal (WaitNode.PrepareSuspend), hence the nil.
		{"xflow.wait", "signal_name", condIn("mode", "signal", nil)},
		{"xflow.wait", "signals", condIn("mode", "signal", nil)},
		{"xflow.wait", "duration", condEq("mode", "timer")},
		{"xflow.wait", "until", condEq("mode", "timer")},
		{"xflow.wait", "quorum", &types.Condition{AllOf: []types.Condition{
			*condIn("mode", "signal", nil),
			{Param: "signals", Truthy: &truthy},
		}}},
		{"xflow.wait", "timeout", nil},
		// switch: mode -> rules, expression. Empty mode runs as rules.
		{"xflow.switch", "rules", condIn("mode", "rules", nil)},
		{"xflow.switch", "expression", condEq("mode", "expression")},
		// http: mode -> body, body_b64. Unset mode is json.
		{"xflow.http", "body", condIn("mode", "json", nil)},
		{"xflow.http", "body_b64", condEq("mode", "raw")},
		// merge: mode=wait_any -> on_others.
		{"xflow.merge", "on_others", condEq("mode", "wait_any")},
		// database: operation -> where, data, columns, limit.
		{"xflow.database", "where", condIn("operation", "select", "update", "delete")},
		{"xflow.database", "data", condIn("operation", "insert", "insert_many", "update")},
		{"xflow.database", "columns", condEq("operation", "select")},
		{"xflow.database", "limit", condEq("operation", "select")},
		// trigger.redis: mode -> stream/group or channel. Unset mode is stream.
		{"xflow.trigger.redis", "stream", condIn("mode", "stream", nil)},
		{"xflow.trigger.redis", "group", condIn("mode", "stream", nil)},
		{"xflow.trigger.redis", "channel", condEq("mode", "pubsub")},
		// The design says "stream mode -> tuning", but tuning.db and
		// tuning.dial_timeout configure the client pubsub mode uses too
		// (newRedisConsumer), so tuning stays visible in both modes.
		{"xflow.trigger.redis", "tuning", nil},
		// trigger.kafka: dead_letter_topic follows its sibling policy field.
		{"xflow.trigger.kafka", "aggregate.dead_letter_topic", condEq("on_overflow", "dead_letter")},
		{"xflow.trigger.kafka", "message_schema.dead_letter_topic", condEq("on_invalid", "dead_letter")},
		// approval: timeout_action only matters once a timeout can fire.
		{"xflow.approval", "timeout_action", &types.Condition{Param: "timeout", Truthy: &truthy}},
	}
	type required struct {
		typ, path string
		want      *types.Condition
	}
	requireds := []required{
		{"xflow.switch", "expression", condEq("mode", "expression")},
		{"xflow.trigger.redis", "stream", condIn("mode", "stream", nil)},
		// Not in the design table, but configFromParams rejects stream mode
		// without a group exactly as it rejects one without a stream.
		{"xflow.trigger.redis", "group", condIn("mode", "stream", nil)},
		{"xflow.trigger.redis", "channel", condEq("mode", "pubsub")},
		{"xflow.trigger.kafka", "aggregate.dead_letter_topic", condEq("on_overflow", "dead_letter")},
		{"xflow.trigger.kafka", "message_schema.dead_letter_topic", condEq("on_invalid", "dead_letter")},
	}
	type enumWhen struct {
		typ, path string
		want      map[string][]any // language value -> runtime values, in order
		whenParam string
	}
	enumWhens := []enumWhen{
		{"xflow.script", "runtime", map[string][]any{
			"js":   {"goja", "qjs"},
			"wasm": {"wazero", "wazero-reactor"},
		}, "language"},
	}
	oneOfs := map[string]types.OneOfGroup{
		"xflow.script":   {Params: []string{"code", "artifact_digest", "__artifact_file_path"}, Mode: types.OneOfExactly},
		"xflow.map":      {Params: []string{"body", "expression"}, Mode: types.OneOfExactly},
		"xflow.function": {Params: []string{"function_name", "code"}, Mode: types.OneOfAtLeast},
	}

	descs := map[string]types.Descriptor{}
	for _, vd := range builtinDescriptors(t) {
		descs[vd.desc.Type] = vd.desc
	}
	lookup := func(typ, path string) (types.ParamSpec, bool) {
		t.Helper()
		d, ok := descs[typ]
		if !ok {
			t.Errorf("%s: descriptor not found", typ)
			return types.ParamSpec{}, false
		}
		p, ok := findParam(d, path)
		if !ok {
			t.Errorf("%s: param %q not declared", typ, path)
		}
		return p, ok
	}

	for _, v := range visibles {
		if p, ok := lookup(v.typ, v.path); ok && !reflect.DeepEqual(p.VisibleWhen, v.want) {
			t.Errorf("%s.%s VisibleWhen = %s, want %s", v.typ, v.path, fmtCond(p.VisibleWhen), fmtCond(v.want))
		}
	}
	for _, r := range requireds {
		if p, ok := lookup(r.typ, r.path); ok && !reflect.DeepEqual(p.RequiredWhen, r.want) {
			t.Errorf("%s.%s RequiredWhen = %s, want %s", r.typ, r.path, fmtCond(p.RequiredWhen), fmtCond(r.want))
		}
	}
	for _, e := range enumWhens {
		p, ok := lookup(e.typ, e.path)
		if !ok {
			continue
		}
		got := map[string][]any{}
		for _, ce := range p.EnumWhen {
			if ce.When.Param != e.whenParam || ce.When.Eq == nil {
				t.Errorf("%s.%s EnumWhen condition %s, want Eq on %q", e.typ, e.path, fmtCond(&ce.When), e.whenParam)
				continue
			}
			got[fmt.Sprint(ce.When.Eq)] = enumValues(ce.Enum)
		}
		if !reflect.DeepEqual(got, e.want) {
			t.Errorf("%s.%s EnumWhen = %v, want %v", e.typ, e.path, got, e.want)
		}
	}
	for typ, want := range oneOfs {
		d := descs[typ]
		if !reflect.DeepEqual(d.OneOf, []types.OneOfGroup{want}) {
			t.Errorf("%s OneOf = %+v, want [%+v]", typ, d.OneOf, want)
		}
	}

	// "language -> code widget" (design §3/§4.4) cannot be declared: a
	// ParamSpec has one Widget and no conditional form. The descriptor
	// declares the js widget; the wasm (base64) variant is the editor's.
	if p, ok := lookup("xflow.script", "code"); ok && p.Widget != "code" {
		t.Errorf("xflow.script.code Widget = %q, want code", p.Widget)
	}
	// __artifact_file_path is declared (so OneOf names a real param) but
	// never visible.
	if p, ok := lookup("xflow.script", "__artifact_file_path"); ok {
		never := &types.Condition{Not: &types.Condition{}}
		if !reflect.DeepEqual(p.VisibleWhen, never) {
			t.Errorf("xflow.script.__artifact_file_path VisibleWhen = %s, want never", fmtCond(p.VisibleWhen))
		}
	}
}

func enumValues(opts []types.EnumOption) []any {
	out := make([]any, len(opts))
	for i, o := range opts {
		out[i] = o.Value
	}
	return out
}

func fmtCond(c *types.Condition) string {
	if c == nil {
		return "<always>"
	}
	return fmt.Sprintf("%+v", *c)
}

// allowedWidgets is the widget column of the node-form design §3 table.
var allowedWidgets = map[string]bool{
	"text": true, "textarea": true, "password": true, "number": true, "switch": true,
	"select": true, "radio": true, "multi-select": true, "tags": true,
	"key-value": true, "key-expression": true, "object-group": true, "array-table": true,
	"json": true, "code": true, "base64": true, "duration": true, "datetime": true,
	"cron": true, "expression": true, "credential-select": true, "port-select": true,
}

// allowedFormats is the Constraints.Format vocabulary of the descriptor
// contract design §2.2.
var allowedFormats = map[string]bool{
	"duration": true, "cron": true, "expression": true, "sha256-digest": true,
	"json": true, "url": true, "host-port": true, "code": true,
}

// enumValueMatchesType reports whether an Enum value's Go type fits the
// param's declared ParamType.
func enumValueMatchesType(v any, pt types.ParamType) bool {
	switch pt {
	case types.ParamString:
		_, ok := v.(string)
		return ok
	case types.ParamNumber:
		switch v.(type) {
		case int, int64, float64:
			return true
		}
		return false
	case types.ParamBool:
		_, ok := v.(bool)
		return ok
	}
	return false
}

// TestBuiltinDescriptorMetadataConsistent checks the internal consistency of
// every builtin descriptor's editor metadata.
func TestBuiltinDescriptorMetadataConsistent(t *testing.T) {
	for _, vd := range builtinDescriptors(t) {
		d := vd.desc
		groups := map[string]bool{}
		for _, g := range d.Groups {
			if g.Key == "" || groups[g.Key] {
				t.Errorf("%s: group key %q empty or duplicated", d.Type, g.Key)
			}
			groups[g.Key] = true
		}
		topLevel := map[string]bool{}
		for _, p := range d.Params {
			topLevel[p.Name] = true
		}
		for _, o := range d.OneOf {
			switch o.Mode {
			case "", types.OneOfExactly, types.OneOfAtMost, types.OneOfAtLeast:
			default:
				t.Errorf("%s: OneOf mode %q is not a known mode", d.Type, o.Mode)
			}
			if len(o.Params) < 2 {
				t.Errorf("%s: OneOf %v names fewer than two params", d.Type, o.Params)
			}
			for _, name := range o.Params {
				if !topLevel[name] {
					t.Errorf("%s: OneOf names %q, which is not a declared param", d.Type, name)
				}
			}
		}
		if from := d.DynamicOutputsFrom; from != "" {
			isArray := false
			for _, p := range d.Params {
				if p.Name == from && p.Type == types.ParamArray {
					isArray = true
				}
			}
			if !isArray {
				t.Errorf("%s: DynamicOutputsFrom %q is not a declared top-level array param", d.Type, from)
			}
		}

		walkParams(d.Params, "", func(path string, p types.ParamSpec, siblings []types.ParamSpec) {
			where := d.Type + "." + path
			checkParamSpec(t, where, p, siblings, groups)
		})
		// Param names are unique at every level.
		var checkUnique func(prefix string, ps []types.ParamSpec)
		checkUnique = func(prefix string, ps []types.ParamSpec) {
			names := map[string]bool{}
			for _, p := range ps {
				if p.Name == "" || names[p.Name] {
					t.Errorf("%s%s: param name empty or duplicated", prefix, p.Name)
				}
				names[p.Name] = true
				checkUnique(prefix+p.Name+".", p.Fields)
				if p.Item != nil {
					checkUnique(prefix+p.Name+"[].", p.Item.Fields)
				}
			}
		}
		checkUnique(d.Type+".", d.Params)
	}
}

func checkParamSpec(t *testing.T, where string, p types.ParamSpec, siblings []types.ParamSpec, groups map[string]bool) {
	t.Helper()
	switch p.Type {
	case types.ParamString, types.ParamNumber, types.ParamBool, types.ParamArray, types.ParamObject:
	default:
		t.Errorf("%s: unknown Type %q", where, p.Type)
	}
	if p.Item != nil && p.Type != types.ParamArray {
		t.Errorf("%s: Item set on a %s param", where, p.Type)
	}
	if len(p.Fields) > 0 && p.Type != types.ParamObject {
		t.Errorf("%s: Fields set on a %s param", where, p.Type)
	}
	if p.Widget != "" && !allowedWidgets[p.Widget] {
		t.Errorf("%s: Widget %q is not in the design's widget set", where, p.Widget)
	}
	if p.Group != "" && !groups[p.Group] {
		t.Errorf("%s: Group %q is not declared in Descriptor.Groups", where, p.Group)
	}

	// Enum and EnumWhen: one Go type throughout, matching Type, no duplicates.
	var enumType reflect.Type
	checkEnum := func(label string, opts []types.EnumOption) {
		seen := map[any]bool{}
		for _, o := range opts {
			if o.Value == nil {
				t.Errorf("%s: %s has a nil value", where, label)
				continue
			}
			if !enumValueMatchesType(o.Value, p.Type) {
				t.Errorf("%s: %s value %#v (%T) does not fit Type %s", where, label, o.Value, o.Value, p.Type)
			}
			if enumType == nil {
				enumType = reflect.TypeOf(o.Value)
			} else if reflect.TypeOf(o.Value) != enumType {
				t.Errorf("%s: %s mixes value types %s and %T", where, label, enumType, o.Value)
			}
			if seen[o.Value] {
				t.Errorf("%s: %s repeats %#v", where, label, o.Value)
			}
			seen[o.Value] = true
		}
	}
	checkEnum("Enum", p.Enum)
	for i, ce := range p.EnumWhen {
		if len(ce.Enum) == 0 {
			t.Errorf("%s: EnumWhen[%d] is empty", where, i)
		}
		checkEnum(fmt.Sprintf("EnumWhen[%d]", i), ce.Enum)
		checkCondition(t, where, fmt.Sprintf("EnumWhen[%d].When", i), &ce.When, p.Name, siblings)
		// Every conditional value must also be a member of the fallback
		// Enum, so an unresolved condition never rejects a valid value.
		if len(p.Enum) > 0 {
			for _, o := range ce.Enum {
				if !enumContains(p.Enum, o.Value) {
					t.Errorf("%s: EnumWhen[%d] value %#v missing from Enum", where, i, o.Value)
				}
			}
		}
	}
	if p.Default != nil && len(p.Enum) > 0 && !enumContains(p.Enum, p.Default) {
		t.Errorf("%s: Default %#v is not a member of Enum %v", where, p.Default, enumValues(p.Enum))
	}

	checkCondition(t, where, "VisibleWhen", p.VisibleWhen, p.Name, siblings)
	checkCondition(t, where, "RequiredWhen", p.RequiredWhen, p.Name, siblings)
	if p.Required && p.RequiredWhen != nil {
		t.Errorf("%s: RequiredWhen set on a Required param (it would be ignored)", where)
	}
	if p.Required && p.Default != nil {
		t.Errorf("%s: Required with a Default; the SDK would never write the Default", where)
	}

	if c := p.Constraints; c != nil {
		if c.Format != "" && !allowedFormats[c.Format] {
			t.Errorf("%s: Format %q is not a known format", where, c.Format)
		}
		if c.Pattern != "" {
			if _, err := regexp.Compile(c.Pattern); err != nil {
				t.Errorf("%s: Pattern %q does not compile: %v", where, c.Pattern, err)
			}
		}
		if (c.Min != nil || c.Max != nil) && p.Type != types.ParamNumber {
			t.Errorf("%s: Min/Max on a %s param", where, p.Type)
		}
		if c.Min != nil && c.Max != nil && *c.Min > *c.Max {
			t.Errorf("%s: Min %v > Max %v", where, *c.Min, *c.Max)
		}
	}
}

func enumContains(opts []types.EnumOption, v any) bool {
	for _, o := range opts {
		if reflect.DeepEqual(o.Value, v) {
			return true
		}
	}
	return false
}

// checkCondition asserts every Param a Condition tree names is a sibling of
// the param carrying it (and not the param itself), and that each leaf tests
// something.
func checkCondition(t *testing.T, where, label string, c *types.Condition, self string, siblings []types.ParamSpec) {
	t.Helper()
	if c == nil {
		return
	}
	hasLeafClause := c.Eq != nil || len(c.In) > 0 || c.Truthy != nil
	if c.Param == "" && hasLeafClause {
		t.Errorf("%s: %s tests a value but names no Param", where, label)
	}
	if c.Param != "" {
		if !hasLeafClause {
			t.Errorf("%s: %s names Param %q but tests nothing", where, label, c.Param)
		}
		if c.Param == self {
			t.Errorf("%s: %s refers to the param itself", where, label)
		}
		found := false
		for _, s := range siblings {
			if s.Name == c.Param {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: %s refers to %q, which is not a sibling param", where, label, c.Param)
		}
	}
	for i := range c.AllOf {
		checkCondition(t, where, fmt.Sprintf("%s.AllOf[%d]", label, i), &c.AllOf[i], self, siblings)
	}
	for i := range c.AnyOf {
		checkCondition(t, where, fmt.Sprintf("%s.AnyOf[%d]", label, i), &c.AnyOf[i], self, siblings)
	}
	checkCondition(t, where, label+".Not", c.Not, self, siblings)
}

// jsonFallbackParams lists the params an editor has no structured control for
// and therefore renders with the JSON editor: an object with neither Fields
// nor a free-map widget, an array with no Item, and an array whose Item is
// itself such an object.
func jsonFallbackParams(typ string, params []types.ParamSpec, prefix string) []string {
	var out []string
	freeMap := map[string]bool{"key-value": true, "key-expression": true}
	var objectFallsBack func(p types.ParamSpec) bool
	objectFallsBack = func(p types.ParamSpec) bool {
		return p.Widget == "json" || (len(p.Fields) == 0 && !freeMap[p.Widget])
	}
	for _, p := range params {
		path := prefix + p.Name
		switch p.Type {
		case types.ParamObject:
			if objectFallsBack(p) {
				out = append(out, typ+"."+path)
			} else {
				out = append(out, jsonFallbackParams(typ, p.Fields, path+".")...)
			}
		case types.ParamArray:
			switch {
			case p.Item == nil || p.Widget == "json":
				out = append(out, typ+"."+path)
			case p.Item.Type == types.ParamObject && objectFallsBack(*p.Item):
				out = append(out, typ+"."+path)
			case p.Item.Type == types.ParamObject:
				out = append(out, jsonFallbackParams(typ, p.Item.Fields, path+"[].")...)
			case p.Item.Type == types.ParamArray:
				out = append(out, typ+"."+path)
			}
		default:
			if p.Widget == "json" {
				out = append(out, typ+"."+path)
			}
		}
	}
	return out
}

// TestBuiltinJSONFallbackMatchesDesign pins the node-form design §3.1 "v1
// degrade list": exactly these params fall back to the JSON editor.
func TestBuiltinJSONFallbackMatchesDesign(t *testing.T) {
	want := []string{
		"xflow.browser.cdp.harvest",
		"xflow.browser.cdp.plan",
		"xflow.browser.cdp.seed_cookies",
		"xflow.browser.cdp.ttl",
		"xflow.database.data",
		"xflow.database.where",
		"xflow.function.params",
		"xflow.grpc.metadata",
		"xflow.grpc.options",
		"xflow.grpc.request",
		"xflow.http.body",
		"xflow.http.options",
		"xflow.map.body",
		"xflow.notification.data",
		"xflow.transform.set.fields",
	}
	var got []string
	for _, vd := range builtinDescriptors(t) {
		got = append(got, jsonFallbackParams(vd.desc.Type, vd.desc.Params, "")...)
	}
	sort.Strings(got)
	got = dedupSorted(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("JSON-fallback params differ from the design's degrade list\n got: %v\nwant: %v", got, want)
	}
}

func dedupSorted(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}
