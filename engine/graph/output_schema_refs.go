package graph

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/xbcio/xflow/types"
)

// nodesFieldPathPattern matches a literal $nodes['name'] subscript followed by
// its static field path: any run of .ident or ['literal'] segments. A computed
// or numeric subscript ends the match, which is where the static path ends.
var nodesFieldPathPattern = regexp.MustCompile(
	`\$nodes\[['"]([^'"]+)['"]\]((?:\.[A-Za-z_$][A-Za-z0-9_$]*|\[['"][^'"]+['"]\])*)`)

var fieldSegmentPattern = regexp.MustCompile(`\.([A-Za-z_$][A-Za-z0-9_$]*)|\[['"]([^'"]+)['"]\]`)

// validateOutputSchemaRefs is the compile-time existence check of spec §6.3:
// a static $nodes['x'].a.b reference to a node that declares output_schema
// must name fields declared under its properties. Checking stops at a level
// without properties (open object, scalar, or array), so only the subset the
// spec defines is enforced. Nodes without output_schema are never checked.
// Body sub-graph parameters are skipped, as in deriveNodesRefs: their
// references resolve inside the body package, not against these nodes.
func validateOutputSchemaRefs(def *types.WorkflowDef) error {
	schemas := make(map[string]map[string]any, len(def.Nodes))
	for _, nd := range def.Nodes {
		if len(nd.OutputSchema) != 0 {
			schemas[nd.Name] = nd.OutputSchema
		}
	}
	if len(schemas) == 0 {
		return nil
	}
	for _, nd := range def.Nodes {
		var refs []string
		hasBody := declaresSubgraphBody(nd.Parameters)
		keys := make([]string, 0, len(nd.Parameters))
		for key := range nd.Parameters {
			if hasBody && key == subgraphBodyKey {
				continue
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			collectStrings(nd.Parameters[key], &refs)
		}
		for _, s := range refs {
			for _, m := range nodesFieldPathPattern.FindAllStringSubmatch(s, -1) {
				schema, ok := schemas[m[1]]
				if !ok {
					continue
				}
				if missing := missingSchemaField(schema, m[2]); missing != "" {
					return fmt.Errorf("node %q: $nodes['%s'] references field %q not declared in its output_schema",
						nd.Name, m[1], missing)
				}
			}
		}
	}
	return nil
}

// missingSchemaField walks path through nested properties and returns the
// dotted path of the first undeclared segment, or "" if none is undeclared.
func missingSchemaField(schema map[string]any, path string) string {
	var walked []string
	for _, seg := range fieldSegmentPattern.FindAllStringSubmatch(path, -1) {
		name := seg[1]
		if name == "" {
			name = seg[2]
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok || len(props) == 0 {
			return ""
		}
		walked = append(walked, name)
		child, ok := props[name]
		if !ok {
			return strings.Join(walked, ".")
		}
		if schema, ok = child.(map[string]any); !ok {
			return ""
		}
	}
	return ""
}

func collectStrings(v any, out *[]string) {
	switch val := v.(type) {
	case string:
		*out = append(*out, val)
	case map[string]any:
		for _, child := range val {
			collectStrings(child, out)
		}
	case []any:
		for _, child := range val {
			collectStrings(child, out)
		}
	}
}
