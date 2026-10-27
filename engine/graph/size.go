package graph

// Rough per-element costs used by EstimatedHeapBytes. They are deliberately on
// the high side: the estimate feeds a cache budget, and over-estimating evicts
// earlier — which costs a re-decode, never correctness.
const (
	graphBaseBytes  = 512
	nodeBaseBytes   = 256
	unitBaseBytes   = 128
	groupBaseBytes  = 256
	mapEntryBytes   = 64
	indexEntryBytes = 48
	portOutBytes    = 32
	edgeBytes       = 48
)

// EstimatedHeapBytes returns a rough heap footprint of the decoded graph.
//
// It exists so the process-local graph caches can bound themselves by size
// without re-marshalling the graph: callers that decoded the graph from JSON
// have the wire length and should use it instead (decoded structures cost a
// small multiple of the JSON), but graphs created in-process have no wire form
// and would otherwise be sized by a flat constant.
//
// The value is an estimate, not an accounting figure: it counts the string and
// collection shapes the decoder allocates and ignores padding, map bucket
// overhead and interface boxing. It also ignores expandedDef, the definition a
// compiled graph retains: graphs decoded from JSON do not carry one, and for a
// compiled graph its node structs and cloned parameter maps add a fraction of
// what the graph layer above already charges.
func (g *Graph) EstimatedHeapBytes() int64 {
	if g == nil {
		return 0
	}
	total := int64(graphBaseBytes) +
		int64(len(g.name)+len(g.graphHash)+len(g.workflowVersion)+len(g.compilerVersion))
	total += int64(len(g.inDegree)) * 8
	total += int64(len(g.index)+len(g.entryIndexes)) * indexEntryBytes

	for i := range g.nodes {
		n := &g.nodes[i]
		total += nodeBaseBytes
		total += int64(len(n.Name) + len(n.Type) + len(n.Kind) + len(n.OnError) + len(n.MergeMode))
		total += mapStringsBytes(n.Parameters) + mapStringsBytes(n.PinOutput)
		total += int64(len(n.PortOuts)) * portOutBytes
		if n.Output != nil {
			total += 128
		}
		if n.RunnerSelector != nil {
			total += 256
		}
		if n.Retry != nil {
			total += 128
		}
		if n.SubWorkflow != nil {
			total += 256
		}
		if n.Body != nil {
			total += 4096
		}
	}
	for i := range g.outEdges {
		total += int64(len(g.outEdges[i])) * edgeBytes
	}
	for i := range g.inEdges {
		total += int64(len(g.inEdges[i])) * edgeBytes
	}

	// The unit layer mirrors the node layer 1:1 when no groups are declared, so
	// leaving it out would undercount every ordinary graph by its scheduling
	// copy — and grouped graphs by more, since each group also carries boundary
	// and error-output endpoints.
	for i := range g.units {
		u := &g.units[i]
		total += unitBaseBytes + int64(len(u.Name))
		if u.RunnerSelector != nil {
			total += 256
		}
		if u.Retry != nil {
			total += 128
		}
	}
	for i := range g.unitOutEdges {
		total += int64(len(g.unitOutEdges[i])) * edgeBytes
	}
	for i := range g.unitInEdges {
		total += int64(len(g.unitInEdges[i])) * edgeBytes
	}
	for i := range g.groups {
		grp := &g.groups[i]
		total += groupBaseBytes + int64(len(grp.Name)+len(grp.OnError))
		total += int64(len(grp.Members)) * 8
		total += int64(len(grp.BoundaryInputs)+len(grp.BoundaryOutputs)+len(grp.ErrorOutputs)) * mapEntryBytes
		if grp.RunnerSelector != nil {
			total += 256
		}
		if grp.Retry != nil {
			total += 128
		}
	}

	total += refMapBytes(g.supplyRefs) + refMapBytes(g.nodesRefs)
	total += int64(len(g.supplyIndexes)) * indexEntryBytes
	total += int64(len(g.params)) * (mapEntryBytes + 128)
	total += mapStringsBytes(g.vars) + mapStringsBytes(g.config)
	return total
}

// refMapBytes sizes map[int][]string indexes (supplyRefs, nodesRefs): one map
// entry per key plus the slice header and contents per value.
func refMapBytes(m map[int][]string) int64 {
	if len(m) == 0 {
		return 0
	}
	total := int64(len(m)) * (mapEntryBytes + 24)
	for _, refs := range m {
		for _, s := range refs {
			total += int64(len(s)) + 16
		}
	}
	return total
}

// mapStringsBytes sizes a decoded JSON object: one map entry plus its key, plus
// whatever the value itself holds.
func mapStringsBytes(m map[string]any) int64 {
	if len(m) == 0 {
		return 0
	}
	total := int64(len(m)) * mapEntryBytes
	for k, v := range m {
		total += int64(len(k)) + valueBytes(v)
	}
	return total
}

// valueBytes walks a decoded JSON value. Parameter values are where the real
// weight sits — request bodies, scripts and templates are strings of arbitrary
// length — so charging them a flat per-entry cost would let a graph full of
// large payloads look tiny to the cache budget.
func valueBytes(v any) int64 { return valueBytesDepth(v, 0) }

// maxValueDepth bounds the walk. Decoded JSON is acyclic and shallow, but a
// graph can also be assembled programmatically, and an accidentally
// self-referential map would otherwise recurse until the stack dies.
const maxValueDepth = 32

func valueBytesDepth(v any, depth int) int64 {
	if depth >= maxValueDepth {
		return 64
	}
	switch t := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(t)) + 16
	case bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 16
	case []any:
		total := int64(24)
		for _, e := range t {
			total += valueBytesDepth(e, depth+1)
		}
		return total
	case []string:
		total := int64(24)
		for _, s := range t {
			total += int64(len(s)) + 16
		}
		return total
	case map[string]any:
		if len(t) == 0 {
			return 0
		}
		total := int64(len(t)) * mapEntryBytes
		for k, e := range t {
			total += int64(len(k)) + valueBytesDepth(e, depth+1)
		}
		return total
	case map[string]string:
		total := int64(len(t)) * mapEntryBytes
		for k, s := range t {
			total += int64(len(k)+len(s)) + 16
		}
		return total
	default:
		// Types reached only from hand-built graphs (structs, typed slices).
		return 32
	}
}
