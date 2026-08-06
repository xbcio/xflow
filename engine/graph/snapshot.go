package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/xbcio/xflow/types"
)

// compilerVersion identifies the graph compiler format. Bumped to v3 when
// group package projection (PackageHash on GroupMeta) was introduced — the
// deterministic package hash now enters the graph hash via GroupMeta.
const compilerVersion = "v3"

// wireNodeMeta is the on-wire representation of one NodeMeta entry. GroupIdx
// is carried as *int so decode can distinguish "field absent" (legacy,
// pre-group snapshot) from "field present with value -1" (modern, explicitly
// ungrouped) — a plain int zero value cannot make that distinction, and 0 is
// itself a legitimate group index. GroupIdxAlt accepts the historical
// Go-default field name "GroupIdx" (no json tag) that earlier snapshots
// (including ones already persisted by this repository before this wire
// format existed) wrote instead of the canonical snake_case "group_idx" —
// decode must accept either key and fail closed if both are present with
// conflicting values, never silently prefer one.
type wireNodeMeta struct {
	Name           string                `json:"Name"`
	Type           string                `json:"Type"`
	Kind           types.NodeKind        `json:"Kind"`
	Version        int                   `json:"Version"`
	OnError        string                `json:"OnError"`
	RunnerSelector *types.RunnerSelector `json:"RunnerSelector"`
	MergeMode      string                `json:"MergeMode"`
	Parameters     map[string]any        `json:"Parameters"`
	PortOuts       []string              `json:"PortOuts"`
	Retry          *types.RetrySettings  `json:"Retry"`
	GroupIdx       *int                  `json:"group_idx,omitempty"`
	GroupIdxAlt    *int                  `json:"GroupIdx,omitempty"`
}

func toWireNodeMeta(n NodeMeta) wireNodeMeta {
	idx := n.GroupIdx
	return wireNodeMeta{
		Name:           n.Name,
		Type:           n.Type,
		Kind:           n.Kind,
		Version:        n.Version,
		OnError:        n.OnError,
		RunnerSelector: n.RunnerSelector,
		MergeMode:      n.MergeMode,
		Parameters:     n.Parameters,
		PortOuts:       n.PortOuts,
		Retry:          n.Retry,
		GroupIdx:       &idx,
	}
}

// nodeGroupIdxPresence is the outcome of inspecting one decoded wireNodeMeta
// for group_idx / GroupIdx presence.
type nodeGroupIdxPresence struct {
	present bool
	value   int
}

// resolveGroupIdxPresence reconciles the canonical "group_idx" key and the
// legacy Go-default "GroupIdx" key on one wire node. Both present with
// different values is a corrupt/untrusted snapshot and fails closed; either
// one present alone is accepted; neither present means legacy-absent.
func resolveGroupIdxPresence(w wireNodeMeta) (nodeGroupIdxPresence, error) {
	switch {
	case w.GroupIdx != nil && w.GroupIdxAlt != nil:
		if *w.GroupIdx != *w.GroupIdxAlt {
			return nodeGroupIdxPresence{}, fmt.Errorf("node %q: conflicting group_idx (%d) and GroupIdx (%d) wire keys", w.Name, *w.GroupIdx, *w.GroupIdxAlt)
		}
		return nodeGroupIdxPresence{present: true, value: *w.GroupIdx}, nil
	case w.GroupIdx != nil:
		return nodeGroupIdxPresence{present: true, value: *w.GroupIdx}, nil
	case w.GroupIdxAlt != nil:
		return nodeGroupIdxPresence{present: true, value: *w.GroupIdxAlt}, nil
	default:
		return nodeGroupIdxPresence{present: false}, nil
	}
}

// decodeWireNodes decodes the wire node array and classifies the snapshot as
// modern (every node carries an explicit group_idx/GroupIdx, whether or not
// any node is actually grouped) or legacy (no node carries the field at all —
// a pre-group snapshot compiled before NodeMeta.GroupIdx existed). A mix of
// present and absent across nodes in the same snapshot is untrusted/corrupt
// and fails closed rather than guessing.
func decodeWireNodes(raw []wireNodeMeta) ([]NodeMeta, bool, error) {
	nodes := make([]NodeMeta, len(raw))
	presentCount := 0
	for i, w := range raw {
		p, err := resolveGroupIdxPresence(w)
		if err != nil {
			return nil, false, err
		}
		if p.present {
			presentCount++
		}
		nodes[i] = NodeMeta{
			Name:           w.Name,
			Type:           w.Type,
			Kind:           w.Kind,
			Version:        w.Version,
			OnError:        w.OnError,
			RunnerSelector: w.RunnerSelector,
			MergeMode:      w.MergeMode,
			Parameters:     w.Parameters,
			PortOuts:       w.PortOuts,
			Retry:          w.Retry,
			GroupIdx:       p.value,
		}
		if !p.present {
			nodes[i].GroupIdx = -1
		}
	}
	if presentCount == 0 {
		// Legacy: no node in this snapshot carries a group_idx/GroupIdx field
		// at all. All nodes are already normalized to GroupIdx=-1 above.
		return nodes, true, nil
	}
	if presentCount != len(raw) {
		return nil, false, fmt.Errorf("graph snapshot: %d of %d nodes carry group_idx/GroupIdx; a snapshot must have it on either all nodes or none (partial presence is untrusted)", presentCount, len(raw))
	}
	return nodes, false, nil
}

func cloneStringAnyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = cloneAny(value)
	}
	return dst
}

// cloneAny returns a deep copy of value via reflection. It is the foundation
// of the Graph's deep-immutable accessors: every map/slice/struct reachable
// from Parameters/Vars/Config is reproduced as an independent tree so callers
// cannot mutate Graph state through a returned reference.
func cloneAny(value any) any {
	if value == nil {
		return nil
	}
	return cloneReflectValue(reflect.ValueOf(value)).Interface()
}

// cloneStringSlice returns a fresh copy of src so callers cannot mutate the
// Graph's internal slice. Returns nil for nil input to preserve the zero value.
func cloneStringSlice(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// cloneRetry returns a fresh *types.RetrySettings so callers cannot mutate the
// Graph's internal value. RetrySettings is a value-only struct (no maps/slices/
// pointers), so a shallow pointer copy is a complete deep copy.
func cloneRetry(r *types.RetrySettings) *types.RetrySettings {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

func cloneReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		return cloneReflectValue(value.Elem())
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			clone.SetMapIndex(iter.Key(), cloneReflectValue(iter.Value()))
		}
		return clone
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			clone.Index(i).Set(cloneReflectValue(value.Index(i)))
		}
		return clone
	case reflect.Array:
		clone := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			clone.Index(i).Set(cloneReflectValue(value.Index(i)))
		}
		return clone
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.New(value.Type().Elem())
		clone.Elem().Set(cloneReflectValue(value.Elem()))
		return clone
	case reflect.Struct:
		// Validator guarantees that every field of a struct in the value domain is
		// exported (CanInterface) and recursively auditable. We therefore build the
		// clone field-by-field rather than shallow-copying the whole struct, which
		// would share unexported mutable fields (maps/slices/pointers) with the
		// original value.
		clone := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			sourceField := value.Field(i)
			targetField := clone.Field(i)
			if !sourceField.CanInterface() || !targetField.CanSet() {
				// Defensive: this branch should be unreachable because the validator
				// rejects structs with unexported fields before cloning.
				continue
			}
			targetField.Set(cloneReflectValue(sourceField))
		}
		return clone
	default:
		return value
	}
}

// supportedValueDomain describes the immutable value domain permitted inside
// Parameters, Vars, and Config. The Graph deep-clones whatever it stores, but
// the domain gate additionally rejects mutable reference types (pointers,
// funcs, chans) and non-string-keyed maps so an arbitrary caller-owned alias
// can never be smuggled into the compiled Graph. The accepted kinds are:
//
//   - nil
//   - bool, string
//   - integers (int*, uint*), floats (float*)
//   - map with string keys, whose values are in the domain
//   - slice / array, whose elements are in the domain
//   - struct, whose fields are in the domain
//
// Rejected kinds: Pointer, Func, Chan, UnsafePointer, and any Map whose key
// kind is not String. A rejected value causes Compile to fail.
func validateValueDomain(path string, value any) error {
	return validateReflectDomain(path, reflect.ValueOf(value))
}

func validateReflectDomain(path string, value reflect.Value) error {
	if !value.IsValid() {
		// nil interface or zero value — allowed.
		return nil
	}
	switch value.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("value domain: %s has map with non-string key %s; only string keys are supported", path, value.Type().Key())
		}
		iter := value.MapRange()
		for iter.Next() {
			if err := validateReflectDomain(path+"."+iter.Key().String(), iter.Value()); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateReflectDomain(fmt.Sprintf("%s[%d]", path, i), value.Index(i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			f := value.Type().Field(i)
			if !f.IsExported() {
				return fmt.Errorf("value domain: %s.%s is an unexported field; it cannot be audited for deep immutability. Convert the value to an exported value struct or map[string]any", path, f.Name)
			}
			if err := validateReflectDomain(path+"."+f.Name, value.Field(i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return validateReflectDomain(path, value.Elem())
	case reflect.Pointer, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return fmt.Errorf("value domain: %s has unsupported mutable %s value; Parameters/Vars/Config must be immutable value types", path, value.Kind())
	default:
		return fmt.Errorf("value domain: %s has unsupported %s value", path, value.Kind())
	}
}

// validateGraphValueDomain ensures every Parameters/Vars/Config value stored on
// the compiled Graph belongs to the supported immutable value domain. It is
// the Compile-time gate that prevents an arbitrary caller-owned mutable
// reference (pointer, func, chan, non-string-keyed map) from being smuggled
// into the Graph. See validateValueDomain for the accepted kinds.
func validateGraphValueDomain(g *Graph) error {
	if err := validateValueDomain("vars", g.vars); err != nil {
		return err
	}
	if err := validateValueDomain("config", g.config); err != nil {
		return err
	}
	for i := range g.nodes {
		name := g.nodes[i].Name
		if err := validateValueDomain("node "+name+" parameters", g.nodes[i].Parameters); err != nil {
			return err
		}
	}
	return nil
}

func assignGraphHash(g *Graph) error {
	payload := graphHashPayload{
		Name:            g.name,
		WorkflowVersion: g.workflowVersion,
		CompilerVersion: g.compilerVersion,
		Nodes:           g.nodes,
		Index:           g.index,
		EntryIndexes:    g.entryIndexes,
		OutEdges:        g.outEdges,
		InEdges:         g.inEdges,
		InDegree:        g.inDegree,
		Vars:            g.vars,
		Config:          g.config,
		AllowCycles:     g.allowCycles,
		StartIdx:        g.startIdx,
		MaxAutoDepth:    g.maxAutoDepth,
		Groups:          g.groups,
		Units:           g.units,
		UnitOutEdges:    g.unitOutEdges,
		UnitInDegree:    g.unitInDegree,
		SupplyRefs:      g.supplyRefs,
		MapBodies:       g.mapBodies,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal graph hash payload: %w", err)
	}
	sum := sha256.Sum256(data)
	g.graphHash = "sha256:" + hex.EncodeToString(sum[:])
	return nil
}

type graphHashPayload struct {
	Name            string
	WorkflowVersion string
	CompilerVersion string
	Nodes           []NodeMeta
	Index           map[string]int
	EntryIndexes    map[string]int
	OutEdges        [][]Edge
	InEdges         [][]Edge
	InDegree        []int
	Vars            map[string]any
	Config          map[string]any
	AllowCycles     bool
	StartIdx        int
	MaxAutoDepth    int
	Groups          []GroupMeta
	Units           []UnitMeta
	UnitOutEdges    [][]UnitEdge
	UnitInDegree    []int
	// SupplyRefs must carry an explicit omitempty: this struct's fields have no
	// json tags, so without it every pre-existing graph's hash payload would
	// gain a "SupplyRefs":null and its graphHash would change, invalidating the
	// hash recorded on every persisted execution.
	SupplyRefs map[int][]string `json:",omitempty"`
	// MapBodies must carry the same explicit omitempty, for the same reason:
	// a graph with no map bodies must hash byte-identically to before this
	// field existed. Unlike SupplyRefs, a body's content DOES belong in the
	// hash -- two workflows differing only in what a map node's body does are
	// different workflows, and the projected package (not just its member
	// names) is what the runner actually executes, so it must be part of the
	// graph's identity the same way GroupMeta.PackageHash already is for groups.
	MapBodies map[int]*MapBodyPackage `json:",omitempty"`
}

// graphSerializedForm is the on-wire / at-rest JSON representation of a Graph.
// It mirrors the private Graph fields so that encoding/json can round-trip the
// compiled graph through Redis (distributed backend) or any other JSON store
// without leaking the private field names into the Graph API.
//
// Groups carries the co-location group definitions (compiler v2). The
// derived two-layer unit IR (units/unitInDegree/unitOutEdges/unitInEdges/
// nodeUnit) is intentionally NOT serialized: UnmarshalJSON re-derives it via
// buildUnits from Nodes[i].GroupIdx + Groups, the same deterministic
// construction Compile uses. This keeps the wire format smaller and — more
// importantly — guarantees the round-tripped Graph's unit topology (in
// particular UnitCount/UnitInDegreeAt, which size the durable Redis
// remaining/in-degree counters) always matches what Compile would have
// produced, instead of silently degrading to zero units after a JSON
// round-trip (see .claude/plans F9).
//
// Nodes uses wireNodeMeta (not NodeMeta directly) so decode can distinguish a
// true legacy snapshot (no node carries group_idx/GroupIdx at all — compiled
// before groups existed) from a modern grouped-or-ungrouped snapshot (every
// node explicitly carries the field, possibly -1). See T1 in
// .claude/plans/2026-07-27-node-group-milestone-b.md: a legacy snapshot must
// degrade to a 1:1 unit mapping, while a snapshot that already has grouped
// nodes (some GroupIdx >= 0) but somehow lost its Groups/units data must fail
// closed instead of silently rebuilding an incorrect ungrouped topology.
type graphSerializedForm struct {
	GraphHash       string         `json:"graph_hash"`
	Name            string         `json:"name"`
	WorkflowVersion string         `json:"workflow_version"`
	CompilerVersion string         `json:"compiler_version"`
	Nodes           []wireNodeMeta `json:"nodes"`
	Index           map[string]int `json:"index"`
	EntryIndexes    map[string]int `json:"entry_indexes"`
	OutEdges        [][]Edge       `json:"out_edges"`
	InEdges         [][]Edge       `json:"in_edges"`
	InDegree        []int          `json:"in_degree"`
	Vars            map[string]any `json:"vars,omitempty"`
	Config          map[string]any `json:"config,omitempty"`
	AllowCycles     bool           `json:"allow_cycles"`
	StartIdx        int            `json:"start_idx"`
	MaxAutoDepth    int            `json:"max_auto_depth"`
	Groups          []GroupMeta    `json:"groups,omitempty"`
	// SupplyRefs maps a consumer node index to the supply node names it depends
	// on. Unlike the unit IR it cannot be re-derived on decode: it comes from
	// WorkflowDef.DependencyEdges, which is not part of the snapshot. The
	// supplyIndexes map, by contrast, IS re-derived from Nodes[i].Kind.
	SupplyRefs map[int][]string `json:"supply_refs,omitempty"`
	// MapBodies maps an xflow.map node's index to its projected body package.
	// This cannot be re-derived on decode either: it comes from the map node's
	// "body" Parameters, projected once at compile time (registerNodes), and
	// Parameters travel on the snapshot but ProjectMapBodyPackage is never
	// re-run on load. Without this field a graph reloaded from Redis (a second
	// server replica, or the same server after its in-memory graph cache is
	// evicted on terminal state) has MapBodyAt return nil for every map node,
	// so BuildSubgraphLease ships an empty Package/PackageHash and every batch
	// fails at the runner with ErrPackageMissing.
	MapBodies map[int]*MapBodyPackage `json:"map_bodies,omitempty"`
}

// MarshalJSON implements json.Marshaler so that encoding/json can serialize a
// Graph even though all its fields are unexported. The output uses
// graphSerializedForm as a stable, versioned wire format.
func (g *Graph) MarshalJSON() ([]byte, error) {
	if g == nil {
		return []byte("null"), nil
	}
	wireNodes := make([]wireNodeMeta, len(g.nodes))
	for i, n := range g.nodes {
		wireNodes[i] = toWireNodeMeta(n)
	}
	return json.Marshal(graphSerializedForm{
		GraphHash:       g.graphHash,
		Name:            g.name,
		WorkflowVersion: g.workflowVersion,
		CompilerVersion: g.compilerVersion,
		Nodes:           wireNodes,
		Index:           g.index,
		EntryIndexes:    g.entryIndexes,
		OutEdges:        g.outEdges,
		InEdges:         g.inEdges,
		InDegree:        g.inDegree,
		Vars:            g.vars,
		Config:          g.config,
		AllowCycles:     g.allowCycles,
		StartIdx:        g.startIdx,
		MaxAutoDepth:    g.maxAutoDepth,
		Groups:          g.groups,
		SupplyRefs:      g.supplyRefs,
		MapBodies:       g.mapBodies,
	})
}

// ErrGroupedSnapshotMissingUnitIR is returned by UnmarshalJSON when a snapshot
// has nodes with an explicit GroupIdx >= 0 (grouped) but empty Groups — the
// snapshot claims to be grouped but carries no group definitions to rebuild
// the unit IR from. This must never be silently treated as ungrouped: doing
// so would zero out GroupIdx and let the durable remaining/in-degree counters
// be seeded from the wrong (ungrouped) unit count.
var ErrGroupedSnapshotMissingUnitIR = errors.New("graph snapshot: grouped nodes present but no group definitions to rebuild unit IR from")

// UnmarshalJSON implements json.Unmarshaler, the inverse of MarshalJSON.
// It populates the Graph's unexported fields from the stable wire format so
// that a deserialized graph is fully functional without recompilation, then
// re-derives the two-layer unit IR (buildUnits) exactly as Compile does. A
// grouped snapshot whose unit topology cannot be rebuilt (e.g. a group
// boundary cycle, or grouped nodes with no matching Groups entries) fails
// closed instead of silently producing a Graph with a zeroed/mismatched
// UnitCount. A true legacy snapshot (no node carries group_idx/GroupIdx at
// all) degrades to a 1:1 unit mapping with every node's GroupIdx normalized
// to -1.
func (g *Graph) UnmarshalJSON(data []byte) error {
	var sf graphSerializedForm
	if err := json.Unmarshal(data, &sf); err != nil {
		return err
	}
	nodes, legacy, err := decodeWireNodes(sf.Nodes)
	if err != nil {
		return fmt.Errorf("decode graph snapshot nodes: %w", err)
	}
	if legacy && len(sf.Groups) > 0 {
		// A snapshot with no per-node group_idx field at all but non-empty
		// Groups is internally inconsistent (Groups references node indexes
		// that decodeWireNodes just normalized to ungrouped) — untrusted,
		// fail closed rather than guess which side is authoritative.
		return fmt.Errorf("graph snapshot: legacy nodes (no group_idx field) but non-empty groups: %w", ErrGroupedSnapshotMissingUnitIR)
	}
	if !legacy {
		hasGrouped := false
		for _, n := range nodes {
			if n.GroupIdx >= 0 {
				hasGrouped = true
				break
			}
		}
		if hasGrouped && len(sf.Groups) == 0 {
			return ErrGroupedSnapshotMissingUnitIR
		}
	}
	g.graphHash = sf.GraphHash
	g.name = sf.Name
	g.workflowVersion = sf.WorkflowVersion
	g.compilerVersion = sf.CompilerVersion
	g.nodes = nodes
	g.index = sf.Index
	g.entryIndexes = sf.EntryIndexes
	g.outEdges = sf.OutEdges
	g.inEdges = sf.InEdges
	g.inDegree = sf.InDegree
	g.vars = sf.Vars
	g.config = sf.Config
	g.allowCycles = sf.AllowCycles
	g.startIdx = sf.StartIdx
	g.maxAutoDepth = sf.MaxAutoDepth
	g.groups = sf.Groups
	g.supplyRefs = sf.SupplyRefs
	g.mapBodies = sf.MapBodies
	// Rebuild supplyIndexes from node kinds — the same deterministic derivation
	// Compile performs, so a round-tripped graph needs no WorkflowDef.
	g.supplyIndexes = make(map[string]int)
	for i := range g.nodes {
		if g.nodes[i].Kind == types.NodeKindSupply {
			g.supplyIndexes[g.nodes[i].Name] = i
		}
	}
	if err := buildUnits(g); err != nil {
		return fmt.Errorf("rebuild units after graph deserialization: %w", err)
	}
	return nil
}
