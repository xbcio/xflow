package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/xbcio/xflow/types"
)

// compilerVersion identifies the graph compiler format.
const compilerVersion = "v1"

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
}

// graphSerializedForm is the on-wire / at-rest JSON representation of a Graph.
// It mirrors the private Graph fields so that encoding/json can round-trip the
// compiled graph through Redis (distributed backend) or any other JSON store
// without leaking the private field names into the Graph API.
type graphSerializedForm struct {
	GraphHash       string         `json:"graph_hash"`
	Name            string         `json:"name"`
	WorkflowVersion string         `json:"workflow_version"`
	CompilerVersion string         `json:"compiler_version"`
	Nodes           []NodeMeta     `json:"nodes"`
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
}

// MarshalJSON implements json.Marshaler so that encoding/json can serialize a
// Graph even though all its fields are unexported. The output uses
// graphSerializedForm as a stable, versioned wire format.
func (g *Graph) MarshalJSON() ([]byte, error) {
	if g == nil {
		return []byte("null"), nil
	}
	return json.Marshal(graphSerializedForm{
		GraphHash:       g.graphHash,
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
	})
}

// UnmarshalJSON implements json.Unmarshaler, the inverse of MarshalJSON.
// It populates the Graph's unexported fields from the stable wire format so
// that a deserialized graph is fully functional without recompilation.
func (g *Graph) UnmarshalJSON(data []byte) error {
	var sf graphSerializedForm
	if err := json.Unmarshal(data, &sf); err != nil {
		return err
	}
	g.graphHash = sf.GraphHash
	g.name = sf.Name
	g.workflowVersion = sf.WorkflowVersion
	g.compilerVersion = sf.CompilerVersion
	g.nodes = sf.Nodes
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
	return nil
}
