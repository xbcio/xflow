package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
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

func cloneAny(value any) any {
	if value == nil {
		return nil
	}
	return cloneReflectValue(reflect.ValueOf(value)).Interface()
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
		clone := reflect.New(value.Type()).Elem()
		clone.Set(value)
		for i := 0; i < value.NumField(); i++ {
			sourceField := value.Field(i)
			targetField := clone.Field(i)
			if targetField.CanSet() && sourceField.CanInterface() {
				targetField.Set(cloneReflectValue(sourceField))
			}
		}
		return clone
	default:
		return value
	}
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
