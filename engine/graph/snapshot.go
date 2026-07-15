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
		Name:            g.Name,
		WorkflowVersion: g.WorkflowVersion,
		CompilerVersion: g.CompilerVersion,
		Nodes:           g.Nodes,
		Index:           g.Index,
		EntryIndexes:    g.EntryIndexes,
		OutEdges:        g.OutEdges,
		InEdges:         g.InEdges,
		InDegree:        g.InDegree,
		Vars:            g.Vars,
		Config:          g.Config,
		AllowCycles:     g.AllowCycles,
		StartIdx:        g.StartIdx,
		MaxAutoDepth:    g.MaxAutoDepth,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal graph hash payload: %w", err)
	}
	sum := sha256.Sum256(data)
	g.GraphHash = "sha256:" + hex.EncodeToString(sum[:])
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
