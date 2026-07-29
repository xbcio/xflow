package engine

import "github.com/xbcio/xflow/types"

// SeededInput carries pre-computed input data for a specific node, allowing
// the engine to override normal fan-in logic during submission.
type SeededInput struct {
	Data    map[string]any
	Inputs  map[string]any
	Runtime *types.Runtime
}

// CloneSeededInputs returns a deep-enough copy of the seed map.
func CloneSeededInputs(seeds map[string]SeededInput) map[string]SeededInput {
	if len(seeds) == 0 {
		return nil
	}
	out := make(map[string]SeededInput, len(seeds))
	for k, v := range seeds {
		out[k] = SeededInput{
			Data:    cloneMap(v.Data),
			Inputs:  cloneMap(v.Inputs),
			Runtime: cloneRuntime(v.Runtime),
		}
	}
	return out
}
