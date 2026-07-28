package groupnode

import (
	"context"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// exitHandler implements types.ActionHandler for xflow.group_exit collector nodes.
type exitHandler struct {
	collector *Collector
	nodeName  string
}

func (h *exitHandler) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type: graph.NodeTypeGroupExit,
	}
}

func (h *exitHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	mapping, ok := h.collector.mappings[h.nodeName]
	if !ok {
		return &types.Output{Data: input.Data}, nil
	}

	result := engine.GroupExitResult{
		NodeName: mapping.srcNode,
		Port:     mapping.port,
		Data:     input.Data,
	}

	h.collector.mu.Lock()
	h.collector.captured = append(h.collector.captured, result)
	h.collector.mu.Unlock()

	return &types.Output{Data: input.Data}, nil
}
