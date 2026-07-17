package rstate

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

func (s *Store) DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	activeFlag := 0
	if portActive {
		activeFlag = 1
	}
	ttl := int(s.getExecTTL(id).Seconds())
	vals, err := propagateLua.Run(ctx, s.rdb,
		[]string{inDegreeKey(id, nodeIdx), activeInputsKey(id, nodeIdx)},
		activeFlag, ttl,
	).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("propagate lua: %w", err)
	}
	if err := s.refreshTransientTTL(ctx, id, inDegreeKey(id, nodeIdx), activeInputsKey(id, nodeIdx)); err != nil {
		return 0, 0, err
	}
	return int(vals[0]), int(vals[1]), nil
}

func (s *Store) CheckCompletion(ctx context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
	s.mu.RLock()
	g := s.graphs[id]
	s.mu.RUnlock()
	if g == nil {
		return false, false, nil
	}

	keys := make([]string, 0, 1+totalNodes)
	keys = append(keys, execKey(id, "status"))
	for i := 0; i < g.NodeCount(); i++ {
		nd := g.NodeAt(i)
		keys = append(keys, nodeStatusKey(id, nd.Name))
	}

	result, err := checkCompletionLua.Run(ctx, s.rdb, keys, s.ttlSec()).Int64()
	if err != nil {
		return false, false, fmt.Errorf("check completion lua: %w", err)
	}
	switch result {
	case 1:
		return true, false, nil
	case -1:
		return true, true, nil
	default:
		return false, false, nil
	}
}

// ---------------------------------------------------------------------------
// Suspend / signal
// ---------------------------------------------------------------------------
