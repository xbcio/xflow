package graph

import (
	"crypto/sha256"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// resolveConcurrency normalizes settings.concurrency onto the compiled graph.
//
// It stores nothing for a zero limit: "no limit" and "limit 0" are the same
// behavior, and keeping the absent case nil is what keeps every unlimited
// graph's hash payload and snapshot byte-identical to a compile that predates
// this setting.
func resolveConcurrency(def *types.WorkflowDef, g *Graph) error {
	if def.Settings == nil || def.Settings.Concurrency == nil {
		return nil
	}
	c := def.Settings.Concurrency
	if c.Limit < 0 {
		return fmt.Errorf("settings.concurrency.limit must be >= 0, got %d", c.Limit)
	}
	if c.MaxQueued < 0 {
		return fmt.Errorf("settings.concurrency.max_queued must be >= 0, got %d", c.MaxQueued)
	}
	if c.QueueTimeout < 0 {
		return fmt.Errorf("settings.concurrency.queue_timeout must be >= 0, got %v", c.QueueTimeout)
	}
	mode := c.EffectiveMode()
	switch mode {
	case types.ConcurrencyModeQueue, types.ConcurrencyModeReject:
	default:
		return fmt.Errorf("settings.concurrency.mode %q is not one of %q, %q",
			c.Mode, types.ConcurrencyModeQueue, types.ConcurrencyModeReject)
	}
	if mode != types.ConcurrencyModeQueue && (c.MaxQueued != 0 || c.QueueTimeout != 0) {
		return fmt.Errorf("settings.concurrency.max_queued/queue_timeout require mode %q, not %q",
			types.ConcurrencyModeQueue, mode)
	}
	if c.Limit == 0 {
		// The controls are meaningless without a limit, but they are not
		// contradictory the way they are in reject mode: a workflow that
		// authorizes a queue and then raises the limit later should not have to
		// delete and re-add its queue settings. Only the mode clash above fails
		// closed; a zero limit simply means the block never takes effect.
		return nil
	}
	cp := *c
	cp.Mode = mode
	g.concurrency = &cp
	return nil
}

// ConcurrencyKey identifies the slot set one workflow's executions contend for.
//
// It is deliberately (namespace, workflow NAME) and not the version: a version
// bump must not orphan the slots held by the previous version's still-running
// executions, and it must not hand them a fresh budget — see the design's
// "槽按 namespace+workflow 键，不按版本". Both parts are length-prefixed so no
// concatenation of two different pairs can collide, and the digest keeps caller
// data (a workflow name may contain anything) out of the Redis key verbatim.
func ConcurrencyKey(ns, workflow string) string {
	payload := fmt.Sprintf("%d:%s%d:%s", len(ns), ns, len(workflow), workflow)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
}
