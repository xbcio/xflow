package internal

import (
	"context"

	"github.com/xbcio/xflow/types"
)

// BaseTrigger embeds BaseNode and provides the Execute entry point shared
// identically by every trigger node. Embed this instead of BaseNode in
// concrete trigger node structs.
//
// OnError and TriggerHandler are intentionally NOT hoisted here: OnError
// returns types.Builder, and *BaseTrigger does not implement that interface
// (it lacks the per-trigger NodeType/RawParams), so each trigger keeps its
// own OnError returning the concrete self. Execute has no self-type
// dependency and is therefore safe to share.
type BaseTrigger struct {
	BaseNode
}

// Execute is the shared trigger entry point: it delegates to the common
// trigger handler wrapper so every trigger node behaves identically when
// invoked as an action.
func (b *BaseTrigger) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return ExecuteTriggerEntry(input)
}
