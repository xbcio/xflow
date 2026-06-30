package xflow

import (
	"context"
	"fmt"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type Entry struct {
	nodeName string
}

func Start() Entry { return Entry{nodeName: "start"} }

func EntryNode(name string) Entry { return Entry{nodeName: name} }

func Trigger(name string) Entry { return Entry{nodeName: name} }

func (e *Engine) Invoke(ctx context.Context, workflowID types.WorkflowID, entry Entry, input map[string]any, opts ...InvokeOption) (types.ExecutionID, error) {
	if entry.nodeName == "" {
		return "", fmt.Errorf("entry node name is required")
	}
	rec, err := e.workflowRegistry.GetWorkflow(ctx, workflowID)
	if err != nil {
		return "", err
	}
	cfg := &invokeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.execTTL > 0 {
		ctx = enginecore.WithExecutionTTL(ctx, cfg.execTTL)
	}
	ctx = enginecore.WithWorkflowDef(ctx, rec.Definition)
	return e.eng.Invoke(ctx, rec.Graph, entry.nodeName, input, cfg.runtime)
}
