package xflow

import (
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestWorkflowBuilderOnErrorSetsSettings(t *testing.T) {
	wf := Workflow("on-error-builder").OnError(types.OnErrorContinue)
	wf.Node("start", node.Start())
	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Settings == nil || def.Settings.OnError != string(types.OnErrorContinue) {
		t.Fatalf("Settings = %+v, want on_error continue", def.Settings)
	}

	plain := Workflow("plain")
	plain.Node("start", node.Start())
	def, err = plain.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Settings != nil {
		t.Fatalf("Settings = %+v, want nil without OnError", def.Settings)
	}
}
