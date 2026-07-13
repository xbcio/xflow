package internal

import (
	"github.com/xbcio/xflow/internal/noderuntime"
	"github.com/xbcio/xflow/types"
)

func Register(h types.ActionHandler) {
	noderuntime.Register(h)
}

func RegisterTrigger(h types.TriggerHandler) {
	noderuntime.RegisterTrigger(h)
}

func Lookup(nodeType string) (types.ActionHandler, bool) {
	return noderuntime.Lookup(nodeType)
}

func LookupTrigger(nodeType string) (types.TriggerHandler, bool) {
	return noderuntime.LookupTrigger(nodeType)
}

func LookupVersion(nodeType string, version int) (types.ActionHandler, bool) {
	return noderuntime.LookupVersion(nodeType, version)
}

func LookupTriggerVersion(nodeType string, version int) (types.TriggerHandler, bool) {
	return noderuntime.LookupTriggerVersion(nodeType, version)
}

func TriggerVersions(nodeType string) []int {
	return noderuntime.TriggerVersions(nodeType)
}

func Versions(nodeType string) []int {
	return noderuntime.Versions(nodeType)
}
