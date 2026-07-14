package transform

import (
	"github.com/xbcio/xflow/node/registry"
)

func init() {
	registry.Register(&SetNode{})
	registry.Register(&PickNode{})
	registry.Register(&RenameNode{})
	registry.Register(&FilterNode{})
	registry.Register(&SortNode{})
	registry.Register(&LimitNode{})
	registry.Register(&RemoveDuplicatesNode{})
	registry.Register(&AggregateNode{})
}
