package transform

import . "github.com/xbcio/xflow/node/internal"

func init() {
	Register(&SetNode{})
	Register(&PickNode{})
	Register(&RenameNode{})
	Register(&FilterNode{})
	Register(&SortNode{})
	Register(&LimitNode{})
	Register(&RemoveDuplicatesNode{})
	Register(&AggregateNode{})
}
