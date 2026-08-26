package store

// UpsertNodeUpdateFields lists the NodeRecord fields that an UpsertNode on an
// existing row must refresh. Both the memstore and sqlstore implementations
// must update exactly this set so a node's lifecycle projection stays
// consistent across backends. CreatedAt is preserved; UpdatedAt is refreshed
// separately by each backend and is deliberately not listed here.
//
// The names are Go field names on NodeRecord, not column names. The sqlstore
// column list is derived from these by reflection over dbNode's gorm tags in
// store/sqlstore's contract test, so a column rename needs no edit here.
//
// This lives in a non-test file on purpose. It used to sit in contract_test.go,
// which made it unreachable from store/sqlstore's tests and left the whole
// contract self-referential: the only test that read it compared the list
// against itself, so deleting a column from the real sqlstore update set was
// invisible. A contract that no implementation is checked against is a comment.
var UpsertNodeUpdateFields = []string{
	"NodeType",
	"Status",
	"LeaseID",
	"LeaseToken",
	"Attempt",
	"Output",
	"Port",
	"SignalName",
	"SignalConfig",
	"Timeout",
}
