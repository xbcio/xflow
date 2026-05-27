package node

import "context"

// DatabaseHandler implements xflow.database — runs a database operation via a named credential.
// Execute is a stub; the real implementation lives in the Worker layer.
type DatabaseHandler struct{}

func (h *DatabaseHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.database",
		DisplayName: "Database",
		Credentials: []string{"db_conn"},
		Params: []ParamSpec{
			{Name: "operation", DisplayName: "Operation", Type: ParamString, Required: true, Description: "DB operation: \"select\"/\"insert\"/\"update\"/\"delete\"/\"insert_many\""},
			{Name: "table", DisplayName: "Table", Type: ParamString, Required: true, Description: "Target table name"},
			{Name: "credential", DisplayName: "Credential", Type: ParamString, Required: true, Description: "Credential reference name for the DB connection"},
			{Name: "where", DisplayName: "Where", Type: ParamObject, Required: false, Description: "Filter conditions (key-value pairs)"},
			{Name: "data", DisplayName: "Data", Type: ParamObject, Required: false, Description: "Row data for insert/update operations"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (h *DatabaseHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.database", "_stub": true}}, nil
}

func init() { Register(&DatabaseHandler{}) }
