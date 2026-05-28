package node

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// DatabaseNode implements xflow.database — runs a database operation via a named credential.
type DatabaseNode struct {
	BaseNode
	Operation  string
	Table      string
	Credential string
	Where      map[string]any
	Data       map[string]any
	Columns    []string
	LimitN     int
}

// Database creates a database operation node.
//
//	node.Database("select", "users", "my_db").SetWhere(w).SetLimit(10)
func Database(operation, table, credential string) *DatabaseNode {
	return &DatabaseNode{Operation: operation, Table: table, Credential: credential}
}

func (n *DatabaseNode) SetWhere(w map[string]any) *DatabaseNode    { n.Where = w; return n }
func (n *DatabaseNode) SetData(d map[string]any) *DatabaseNode     { n.Data = d; return n }
func (n *DatabaseNode) SetColumns(cols ...string) *DatabaseNode     { n.Columns = cols; return n }
func (n *DatabaseNode) SetLimit(limit int) *DatabaseNode            { n.LimitN = limit; return n }

func (n *DatabaseNode) Descriptor() Descriptor {
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
			{Name: "columns", DisplayName: "Columns", Type: ParamArray, Required: false, Description: "Columns to select (default: all)"},
			{Name: "limit", DisplayName: "Limit", Type: ParamNumber, Required: false, Description: "Max rows to return for select"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *DatabaseNode) NodeType() string { return "xflow.database" }
func (n *DatabaseNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *DatabaseNode) RawParams() any {
	params := map[string]any{
		"operation":  n.Operation,
		"table":      n.Table,
		"credential": n.Credential,
	}
	if n.Where != nil {
		params["where"] = n.Where
	}
	if n.Data != nil {
		params["data"] = n.Data
	}
	if len(n.Columns) > 0 {
		cols := make([]any, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = c
		}
		params["columns"] = cols
	}
	if n.LimitN > 0 {
		params["limit"] = n.LimitN
	}
	return params
}

func (n *DatabaseNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	credName, _ := input.Params["credential"].(string)
	if credName == "" {
		return nil, fmt.Errorf("xflow.database: credential parameter is required")
	}
	cred := input.Credential(credName)
	if cred == nil {
		return nil, fmt.Errorf("xflow.database: credential %q not found", credName)
	}

	dsn, _ := cred["dsn"].(string)
	if dsn == "" {
		return nil, fmt.Errorf("xflow.database: credential %q missing dsn", credName)
	}

	driver, _ := cred["driver"].(string)
	if driver == "" {
		driver = "mysql"
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("xflow.database: open connection: %w", err)
	}
	defer db.Close()

	operation, _ := input.Params["operation"].(string)
	table, _ := input.Params["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("xflow.database: table parameter is required")
	}

	if !isValidIdentifier(table) {
		return nil, fmt.Errorf("xflow.database: invalid table name %q", table)
	}

	switch operation {
	case "select":
		return n.execSelect(ctx, db, table, input)
	case "insert":
		return n.execInsert(ctx, db, table, input)
	case "insert_many":
		return n.execInsertMany(ctx, db, table, input)
	case "update":
		return n.execUpdate(ctx, db, table, input)
	case "delete":
		return n.execDelete(ctx, db, table, input)
	default:
		return nil, fmt.Errorf("xflow.database: unknown operation %q", operation)
	}
}

func (n *DatabaseNode) execSelect(ctx context.Context, db *sql.DB, table string, input *Input) (*Output, error) {
	cols := "*"
	if columns, ok := input.Params["columns"].([]any); ok && len(columns) > 0 {
		colNames := make([]string, 0, len(columns))
		for _, c := range columns {
			s, _ := c.(string)
			if s != "" && isValidIdentifier(s) {
				colNames = append(colNames, s)
			}
		}
		if len(colNames) > 0 {
			cols = strings.Join(colNames, ", ")
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", cols, table)
	args := []any{}

	if where, ok := input.Params["where"].(map[string]any); ok && len(where) > 0 {
		clauses, whereArgs := buildWhere(where)
		query += " WHERE " + clauses
		args = append(args, whereArgs...)
	}

	if limit, ok := toInt(input.Params["limit"]); ok && limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			row[col] = val
		}
		results = append(results, row)
	}

	return &Output{Data: map[string]any{"rows": results, "count": len(results)}}, nil
}

func (n *DatabaseNode) execInsert(ctx context.Context, db *sql.DB, table string, input *Input) (*Output, error) {
	data, ok := input.Params["data"].(map[string]any)
	if !ok || len(data) == 0 {
		return nil, fmt.Errorf("xflow.database: data parameter is required for insert")
	}

	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	args := make([]any, 0, len(data))
	for k, v := range data {
		if !isValidIdentifier(k) {
			return nil, fmt.Errorf("xflow.database: invalid column name %q", k)
		}
		columns = append(columns, k)
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}

	lastID, _ := result.LastInsertId()
	affected, _ := result.RowsAffected()
	return &Output{Data: map[string]any{"last_insert_id": lastID, "rows_affected": affected}}, nil
}

func (n *DatabaseNode) execInsertMany(ctx context.Context, db *sql.DB, table string, input *Input) (*Output, error) {
	rows, ok := input.Params["data"].([]any)
	if !ok || len(rows) == 0 {
		return nil, fmt.Errorf("xflow.database: data parameter must be a non-empty array for insert_many")
	}

	firstRow, ok := rows[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("xflow.database: each row must be an object")
	}

	columns := make([]string, 0, len(firstRow))
	for k := range firstRow {
		if !isValidIdentifier(k) {
			return nil, fmt.Errorf("xflow.database: invalid column name %q", k)
		}
		columns = append(columns, k)
	}

	rowPlaceholder := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	allPlaceholders := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*len(columns))

	for _, r := range rows {
		row, ok := r.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("xflow.database: each row must be an object")
		}
		allPlaceholders = append(allPlaceholders, rowPlaceholder)
		for _, col := range columns {
			args = append(args, row[col])
		}
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table, strings.Join(columns, ", "), strings.Join(allPlaceholders, ", "))

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}

	affected, _ := result.RowsAffected()
	return &Output{Data: map[string]any{"rows_affected": affected}}, nil
}

func (n *DatabaseNode) execUpdate(ctx context.Context, db *sql.DB, table string, input *Input) (*Output, error) {
	data, ok := input.Params["data"].(map[string]any)
	if !ok || len(data) == 0 {
		return nil, fmt.Errorf("xflow.database: data parameter is required for update")
	}

	setClauses := make([]string, 0, len(data))
	args := make([]any, 0, len(data))
	for k, v := range data {
		if !isValidIdentifier(k) {
			return nil, fmt.Errorf("xflow.database: invalid column name %q", k)
		}
		setClauses = append(setClauses, k+" = ?")
		args = append(args, v)
	}

	query := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(setClauses, ", "))

	where, ok := input.Params["where"].(map[string]any)
	if !ok || len(where) == 0 {
		return nil, fmt.Errorf("xflow.database: where parameter is required for update (safety)")
	}
	clauses, whereArgs := buildWhere(where)
	query += " WHERE " + clauses
	args = append(args, whereArgs...)

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}

	affected, _ := result.RowsAffected()
	return &Output{Data: map[string]any{"rows_affected": affected}}, nil
}

func (n *DatabaseNode) execDelete(ctx context.Context, db *sql.DB, table string, input *Input) (*Output, error) {
	where, ok := input.Params["where"].(map[string]any)
	if !ok || len(where) == 0 {
		return nil, fmt.Errorf("xflow.database: where parameter is required for delete (safety)")
	}

	clauses, args := buildWhere(where)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, clauses)

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}

	affected, _ := result.RowsAffected()
	return &Output{Data: map[string]any{"rows_affected": affected}}, nil
}

func buildWhere(where map[string]any) (string, []any) {
	clauses := make([]string, 0, len(where))
	args := make([]any, 0, len(where))
	for k, v := range where {
		if !isValidIdentifier(k) {
			continue
		}
		clauses = append(clauses, k+" = ?")
		args = append(args, v)
	}
	return strings.Join(clauses, " AND "), args
}

func isValidIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			continue
		}
		if i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

func init() { Register(&DatabaseNode{}) }
