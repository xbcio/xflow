package store

import (
	"time"

	"github.com/xbcio/xflow/types"
)

// ExecutionRecord holds the persistent state of a workflow execution.
type ExecutionRecord struct {
	ID           uint64                `gorm:"column:id;primaryKey;autoIncrement"`
	ExecutionID  types.ExecutionID     `gorm:"column:execution_id;type:varchar(64);uniqueIndex:uk_execution_id"`
	WorkflowName string                `gorm:"column:workflow_name;type:varchar(255)"`
	WorkflowDef  []byte                `gorm:"column:workflow_def;type:json"`
	Params       []byte                `gorm:"column:params;type:json"`
	Runtime      []byte                `gorm:"column:runtime;type:json"`
	Status       types.ExecutionStatus `gorm:"column:status;type:varchar(20)"`
	Error        string                `gorm:"column:error_msg;type:text"`
	CreatedAt    time.Time             `gorm:"column:created_at;autoCreateTime:milli"`
	UpdatedAt    time.Time             `gorm:"column:updated_at;autoUpdateTime:milli"`
}

func (ExecutionRecord) TableName() string { return "xflow_executions" }

// NodeRecord holds the persistent state of a single node within an execution.
type NodeRecord struct {
	ID           uint64            `gorm:"column:id;primaryKey;autoIncrement"`
	ExecutionID  types.ExecutionID `gorm:"column:execution_id;type:varchar(64);uniqueIndex:uk_exec_node"`
	NodeName     string            `gorm:"column:node_name;type:varchar(255);uniqueIndex:uk_exec_node"`
	NodeType     string            `gorm:"column:node_type;type:varchar(255)"`
	Status       types.NodeStatus  `gorm:"column:status;type:varchar(20)"`
	LeaseID      string            `gorm:"column:lease_id;type:varchar(96)"`
	LeaseToken   string            `gorm:"column:lease_token;type:varchar(96)"`
	Attempt      int               `gorm:"column:attempt"`
	Output       []byte            `gorm:"column:output;type:json"`
	Port         string            `gorm:"column:port;type:varchar(50)"`
	SignalName   string            `gorm:"column:signal_name;type:varchar(255)"`
	SignalConfig []byte            `gorm:"column:signal_config;type:json"`
	Timeout      *time.Time        `gorm:"column:timeout_at"`
	CreatedAt    time.Time         `gorm:"column:created_at;autoCreateTime:milli"`
	UpdatedAt    time.Time         `gorm:"column:updated_at;autoUpdateTime:milli"`
}

func (NodeRecord) TableName() string { return "xflow_nodes" }

// SignalRecord holds a signal payload delivered to a workflow execution.
type SignalRecord struct {
	ID          uint64            `gorm:"column:id;primaryKey;autoIncrement"`
	ExecutionID types.ExecutionID `gorm:"column:execution_id;type:varchar(64);uniqueIndex:uk_exec_signal"`
	SignalName  string            `gorm:"column:signal_name;type:varchar(255);uniqueIndex:uk_exec_signal"`
	Payload     []byte            `gorm:"column:payload;type:json"`
	Status      string            `gorm:"column:status;type:varchar(16)"`
	CreatedAt   time.Time         `gorm:"column:created_at;autoCreateTime:milli"`
	UpdatedAt   time.Time         `gorm:"column:updated_at;autoUpdateTime:milli"`
}

func (SignalRecord) TableName() string { return "xflow_signals" }
