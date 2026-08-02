package sqlstore

import (
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// dbSupply is the GORM persistence type for store.SupplyResource.
type dbSupply struct {
	ID          uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	Namespace   string     `gorm:"column:namespace;type:varchar(64);uniqueIndex:uk_ns_name"`
	Name        string     `gorm:"column:name;type:varchar(255);uniqueIndex:uk_ns_name"`
	Content     []byte     `gorm:"column:content;type:mediumblob"`
	ContentType string     `gorm:"column:content_type;type:varchar(128)"`
	Revision    uint64     `gorm:"column:revision"`
	ContentHash string     `gorm:"column:content_hash;type:varchar(80)"`
	UpdatedBy   string     `gorm:"column:updated_by;type:varchar(255)"`
	LastFetchAt *time.Time `gorm:"column:last_fetch_at"`
	LastError   string     `gorm:"column:last_error;type:varchar(512)"`
	CreatedAt   time.Time  `gorm:"column:created_at;autoCreateTime:milli"`
	UpdatedAt   time.Time  `gorm:"column:updated_at;autoUpdateTime:milli"`
}

func (dbSupply) TableName() string { return "xflow_supplies" }

func fromDBSupply(d *dbSupply) *store.SupplyResource {
	var lastFetchAt time.Time
	if d.LastFetchAt != nil {
		lastFetchAt = *d.LastFetchAt
	}
	return &store.SupplyResource{
		Namespace:   d.Namespace,
		Name:        d.Name,
		Content:     d.Content,
		ContentType: d.ContentType,
		Revision:    d.Revision,
		ContentHash: d.ContentHash,
		UpdatedAt:   d.UpdatedAt,
		UpdatedBy:   d.UpdatedBy,
		LastFetchAt: lastFetchAt,
		LastError:   d.LastError,
	}
}

// dbExecution is the internal GORM persistence type for store.ExecutionRecord.
// It carries all GORM schema annotations; the domain type store.ExecutionRecord
// is kept free of ORM concerns.
type dbExecution struct {
	ID           uint64                `gorm:"column:id;primaryKey;autoIncrement"`
	ExecutionID  types.ExecutionID     `gorm:"column:execution_id;type:varchar(64);uniqueIndex:uk_execution_id"`
	WorkflowName string                `gorm:"column:workflow_name;type:varchar(255)"`
	WorkflowDef  []byte                `gorm:"column:workflow_def;type:json"`
	Params       []byte                `gorm:"column:params;type:json"`
	Runtime      []byte                `gorm:"column:runtime;type:json"`
	TraceID      string                `gorm:"column:trace_id;type:varchar(64)"`
	SpanID       string                `gorm:"column:span_id;type:varchar(32)"`
	Status       types.ExecutionStatus `gorm:"column:status;type:varchar(20)"`
	Error        string                `gorm:"column:error_msg;type:text"`
	CreatedAt    time.Time             `gorm:"column:created_at;autoCreateTime:milli"`
	UpdatedAt    time.Time             `gorm:"column:updated_at;autoUpdateTime:milli"`
}

func (dbExecution) TableName() string { return "xflow_executions" }

func toDBExecution(r *store.ExecutionRecord) *dbExecution {
	return &dbExecution{
		ID:           r.ID,
		ExecutionID:  r.ExecutionID,
		WorkflowName: r.WorkflowName,
		WorkflowDef:  r.WorkflowDef,
		Params:       r.Params,
		Runtime:      r.Runtime,
		TraceID:      r.TraceID,
		SpanID:       r.SpanID,
		Status:       r.Status,
		Error:        r.Error,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
	}
}

func fromDBExecution(d *dbExecution) *store.ExecutionRecord {
	return &store.ExecutionRecord{
		ID:           d.ID,
		ExecutionID:  d.ExecutionID,
		WorkflowName: d.WorkflowName,
		WorkflowDef:  d.WorkflowDef,
		Params:       d.Params,
		Runtime:      d.Runtime,
		TraceID:      d.TraceID,
		SpanID:       d.SpanID,
		Status:       d.Status,
		Error:        d.Error,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	}
}

// dbNode is the internal GORM persistence type for store.NodeRecord.
type dbNode struct {
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

func (dbNode) TableName() string { return "xflow_nodes" }

func toDBNode(r *store.NodeRecord) *dbNode {
	return &dbNode{
		ID:           r.ID,
		ExecutionID:  r.ExecutionID,
		NodeName:     r.NodeName,
		NodeType:     r.NodeType,
		Status:       r.Status,
		LeaseID:      r.LeaseID,
		LeaseToken:   r.LeaseToken,
		Attempt:      r.Attempt,
		Output:       r.Output,
		Port:         r.Port,
		SignalName:   r.SignalName,
		SignalConfig: r.SignalConfig,
		Timeout:      r.Timeout,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
	}
}

func fromDBNode(d *dbNode) *store.NodeRecord {
	return &store.NodeRecord{
		ID:           d.ID,
		ExecutionID:  d.ExecutionID,
		NodeName:     d.NodeName,
		NodeType:     d.NodeType,
		Status:       d.Status,
		LeaseID:      d.LeaseID,
		LeaseToken:   d.LeaseToken,
		Attempt:      d.Attempt,
		Output:       d.Output,
		Port:         d.Port,
		SignalName:   d.SignalName,
		SignalConfig: d.SignalConfig,
		Timeout:      d.Timeout,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	}
}

func fromDBNodes(ds []*dbNode) []*store.NodeRecord {
	recs := make([]*store.NodeRecord, len(ds))
	for i, d := range ds {
		recs[i] = fromDBNode(d)
	}
	return recs
}

// dbSignal is the internal GORM persistence type for store.SignalRecord.
type dbSignal struct {
	ID          uint64             `gorm:"column:id;primaryKey;autoIncrement"`
	ExecutionID types.ExecutionID  `gorm:"column:execution_id;type:varchar(64);uniqueIndex:uk_exec_signal"`
	SignalName  string             `gorm:"column:signal_name;type:varchar(255);uniqueIndex:uk_exec_signal"`
	Payload     []byte             `gorm:"column:payload;type:json"`
	Status      types.SignalStatus `gorm:"column:status;type:varchar(16)"`
	CreatedAt   time.Time          `gorm:"column:created_at;autoCreateTime:milli"`
	UpdatedAt   time.Time          `gorm:"column:updated_at;autoUpdateTime:milli"`
}

func (dbSignal) TableName() string { return "xflow_signals" }

func toDBSignal(r *store.SignalRecord) *dbSignal {
	return &dbSignal{
		ID:          r.ID,
		ExecutionID: r.ExecutionID,
		SignalName:  r.SignalName,
		Payload:     r.Payload,
		Status:      r.Status,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}

func fromDBSignal(d *dbSignal) *store.SignalRecord {
	return &store.SignalRecord{
		ID:          d.ID,
		ExecutionID: d.ExecutionID,
		SignalName:  d.SignalName,
		Payload:     d.Payload,
		Status:      d.Status,
		CreatedAt:   d.CreatedAt,
		UpdatedAt:   d.UpdatedAt,
	}
}

func fromDBSignals(ds []*dbSignal) []*store.SignalRecord {
	recs := make([]*store.SignalRecord, len(ds))
	for i, d := range ds {
		recs[i] = fromDBSignal(d)
	}
	return recs
}

// dbArtifactBlob is the GORM persistence type for the xflow_artifact_blobs
// table — the global content-addressed byte store. Primary key is the digest
// string, not an auto-increment id, because addressing is by content hash.
type dbArtifactBlob struct {
	ContentHash string    `gorm:"column:content_hash;type:varchar(80);primaryKey"`
	Content     []byte    `gorm:"column:content;type:mediumblob"`
	SizeBytes   uint64    `gorm:"column:size_bytes"`
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime:milli"`
}

func (dbArtifactBlob) TableName() string { return "xflow_artifact_blobs" }

// dbArtifact is the GORM persistence type for the xflow_artifacts table — the
// tenant-scoped identity layer that binds (namespace, filename, version) to a
// content hash. The binding is immutable once created.
type dbArtifact struct {
	ID          uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Namespace   string    `gorm:"column:namespace;type:varchar(64);uniqueIndex:uk_identity"`
	Filename    string    `gorm:"column:filename;type:varchar(255);uniqueIndex:uk_identity"`
	Version     string    `gorm:"column:version;type:varchar(64);uniqueIndex:uk_identity"`
	ContentHash string    `gorm:"column:content_hash;type:varchar(80);index:idx_content_hash"`
	ContentType string    `gorm:"column:content_type;type:varchar(128)"`
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime:milli"`
}

func (dbArtifact) TableName() string { return "xflow_artifacts" }
