package sqlstore

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// b64Bytes is a []byte persisted as base64 text.
//
// The columns behind it are LONGTEXT, not a blob type: the deployment's schema
// review rejects blob columns outright. Encoding happens at the database/sql
// boundary via driver.Valuer/sql.Scanner so callers keep handling raw bytes.
//
// The backing column MUST be COLLATE utf8mb4_bin. base64 is case-sensitive;
// under a _ci collation two different payloads can compare equal, which would
// silently corrupt any lookup or uniqueness constraint over the column.
type b64Bytes []byte

// Value returns "" rather than nil for empty input: both columns are NOT NULL.
func (b b64Bytes) Value() (driver.Value, error) {
	if len(b) == 0 {
		return "", nil
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (b *b64Bytes) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*b = nil
		return nil
	case string:
		return b.decode(v)
	case []byte:
		return b.decode(string(v))
	default:
		return fmt.Errorf("sqlstore: cannot scan %T into b64Bytes", src)
	}
}

func (b *b64Bytes) decode(s string) error {
	if s == "" {
		*b = nil
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		// Do not include the column value in the error: for xflow_supplies it
		// is ciphertext of something that may be a credential.
		return fmt.Errorf("sqlstore: decode base64 column: %w", err)
	}
	*b = raw
	return nil
}

// dbSupply is the GORM persistence type for store.SupplyResource.
type dbSupply struct {
	ID          uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	Namespace   string     `gorm:"column:namespace;type:varchar(64);uniqueIndex:uk_ns_name"`
	Name        string     `gorm:"column:name;type:varchar(255);uniqueIndex:uk_ns_name"`
	Content     b64Bytes   `gorm:"column:content;type:LONGTEXT COLLATE utf8mb4_bin;not null"`
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
		Content:     []byte(d.Content),
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
	Content     b64Bytes  `gorm:"column:content;type:LONGTEXT COLLATE utf8mb4_bin;not null"`
	SizeBytes   uint64    `gorm:"column:size_bytes"`
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime:milli"`
}

func (dbArtifactBlob) TableName() string { return "xflow_artifact_blobs" }

// dbArtifact is the GORM persistence type for the xflow_artifacts table — the
// namespace-scoped identity layer that binds (namespace, filename, version) to a
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

// dbRegistrationCode persists a reusable runner enrollment credential. Only the
// sha256 of the code is stored — code_hash is the raw 32 bytes, not hex, so a
// truncating column type would corrupt every future lookup rather than fail
// loudly. Keep it BINARY(32).
type dbRegistrationCode struct {
	ID                string `gorm:"column:id;primaryKey;size:64"`
	CodeHash          []byte `gorm:"column:code_hash;type:binary(32);not null;uniqueIndex:uk_code_hash"`
	AllowedNamespaces string `gorm:"column:allowed_namespaces;type:text"`
	AllowedNodeTypes  string `gorm:"column:allowed_node_types;type:text"`
	// OwnerNamespace mirrors store.RegistrationCode.OwnerNamespace. NOT NULL
	// with a "" default; "" marks a row written before this column existed —
	// no code path writes "" today — and such a row is visible only to a
	// principal holding the corresponding _global scope, which is the
	// fail-closed reading.
	OwnerNamespace string    `gorm:"column:owner_namespace;size:64;not null;default:''"`
	Revoked        bool      `gorm:"column:revoked;not null;default:false"`
	CreatedAt      time.Time `gorm:"column:created_at;not null"`
	// ExpiresAt mirrors store.RegistrationCode.ExpiresAt. It is *time.Time and
	// the column is NULL-able because the domain's zero value means "never
	// expires" and MySQL 8 strict mode rejects '0000-00-00' outright — the
	// same split dbIssuedIdentity.ExpiresAt already carries.
	ExpiresAt *time.Time `gorm:"column:expires_at"`
	// MaxUses/UseCount bound how many runners one code may enroll. 0 max_uses
	// means unlimited, so the NOT NULL DEFAULT 0 is also the migration story:
	// rows written before these columns existed read back as unbounded, exactly
	// as they behaved.
	MaxUses  int `gorm:"column:max_uses;not null;default:0"`
	UseCount int `gorm:"column:use_count;not null;default:0"`
}

func (dbRegistrationCode) TableName() string { return "xflow_registration_codes" }

// dbEnrollAudit is one enrollment attempt, successful or not. The reason column
// holds the server-side detail that is deliberately never returned to the
// caller; this table is the only place it survives.
type dbEnrollAudit struct {
	ID       uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	CodeID   string    `gorm:"column:code_id;size:64;not null;index:idx_enroll_audit_code"`
	Success  bool      `gorm:"column:success;not null"`
	Reason   string    `gorm:"column:reason;size:255;not null"`
	RunnerID string    `gorm:"column:runner_id;size:128;not null"`
	SourceIP string    `gorm:"column:source_ip;size:64;not null"`
	At       time.Time `gorm:"column:at;not null"`
}

func (dbEnrollAudit) TableName() string { return "xflow_enroll_audit" }

// dbIssuedIdentity is a runner credential minted by enroll. scope_* mirror
// RunnerPolicy's list fields; id_prefix mirrors its remaining scalar field.
// The policy Name is derived from RunnerID on read, so it is not stored.
//
// id_prefix exists because RunnerPolicy has FOUR fields (Name, IDPrefix,
// AllowedNodeTypes, AllowedNamespaces), not two — dropping it here would be a
// silent privilege escalation the day something upstream starts setting it on
// an issued scope (task-7-addendum.md correction 3): the in-memory store
// would honor an IDPrefix ceiling, this one would silently discard it.
//
// IssuedAt is *time.Time, not time.Time: the storecontract.RunIssuedIdentityStoreContract
// "list" subtest issues identities with a zero-value IssuedAt (it only
// asserts List's length, not this field), and MemoryIssuedIdentityStore
// accepts that silently. A NOT NULL DATETIME column does not — MySQL 8's
// default strict sql_mode (NO_ZERO_DATE) rejects the zero time.Time the
// driver sends as the literal '0000-00-00', so the two backends disagreed
// exactly where this contract exists to catch it. Nullable + a nil-in/zero-out
// mapping (rowToIssuedIdentity) makes the round trip agree again, the same
// pattern dbSupply.LastFetchAt already uses for its own optional timestamp.
type dbIssuedIdentity struct {
	RunnerID        string     `gorm:"column:runner_id;primaryKey;size:128"`
	TokenHash       []byte     `gorm:"column:token_hash;type:binary(32);not null"`
	IDPrefix        string     `gorm:"column:id_prefix;type:varchar(64);not null"`
	ScopeNamespaces string     `gorm:"column:scope_namespaces;type:text"`
	ScopeNodeTypes  string     `gorm:"column:scope_node_types;type:text"`
	CodeID          string     `gorm:"column:code_id;size:64;not null;index:idx_issued_identity_code"`
	OwnerNamespace  string     `gorm:"column:owner_namespace;size:64;not null;default:''"`
	IssuedAt        *time.Time `gorm:"column:issued_at"`
	// ExpiresAt / RevokedAt are pointers for the same reason IssuedAt above is:
	// MySQL 8 strict mode (NO_ZERO_DATE) rejects '0000-00-00', so "no expiry"
	// and "not revoked" must be NULL rather than a zero timestamp.
	ExpiresAt *time.Time `gorm:"column:expires_at"`
	RevokedAt *time.Time `gorm:"column:revoked_at"`
}

func (dbIssuedIdentity) TableName() string { return "xflow_issued_identities" }
