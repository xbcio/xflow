-- XFlow 执行状态 Schema (MySQL 8.0+)
--
-- 工作流执行持久化所需表。
-- 使用 WithMySQL(dsn) 或 WithStore(s) 前需先创建这些表。

-- 工作流执行记录
CREATE TABLE IF NOT EXISTS xflow_executions (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    execution_id VARCHAR(64)  NOT NULL              COMMENT '执行唯一标识 (exec-<hex>)',
    workflow_name VARCHAR(255) NOT NULL DEFAULT ''   COMMENT '工作流名称，用于查询过滤',
    workflow_def JSON         NOT NULL              COMMENT '完整 WorkflowDef JSON',
    params       JSON                               COMMENT '提交时的输入参数 JSON',
    runtime      JSON                               COMMENT '提交时的运行时上下文 JSON',
    trace_id     VARCHAR(64)  NOT NULL DEFAULT ''   COMMENT '提交时指定的 Trace ID',
    span_id      VARCHAR(32)  NOT NULL DEFAULT ''   COMMENT '提交时指定的 Span ID',
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending' COMMENT '生命周期状态',
    error_msg    TEXT                               COMMENT '失败时的错误信息',
    created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE INDEX uk_execution_id (execution_id),
    INDEX idx_status (status),
    INDEX idx_workflow_name (workflow_name),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 节点执行状态
CREATE TABLE IF NOT EXISTS xflow_nodes (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    execution_id VARCHAR(64)  NOT NULL              COMMENT '所属执行 ID',
    node_name    VARCHAR(255) NOT NULL              COMMENT '工作流内的节点名称',
    node_type    VARCHAR(255) NOT NULL DEFAULT ''   COMMENT '处理器类型 (如 xflow.http)',
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending' COMMENT '节点生命周期状态',
    lease_id     VARCHAR(96)  NOT NULL DEFAULT ''   COMMENT '当前任务租约 ID',
    lease_token  VARCHAR(96)  NOT NULL DEFAULT ''   COMMENT '当前任务租约 fencing token',
    attempt      INT          NOT NULL DEFAULT 0    COMMENT '节点执行尝试次数',
    output       JSON                               COMMENT '节点输出数据 JSON',
    port         VARCHAR(50)  NOT NULL DEFAULT ''   COMMENT '活跃输出端口 (main/error/timeout)',
    signal_name  VARCHAR(255) NOT NULL DEFAULT ''   COMMENT '挂起节点等待的信号名称',
    signal_config JSON                              COMMENT '多信号等待配置 JSON',
    timeout_at   DATETIME(3)                        COMMENT '挂起超时截止时间',
    created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE INDEX uk_execution_id_node_name (execution_id, node_name),
    INDEX idx_status_timeout_at (status, timeout_at),
    INDEX idx_status_node_type (status, node_type),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 信号投递记录
CREATE TABLE IF NOT EXISTS xflow_signals (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    execution_id VARCHAR(64)     NOT NULL              COMMENT '目标执行 ID',
    signal_name  VARCHAR(255)    NOT NULL              COMMENT '信号名称',
    payload      JSON            NOT NULL              COMMENT '信号数据 JSON',
    status       VARCHAR(16)     NOT NULL DEFAULT 'active' COMMENT '信号状态: active/consumed/revoked',
    created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE INDEX uk_execution_id_signal_name (execution_id, signal_name),
    INDEX idx_execution_status (execution_id, status),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- supply 内容快照（namespace 内具名、可变、带版本）
-- content 是平台不解释的字节；它不是 secret，禁止存放凭证。
-- revision 每次写入 +1（即使内容未变）用于写侧 CAS；content_hash 供读侧比较。
CREATE TABLE IF NOT EXISTS xflow_supplies (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    namespace     VARCHAR(64)     NOT NULL              COMMENT '租户/命名空间，服务端注入',
    name          VARCHAR(255)    NOT NULL              COMMENT 'supply 节点名，namespace 内唯一',
    content       MEDIUMBLOB      NOT NULL              COMMENT '不透明内容字节',
    content_type  VARCHAR(128)    NOT NULL DEFAULT ''   COMMENT '建议性 MIME，平台不据此解析',
    revision      BIGINT UNSIGNED NOT NULL DEFAULT 0    COMMENT '单调递增，写侧 CAS',
    content_hash  VARCHAR(80)     NOT NULL DEFAULT ''   COMMENT 'sha256:<hex>，读侧比较',
    updated_by    VARCHAR(255)    NOT NULL DEFAULT ''   COMMENT '写入方 principal',
    last_fetch_at DATETIME(3)     NULL                  COMMENT '最近一次成功采集（pull 模式）',
    last_error    VARCHAR(512)    NOT NULL DEFAULT ''   COMMENT '最近一次采集失败原因',
    created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE INDEX uk_ns_name (namespace, name),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 字节层：全局按内容寻址，不可变，跨 namespace 去重
CREATE TABLE IF NOT EXISTS xflow_artifact_blobs (
    content_hash VARCHAR(80)     NOT NULL              COMMENT 'sha256:<hex>，全局去重键',
    content      MEDIUMBLOB      NOT NULL              COMMENT '资源原始字节；平台不解释',
    size_bytes   BIGINT UNSIGNED NOT NULL              COMMENT '字节数，取回后校验完整性',
    created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '首次上传时间',
    PRIMARY KEY (content_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 身份层：租户内具名版本 → 字节的引用。本表同时即引用索引。
CREATE TABLE IF NOT EXISTS xflow_artifacts (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    namespace    VARCHAR(64)     NOT NULL              COMMENT '租户，参与唯一键与取回鉴权',
    filename     VARCHAR(255)    NOT NULL              COMMENT '仅基名，不含目录；见 §10',
    version      VARCHAR(64)     NOT NULL              COMMENT '版本号；缺省 sha256-<前12位>',
    content_hash VARCHAR(80)     NOT NULL              COMMENT '指向 blobs；绑定一经建立不可变',
    content_type VARCHAR(128)    NOT NULL DEFAULT ''   COMMENT '建议性 MIME；空则由 filename 后缀推断',
    created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE INDEX uk_identity (namespace, filename, version),
    INDEX idx_content_hash (content_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 授权 / 变更审计事件（append-only，不可变）
-- B3 durable audit sink 的权威 reconcile 目标。仅记录身份、操作、资源 ID、
-- 决策、原因、outcome、trace 关联；绝不含 token/payload/凭证等敏感字段。
CREATE TABLE IF NOT EXISTS xflow_audit_events (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id   VARCHAR(128) NOT NULL DEFAULT ''  COMMENT '请求关联 ID（不可信为身份）',
    principal    VARCHAR(255) NOT NULL DEFAULT ''  COMMENT '服务端注入的主体',
    tenant_id    VARCHAR(128) NOT NULL DEFAULT ''  COMMENT '租户',
    operation    VARCHAR(64)  NOT NULL DEFAULT ''  COMMENT '操作词汇 (workflow.create 等)',
    resource     VARCHAR(255) NOT NULL DEFAULT ''  COMMENT '资源描述',
    workflow_id  VARCHAR(255) NOT NULL DEFAULT ''  COMMENT '工作流 ID',
    execution_id VARCHAR(64)  NOT NULL DEFAULT ''  COMMENT '执行 ID',
    decision     VARCHAR(16)  NOT NULL DEFAULT ''  COMMENT 'allow/deny',
    reason       VARCHAR(128) NOT NULL DEFAULT ''  COMMENT '拒绝原因码（非自由文本）',
    outcome      VARCHAR(32)  NOT NULL DEFAULT ''  COMMENT 'admitted/denied/reconciled',
    trace_id     VARCHAR(64)  NOT NULL DEFAULT ''  COMMENT 'OTel trace 关联',
    ts           DATETIME(3)  NOT NULL              COMMENT '事件时间戳',
    created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    -- T4: receipt-correlation fields populated only by the dead-letter receipt
    -- projector (and T9's outcome-phase worker). Admission/outcome rows leave
    -- them empty. receipt_audit_id is the Redis receipt's audit_id and the
    -- projector's idempotency key.
    -- T9: phase discriminator. Each audit row belongs to exactly one
    -- immutable phase: 'admission' (the pre-handler fail-closed admission
    -- audit), 'outcome' (the post-handler reconciled/failed outcome, whether
    -- written inline by the authz wrapper or by the crash-safe reconcile
    -- worker), or 'receipt' (the T4 dead-letter replay receipt projection).
    -- Admission/outcome rows for the same RequestID are joinable on
    -- (tenant_id, request_id); the phase column lets the reconcile worker's
    -- pending scan index "admitted but no outcome" efficiently and makes the
    -- append-only one-row-per-phase contract explicit.
    phase            VARCHAR(16)  NOT NULL DEFAULT ''  COMMENT '事件阶段 admission/outcome/receipt',
    node_id         VARCHAR(255) NOT NULL DEFAULT ''  COMMENT 'receipt 关联：节点名',
    activation_id   VARCHAR(64)  NOT NULL DEFAULT ''  COMMENT 'receipt 关联：activation',
    entry_id        VARCHAR(255) NOT NULL DEFAULT ''  COMMENT 'receipt 关联：dead-letter entry',
    receipt_audit_id VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'receipt 关联：Redis receipt audit_id（幂等键）',
    INDEX idx_principal (principal),
    INDEX idx_operation (operation),
    INDEX idx_execution_id (execution_id),
    INDEX idx_outcome (outcome),
    INDEX idx_ts (ts),
    INDEX idx_receipt_audit_id (receipt_audit_id),
    INDEX idx_tenant_request_phase (tenant_id, request_id, phase),
    -- Idempotency for outcome-phase rows: at most one outcome row per
    -- (tenant_id, request_id). Expressed via a generated column that is NULL
    -- unless phase='outcome' (and request_id is set), so admission rows
    -- (phase='admission', written once per request) and receipt projection
    -- rows (phase='receipt', deduped by ReceiptAuditID via
    -- AppendAuditIfAbsent) do NOT collide under the UNIQUE index — only the
    -- T9 reconcile worker's outcome appends are idempotency-guarded. MySQL
    -- permits multiple NULLs. A concurrent/duplicate outcome append for the
    -- same RequestID violates this index and is treated as appended=false by
    -- the worker (check-then-append + unique index = crash-safe idempotency
    -- across leader switches).
    phase_key VARCHAR(320) GENERATED ALWAYS AS (
        CASE WHEN phase = 'outcome' AND request_id <> ''
             THEN CONCAT(tenant_id, '|', request_id, '|', phase)
             ELSE NULL END
    ) STORED COMMENT '幂等键：仅 outcome 行 (tenant_id, request_id)；NULL 跳过其他行',
    UNIQUE INDEX uk_phase_key (phase_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- T4 receipt-correlation columns are additive. CREATE TABLE IF NOT EXISTS is a
-- no-op when the table already exists (e.g. an upgraded deployment), so these
-- ALTERs back-fill the columns on pre-existing tables. They are idempotent via
-- an INFORMATION_SCHEMA guard so re-applying the schema is always safe; MySQL
-- (unlike MariaDB) has no ADD COLUMN IF NOT EXISTS, so the guard is required.
DROP PROCEDURE IF EXISTS xflow_add_receipt_audit_columns;
DELIMITER $$
CREATE PROCEDURE xflow_add_receipt_audit_columns()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND COLUMN_NAME = 'node_id'
    ) THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN node_id          VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'receipt 关联：节点名' AFTER trace_id,
            ADD COLUMN activation_id    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT 'receipt 关联：activation' AFTER node_id,
            ADD COLUMN entry_id          VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'receipt 关联：dead-letter entry' AFTER activation_id,
            ADD COLUMN receipt_audit_id  VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'receipt 关联：Redis receipt audit_id（幂等键）' AFTER entry_id,
            ADD INDEX idx_receipt_audit_id (receipt_audit_id);
    END IF;
END$$
DELIMITER ;
CALL xflow_add_receipt_audit_columns();
DROP PROCEDURE IF EXISTS xflow_add_receipt_audit_columns;

-- T9 audit-phase column + idempotency index. CREATE TABLE IF NOT EXISTS is a
-- no-op on a pre-existing table, so the phase column, the (tenant, request,
-- phase) scan index, and the generated phase_key unique index are back-filled
-- here via an INFORMATION_SCHEMA guard (MySQL lacks ADD COLUMN IF NOT EXISTS).
-- The generated phase_key is NULL for rows with empty phase/request_id so
-- admission rows that legitimately share an empty tuple do not collide under
-- the UNIQUE index. Idempotent: re-applying the schema is always safe.
DROP PROCEDURE IF EXISTS xflow_add_phase_column;
DELIMITER $$
CREATE PROCEDURE xflow_add_phase_column()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND COLUMN_NAME = 'phase'
    ) THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN phase VARCHAR(16) NOT NULL DEFAULT '' COMMENT '事件阶段 admission/outcome/receipt' AFTER outcome,
            ADD COLUMN phase_key VARCHAR(320) GENERATED ALWAYS AS (
                CASE WHEN phase = 'outcome' AND request_id <> ''
                     THEN CONCAT(tenant_id, '|', request_id, '|', phase)
                     ELSE NULL END
            ) STORED COMMENT '幂等键：仅 outcome 行 (tenant_id, request_id)；NULL 跳过其他行',
            ADD INDEX idx_tenant_request_phase (tenant_id, request_id, phase),
            ADD UNIQUE INDEX uk_phase_key (phase_key);
    END IF;
END$$
DELIMITER ;
CALL xflow_add_phase_column();
DROP PROCEDURE IF EXISTS xflow_add_phase_column;

-- supply 版本溯源列。CREATE TABLE IF NOT EXISTS 对已存在的表是 no-op，
-- 故用 INFORMATION_SCHEMA 守卫补列（MySQL 无 ADD COLUMN IF NOT EXISTS）。
-- 只记版本号，绝不记内容。
DROP PROCEDURE IF EXISTS xflow_add_audit_revision_column;
DELIMITER $$
CREATE PROCEDURE xflow_add_audit_revision_column()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND COLUMN_NAME = 'revision'
    ) THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN revision BIGINT UNSIGNED NOT NULL DEFAULT 0
                COMMENT '资源版本号（supply 写入后的 revision）；绝不记内容' AFTER phase;
    END IF;
END$$
DELIMITER ;
CALL xflow_add_audit_revision_column();
DROP PROCEDURE IF EXISTS xflow_add_audit_revision_column;
