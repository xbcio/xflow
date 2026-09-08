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

-- 注册码。明文永不落库，只存 sha256 的 32 字节原文（不是 hex）。
-- code_hash 必须是 BINARY(32)：任何会截断或补齐的列类型都会让此后每一次
-- 查找都对不上，而且是静默对不上。
CREATE TABLE IF NOT EXISTS xflow_registration_codes (
    id                 VARCHAR(64)  NOT NULL              COMMENT '注册码 ID',
    code_hash          BINARY(32)   NOT NULL              COMMENT 'sha256(明文)，原始 32 字节，非 hex',
    allowed_namespaces TEXT                                COMMENT '允许的 namespace JSON 数组；"*" 表示不限',
    allowed_node_types TEXT                                COMMENT '允许的节点类型 JSON 数组；"*" 表示不限',
    revoked            TINYINT(1)   NOT NULL DEFAULT 0    COMMENT '是否已吊销',
    created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE INDEX uk_code_hash (code_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 每一次 enroll 尝试，成功与失败都记。reason 是服务端理由，故意不回给调用方，
-- 这张表是它唯一的落脚处；不记失败，暴力破解就在服务端完全不可见。
CREATE TABLE IF NOT EXISTS xflow_enroll_audit (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    code_id    VARCHAR(64)     NOT NULL DEFAULT ''  COMMENT '关联的注册码 ID；未解析出码时为空',
    success    TINYINT(1)      NOT NULL              COMMENT '本次尝试是否成功',
    reason     VARCHAR(255)    NOT NULL DEFAULT ''  COMMENT '服务端拒绝理由，绝不回传调用方',
    runner_id  VARCHAR(128)    NOT NULL DEFAULT ''  COMMENT '签发成功时的 runner ID',
    source_ip  VARCHAR(64)     NOT NULL DEFAULT ''  COMMENT '来源 IP，限流按此分桶',
    at         DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_enroll_audit_code (code_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- enroll 签发出的 runner 身份。与注册码各自独立吊销：吊销一张泄漏的码，
-- 不应该把靠它上线的每个 runner 一起打掉。
-- id_prefix 对应 RunnerPolicy 的第四个字段；enroll 今天从不写非空值，但列
-- 必须存在，否则将来注册码一旦带上 id_prefix 上限，本表会静默丢弃它。
CREATE TABLE IF NOT EXISTS xflow_issued_identities (
    runner_id        VARCHAR(128) NOT NULL              COMMENT 'enroll 生成的 runner ID',
    token_hash       BINARY(32)   NOT NULL              COMMENT 'sha256(token)，原始 32 字节，非 hex',
    id_prefix        VARCHAR(64)  NOT NULL DEFAULT ''  COMMENT 'RunnerPolicy.IDPrefix；enroll 今天恒为空',
    scope_namespaces TEXT                                COMMENT '签发时确定的 namespace 范围 JSON 数组',
    scope_node_types TEXT                                COMMENT '签发时确定的节点类型范围 JSON 数组',
    code_id          VARCHAR(64)  NOT NULL DEFAULT ''  COMMENT '签发所用的注册码 ID',
    issued_at        DATETIME(3)  NULL                  COMMENT '签发时间；契约测试允许零值，故列可空，语义同 last_fetch_at',
    PRIMARY KEY (runner_id),
    INDEX idx_issued_identity_code (code_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 授权 / 变更审计事件（append-only，不可变）
-- B3 durable audit sink 的权威 reconcile 目标。仅记录身份、操作、资源 ID、
-- 决策、原因、outcome、trace 关联；绝不含 token/payload/凭证等敏感字段。
CREATE TABLE IF NOT EXISTS xflow_audit_events (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id   VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''  COMMENT '请求关联 ID（不可信为身份）',
    principal    VARCHAR(255) NOT NULL DEFAULT ''  COMMENT '服务端注入的主体',
    namespace    VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''  COMMENT '租户',
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
    -- (namespace, request_id); the phase column lets the reconcile worker's
    -- pending scan index "admitted but no outcome" efficiently and makes the
    -- append-only one-row-per-phase contract explicit.
    phase            VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT '事件阶段 admission/outcome/receipt',
    -- Resource version number (the revision supply wrote); never the content
    -- itself. Also back-filled onto pre-existing tables by the guarded
    -- ALTER below (xflow_add_audit_revision_column) — that guard stays for
    -- upgraded deployments; this literal column is what a fresh CREATE TABLE
    -- gets immediately, keeping it visible to the schema-pairing test's
    -- CREATE-TABLE-body parser instead of only appearing after a later ALTER.
    revision         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '资源版本号（supply 写入后的 revision）；绝不记内容',
    -- Application-owned nullable idempotency key. Historical rows remain
    -- NULL; only newly written eligible outcome rows receive a key.
    phase_key        VARCHAR(320) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL DEFAULT NULL COMMENT '应用写入的 outcome 幂等键；历史行可为 NULL',
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
    INDEX idx_namespace_request_phase (namespace, request_id, phase),
    -- MySQL permits multiple NULLs in a UNIQUE index, so historical rows can
    -- remain unkeyed while application-written outcome keys stay unique.
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

-- T9 audit-phase migration.
--
-- BREAKING / OFFLINE MIGRATION: stop every audit writer before applying this
-- block, apply the complete schema, deploy the new binary everywhere, and only
-- then restart writers. Old and new audit writers must never run concurrently;
-- mixed-version operation is unsupported. The migration preserves every audit
-- row and fails closed rather than deleting, truncating, or silently rewriting
-- conflicting idempotency keys.
--
-- CREATE TABLE IF NOT EXISTS is a no-op for upgraded deployments, so every
-- column and index has its own INFORMATION_SCHEMA guard. Ordering is
-- intentional: a legacy generated phase_key can depend on tenant_id and must be
-- replaced before tenant_id is renamed or dropped. Historical outcome groups
-- are then backfilled with the exact application hash on one canonical row.

DROP PROCEDURE IF EXISTS xflow_ensure_audit_phase;
DELIMITER $$
CREATE PROCEDURE xflow_ensure_audit_phase()
BEGIN
    DECLARE v_exists INT DEFAULT 0;
    DECLARE v_data_type VARCHAR(64) DEFAULT '';
    DECLARE v_length BIGINT DEFAULT 0;
    DECLARE v_nullable VARCHAR(3) DEFAULT '';
    DECLARE v_default VARCHAR(64) DEFAULT NULL;
    DECLARE v_collation VARCHAR(64) DEFAULT '';

    SELECT COUNT(*) INTO v_exists
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'phase';

    IF v_exists = 0 THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN phase VARCHAR(16)
                CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                NOT NULL DEFAULT ''
                COMMENT '事件阶段 admission/outcome/receipt' AFTER outcome;
    ELSE
        UPDATE xflow_audit_events SET phase = '' WHERE phase IS NULL;

        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
               COLUMN_DEFAULT, COALESCE(COLLATION_NAME, '')
          INTO v_data_type, v_length, v_nullable, v_default, v_collation
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND COLUMN_NAME = 'phase';

        IF v_data_type <> 'varchar'
           OR v_length <> 16
           OR v_nullable <> 'NO'
           OR NOT (v_default <=> '')
           OR v_collation <> 'utf8mb4_0900_bin' THEN
            ALTER TABLE xflow_audit_events
                MODIFY COLUMN phase VARCHAR(16)
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                    NOT NULL DEFAULT ''
                    COMMENT '事件阶段 admission/outcome/receipt';
        END IF;
    END IF;
END$$
DELIMITER ;
CALL xflow_ensure_audit_phase();
DROP PROCEDURE IF EXISTS xflow_ensure_audit_phase;

-- Assert before any legacy generated column/index can be removed. Dynamic SQL
-- lets the guard run on intermediate schemas where phase_key does not exist yet.
DROP PROCEDURE IF EXISTS xflow_assert_audit_phase_keys_unique;
DELIMITER $$
CREATE PROCEDURE xflow_assert_audit_phase_keys_unique()
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(*) INTO v_exists
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'phase_key';

    IF v_exists = 1 THEN
        SET @xflow_duplicate_phase_keys = 0;
        SET @xflow_duplicate_phase_key_sql =
            'SELECT COUNT(*) INTO @xflow_duplicate_phase_keys FROM (SELECT phase_key FROM xflow_audit_events WHERE phase_key IS NOT NULL GROUP BY phase_key HAVING COUNT(*) > 1) AS duplicate_keys';
        PREPARE xflow_duplicate_phase_key_stmt FROM @xflow_duplicate_phase_key_sql;
        EXECUTE xflow_duplicate_phase_key_stmt;
        DEALLOCATE PREPARE xflow_duplicate_phase_key_stmt;

        IF @xflow_duplicate_phase_keys > 0 THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'xflow_audit_events contains duplicate non-NULL phase_key values';
        END IF;

        SET @xflow_duplicate_phase_keys = NULL;
        SET @xflow_duplicate_phase_key_sql = NULL;
    END IF;
END$$
DELIMITER ;
CALL xflow_assert_audit_phase_keys_unique();
DROP PROCEDURE IF EXISTS xflow_assert_audit_phase_keys_unique;

DROP PROCEDURE IF EXISTS xflow_ensure_audit_phase_key;
DELIMITER $$
CREATE PROCEDURE xflow_ensure_audit_phase_key()
BEGIN
    DECLARE v_exists INT DEFAULT 0;
    DECLARE v_data_type VARCHAR(64) DEFAULT '';
    DECLARE v_length BIGINT DEFAULT 0;
    DECLARE v_nullable VARCHAR(3) DEFAULT '';
    DECLARE v_default VARCHAR(320) DEFAULT NULL;
    DECLARE v_extra VARCHAR(255) DEFAULT '';
    DECLARE v_collation VARCHAR(64) DEFAULT '';

    SELECT COUNT(*) INTO v_exists
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'phase_key';

    IF v_exists = 0 THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN phase_key VARCHAR(320)
                CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                NULL DEFAULT NULL
                COMMENT '应用写入的 outcome 幂等键；历史行可为 NULL' AFTER phase;
    ELSE
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
               COLUMN_DEFAULT, EXTRA, COALESCE(COLLATION_NAME, '')
          INTO v_data_type, v_length, v_nullable, v_default, v_extra, v_collation
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND COLUMN_NAME = 'phase_key';

        IF UPPER(v_extra) LIKE '%GENERATED%' THEN
            -- The generated expression is not audit data. Its values disappear
            -- with the expression, then the canonical backfill below recreates
            -- one application-compatible key per exact historical identity.
            IF EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'xflow_audit_events'
                  AND INDEX_NAME = 'uk_phase_key'
            ) THEN
                ALTER TABLE xflow_audit_events DROP INDEX uk_phase_key;
            END IF;

            ALTER TABLE xflow_audit_events DROP COLUMN phase_key;
            ALTER TABLE xflow_audit_events
                ADD COLUMN phase_key VARCHAR(320)
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                    NULL DEFAULT NULL
                    COMMENT '应用写入的 outcome 幂等键；历史行可为 NULL' AFTER phase;
        ELSEIF v_data_type <> 'varchar'
            OR v_length <> 320
            OR v_nullable <> 'YES'
            OR v_default IS NOT NULL
            OR v_collation <> 'utf8mb4_0900_bin' THEN
            -- Preserve every ordinary-column key. Unsafe narrowing or invalid
            -- values make ALTER fail closed instead of clearing data.
            ALTER TABLE xflow_audit_events
                MODIFY COLUMN phase_key VARCHAR(320)
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                    NULL DEFAULT NULL
                    COMMENT '应用写入的 outcome 幂等键；历史行可为 NULL';
        END IF;
    END IF;
END$$
DELIMITER ;
CALL xflow_ensure_audit_phase_key();
DROP PROCEDURE IF EXISTS xflow_ensure_audit_phase_key;

-- Remove the obsolete name independently. It can survive an earlier column
-- rename even though its indexed column has already become namespace.
DROP PROCEDURE IF EXISTS xflow_drop_legacy_audit_phase_index;
DELIMITER $$
CREATE PROCEDURE xflow_drop_legacy_audit_phase_index()
BEGIN
    IF EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND INDEX_NAME = 'idx_tenant_request_phase'
    ) THEN
        ALTER TABLE xflow_audit_events DROP INDEX idx_tenant_request_phase;
    END IF;
END$$
DELIMITER ;
CALL xflow_drop_legacy_audit_phase_index();
DROP PROCEDURE IF EXISTS xflow_drop_legacy_audit_phase_index;

DROP PROCEDURE IF EXISTS xflow_canonicalize_audit_identity;
DELIMITER $$
CREATE PROCEDURE xflow_canonicalize_audit_identity()
BEGIN
    DECLARE v_has_tenant INT DEFAULT 0;
    DECLARE v_has_namespace INT DEFAULT 0;
    DECLARE v_has_request INT DEFAULT 0;
    DECLARE v_data_type VARCHAR(64) DEFAULT '';
    DECLARE v_length BIGINT DEFAULT 0;
    DECLARE v_nullable VARCHAR(3) DEFAULT '';
    DECLARE v_default VARCHAR(512) DEFAULT NULL;
    DECLARE v_collation VARCHAR(64) DEFAULT '';

    SELECT COUNT(*) INTO v_has_tenant
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'tenant_id';

    SELECT COUNT(*) INTO v_has_namespace
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'namespace';

    IF v_has_tenant = 1 AND v_has_namespace = 0 THEN
        ALTER TABLE xflow_audit_events RENAME COLUMN tenant_id TO namespace;
    ELSEIF v_has_tenant = 1 AND v_has_namespace = 1 THEN
        -- Dynamic SQL keeps tenant_id references out of executions where the
        -- legacy column no longer exists. CAST(... AS BINARY) plus byte length
        -- prevents case, accent, or trailing-space equivalence from merging
        -- distinct identities.
        SET @xflow_namespace_conflicts = 0;
        SET @xflow_namespace_sql =
            'SELECT COUNT(*) INTO @xflow_namespace_conflicts FROM xflow_audit_events WHERE OCTET_LENGTH(namespace) > 0 AND OCTET_LENGTH(tenant_id) > 0 AND CAST(namespace AS BINARY) <> CAST(tenant_id AS BINARY)';
        PREPARE xflow_namespace_stmt FROM @xflow_namespace_sql;
        EXECUTE xflow_namespace_stmt;
        DEALLOCATE PREPARE xflow_namespace_stmt;

        IF @xflow_namespace_conflicts > 0 THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'xflow_audit_events tenant_id/namespace conflict';
        END IF;

        SET @xflow_namespace_sql =
            'UPDATE xflow_audit_events SET namespace = tenant_id WHERE (namespace IS NULL OR OCTET_LENGTH(namespace) = 0) AND tenant_id IS NOT NULL';
        PREPARE xflow_namespace_stmt FROM @xflow_namespace_sql;
        EXECUTE xflow_namespace_stmt;
        DEALLOCATE PREPARE xflow_namespace_stmt;

        ALTER TABLE xflow_audit_events DROP COLUMN tenant_id;
    ELSEIF v_has_tenant = 0 AND v_has_namespace = 0 THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN namespace VARCHAR(128)
                CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                NOT NULL DEFAULT '' COMMENT '租户' AFTER principal;
    END IF;

    UPDATE xflow_audit_events SET namespace = '' WHERE namespace IS NULL;

    SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
           COLUMN_DEFAULT, COALESCE(COLLATION_NAME, '')
      INTO v_data_type, v_length, v_nullable, v_default, v_collation
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'namespace';

    IF v_data_type <> 'varchar'
       OR v_length <> 128
       OR v_nullable <> 'NO'
       OR NOT (v_default <=> '')
       OR v_collation <> 'utf8mb4_0900_bin' THEN
        ALTER TABLE xflow_audit_events
            MODIFY COLUMN namespace VARCHAR(128)
                CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                NOT NULL DEFAULT '' COMMENT '租户';
    END IF;

    SELECT COUNT(*) INTO v_has_request
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND COLUMN_NAME = 'request_id';

    IF v_has_request = 0 THEN
        ALTER TABLE xflow_audit_events
            ADD COLUMN request_id VARCHAR(128)
                CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                NOT NULL DEFAULT ''
                COMMENT '请求关联 ID（不可信为身份）' AFTER id;
    ELSE
        UPDATE xflow_audit_events SET request_id = '' WHERE request_id IS NULL;

        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
               COLUMN_DEFAULT, COALESCE(COLLATION_NAME, '')
          INTO v_data_type, v_length, v_nullable, v_default, v_collation
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'xflow_audit_events'
          AND COLUMN_NAME = 'request_id';

        IF v_data_type <> 'varchar'
           OR v_length <> 128
           OR v_nullable <> 'NO'
           OR NOT (v_default <=> '')
           OR v_collation <> 'utf8mb4_0900_bin' THEN
            ALTER TABLE xflow_audit_events
                MODIFY COLUMN request_id VARCHAR(128)
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
                    NOT NULL DEFAULT ''
                    COMMENT '请求关联 ID（不可信为身份）';
        END IF;
    END IF;

    SET @xflow_namespace_sql = NULL;
    SET @xflow_namespace_conflicts = NULL;
END$$
DELIMITER ;
CALL xflow_canonicalize_audit_identity();
DROP PROCEDURE IF EXISTS xflow_canonicalize_audit_identity;

-- Backfill exactly one canonical row (MIN(id)) for each byte-exact historical
-- outcome identity that has no existing key. This byte stream is identical to
-- auditPhaseKey in store/sqlstore/audit_repo.go:
--   domain "xflow:audit-phase-key:v1\\0";
--   uint64 big-endian byte length + raw bytes for namespace, request_id,
--   and literal "outcome"; SHA-256 encoded as lowercase hexadecimal.
DROP PROCEDURE IF EXISTS xflow_backfill_audit_phase_keys;
DELIMITER $$
CREATE PROCEDURE xflow_backfill_audit_phase_keys()
BEGIN
    DECLARE v_candidate_duplicates BIGINT DEFAULT 0;
    DECLARE v_existing_conflicts BIGINT DEFAULT 0;

    DROP TEMPORARY TABLE IF EXISTS xflow_audit_phase_backfill;
    CREATE TEMPORARY TABLE xflow_audit_phase_backfill (
        id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        phase_key VARCHAR(64) NOT NULL
    ) ENGINE=InnoDB;

    INSERT INTO xflow_audit_phase_backfill (id, phase_key)
    SELECT canonical.id,
           LOWER(SHA2(CONCAT(
               UNHEX('78666c6f773a61756469742d70686173652d6b65793a763100'),
               UNHEX(LPAD(HEX(OCTET_LENGTH(CAST(canonical.namespace AS BINARY))), 16, '0')),
               CAST(canonical.namespace AS BINARY),
               UNHEX(LPAD(HEX(OCTET_LENGTH(CAST(canonical.request_id AS BINARY))), 16, '0')),
               CAST(canonical.request_id AS BINARY),
               UNHEX('0000000000000007'),
               CAST('outcome' AS BINARY)
           ), 256))
    FROM xflow_audit_events AS canonical
    INNER JOIN (
        SELECT MIN(id) AS id
        FROM xflow_audit_events
        WHERE phase = 'outcome'
          AND OCTET_LENGTH(namespace) > 0
          AND OCTET_LENGTH(request_id) > 0
        GROUP BY CAST(namespace AS BINARY), CAST(request_id AS BINARY)
        HAVING COUNT(phase_key) = 0
    ) AS identity ON identity.id = canonical.id;

    SELECT COUNT(*) INTO v_candidate_duplicates
    FROM (
        SELECT phase_key
        FROM xflow_audit_phase_backfill
        GROUP BY phase_key
        HAVING COUNT(*) > 1
    ) AS duplicate_candidates;

    SELECT COUNT(*) INTO v_existing_conflicts
    FROM xflow_audit_phase_backfill AS candidate
    INNER JOIN xflow_audit_events AS existing
        ON existing.phase_key = candidate.phase_key;

    IF v_candidate_duplicates > 0 OR v_existing_conflicts > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'xflow audit phase-key backfill conflicts with an existing key';
    END IF;

    UPDATE xflow_audit_events AS target
    INNER JOIN xflow_audit_phase_backfill AS candidate
        ON candidate.id = target.id
    SET target.phase_key = candidate.phase_key
    WHERE target.phase_key IS NULL;

    DROP TEMPORARY TABLE xflow_audit_phase_backfill;
END$$
DELIMITER ;
CALL xflow_backfill_audit_phase_keys();
DROP PROCEDURE IF EXISTS xflow_backfill_audit_phase_keys;

DROP PROCEDURE IF EXISTS xflow_ensure_audit_join_index;
DELIMITER $$
CREATE PROCEDURE xflow_ensure_audit_join_index()
BEGIN
    DECLARE v_parts INT DEFAULT 0;
    DECLARE v_non_unique INT DEFAULT 1;
    DECLARE v_columns TEXT DEFAULT '';
    DECLARE v_prefix_parts INT DEFAULT 0;
    DECLARE v_index_type VARCHAR(32) DEFAULT '';
    DECLARE v_visible VARCHAR(3) DEFAULT '';

    SELECT COUNT(*), COALESCE(MIN(NON_UNIQUE), 1),
           COALESCE(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), ''),
           COALESCE(SUM(SUB_PART IS NOT NULL), 0),
           COALESCE(MIN(INDEX_TYPE), ''), COALESCE(MIN(IS_VISIBLE), '')
      INTO v_parts, v_non_unique, v_columns, v_prefix_parts,
           v_index_type, v_visible
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND INDEX_NAME = 'idx_namespace_request_phase';

    IF v_parts > 0
       AND (v_parts <> 3 OR v_non_unique <> 1
            OR v_columns <> 'namespace,request_id,phase'
            OR v_prefix_parts <> 0 OR v_index_type <> 'BTREE'
            OR v_visible <> 'YES') THEN
        ALTER TABLE xflow_audit_events DROP INDEX idx_namespace_request_phase;
        SET v_parts = 0;
    END IF;

    IF v_parts = 0 THEN
        ALTER TABLE xflow_audit_events
            ADD INDEX idx_namespace_request_phase
                (namespace, request_id, phase) USING BTREE;
    END IF;
END$$
DELIMITER ;
CALL xflow_ensure_audit_join_index();
DROP PROCEDURE IF EXISTS xflow_ensure_audit_join_index;

DROP PROCEDURE IF EXISTS xflow_ensure_audit_phase_key_index;
DELIMITER $$
CREATE PROCEDURE xflow_ensure_audit_phase_key_index()
BEGIN
    DECLARE v_parts INT DEFAULT 0;
    DECLARE v_non_unique INT DEFAULT 1;
    DECLARE v_columns TEXT DEFAULT '';
    DECLARE v_prefix_parts INT DEFAULT 0;
    DECLARE v_index_type VARCHAR(32) DEFAULT '';
    DECLARE v_visible VARCHAR(3) DEFAULT '';
    DECLARE v_duplicate_keys BIGINT DEFAULT 0;

    -- Detect corruption before either dropping a malformed named index or
    -- creating the unique index. Never erase keys to make DDL succeed.
    SELECT COUNT(*) INTO v_duplicate_keys
    FROM (
        SELECT phase_key
        FROM xflow_audit_events
        WHERE phase_key IS NOT NULL
        GROUP BY phase_key
        HAVING COUNT(*) > 1
    ) AS duplicate_keys;

    IF v_duplicate_keys > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'xflow_audit_events contains duplicate non-NULL phase_key values';
    END IF;

    SELECT COUNT(*), COALESCE(MIN(NON_UNIQUE), 1),
           COALESCE(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), ''),
           COALESCE(SUM(SUB_PART IS NOT NULL), 0),
           COALESCE(MIN(INDEX_TYPE), ''), COALESCE(MIN(IS_VISIBLE), '')
      INTO v_parts, v_non_unique, v_columns, v_prefix_parts,
           v_index_type, v_visible
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'xflow_audit_events'
      AND INDEX_NAME = 'uk_phase_key';

    IF v_parts > 0
       AND (v_parts <> 1 OR v_non_unique <> 0
            OR v_columns <> 'phase_key' OR v_prefix_parts <> 0
            OR v_index_type <> 'BTREE' OR v_visible <> 'YES') THEN
        ALTER TABLE xflow_audit_events DROP INDEX uk_phase_key;
        SET v_parts = 0;
    END IF;

    IF v_parts = 0 THEN
        ALTER TABLE xflow_audit_events
            ADD UNIQUE INDEX uk_phase_key (phase_key) USING BTREE;
    END IF;
END$$
DELIMITER ;
CALL xflow_ensure_audit_phase_key_index();
DROP PROCEDURE IF EXISTS xflow_ensure_audit_phase_key_index;

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
