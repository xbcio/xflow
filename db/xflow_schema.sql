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
    INDEX idx_principal (principal),
    INDEX idx_operation (operation),
    INDEX idx_execution_id (execution_id),
    INDEX idx_outcome (outcome),
    INDEX idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
