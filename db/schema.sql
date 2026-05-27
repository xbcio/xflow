-- XFlow Raft Schema (MySQL 8.0+)
-- Master 集群 Leader 选举用，写入量极小（选举日志 + 心跳），对业务库无性能影响
-- 由 RaftManager 启动时自动检查并创建

-- Raft 日志表
CREATE TABLE IF NOT EXISTS raft_logs (
    idx  BIGINT UNSIGNED  NOT NULL PRIMARY KEY COMMENT 'Raft 日志索引',
    term BIGINT UNSIGNED  NOT NULL             COMMENT '选举任期',
    type TINYINT UNSIGNED NOT NULL             COMMENT '日志类型(0=Command,1=Noop,2=Config,...)',
    data BLOB                                  COMMENT '日志数据'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Raft 稳定存储表（currentTerm、lastVotedFor 等元数据）
CREATE TABLE IF NOT EXISTS raft_stable (
    k VARCHAR(255) NOT NULL PRIMARY KEY,
    v BLOB         NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
