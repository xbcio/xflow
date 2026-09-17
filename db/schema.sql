-- XFlow Raft Schema — UNUSED / LEGACY (MySQL 8.0+)
--
-- DO NOT APPLY. These tables are not part of any supported deployment.
--
-- This file described a Raft-based control-plane HA design that was abandoned:
-- the implemented election is RedisLeaderElector (Redis lease, 15s TTL) in
-- backend/providers/distributed/leader.go, and docs/design/CORE-COMPONENTS.md
-- states outright that Raft is neither present in the code nor planned.
--
-- Two consequences worth stating, because the previous header claimed the
-- opposite and invited a wrong deployment:
--   * NOTHING creates these tables at runtime. The old comment here said a
--     "RaftManager" checked and created them at startup; no such component
--     exists, and `git grep raft` finds no tracked code referencing either table.
--   * No tracked code reads or writes them. The schema a deployment actually
--     applies is db/xflow_schema.sql (see Makefile `db-migrate`, CI, and
--     test/env/migrate.sh).
--
-- Retained only as a record of the abandoned design. Delete when no longer
-- useful as history; it must not be applied to any environment.

-- Raft 日志表 (unused)
CREATE TABLE IF NOT EXISTS raft_logs (
    idx  BIGINT UNSIGNED  NOT NULL PRIMARY KEY COMMENT 'Raft 日志索引',
    term BIGINT UNSIGNED  NOT NULL             COMMENT '选举任期',
    type TINYINT UNSIGNED NOT NULL             COMMENT '日志类型(0=Command,1=Noop,2=Config,...)',
    data BLOB                                  COMMENT '日志数据'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Raft 稳定存储表（currentTerm、lastVotedFor 等元数据）(unused)
CREATE TABLE IF NOT EXISTS raft_stable (
    k VARCHAR(255) NOT NULL PRIMARY KEY,
    v BLOB         NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
