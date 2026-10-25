import * as React from "react";
import { HistoryIcon } from "@xflow/icons";
import { Button, Popconfirm, Tooltip } from "antd";
import { versionHashPrefix, versionLabel } from "../versioning";

/**
 * §6.10 version history: the workflow's version labels, current first (the
 * server orders the list), each with its hash and the two operations 查看 /
 * 回滚到此. A row the server refused with `workflow_version_unavailable` stays
 * disabled with the reason on the row — the failure is a property of that
 * version, not of the session.
 *
 * The host owns the data and the actions (the editor never requests); this
 * panel only renders what it is given.
 */

/** Structurally `@xflow/api`'s WorkflowVersion; declared here so the editor keeps its package boundary. */
export interface XFlowWorkflowVersion {
  version: string;
  definitionHash: string;
  registryRevision?: number;
  current: boolean;
  archived: boolean;
}

/** Structurally `@xflow/api`'s WorkflowVersionPage. */
export interface XFlowWorkflowVersions {
  list: XFlowWorkflowVersion[];
  total: number;
}

export interface VersionHistoryProps {
  versions?: XFlowWorkflowVersions;
  loading?: boolean;
  /** The load failure, already phrased for the reader. */
  error?: string;
  /** version → the server's refusal reason (workflow_version_unavailable). */
  blocked: Record<string, string>;
  /** The version whose rollback is in flight. */
  rollingBackVersion?: string;
  onReload: () => void;
  onViewVersion?: (version: string) => void;
  onRollback?: (version: string) => void;
}

/**
 * The server's default archive retention (§6.10: `versions.retention`). It is a
 * registry configuration the versions response does not carry, so the note
 * states the default the spec pins rather than pretending to read it.
 */
export const VERSION_RETENTION = 50;

export function VersionHistory({
  versions,
  loading = false,
  error,
  blocked,
  rollingBackVersion,
  onReload,
  onViewVersion,
  onRollback
}: VersionHistoryProps): React.ReactElement {
  const rows = versions?.list ?? [];
  return (
    <section aria-label="版本历史" className="xflow-editor-version-history">
      <header className="xflow-editor-version-history__head">
        <span aria-hidden="true" className="xflow-editor-version-history__icon">
          <HistoryIcon />
        </span>
        <h3 className="xflow-editor-version-history__title">版本历史</h3>
        {versions ? <span className="xflow-editor-version-history__count">{versions.total}</span> : null}
      </header>
      {loading ? (
        <p className="xflow-editor-version-history__state" role="status">
          正在载入版本历史…
        </p>
      ) : error ? (
        <div className="xflow-editor-version-history__state" role="alert">
          <span>{error}</span>
          <Button size="small" onClick={onReload}>
            重试
          </Button>
        </div>
      ) : rows.length === 0 ? (
        <p className="xflow-editor-version-history__state">没有版本历史。</p>
      ) : (
        <ol className="xflow-editor-version-history__rows">
          {rows.map((row) => {
            const reason = blocked[row.version];
            const rolling = rollingBackVersion === row.version;
            const label = versionLabel(row.version);
            return (
              <li
                className="xflow-editor-version-history__row"
                data-blocked={reason}
                data-online={row.current ? "" : undefined}
                data-version={row.version}
                key={row.version}
              >
                <span className="xflow-editor-version-history__version">{label}</span>
                <span className="xflow-editor-version-history__meta">
                  {row.current ? (
                    <span className="xflow-editor-chip" data-tone="success">
                      线上
                    </span>
                  ) : null}
                  <span className="xflow-editor-version-history__hash" title={row.definitionHash}>
                    {versionHashPrefix(row.definitionHash)}
                  </span>
                </span>
                <span className="xflow-editor-version-history__ops">
                  <Button
                    data-op="view"
                    disabled={!onViewVersion}
                    size="small"
                    type="text"
                    onClick={() => onViewVersion?.(row.version)}
                  >
                    查看
                  </Button>
                  {row.current ? null : reason ? (
                    <Tooltip title={reason}>
                      <span>
                        <Button data-op="rollback-blocked" disabled size="small">
                          不可回滚
                        </Button>
                      </span>
                    </Tooltip>
                  ) : onRollback ? (
                    <Popconfirm
                      cancelText="取消"
                      description={`把 ${label} 装回线上？当前线上版本会进入历史。`}
                      okText="回滚"
                      title="回滚到此版本？"
                      onConfirm={() => onRollback(row.version)}
                    >
                      <Button data-op="rollback" disabled={rolling} loading={rolling} size="small">
                        回滚到此
                      </Button>
                    </Popconfirm>
                  ) : null}
                </span>
                {reason ? (
                  <small className="xflow-editor-version-history__blocked" data-version-blocked={reason}>
                    {reason}
                  </small>
                ) : null}
              </li>
            );
          })}
        </ol>
      )}
      <p className="xflow-editor-version-history__note">
        「查看」在只读编辑器中打开该版本定义；回滚 = 把归档版本原样装回线上，当前线上版本进入历史，历史永不改写。
        保留最近 {VERSION_RETENTION} 个归档版本（
        <span className="xflow-editor-version-history__mono">versions.retention</span>），更早的已清理。
      </p>
    </section>
  );
}
