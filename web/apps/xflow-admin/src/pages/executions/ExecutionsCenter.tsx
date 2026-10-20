import { DownOutlined, HistoryOutlined } from '@ant-design/icons';
import {
  createXFlowApiClient,
  XFlowApiError,
  type ExecutionDetail,
  type ExecutionListItem,
  type RerunExecutionResult,
} from '@xflow/api';
import {
  executionOpLabel,
  workflowStatusMeta,
  type ExecutionOp,
  type ExecutionStatusKey,
} from '@xflow/core';
import {
  Alert,
  App,
  Button,
  Checkbox,
  Modal,
  Radio,
  Segmented,
  Table,
  Tooltip,
  type TableColumnsType,
} from 'antd';
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';

import { authorizedFetch } from '../../services/http';
import { PanelTitle, StatusChip } from '../workflows/parts';

/** Server page size for the execution table (OpenAPI `page_size`). */
export const EXECUTION_PAGE_SIZE = 20;

/**
 * One server-supported filter value. The list endpoint narrows by a SINGLE
 * lifecycle status; there is no multi-select and no workflow parameter yet.
 */
type StatusFilter = 'all' | ExecutionStatusKey;

type RerunOrigin = 'head' | 'failed';

function errorMessage(reason: unknown, fallback: string): string {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}

/** Local `MM-DD HH:mm:ss`; a value the server sent unparsable is shown verbatim. */
function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const pad = (part: number) => String(part).padStart(2, '0');
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(
    date.getMinutes(),
  )}:${pad(date.getSeconds())}`;
}

/**
 * `updated_at − created_at`, only for terminal executions (§6.8). A live
 * execution's duration is still growing; drawing it would be a stale number
 * that changes on every read.
 */
function formatDuration(row: ExecutionListItem): string {
  if (!workflowStatusMeta[row.status].terminal) return '—';
  const created = Date.parse(row.createdAt);
  const updated = Date.parse(row.updatedAt);
  if (!Number.isFinite(created) || !Number.isFinite(updated) || updated < created) return '—';
  const ms = updated - created;
  if (ms < 1000) return `${ms} ms`;
  // Round once, in the displayed unit. Rounding the minute and the second
  // remainder separately produced impossible values like "1m 60s".
  const tenths = Math.round(ms / 100);
  if (tenths < 600) return `${(tenths / 10).toFixed(1)} s`;
  const totalSeconds = Math.round(ms / 1000);
  return `${Math.floor(totalSeconds / 60)}m ${totalSeconds % 60}s`;
}

/** Portal target: the shell root carries the `--xflow-*` tokens (tokens.css). */
function portalContainer(): HTMLElement {
  return document.querySelector<HTMLElement>('.xflow-admin') ?? document.body;
}

function firstLine(value: string): string {
  return value.split(/\r?\n/, 1)[0] ?? '';
}

/** Ruling 23: a duplicate rerun points at the execution the first attempt created. */
function rerunToast(result: RerunExecutionResult): ReactNode {
  const queue =
    result.status === 'queued' && result.queuePosition
      ? `（排队第 ${result.queuePosition} 位）`
      : '';
  return (
    <span>
      {result.duplicate ? '该重跑已提交' : '已提交重跑'}{' '}
      <span className="xflow-admin-mono">{result.executionId}</span>
      {queue}
    </span>
  );
}

/**
 * Ruling 23 / §6.9 failure text. Codes are the OpenAPI contract, not HTTP
 * status alone: a 409 can mean four different rerun-source problems, and the
 * operator needs to know which one.
 */
function rerunErrorMessage(reason: unknown): string {
  if (reason instanceof XFlowApiError) {
    switch (reason.code) {
      case 'execution_not_rerunnable':
        return '该执行当前不可重跑：只有失败、已取消、超时或成功的执行可以重跑。';
      case 'rerun_upstream_incomplete':
        return '起点上游有节点没有可沿用的结果，请改从该节点重跑。';
      case 'rerun_graph_unsupported':
        return '该执行的图不支持从节点重跑（例如环或分组结构）。';
      case 'rerun_node_not_found':
        return '起点节点不在该执行的图里，请刷新后重试。';
      case 'idempotency_key_conflict':
        return '该幂等键已用于另一个重跑请求，请关闭对话框后重新打开。';
      case 'idempotency_key_in_progress':
        return '该重跑正在创建中，请稍后重试。';
      case 'execution_not_found':
        return '执行不存在，或已超出保留期。';
      case 'execution_rerun_unsupported':
        return '当前部署不支持重跑。';
      // OpenAPI 429 covers both a spent namespace rate budget and the
      // workflow concurrency gate; only the latter means "queue is full".
      case 'rate_limited':
        return '请求过于频繁，请稍后重试。';
      default:
        break;
    }
    if (reason.status === 429) {
      return '并发上限已满，请稍后重试。';
    }
  }
  return errorMessage(reason, '重跑提交失败，请稍后重试。');
}

/**
 * Execution list (§6.6 / §6.9).
 *
 * Everything that describes what a status can do is read from
 * `workflowStatusMeta`: the chip, the terminal-only duration and the row
 * operations all come from the same table, so a status can only be drawn one
 * way. The table itself is server-paginated and server-filtered — there is no
 * client-side filter over the current page, because that would silently drop
 * rows the server has not sent yet.
 *
 * Ruling 23: a successful rerun stays on this page. The dialog closes, page 1
 * is refetched (the new execution sorts first) and its row is highlighted.
 */
export function ExecutionsCenter() {
  const { message } = App.useApp();
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );

  const [rows, setRows] = useState<ExecutionListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [seenStatuses, setSeenStatuses] = useState<ExecutionStatusKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string>();
  const [listAttempt, setListAttempt] = useState(0);
  const [highlightId, setHighlightId] = useState<string>();

  // A failed fetch keeps the loaded page (the loading state does the same);
  // 重试 refetches exactly the page and filter that failed.
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api.listExecutions({
      page,
      pageSize: EXECUTION_PAGE_SIZE,
      ...(statusFilter === 'all' ? {} : { status: statusFilter }),
    }).then(
      (result) => {
        if (cancelled) return;
        setRows(result.list);
        setTotal(result.total);
        setListError(undefined);
        setLoading(false);
        setSeenStatuses((previous) => {
          const next = [...previous];
          for (const row of result.list) {
            if (!next.includes(row.status)) next.push(row.status);
          }
          return next;
        });
      },
      (reason: unknown) => {
        if (cancelled) return;
        setListError(errorMessage(reason, '运行记录加载失败，请稍后重试。'));
        setLoading(false);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, page, statusFilter, listAttempt]);

  // Ruling 21: two origins, defaulting to the snapshot's first failed node.
  // For a success source (ruling 22) the failed option is removed entirely.
  const [rerunRow, setRerunRow] = useState<ExecutionListItem>();
  const [snapshot, setSnapshot] = useState<ExecutionDetail>();
  const [snapshotError, setSnapshotError] = useState<string>();
  const [snapshotAttempt, setSnapshotAttempt] = useState(0);
  const [origin, setOrigin] = useState<RerunOrigin>('head');
  const [confirmSuccess, setConfirmSuccess] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState<string>();

  // Baseline list-exec puts the filter row in the shell's top bar. The page
  // renders into that slot when the shell is present; a bare render (tests)
  // falls back to the page body. The slot is committed before this effect.
  const [topSlot, setTopSlot] = useState<HTMLElement>();
  useEffect(() => {
    setTopSlot(document.querySelector<HTMLElement>('[data-admin-top-slot]') ?? undefined);
  }, []);

  useEffect(() => {
    if (!rerunRow) return;
    let cancelled = false;
    setSnapshot(undefined);
    setSnapshotError(undefined);
    api.getExecution(rerunRow.executionId).then(
      (detail) => {
        if (cancelled) return;
        setSnapshot(detail);
        const firstFailed = detail.nodes?.find((node) => node.status === 'failed');
        setOrigin(rerunRow.status !== 'success' && firstFailed ? 'failed' : 'head');
      },
      (reason: unknown) => {
        if (cancelled) return;
        setSnapshotError(errorMessage(reason, '执行快照加载失败，请稍后重试。'));
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, rerunRow, snapshotAttempt]);

  /** Ruling 23: one Idempotency-Key per dialog open; retries reuse it. */
  const openRerun = (row: ExecutionListItem) => {
    setRerunRow(row);
    setSnapshot(undefined);
    setSnapshotError(undefined);
    setSnapshotAttempt(0);
    setOrigin('head');
    setConfirmSuccess(false);
    setSubmitting(false);
    setIdempotencyKey(crypto.randomUUID());
  };

  const closeRerun = () => {
    if (!submitting) setRerunRow(undefined);
  };

  const successSource = rerunRow?.status === 'success';
  const failedNodes = snapshot?.nodes?.filter((node) => node.status === 'failed') ?? [];
  const failedNode = failedNodes[0]?.name;
  // OpenAPI exposes no entry-node field on an execution, so "从头" is the
  // first node of the stored graph snapshot (the order Inspect reports).
  const headNode = snapshot?.nodes?.[0]?.name;
  const fromNode = origin === 'failed' ? failedNode : headNode;
  const canSubmit = !!snapshot && !!fromNode && (!successSource || confirmSuccess);

  const submitRerun = async () => {
    if (!rerunRow || !canSubmit || !idempotencyKey || submitting) return;
    setSubmitting(true);
    try {
      const result = await api.rerunExecution(rerunRow.executionId, {
        fromNode: fromNode as string,
        includeSuccess: successSource === true,
        idempotencyKey,
      });
      setRerunRow(undefined);
      // Ruling 23: the new execution is queued/running, so it can never match
      // a failed/success filter. Reset to 全部 before refetching, otherwise
      // "page 1 with the new row highlighted" silently does nothing.
      setStatusFilter('all');
      setHighlightId(result.executionId);
      setPage(1);
      setListAttempt((attempt) => attempt + 1);
      message.success(rerunToast(result));
    } catch (reason: unknown) {
      message.error(rerunErrorMessage(reason));
    } finally {
      setSubmitting(false);
    }
  };

  const filterOptions = useMemo(() => {
    const seen = new Set(seenStatuses);
    return [
      { label: '全部', value: 'all' as StatusFilter },
      ...(Object.keys(workflowStatusMeta) as ExecutionStatusKey[])
        .filter((status) => seen.has(status))
        .map((status) => ({ label: workflowStatusMeta[status].label, value: status })),
    ];
  }, [seenStatuses]);

  const pageCount = Math.max(1, Math.ceil(total / EXECUTION_PAGE_SIZE));

  const columns: TableColumnsType<ExecutionListItem> = [
    {
      title: '执行',
      dataIndex: 'executionId',
      className: 'xflow-admin-col-exec',
      width: 120,
      // Real ids are `exec-<uuid>` (41 chars); keep the row one line high and
      // put the full value in the tooltip.
      render: (id: string) => (
        <Tooltip title={id}>
          <span className="xflow-admin-mono xflow-admin-ellipsis">{id}</span>
        </Tooltip>
      ),
    },
    {
      title: '工作流',
      dataIndex: 'workflowName',
      className: 'xflow-admin-col-workflow',
      width: 168,
    },
    {
      title: '状态',
      dataIndex: 'status',
      className: 'xflow-admin-col-status',
      width: 116,
      render: (status: ExecutionStatusKey) => <StatusChip status={status} />,
    },
    {
      title: '创建',
      dataIndex: 'createdAt',
      className: 'xflow-admin-col-created',
      width: 140,
      render: (value: string) => <span className="xflow-admin-mono">{formatTimestamp(value)}</span>,
    },
    {
      title: '时长',
      key: 'duration',
      className: 'xflow-admin-col-duration',
      width: 88,
      render: (_value: unknown, row) => formatDuration(row),
    },
    {
      title: '错误',
      dataIndex: 'error',
      className: 'xflow-admin-col-error',
      render: (value: string) => {
        const line = firstLine(value);
        return line ? (
          <Tooltip title={value}>
            <span className="xflow-admin-err">{line}</span>
          </Tooltip>
        ) : (
          <span className="xflow-admin-faint">—</span>
        );
      },
    },
    {
      title: '',
      key: 'ops',
      className: 'xflow-admin-col-ops',
      align: 'right',
      width: 168,
      render: (_value: unknown, row) => {
        const meta = workflowStatusMeta[row.status];
        return (
          <span className="xflow-admin-ops" data-ops={meta.ops.join(',')}>
            {meta.ops.map((op: ExecutionOp) =>
              op === 'rerun' ? (
                <Button size="small" type="text" key={op} data-op={op} onClick={() => openRerun(row)}>
                  {executionOpLabel[op]}
                </Button>
              ) : (
                <Tooltip title="未接入" key={op}>
                  <span className="xflow-admin-control">
                    <Button size="small" type="text" data-op={op} disabled aria-description="未接入">
                      {executionOpLabel[op]}
                    </Button>
                  </span>
                </Tooltip>
              ),
            )}
          </span>
        );
      },
    },
  ];

  const filterBar = (
    <div className="flex w-full flex-wrap items-center gap-2" data-panel="exec-filters">
      <Segmented
        size="small"
        value={statusFilter}
        options={filterOptions}
        onChange={(value) => {
          setStatusFilter(value as StatusFilter);
          setPage(1);
        }}
      />
      <Tooltip title="未接入">
        <span className="xflow-admin-control">
          <Button size="small" icon={<DownOutlined />} iconPlacement="end" disabled aria-description="未接入">
            最近 24 小时
          </Button>
        </span>
      </Tooltip>
      <Tooltip title="列表接口没有按工作流筛选的参数（§6.6）">
        <span className="xflow-admin-control">
          <Button size="small" icon={<DownOutlined />} iconPlacement="end" disabled aria-description="需新字段">
            工作流：全部
          </Button>
        </span>
      </Tooltip>
      <span className="flex-1" />
      <span className="xflow-admin-faint">没有批量接口，不做批量操作</span>
    </div>
  );

  return (
    <div className="flex flex-col gap-4">
      {topSlot ? createPortal(filterBar, topSlot) : filterBar}

      <div className="xflow-admin-card xflow-admin-tablecard" data-table="executions">
        <PanelTitle icon={<HistoryOutlined />} title="运行记录" count={total} />
        {listError ? (
          <div className="xflow-admin-banner">
            <Alert
              type="error"
              showIcon
              title="运行记录加载失败"
              description={listError}
              action={
                <Button size="small" onClick={() => setListAttempt((attempt) => attempt + 1)}>
                  重试
                </Button>
              }
            />
          </div>
        ) : null}
        <Table
          size="small"
          rowKey="executionId"
          pagination={false}
          loading={loading}
          locale={{ emptyText: '没有匹配的记录' }}
          dataSource={rows}
          rowClassName={(row) =>
            row.executionId === highlightId ? 'xflow-admin-row-highlight' : ''
          }
          onRow={(row) =>
            ({
              'data-exec': row.executionId,
              'data-status': row.status,
              'data-workflow': row.workflowName,
            }) as Record<string, string>
          }
          columns={columns}
        />
        <div className="xflow-admin-page" data-page={page} data-page-count={pageCount}>
          第 {page} / {pageCount} 页
          <Button size="small" disabled={page <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>
            上一页
          </Button>
          <Button size="small" disabled={page >= pageCount} onClick={() => setPage((current) => current + 1)}>
            下一页
          </Button>
        </div>
      </div>

      <Modal
        open={rerunRow !== undefined}
        title="重跑执行"
        okText="从这里重跑"
        cancelText="取消"
        confirmLoading={submitting}
        okButtonProps={{ disabled: !canSubmit }}
        // Portal into the shell so `--xflow-*` resolve inside the dialog;
        // the semantic root class keeps the fixed overlay out of its grid.
        getContainer={portalContainer}
        rootClassName="xflow-admin-modal-root"
        onOk={() => {
          void submitRerun();
        }}
        onCancel={closeRerun}
        destroyOnHidden
      >
        {rerunRow ? (
          <div className="flex flex-col gap-3">
            <p className="xflow-admin-dialog-sub">
              <span className="xflow-admin-mono">{rerunRow.executionId}</span> · {rerunRow.workflowName}
            </p>
            {snapshotError ? (
              <Alert
                type="error"
                showIcon
                title="执行快照加载失败"
                description={snapshotError}
                action={
                  <Button size="small" onClick={() => setSnapshotAttempt((attempt) => attempt + 1)}>
                    重试
                  </Button>
                }
              />
            ) : snapshot === undefined ? (
              <p className="xflow-admin-hint">正在读取执行快照…</p>
            ) : (
              <>
                <div className="flex flex-col gap-1">
                  <span className="xflow-admin-field-label">起点节点</span>
                  <Radio.Group
                    aria-label="起点节点"
                    value={origin}
                    onChange={(event) => setOrigin(event.target.value as RerunOrigin)}
                  >
                    <Radio value="head">从头</Radio>
                    {successSource ? null : (
                      <Radio value="failed" disabled={!failedNode}>
                        从失败节点{failedNode ? ` · ${failedNode}` : '（无失败节点）'}
                      </Radio>
                    )}
                  </Radio.Group>
                  <span className="xflow-admin-hint">
                    {successSource
                      ? '源执行已成功：整张图重新执行。'
                      : '上游节点的结果沿用上一次运行；起点之后重新执行。默认选中快照里第一个失败节点。'}
                  </span>
                </div>
                {!successSource && failedNodes.length > 0 ? (
                  <p className="xflow-admin-hint">
                    失败节点 {failedNodes.length} 个 · {failedNodes.map((node) => node.name).join('、')}
                  </p>
                ) : null}
                {successSource ? (
                  <Checkbox
                    checked={confirmSuccess}
                    onChange={(event) => setConfirmSuccess(event.target.checked)}
                  >
                    确认重跑已成功的执行
                  </Checkbox>
                ) : null}
              </>
            )}
          </div>
        ) : null}
      </Modal>
    </div>
  );
}
