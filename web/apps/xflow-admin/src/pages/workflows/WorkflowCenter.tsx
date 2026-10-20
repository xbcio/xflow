import {
  AppstoreOutlined,
  BranchesOutlined,
  CloudServerOutlined,
  CodeOutlined,
  HistoryOutlined,
} from '@ant-design/icons';
import { createXFlowApiClient, type WorkflowSummary } from '@xflow/api';
import { history, useLocation } from '@umijs/max';
import { Alert, Button, Table, Tooltip, type TableColumnsType } from 'antd';
import { useEffect, useMemo, useState } from 'react';

import { fetchRecentExecutions, type RecentExecution } from '../../services/executions';
import { authorizedFetch } from '../../services/http';
import { fetchRunnerPools, fetchRunners } from '../../services/management';
import { NewFieldMark, PanelTitle, StatusChip } from './parts';

/** Server page size for the workflow table (OpenAPI: `page_size`). */
export const PAGE_SIZE = 20;
/** Ruling 26: the recent-run card shows exactly three rows. */
export const RECENT_RUNS = 3;

interface EnvCards {
  state: 'loading' | 'ready' | 'hidden';
  runners?: { online: number; total: number };
  pools?: number;
  nodeTypes?: number;
}

function errorMessage(reason: unknown, fallback: string): string {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}

/**
 * Workflow centre (§6.6, ruling 20). Columns only carry what the backend really
 * has: `GET /v1/workflows` returns id / name / version, so the removed 节点
 * column stays gone until the list can answer node counts. 触发 and 最近运行
 * are drawn as 需新字段 placeholders — the list endpoint has neither field,
 * and querying one execution per row is not an option.
 *
 * Server pagination is real (`page` / `page_size`). Search has no server
 * parameter in the OpenAPI contract, so the page never applies a client-side
 * filter over one page; a note says the condition is not applied yet.
 */
export function WorkflowCenter() {
  // `@xflow/api` falls back to the global fetch, which does not carry the
  // admin session's Bearer token; authorizedFetch is what makes these two
  // reads (workflows, node types) authenticate like the umi request services.
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );
  const { search } = useLocation();
  const query = new URLSearchParams(search).get('q')?.trim() ?? '';

  const [env, setEnv] = useState<EnvCards>({ state: 'loading' });
  const [rows, setRows] = useState<WorkflowSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string>();
  const [listAttempt, setListAttempt] = useState(0);

  const [recent, setRecent] = useState<RecentExecution[]>();
  const [recentError, setRecentError] = useState<string>();
  const [recentAttempt, setRecentAttempt] = useState(0);

  // Three status cards share the management module's availability: when both
  // management calls answer 403/404 (module disabled) the whole group is
  // hidden rather than drawing misleading zeroes. Node types is independent,
  // but it is still part of the hidden group because §6.6 hides the row.
  useEffect(() => {
    let cancelled = false;
    Promise.allSettled([fetchRunners(), fetchRunnerPools(), api.listNodeTypes()]).then(
      ([runners, pools, types]) => {
        if (cancelled) return;
        if (runners.status === 'rejected' && pools.status === 'rejected') {
          setEnv({ state: 'hidden' });
          return;
        }
        setEnv({
          state: 'ready',
          runners:
            runners.status === 'fulfilled'
              ? {
                  online: runners.value.filter((runner) => runner.state === 'online').length,
                  total: runners.value.length,
                }
              : undefined,
          pools: pools.status === 'fulfilled' ? pools.value.length : undefined,
          nodeTypes: types.status === 'fulfilled' ? types.value.node_types.length : undefined,
        });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api]);

  // A failed page keeps the rows already on screen (the loading state does the
  // same), and the banner names the failure; 重试 refetches the same page.
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api.listWorkflows({ page, pageSize: PAGE_SIZE }).then(
      (result) => {
        if (cancelled) return;
        setRows(result.list);
        setTotal(result.total);
        setListError(undefined);
        setLoading(false);
      },
      (reason: unknown) => {
        if (cancelled) return;
        setListError(errorMessage(reason, '工作流列表加载失败，请稍后重试。'));
        setLoading(false);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, page, listAttempt]);

  useEffect(() => {
    let cancelled = false;
    fetchRecentExecutions(RECENT_RUNS).then(
      (list) => {
        if (cancelled) return;
        setRecent(list);
        setRecentError(undefined);
      },
      (reason: unknown) => {
        if (cancelled) return;
        setRecentError(errorMessage(reason, '最近运行加载失败。'));
      },
    );
    return () => {
      cancelled = true;
    };
  }, [recentAttempt]);

  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const columns: TableColumnsType<WorkflowSummary> = [
    {
      title: '名称',
      dataIndex: 'name',
      className: 'xflow-admin-col-name',
      render: (_value: string, row) => (
        <span className="xflow-admin-name">
          <strong>{row.name}</strong>
          <small>{row.id}</small>
        </span>
      ),
    },
    { title: '版本', dataIndex: 'version', className: 'xflow-admin-col-version', width: 96 },
    {
      title: <NewFieldMark title="列表接口没有入口类型字段（§6.6）">触发</NewFieldMark>,
      key: 'entry',
      className: 'xflow-admin-col-entry',
      width: 110,
      render: () => <span className="xflow-admin-faint">—</span>,
    },
    {
      title: <NewFieldMark title="列表接口不带最近执行，逐行再查不可取（§6.6）">最近运行</NewFieldMark>,
      key: 'lastRun',
      className: 'xflow-admin-col-lastrun',
      width: 168,
      render: () => <span className="xflow-admin-faint">—</span>,
    },
    {
      title: '',
      key: 'ops',
      className: 'xflow-admin-col-ops',
      align: 'right',
      width: 88,
      render: (_value: unknown, row) => (
        <span className="xflow-admin-ops">
          <Button size="small" type="text" data-op="open" onClick={() => history.push(`/workflows/${row.id}`)}>
            打开
          </Button>
        </span>
      ),
    },
  ];

  return (
    <>
      {query ? (
        <p className="xflow-admin-note" data-testid="search-note">
          服务端列表接口暂不支持按名称 / ID 搜索，当前显示全部工作流（搜索条件仍保留在地址栏）。
        </p>
      ) : null}

      {env.state === 'ready' ? (
        <section className="grid grid-cols-3 gap-3" data-panel="env-cards" aria-label="集群概览">
          <div className="xflow-admin-card flex min-w-0 items-center gap-3 px-4 py-3" data-env="runners">
            <span className="xflow-admin-card-icon" aria-hidden="true">
              <CloudServerOutlined />
            </span>
            <span>
              <span className="xflow-admin-card-v">
                {env.runners ? `${env.runners.online} / ${env.runners.total}` : '—'}
              </span>
              <span className="xflow-admin-card-l">Runner 在线</span>
            </span>
            <span className="xflow-admin-card-src">management/runners</span>
          </div>
          <div className="xflow-admin-card flex min-w-0 items-center gap-3 px-4 py-3" data-env="pools">
            <span className="xflow-admin-card-icon" aria-hidden="true">
              <AppstoreOutlined />
            </span>
            <span>
              <span className="xflow-admin-card-v">{env.pools ?? '—'}</span>
              <span className="xflow-admin-card-l">Runner 池</span>
            </span>
            <span className="xflow-admin-card-src">management/runner-pools</span>
          </div>
          <div className="xflow-admin-card flex min-w-0 items-center gap-3 px-4 py-3" data-env="types">
            <span className="xflow-admin-card-icon" aria-hidden="true">
              <CodeOutlined />
            </span>
            <span>
              <span className="xflow-admin-card-v">{env.nodeTypes ?? '—'}</span>
              <span className="xflow-admin-card-l">可用节点类型</span>
            </span>
            <span className="xflow-admin-card-src">node-types</span>
          </div>
        </section>
      ) : null}

      {/* Stable hook for the e2e smoke: this master grid is pure Tailwind. */}
      <div
        className="grid grid-cols-[minmax(0,1fr)_320px] items-start gap-4"
        data-testid="workflow-center-layout"
      >
        <div className="xflow-admin-card xflow-admin-tablecard" data-table="workflows">
          <PanelTitle icon={<AppstoreOutlined />} title="工作流" count={total} />
          {listError ? (
            <div className="xflow-admin-banner">
              <Alert
                type="error"
                showIcon
                title="工作流列表加载失败"
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
            rowKey="id"
            pagination={false}
            loading={loading}
            locale={{ emptyText: '没有匹配的记录' }}
            dataSource={rows}
            onRow={(row) => ({ 'data-workflow': row.id }) as Record<string, string>}
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

        <div className="flex min-w-0 flex-col gap-3">
          <div className="xflow-admin-card xflow-admin-new flex min-w-0 items-start gap-3 px-3 py-3" data-create="blank">
            <span className="xflow-admin-card-icon xflow-admin-card-icon--code" aria-hidden="true">
              <BranchesOutlined />
            </span>
            <div>
              <h4>从空白创建</h4>
              <p>进入编辑器，从触发器开始搭建。</p>
              <Button size="small" onClick={() => history.push('/workflows/new')}>
                新建
              </Button>
            </div>
          </div>

          <div className="xflow-admin-card xflow-admin-new flex min-w-0 items-start gap-3 px-3 py-3" data-create="import">
            <span className="xflow-admin-card-icon xflow-admin-card-icon--code" aria-hidden="true">
              <CodeOutlined />
            </span>
            <div>
              <h4>导入 DSL</h4>
              <p>导入 YAML / JSON 定义，注册前完成校验。</p>
              <Tooltip title="未接入">
                <span className="xflow-admin-control">
                  <Button size="small" disabled aria-description="未接入">
                    选择文件
                  </Button>
                </span>
              </Tooltip>
            </div>
          </div>

          <div className="xflow-admin-card xflow-admin-tablecard" data-panel="recent-runs">
            <PanelTitle icon={<HistoryOutlined />} title="最近运行" count={recent?.length} />
            {recentError ? (
              <p className="xflow-admin-run-empty" data-state="error">
                {recentError}
                <Button size="small" type="link" onClick={() => setRecentAttempt((attempt) => attempt + 1)}>
                  重试
                </Button>
              </p>
            ) : recent === undefined ? (
              <p className="xflow-admin-run-empty" data-state="loading">
                加载中…
              </p>
            ) : recent.length === 0 ? (
              <p className="xflow-admin-run-empty" data-state="empty">
                暂无运行记录
              </p>
            ) : (
              recent.map((row) => (
                <div
                  className="xflow-admin-run"
                  key={row.executionId}
                  data-exec={row.executionId}
                  data-status={row.status}
                  data-workflow={row.workflowName}
                  title={row.executionId}
                >
                  <span className="xflow-admin-run-id">{row.executionId}</span>
                  <span className="xflow-admin-run-name">{row.workflowName}</span>
                  <StatusChip status={row.status} />
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </>
  );
}
