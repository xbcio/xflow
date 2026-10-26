import {
  CodeIcon,
  CopyIcon,
  GridIcon,
  HistoryIcon,
  MoreIcon,
  PlusIcon,
  SearchIcon,
  ServerIcon,
  UploadIcon,
} from '@xflow/icons';
import {
  createXFlowApiClient,
  type ExecutionListItem,
  type RegisterWorkflowResult,
  type WorkflowStatsWindow,
  type WorkflowSummary,
} from '@xflow/api';
import { history, useAccess, useLocation, useModel } from '@umijs/max';
import {
  Alert,
  App,
  Button,
  Dropdown,
  Empty,
  Input,
  Segmented,
  Table,
  type InputRef,
  type MenuProps,
  type TableColumnsType,
} from 'antd';
import { dump as dumpYaml } from 'js-yaml';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import type { InitialState } from '../../app';
import { PageHeader } from '../../components/PageHeader';
import { TableProgressBar } from '../../components/TableProgressBar';
import { TableToolbar } from '../../components/TableToolbar';
import { useSearchShortcut } from '../../components/useSearchShortcut';
import { authorizedFetch } from '../../services/http';
import { fetchRunnerPools, fetchRunners } from '../../services/management';
import { useAdminWorkflowCount } from '../../layouts/AdminShell';
import { withParam } from '../../layouts/useAdminNamespace';
import { MANAGEMENT_READ_SCOPES } from '../../layouts/useClusterStatus';
import { useWorkflowFavorites } from './favorites';
import { ImportWorkflowDialog } from './ImportWorkflowDialog';
import { isRowControl, PanelTitle, TableSkeleton } from './parts';
import {
  DurationCell,
  RunStrip,
  StarIcon,
  SuccessRateCell,
  TriggerSummary,
  failureSummary,
  relativeTime,
  updatedStamp,
  useMinWidth,
  versionLabel,
  windowLabel,
} from './row-parts';
import './workflow-row.css';

/** Server page size for the workflow table (OpenAPI: `page_size`). */
export const PAGE_SIZE = 20;
/** R2: the rail lists at most five recent failures in a fixed 24h window. */
export const RECENT_FAILURES = 5;
const RECENT_FAILURE_WINDOW_MS = 24 * 60 * 60 * 1000;

/** One overview card: still loading, loaded, or unavailable (with why). */
type EnvValue<T> =
  | { state: 'loading' }
  | { state: 'ready'; value: T }
  | { state: 'error'; forbidden: boolean };

interface EnvCards {
  runners: EnvValue<{ online: number; total: number }>;
  pools: EnvValue<number>;
  nodeTypes: EnvValue<number>;
}

function statusOf(reason: unknown): number | undefined {
  const direct = (reason as { status?: unknown } | null)?.status;
  if (typeof direct === 'number') return direct;
  const response = (reason as { response?: { status?: unknown } } | null)?.response?.status;
  return typeof response === 'number' ? response : undefined;
}

/** A settled overview read → the KPI's state; a 403 is named 无权限. */
function envCard<T, U = T>(
  result: PromiseSettledResult<T>,
  map?: (value: T) => U,
): EnvValue<U> {
  if (result.status === 'fulfilled') {
    return { state: 'ready', value: map ? map(result.value) : (result.value as unknown as U) };
  }
  return { state: 'error', forbidden: statusOf(result.reason) === 403 };
}

function errorMessage(reason: unknown, fallback: string): string {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}

/**
 * The recent-run card always draws Chinese copy: the HTTP client's text
 * ("Request failed with status code 500") is not a message a caller should
 * see. The status code and transport text are kept in the card's `title` and
 * `data-error-detail` so a failure is still diagnosable.
 */
interface CardFailure {
  /** Localized copy rendered in the card. */
  message: string;
  /** Status/transport detail for troubleshooting; never rendered as copy. */
  detail?: string;
}

function recentFailure(reason: unknown): CardFailure {
  const status = statusOf(reason);
  const code = status === undefined ? undefined : `HTTP ${status}`;
  const transport = reason instanceof Error && reason.message ? reason.message : undefined;
  return {
    message: '加载失败',
    detail: [code, transport].filter(Boolean).join(' · ') || undefined,
  };
}

/**
 * Workflow centre (§6.6, ruling 20, A/B row rework). A row is exactly what
 * `GET /v1/workflows?include=stats,recent` returns: id / name / version /
 * description plus the server-derived 触发 (`triggers`, `cron_expressions`),
 * 最近运行 (`latest_execution`), window stats and the newest ten executions.
 * Neither the health columns nor the run strip needs a per-row follow-up read.
 *
 * Execution-derived fields are omitted by the server when the caller lacks
 * `execution.read` (or the deployment cannot answer); the row then draws "—"
 * rather than a fabricated rate, duration or run.
 *
 * Server pagination is real (`page` / `page_size`), and `q` is the server's
 * name filter, applied before pagination. The page returns to 1 whenever the
 * query or the stats window changes.
 */
export function WorkflowCenter() {
  // `@xflow/api` falls back to the global fetch, which does not carry the
  // admin session's Bearer token; authorizedFetch is what makes these two
  // reads (workflows, node types) authenticate like the umi request services.
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );
  const { pathname, search } = useLocation();
  // The failure rail reads executions, so it shares the route-level bit with
  // /overview and /executions: no grant means no request and no error card.
  const access = useAccess();
  const canViewExecutions = access?.canViewExecutions === true;
  const params = new URLSearchParams(search);
  const query = params.get('q')?.trim() ?? '';
  // The shell's 导入 DSL button cannot reach this page's dialog directly; it
  // navigates with ?import=1 (the same URL-parameter pattern as `q`) and this
  // page, which owns the dialog and its success path, consumes the request.
  const importRequested = params.get('import') === '1';
  const { message, notification } = App.useApp();
  const [importOpen, setImportOpen] = useState(false);

  useEffect(() => {
    if (importRequested) setImportOpen(true);
  }, [importRequested]);

  const closeImport = useCallback(() => {
    setImportOpen(false);
    if (importRequested) {
      // Consume the one-shot flag: leaving it in the URL would make the next
      // top-bar click a no-op, because the search would not change.
      history.replace(withParam(pathname, search, 'import', null));
    }
  }, [importRequested, pathname, search]);

  const [env, setEnv] = useState<EnvCards>({
    runners: { state: 'loading' },
    pools: { state: 'loading' },
    nodeTypes: { state: 'loading' },
  });
  const [rows, setRows] = useState<WorkflowSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  // `include=stats,recent` is the row's health data. The window is a toolbar
  // control (24h default, matching the 成功率 column), and switching it resets
  // the page: the server aggregate changed, so page N of the old window is not
  // the page the caller is now looking at.
  const [statsWindow, setStatsWindow] = useState<WorkflowStatsWindow>('24h');
  const changeWindow = (next: WorkflowStatsWindow) => {
    if (next === statsWindow) return;
    setStatsWindow(next);
    setPage(1);
  };
  // The P50 and 更新 columns are secondary: below 1280 they are dropped from
  // the column list entirely (not merely hidden), so no horizontal scroll
  // appears at 1024.
  const showSecondaryColumns = useMinWidth('(min-width: 1280px)');
  const [favoritesOnly, setFavoritesOnly] = useState(false);
  const toggleFavoritesOnly = () => setFavoritesOnly((current) => !current);
  // Favourites are per subject+namespace (P6): the shell already fetched the
  // identity, so reading it here costs no request.
  const { initialState } = useModel('@@initialState') as { initialState?: InitialState };
  const favorites = useWorkflowFavorites(
    initialState?.currentUser?.subject,
    initialState?.currentUser?.namespace,
  );
  const [runningId, setRunningId] = useState<string>();
  // `q` lives in the URL rather than in this component, so a query change
  // arrives as a re-render. Reset to page 1 during render (React's documented
  // "adjust state when a prop changes" pattern) so the fetch effect below
  // never fires a request for the old page under the new query.
  const [queryAtPage, setQueryAtPage] = useState(query);
  if (queryAtPage !== query) {
    setQueryAtPage(query);
    setPage(1);
  }
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<CardFailure>();
  const [listAttempt, setListAttempt] = useState(0);

  const [recentFailures, setRecentFailures] = useState<ExecutionListItem[]>();
  const [failuresError, setFailuresError] = useState<CardFailure>();
  const [failuresAttempt, setFailuresAttempt] = useState(0);

  // The three overview cards settle independently. AD-07: the grid always
  // keeps three columns; a failed read draws "—" plus a short reason instead of
  // dropping the card (or the API path it came from).
  //
  // The two management reads are scope-gated: without the scope they settle as
  // a 403-equivalent failure and no request is sent, so an ordinary console
  // does not spray forbidden requests into the browser console. Node types
  // ride the workflow scope and are always read.
  const scopes = initialState?.currentUser?.scopes ?? [];
  const canReadRunners = scopes.includes(MANAGEMENT_READ_SCOPES.runners);
  const canReadPools = scopes.includes(MANAGEMENT_READ_SCOPES.pools);
  useEffect(() => {
    let cancelled = false;
    const denied = (scope: string) => Object.assign(new Error(`missing ${scope}`), { status: 403 });
    Promise.allSettled([
      canReadRunners ? fetchRunners() : Promise.reject(denied(MANAGEMENT_READ_SCOPES.runners)),
      canReadPools ? fetchRunnerPools() : Promise.reject(denied(MANAGEMENT_READ_SCOPES.pools)),
      api.listNodeTypes(),
    ]).then(
      ([runners, pools, types]) => {
        if (cancelled) return;
        setEnv({
          runners: envCard(runners, (list) => ({
            online: list.filter((runner) => runner.state === 'online').length,
            total: list.length,
          })),
          pools: envCard(pools, (list) => list.length),
          nodeTypes: envCard(types, (result) => result.node_types.length),
        });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, canReadRunners, canReadPools]);

  // A failed page keeps the rows already on screen (the loading state does the
  // same), and the banner names the failure; 重试 refetches the same page.
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .listWorkflows({
        page,
        pageSize: PAGE_SIZE,
        q: query || undefined,
        include: ['stats', 'recent'],
        window: statsWindow,
      })
      .then(
        (result) => {
          if (cancelled) return;
          setRows(result.list);
          setTotal(result.total);
          setListError(undefined);
          setLoading(false);
        },
        (reason: unknown) => {
          if (cancelled) return;
          setListError(recentFailure(reason));
          setLoading(false);
        },
      );
    return () => {
      cancelled = true;
    };
  }, [api, page, query, listAttempt, statsWindow]);

  useEffect(() => {
    if (!canViewExecutions) return;
    let cancelled = false;
    api
      .listExecutions({
        pageSize: RECENT_FAILURES,
        statuses: ['failed', 'timeout'],
        createdAfter: new Date(Date.now() - RECENT_FAILURE_WINDOW_MS).toISOString(),
      })
      .then(
        (result) => {
          if (cancelled) return;
          setRecentFailures(result.list);
          setFailuresError(undefined);
        },
        (reason: unknown) => {
          if (cancelled) return;
          setFailuresError(recentFailure(reason));
        },
      );
    return () => {
      cancelled = true;
    };
  }, [api, canViewExecutions, failuresAttempt]);

  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
  // Only the first page draws a skeleton; later pages keep the previous rows
  // and show the 2px TableProgressBar (AL-28) instead of a Spin overlay.
  const firstLoad = loading && rows.length === 0 && !listError;
  // A later read in flight: the old rows stay up, but they are being replaced.
  const refreshing = loading && !firstLoad;
  const clearSearch = () => history.replace(withParam(pathname, search, 'q', null));
  // The list owns its search now (提案 C); `/` focuses it through the same
  // shortcut the shell used to host.
  const searchRef = useRef<InputRef>(null);
  useSearchShortcut(searchRef);
  const [searchValue, setSearchValue] = useState(query);
  useEffect(() => {
    setSearchValue(query);
  }, [query]);
  const updateSearch = (next: string) => {
    setSearchValue(next);
    history.replace(withParam(pathname, search, 'q', next.trim() || null));
  };
  // AL-11: the short id is a copy affordance; the row click must not fire.
  const copyId = useCallback(
    (id: string) => {
      const clipboard = typeof navigator === 'undefined' ? undefined : navigator.clipboard;
      if (!clipboard?.writeText) {
        message?.error?.('复制失败');
        return;
      }
      clipboard.writeText(id).then(
        () => message?.success?.('已复制工作流 ID'),
        () => message?.error?.('复制失败'),
      );
    },
    [message],
  );
  const openRecent = (row: ExecutionListItem) => {
    if (row.workflowId) {
      history.push(`/workflows/${row.workflowId}?exec=${encodeURIComponent(row.executionId)}`);
      return;
    }
    // An unattributed row (no workflow id) still has a row in the executions list.
    history.push('/executions');
  };
  // Row-end ⋯: 运行 submits the stored definition and then opens the run
  // record, the same destination every other run entry uses. The server stays
  // the authority on whether this caller may run it.
  const runRow = useCallback(
    async (row: WorkflowSummary) => {
      setRunningId(row.id);
      try {
        const { executionId } = await api.runWorkflow(row.id);
        message?.success?.(`已启动「${row.name}」`);
        history.push(`/workflows/${row.id}?exec=${encodeURIComponent(executionId)}`);
      } catch (reason) {
        message?.error?.(errorMessage(reason, '运行失败，请稍后重试。'));
      } finally {
        setRunningId(undefined);
      }
    },
    [api, message],
  );
  // 导出 DSL reads the definition once (the list DTO deliberately has none)
  // and downloads it as the YAML the import dialog round-trips.
  const exportRow = useCallback(
    async (row: WorkflowSummary) => {
      try {
        const { workflow } = await api.getWorkflow(row.id);
        const text = dumpYaml(workflow, { noRefs: true, lineWidth: -1 });
        const url = URL.createObjectURL(new Blob([text], { type: 'application/yaml' }));
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = `${(workflow.name || row.name).trim().replace(/[\\/:*?"<>|]+/g, '-') || 'workflow'}.yaml`;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(url);
        message?.success?.('已导出 DSL');
      } catch (reason) {
        message?.error?.(errorMessage(reason, '导出失败，请稍后重试。'));
      }
    },
    [api, message],
  );
  // 只看收藏 is a local, current-page filter: favourites have no server index
  // (P6), so pagination and the toolbar total stay server-owned; the button's
  // title says the scope.
  const visibleRows = favoritesOnly ? rows.filter((row) => favorites.isFavorite(row.id)) : rows;
  // AD-05: an empty table with no query is a first-run state that offers the
  // two entry points; a query with no rows is a filter result that offers a
  // way back.
  const emptyState = favoritesOnly ? (
    <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="当前页没有收藏的工作流">
      <Button size="small" onClick={toggleFavoritesOnly}>
        显示全部
      </Button>
    </Empty>
  ) : query ? (
    <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="没有匹配的工作流">
      <Button size="small" onClick={clearSearch}>
        清除搜索
      </Button>
    </Empty>
  ) : (
    <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="还没有工作流">
      <div className="flex items-center justify-center gap-2">
        <Button type="primary" size="small" onClick={() => history.push('/workflows/new')}>
          新建工作流
        </Button>
        <Button size="small" onClick={() => setImportOpen(true)}>
          导入 DSL
        </Button>
      </div>
    </Empty>
  );

  // ED-01: the import dialog only pre-checks the shape and calls the existing
  // registration endpoint; the server compiles and reports warnings/
  // param_issues, which stay visible after entering the editor.
  const handleImported = useCallback(
    (result: RegisterWorkflowResult) => {
      // Same close path as the cancel button, so a shell-issued ?import=1 is
      // consumed here too and Back from the editor does not reopen the dialog.
      closeImport();
      const notices = [
        ...(result.warnings ?? []),
        ...(result.paramIssues ?? []).map(
          (issue) => `${issue.node}${issue.path}：${issue.message}`,
        ),
      ];
      history.push(`/workflows/${result.workflowId}`);
      // One macrotask later the editor route has replaced the console shell.
      // The App toast holder resolves its container at render time; deferring
      // keeps it from portalling into `.xflow-admin` only to be unmounted.
      window.setTimeout(() => {
        if (notices.length > 0) {
          notification.warning({
            key: 'workflow-import-warnings',
            title: `已导入工作流 ${result.workflowId}`,
            description: (
              <div className="flex flex-col gap-1">
                <span>服务端返回 {notices.length} 条提示：</span>
                <ul className="m-0 pl-4">
                  {notices.map((notice, index) => (
                    <li key={`${index}-${notice}`}>{notice}</li>
                  ))}
                </ul>
              </div>
            ),
            duration: false,
          });
        } else {
          message.success(`已导入工作流 ${result.workflowId}`);
        }
      }, 0);
    },
    [closeImport, message, notification],
  );

  // The shell's 工作流 badge shows the same server total as the table title;
  // while the first page is loading (or after a failed load) it stays hidden.
  useAdminWorkflowCount(loading || listError ? undefined : total);

  const columns: TableColumnsType<WorkflowSummary> = [
    {
      title: (
        <span className="xflow-row-fav-head" title="收藏">
          <StarIcon />
          <span className="sr-only">收藏</span>
        </span>
      ),
      key: 'favorite',
      className: 'xflow-admin-col-favorite',
      width: 48,
      render: (_value: unknown, row) => {
        const favorite = favorites.isFavorite(row.id);
        return (
          <button
            type="button"
            className="xflow-row-favorite"
            data-fav={favorite ? 'true' : 'false'}
            aria-pressed={favorite}
            aria-label={favorite ? `取消收藏 ${row.name}` : `收藏 ${row.name}`}
            title={favorite ? '取消收藏' : '收藏'}
            onClick={(event) => {
              event.stopPropagation();
              favorites.toggle(row.id);
            }}
          >
            <StarIcon filled={favorite} />
          </button>
        );
      },
    },
    {
      title: '名称',
      dataIndex: 'name',
      className: 'xflow-admin-col-name',
      // The name is the one column that takes the spare pixels; every other
      // column is fixed so the table never scrolls inside its card at 1024.
      minWidth: 180,
      render: (_value: string, row) => {
        const description = row.description?.trim();
        return (
          <span className="xflow-admin-name xflow-row-name">
            <strong className="xflow-row-name__title" title={row.name}>
              {/* A real link: ⌘/middle click, copy-link and the screen reader's
                  link role work. A plain click still routes in-app. */}
              <a
                className="xflow-row-name__link"
                href={`/workflows/${row.id}`}
                onClick={(event) => {
                  if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
                  event.preventDefault();
                  history.push(`/workflows/${row.id}`);
                }}
              >
                {row.name}
              </a>
            </strong>
            <small className="xflow-row-name__meta">
              {description ? (
                <span className="xflow-row-name__desc" title={description}>
                  {description}
                </span>
              ) : null}
              <span className="xflow-row-name__version" data-version={row.version}>
                {row.version ? versionLabel(row.version) : '—'}
              </span>
              <button
                type="button"
                className="xflow-admin-short-id"
                data-copy-id={row.id}
                title={row.id}
                aria-label={`复制工作流 ID ${row.id}`}
                onClick={(event) => {
                  event.stopPropagation();
                  copyId(row.id);
                }}
              >
                {row.id.slice(0, 8)}
                <CopyIcon />
              </button>
            </small>
          </span>
        );
      },
    },
    {
      title: '触发',
      key: 'triggers',
      className: 'xflow-admin-col-entry',
      width: 140,
      render: (_value: unknown, row) => (
        <TriggerSummary triggers={row.triggers} cronExpressions={row.cronExpressions} />
      ),
    },
    {
      title: '最近运行',
      key: 'recentRuns',
      className: 'xflow-admin-col-recent',
      width: 128,
      render: (_value: unknown, row) => (
        <RunStrip
          executions={row.recentExecutions}
          onOpen={(executionId) =>
            history.push(`/workflows/${row.id}?exec=${encodeURIComponent(executionId)}`)
          }
        />
      ),
    },
    {
      title: <span title={`成功率按${windowLabel(statsWindow)}统计`}>成功率</span>,
      key: 'successRate',
      className: 'xflow-admin-col-rate',
      width: 76,
      render: (_value: unknown, row) => <SuccessRateCell stats={row.stats} />,
    },
    ...(showSecondaryColumns
      ? ([
          {
            title: <span title={`P50 耗时按${windowLabel(statsWindow)}统计`}>P50</span>,
            key: 'p50',
            className: 'xflow-admin-col-p50',
            width: 76,
            render: (_value: unknown, row: WorkflowSummary) => <DurationCell stats={row.stats} />,
          },
          {
            title: '更新',
            key: 'updated',
            className: 'xflow-admin-col-updated',
            width: 104,
            render: (_value: unknown, row: WorkflowSummary) => {
              // P3: the registry stamps `updated_at` when it registers or
              // replaces the workflow. Rows written before that column exist
              // fall back to the freshest execution timestamp, and the title
              // names which source the cell is showing. Absent stays "—".
              const updated = updatedStamp(row);
              if (!updated) {
                return (
                  <span className="xflow-admin-faint" data-updated="missing">
                    —
                  </span>
                );
              }
              return (
                <time
                  className="xflow-row-updated"
                  dateTime={updated.value}
                  data-updated-source={updated.source}
                  title={updated.title}
                  aria-label={updated.title}
                >
                  {relativeTime(updated.value)}
                </time>
              );
            },
          },
        ] satisfies TableColumnsType<WorkflowSummary>)
      : []),
    {
      title: <span className="sr-only">操作</span>,
      key: 'ops',
      className: 'xflow-admin-col-ops',
      align: 'right',
      width: 56,
      render: (_value: unknown, row) => {
        const items: MenuProps['items'] = [
          { key: 'open', label: '打开' },
          {
            key: 'run',
            label: runningId === row.id ? '正在运行…' : '运行',
            // Running starts a real execution, so it never starts from rows
            // that a refetch is about to replace.
            disabled: runningId !== undefined || refreshing,
          },
          { key: 'export', label: '导出 DSL' },
          { type: 'divider' },
          { key: 'copy', label: '复制 ID' },
        ];
        return (
          <span className="xflow-row-ops">
            <Dropdown
              trigger={['click']}
              menu={{
                items,
                onClick: ({ key, domEvent }) => {
                  domEvent.stopPropagation();
                  if (key === 'open') {
                    history.push(`/workflows/${row.id}`);
                  } else if (key === 'run') {
                    void runRow(row);
                  } else if (key === 'export') {
                    void exportRow(row);
                  } else if (key === 'copy') {
                    copyId(row.id);
                  }
                },
              }}
            >
              <Button
                className="xflow-row-ops-btn"
                size="small"
                type="text"
                aria-label={`更多操作：${row.name}`}
                title="更多操作"
                icon={<MoreIcon />}
                onClick={(event) => event.stopPropagation()}
              />
            </Dropdown>
          </span>
        );
      },
    },
  ];

  // AL-08: compact KPI strip. A metric whose read failed is not drawn at all
  // (no em dash, no "无权限" footnote); when every read failed the strip is
  // gone instead of leaving a row of dead cells.
  const kpiItems: { id: string; icon: ReactNode; label: string; value: string }[] = [];
  if (env.runners.state !== 'error') {
    kpiItems.push({
      id: 'runners',
      icon: <ServerIcon />,
      label: 'Runner 在线',
      value:
        env.runners.state === 'ready'
          ? `${env.runners.value.online} / ${env.runners.value.total}`
          : '…',
    });
  }
  if (env.pools.state !== 'error') {
    kpiItems.push({
      id: 'pools',
      icon: <GridIcon />,
      label: 'Runner 池',
      value: env.pools.state === 'ready' ? String(env.pools.value) : '…',
    });
  }
  if (env.nodeTypes.state !== 'error') {
    kpiItems.push({
      id: 'types',
      icon: <CodeIcon />,
      label: '可用节点类型',
      value: env.nodeTypes.state === 'ready' ? String(env.nodeTypes.value) : '…',
    });
  }

  return (
    <>
      <PageHeader
        title="工作流"
        actions={
          <>
            <Button icon={<UploadIcon />} onClick={() => setImportOpen(true)}>
              导入 DSL
            </Button>
            <Button type="primary" icon={<PlusIcon />} onClick={() => history.push('/workflows/new')}>
              新建工作流
            </Button>
          </>
        }
      />
      {kpiItems.length > 0 ? (
        <section className="xflow-admin-kpi" data-panel="env-kpis" aria-label="集群概览">
          {kpiItems.map((item) => (
            <div className="xflow-admin-kpi-item" data-env={item.id} key={item.id}>
              <span className="xflow-admin-kpi-icon" aria-hidden="true">
                {item.icon}
              </span>
              <span className="xflow-admin-kpi-value">{item.value}</span>
              <span className="xflow-admin-kpi-label">{item.label}</span>
            </div>
          ))}
        </section>
      ) : null}

      {/* Stable hook for the e2e smoke: this master grid is pure Tailwind.
          AD-01/AL-01: below 1440 the right rail drops under the table. At 1280
          a 320px rail left the table 682px wide — under the table's own
          minimum — so it scrolled and clipped the 打开 column. */}
      <div
        className="grid grid-cols-1 items-start gap-4 min-[1440px]:grid-cols-[minmax(0,1fr)_320px]"
        data-testid="workflow-center-layout"
      >
        <div
          className="xflow-admin-card xflow-admin-tablecard"
          data-table="workflows"
          aria-busy={refreshing}
        >
          <TableToolbar label="工作流列表工具栏" className="xflow-row-toolbar">
            <Input
              ref={searchRef}
              className="xflow-admin-search"
              aria-label="搜索工作流"
              placeholder="按名称或 ID 搜索工作流"
              prefix={<SearchIcon />}
              suffix={
                <kbd className="xflow-admin-kbd" aria-hidden="true">
                  /
                </kbd>
              }
              allowClear
              value={searchValue}
              onChange={(event) => updateSearch(event.target.value)}
            />
            <Segmented<WorkflowStatsWindow>
              className="xflow-row-window"
              aria-label="统计窗口"
              size="small"
              value={statsWindow}
              onChange={changeWindow}
              options={[
                { value: '24h', label: '24h' },
                { value: '7d', label: '7d' },
                { value: '30d', label: '30d' },
              ]}
            />
            <Button
              size="small"
              className="xflow-row-fav-filter"
              data-active={favoritesOnly}
              aria-pressed={favoritesOnly}
              icon={<StarIcon filled={favoritesOnly} />}
              title="仅筛选当前页已收藏的工作流"
              onClick={toggleFavoritesOnly}
            >
              只看收藏
            </Button>
            <span className="xflow-admin-grow" />
            {listError ? null : favoritesOnly ? (
              <span className="xflow-admin-panel-count" data-filtered-count={visibleRows.length}>
                收藏 {visibleRows.length}
              </span>
            ) : (
              <span className="xflow-admin-panel-count">{total}</span>
            )}
          </TableToolbar>
          {listError ? (
            <div className="xflow-admin-banner">
              <Alert
                type="error"
                showIcon
                title="工作流列表加载失败"
                // Fixed Chinese copy; the server's text and status stay in the
                // detail attribute. The leading space keeps the headline and
                // the hint apart for innerText and screen readers.
                description={<> 请稍后重试。</>}
                data-error-detail={listError.detail}
                action={
                  <Button size="small" onClick={() => setListAttempt((attempt) => attempt + 1)}>
                    重试
                  </Button>
                }
              />
            </div>
          ) : null}
          <TableProgressBar loading={refreshing} label="正在刷新工作流" />
          <Table
            className="xflow-admin-table"
            size="small"
            rowKey="id"
            tableLayout="fixed"
            // A/B calibration: the fixed columns (favourite 48 + trigger 140 +
            // recent 128 + rate 76 + ops 56, plus P50 76 and 更新 104 from
            // 1280) never exceed the card at 1024; the name column takes every
            // spare pixel, so the table itself needs no horizontal scroll.
            scroll={{ x: 640 }}
            pagination={false}
            // AL-28: a refresh keeps the rows; TableProgressBar above is the cue.
            // AD-04: an error is not an empty result; the banner owns the state.
            locale={{
              emptyText: listError ? null : firstLoad ? (
                <TableSkeleton rows={5} density="two-line" label="正在加载工作流" />
              ) : (
                emptyState
              ),
            }}
            dataSource={visibleRows}
            onRow={(row) => ({
              'data-workflow': row.id,
              tabIndex: 0,
              className: 'xflow-admin-row-clickable',
              onClick: (event) => {
                if (isRowControl(event.target)) return;
                history.push(`/workflows/${row.id}`);
              },
              onKeyDown: (event) => {
                if (event.key !== 'Enter' || isRowControl(event.target)) return;
                event.preventDefault();
                history.push(`/workflows/${row.id}`);
              },
            })}
            columns={columns}
          />
          {/* AD-18: one page needs no footer; more pages also state the total. */}
          {listError || pageCount <= 1 ? null : (
            <div className="xflow-admin-page" data-page={page} data-page-count={pageCount}>
              {total > 0 ? <span className="xflow-admin-num">共 {total} 条</span> : null}
              第 {page} / {pageCount} 页
              <Button className="xflow-admin-btn-sm" size="small" disabled={page <= 1 || loading} onClick={() => setPage((current) => Math.max(1, current - 1))}>
                上一页
              </Button>
              <Button className="xflow-admin-btn-sm" size="small" disabled={page >= pageCount || loading} onClick={() => setPage((current) => current + 1)}>
                下一页
              </Button>
            </div>
          )}
        </div>

        <div className="flex min-w-0 flex-col gap-3">
          {/* R2: the rail used to repeat the table's 最近运行 column. It now
              answers the complementary question — what failed lately — with a
              single-line 错误摘要 and a direct jump into the run record. */}
          <div className="xflow-admin-card xflow-admin-tablecard" data-panel="recent-failures">
            <PanelTitle
              icon={<HistoryIcon />}
              title="最近失败"
              count={canViewExecutions ? recentFailures?.length : undefined}
            />
            {!canViewExecutions ? (
              // Same degradation as the overview's cluster segment: the read is
              // never sent, and the card says which grant it would need.
              <p className="xflow-admin-run-empty" data-state="forbidden">
                需要 execution 权限才能查看最近失败。
              </p>
            ) : failuresError ? (
              <p
                className="xflow-admin-run-empty"
                data-state="error"
                data-error-detail={failuresError.detail}
                title={failuresError.detail}
              >
                {failuresError.message}
                <Button size="small" type="link" onClick={() => setFailuresAttempt((attempt) => attempt + 1)}>
                  重试
                </Button>
              </p>
            ) : recentFailures === undefined ? (
              <p className="xflow-admin-run-empty" data-state="loading">
                加载中…
              </p>
            ) : recentFailures.length === 0 ? (
              // A positive empty state: no failure is good news, not "no data".
              <p className="xflow-admin-run-empty" data-state="empty" data-tone="good">
                最近 24h 无失败
              </p>
            ) : (
              recentFailures.map((row) => {
                const statusLabel = row.status === 'timeout' ? '超时' : '失败';
                // Only the display-safe summary leaves the list: the raw
                // message can name internal protocol paths and Go types, and
                // it stays in the run record the row opens.
                const summary = failureSummary(row.error);
                const time = relativeTime(row.createdAt);
                const title = `${row.workflowName} · ${statusLabel} · ${time}${summary ? ` · ${summary}` : ''}`;
                return (
                  <div
                    className="xflow-admin-run xflow-admin-run--failed xflow-admin-row-clickable"
                    key={row.executionId}
                    data-exec={row.executionId}
                    data-status={row.status}
                    data-workflow={row.workflowName}
                    title={title}
                    role="button"
                    tabIndex={0}
                    aria-label={`打开${statusLabel}运行：${title}`}
                    onClick={() => openRecent(row)}
                    onKeyDown={(event) => {
                      if (event.key !== 'Enter') return;
                      event.preventDefault();
                      openRecent(row);
                    }}
                  >
                    <span className="xflow-admin-run-name" title={row.workflowName}>
                      {row.workflowName}
                    </span>
                    <span className="xflow-admin-run-time">{time}</span>
                    <span className="xflow-admin-run-error" title={summary || '无错误信息'}>
                      {summary || '无错误信息'}
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>
      </div>

      <ImportWorkflowDialog
        open={importOpen}
        api={api}
        onClose={closeImport}
        onImported={handleImported}
      />
    </>
  );
}
