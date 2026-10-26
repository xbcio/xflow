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
  BranchIcon,
  ClockIcon,
  ConnectIcon,
  CopyIcon,
  CursorIcon,
  DbIcon,
  GlobeIcon,
  RedoIcon,
  SearchIcon,
} from '@xflow/icons';
import { history, useLocation } from '@umijs/max';
import {
  Alert,
  App,
  Button,
  Checkbox,
  Input,
  Modal,
  Radio,
  Select,
  Table,
  Tooltip,
  type InputRef,
  type TableColumnsType,
} from 'antd';
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { PageHeader } from '../../components/PageHeader';
import { TableProgressBar } from '../../components/TableProgressBar';
import { TableToolbar } from '../../components/TableToolbar';
import { useActionToast, type ToastAction } from '../../components/useActionToast';
import { useSearchShortcut } from '../../components/useSearchShortcut';
import { withParam } from '../../layouts/useAdminNamespace';
import { authorizedFetch } from '../../services/http';
import { isRowControl, StatusChip, TableSkeleton } from '../workflows/parts';
import {
  TRIGGER_LABELS,
  formatDuration,
  fullTimestamp,
  relativeTime,
  useMinWidth,
} from '../workflows/row-parts';
import './execution-row.css';

/** Server page size for the execution table (OpenAPI `page_size`). */
export const EXECUTION_PAGE_SIZE = 20;

/**
 * The search box narrows by the id prefix the server matches (`execution_id`
 * or `workflow_id`, OpenAPI `q`), and waits for the typing to settle before it
 * changes the server query.
 */
export const SEARCH_DEBOUNCE_MS = 300;

type RerunOrigin = 'head' | 'failed';

/** Row controls the list can issue; rerun goes through its own dialog. */
type ControlOp = 'pause' | 'resume' | 'cancel';

/** LE-01: the design's time-range dropdown, server-side via `created_after`. */
type TimeRange = 'all' | '1h' | '24h' | '7d';

const TIME_RANGE_OPTIONS: { label: string; value: TimeRange }[] = [
  // "全部时间" rather than "全部": the status selector owns that word, and two
  // controls showing the same label would read as one filter.
  { label: '全部时间', value: 'all' },
  { label: '最近 1 小时', value: '1h' },
  { label: '最近 24 小时', value: '24h' },
  { label: '最近 7 天', value: '7d' },
];

const TIME_RANGE_MS: Record<Exclude<TimeRange, 'all'>, number> = {
  '1h': 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
};

/** LE-02: one page holds at most the server's 200-row page_size cap. */
const WORKFLOW_OPTION_PAGE_SIZE = 200;
const ALL_WORKFLOWS = '';

/** RFC3339 lower bound, exclusive server-side; `Date.now()` is the baseline. */
function createdAfterFor(range: TimeRange): string | undefined {
  if (range === 'all') return undefined;
  return new Date(Date.now() - TIME_RANGE_MS[range]).toISOString();
}

/**
 * Status filter options, grouped by what an operator is looking for. The
 * terminal group is complete (成功/已取消) and 需关注 carries the two failure
 * outcomes; 进行中 keeps the live states the old segment offered. Labels come
 * from `workflowStatusMeta`, never a second copy.
 */
const STATUS_GROUPS: { label: string; statuses: ExecutionStatusKey[] }[] = [
  { label: '需关注', statuses: ['failed', 'timeout'] },
  { label: '进行中', statuses: ['pending', 'queued', 'running', 'paused', 'canceling'] },
  { label: '终态', statuses: ['success', 'canceled'] },
];

/** Stable order for URL normalization and request values. */
const STATUS_ORDER = STATUS_GROUPS.flatMap((group) => group.statuses);

const STATUS_OPTIONS = STATUS_GROUPS.map((group) => ({
  label: group.label,
  options: group.statuses.map((status) => ({
    value: status,
    label: workflowStatusMeta[status].label,
  })),
}));

/**
 * The URL is the reloadable copy of the status filter. Both spellings the
 * server accepts are read (`?status=failed,timeout` and repeated `status=`),
 * and unknown values are dropped rather than sent back as a 400.
 */
function statusesFromSearch(search: string): ExecutionStatusKey[] {
  const params = new URLSearchParams(search);
  const selected = new Set(
    params
      .getAll('status')
      .flatMap((value) => value.split(','))
      .map((value) => value.trim()),
  );
  return STATUS_ORDER.filter((status) => selected.has(status));
}

function errorMessage(reason: unknown, fallback: string): string {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}

/** The trigger kinds the server derives (`trigger_kind`), with an icon each. */
const TRIGGER_ICONS: Record<string, ReactNode> = {
  api: <CursorIcon />,
  rerun: <RedoIcon />,
  subworkflow: <BranchIcon />,
  timer: <ClockIcon />,
  cron: <ClockIcon />,
  webhook: <GlobeIcon />,
  kafka: <ConnectIcon />,
  redis: <DbIcon />,
};


/** Portal target: the shell root carries the `--xflow-*` tokens (tokens.css). */
function portalContainer(): HTMLElement {
  return document.querySelector<HTMLElement>('.xflow-admin') ?? document.body;
}

function firstLine(value: string): string {
  return value.split(/\r?\n/, 1)[0] ?? '';
}

/** Every P3 field is optional on old rows; absence is UNKNOWN, drawn as "—". */
function EmDash() {
  return <span className="xflow-admin-faint">—</span>;
}

/**
 * The execution id is opaque and long. The cell shows eight characters of it,
 * after the fixed `exec-` prefix (which says nothing about which run it is);
 * the whole id is the tooltip, the accessible name and the copy target (never a
 * workspace leak). Kept local to this directory; it can be merged with the
 * workflow list's short-id button later.
 */
const SHORT_ID_LENGTH = 8;
const EXEC_ID_PREFIX = 'exec-';

function shortExecutionId(id: string): string {
  const bare = id.startsWith(EXEC_ID_PREFIX) ? id.slice(EXEC_ID_PREFIX.length) : id;
  return bare.slice(0, SHORT_ID_LENGTH);
}

/**
 * The table lays out fixed, so a column's width is its cell width and the id
 * column has to hold eight mono glyphs (about 7.22px each at the 12px cell
 * font, 58px) plus the first-cell padding (16px lead + 12px = 28px), and from
 * 1280 the copy icon (12px box + 4px gap = 16px). 58 + 28 = 86 and 86 + 16 = 102;
 * 2px more keeps a wider fallback mono font from tipping the last glyph into
 * the ellipsis. The extra width is taken from columns whose content has room
 * (see the column list), so the table width does not change.
 */
const ID_COLUMN_WIDTH = 88;
const ID_COLUMN_WIDTH_WITH_COPY = 104;

function ExecutionIdCell({ id, onCopy }: { id: string; onCopy: (id: string) => void }) {
  if (!id) return <EmDash />;
  return (
    <button
      type="button"
      className="xflow-exec-id"
      data-copy-exec={id}
      title={id}
      aria-label={`复制执行 ID ${id}`}
      onClick={(event) => {
        event.stopPropagation();
        onCopy(id);
      }}
    >
      <span className="xflow-admin-mono xflow-admin-ellipsis">{shortExecutionId(id)}</span>

      <span className="xflow-exec-id__copy" aria-hidden="true">
        <CopyIcon />
      </span>
    </button>
  );
}

/** `trigger_kind` → its Chinese label (shared with the workflow list) + glyph. */
function TriggerCell({ kind }: { kind?: string }) {
  if (!kind) return <EmDash />;
  const label = TRIGGER_LABELS[kind] ?? kind;
  return (
    <span className="xflow-exec-trigger" data-trigger={kind} title={label}>
      <span className="xflow-exec-trigger__icon" aria-hidden="true">
        {TRIGGER_ICONS[kind] ?? <GlobeIcon />}
      </span>
      <span className="xflow-admin-ellipsis">{label}</span>
    </span>
  );
}

/** A thin bar plus the exact count; a count without a graph size is unknown. */
function NodeProgressCell({ progress }: { progress?: ExecutionListItem['nodeProgress'] }) {
  if (!progress || progress.total <= 0) return <EmDash />;
  const percent = Math.min(100, Math.round((progress.done / progress.total) * 100));
  const label = `节点进度 ${progress.done}/${progress.total}`;
  return (
    <span
      className="xflow-exec-progress"
      data-progress={`${progress.done}/${progress.total}`}
      title={label}
      aria-label={label}
    >
      <span className="xflow-exec-progress__track" aria-hidden="true">
        <span
          className="xflow-exec-progress__fill"
          style={{ width: `${percent}%` }}
          data-percent={percent}
        />
      </span>
      <span className="xflow-exec-progress__text xflow-admin-num">
        {progress.done}/{progress.total}
      </span>
    </span>
  );
}

/** Duration exists only for terminal rows; a live row's absence is not zero. */
function DurationCell({ ms }: { ms?: number }) {
  if (ms === undefined) return <EmDash />;
  return (
    <span className="xflow-exec-duration xflow-admin-num" data-ms={ms} title={`${ms} ms`}>
      {formatDuration(ms)}
    </span>
  );
}

/** Only a positive retry count earns the badge; zero is quiet. */
function RetryCell({ retries }: { retries?: number }) {
  if (retries === undefined) return <EmDash />;
  if (retries <= 0) return <span className="xflow-admin-faint xflow-admin-num">0</span>;
  return (
    <span
      className="xflow-exec-retry xflow-admin-num"
      data-retries={retries}
      title={`重试 ${retries} 次`}
    >
      {retries}
    </span>
  );
}

/** Relative time in the cell, the full local timestamp in the tooltip. */
function TimeCell({ value }: { value?: string }) {
  if (!value) return <EmDash />;
  const full = fullTimestamp(value);
  return (
    <time className="xflow-exec-time xflow-admin-num" dateTime={value} title={full} aria-label={full}>
      {relativeTime(value)}
    </time>
  );
}

/** §6.6b: a row opens the run-record mode of its workflow, on that execution. */
function runRecordHref(row: Pick<ExecutionListItem, 'workflowId' | 'executionId'>): string {
  return `/workflows/${row.workflowId}?exec=${encodeURIComponent(row.executionId)}`;
}

function openInRunRecord(row: Pick<ExecutionListItem, 'workflowId' | 'executionId'>): void {
  if (!row.workflowId) return;
  history.push(runRecordHref(row));
}

/**
 * The workflow name is a real link so the run record can be opened in a new
 * tab or copied. A plain left click stays an in-app navigation; a modified or
 * non-primary click is left to the browser.
 */
function WorkflowNameLink({
  row,
}: {
  row: Pick<ExecutionListItem, 'workflowId' | 'executionId' | 'workflowName'>;
}) {
  if (!row.workflowName) return <EmDash />;
  if (!row.workflowId) {
    return (
      <span className="xflow-admin-ellipsis" title={row.workflowName}>
        {row.workflowName}
      </span>
    );
  }
  return (
    <a
      className="xflow-admin-ellipsis xflow-exec-name-link"
      href={runRecordHref(row)}
      title={row.workflowName}
      onClick={(event) => {
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
          return;
        }
        event.preventDefault();
        openInRunRecord(row);
      }}
    >
      {row.workflowName}
    </a>
  );
}

/** AL-29: a result toast's 打开执行 follows the execution into its run record. */
function openExecutionAction(
  row: Pick<ExecutionListItem, 'workflowId'>,
  executionId: string,
): ToastAction | undefined {
  if (!row.workflowId) return undefined;
  return {
    label: '打开执行',
    onClick: () => openInRunRecord({ workflowId: row.workflowId, executionId }),
  };
}

/**
 * AD-04: the list banner speaks Chinese like the workflow centre's recent-run
 * card. The transport text ("Request failed with status code 500") and the
 * status stay available for troubleshooting as the detail, never as copy.
 */
interface ListFailure {
  message: string;
  code?: string;
  detail?: string;
}

function listFailure(reason: unknown): ListFailure {
  const status = (reason as { response?: { status?: unknown } } | null)?.response?.status;
  const code = typeof status === 'number' ? `HTTP ${status}` : undefined;
  const transport = reason instanceof Error && reason.message ? reason.message : undefined;
  return {
    message: '请稍后重试。',
    code,
    detail: [code, transport].filter(Boolean).join(' · ') || undefined,
  };
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
  // Never the server's own text: it can carry internal addresses or token-like
  // strings, so an unmapped failure reads as one fixed phrase.
  return '重跑提交失败，请稍后重试。';
}

/**
 * LE-03 failure text. Pause/resume need `execution.control`; cancel rides the
 * `execution` scope, so a 403 names the grant the caller is actually missing.
 */
function controlErrorMessage(reason: unknown, op: ControlOp): string {
  if (reason instanceof XFlowApiError) {
    switch (reason.code) {
      case 'execution_not_pausable':
        return '该执行当前不可暂停（排队中或已结束），可改用取消。';
      case 'execution_not_paused':
        return '该执行当前不是已暂停状态，无法继续。';
      case 'execution_control_unsupported':
        return '当前部署不支持暂停或继续执行。';
      case 'execution_not_found':
        return '执行不存在，或已超出保留期。';
      default:
        break;
    }
    if (reason.status === 403) {
      return op === 'cancel'
        ? '当前凭据缺少 execution 权限，无法取消执行。'
        : '当前凭据缺少 execution.control 权限，无法暂停或继续执行。';
    }
  }
  return '操作失败，请稍后重试。';
}

/**
 * Execution list (§6.6 / §6.9).
 *
 * Everything that describes what a status can do is read from
 * `workflowStatusMeta`: the chip, the update column and the row operations all
 * come from the same table, so a status can only be drawn one way. The table
 * itself is server-paginated and server-filtered — there is no client-side
 * filter over the current page, because that would silently drop rows the
 * server has not sent yet.
 *
 * The filter row is server-backed and URL-reloadable: `q` narrows by id
 * prefix, and the status control is a multi-select whose values are ORed. Every
 * terminal outcome is reachable from the 终态 group, while 需关注 collects the
 * two failure outcomes so an operator does not have to hunt for them.
 *
 * Ruling 23: a successful rerun stays on this page. The dialog closes, page 1
 * is refetched (the new execution sorts first) and its row is highlighted.
 */
export function ExecutionsCenter() {
  const { message } = App.useApp();
  const toast = useActionToast();
  const { pathname, search } = useLocation();
  // Below 1280 the secondary columns (结束时间/重试) are dropped from the
  // column list entirely, so the table never needs a horizontal scrollbar.
  const showSecondaryColumns = useMinWidth('(min-width: 1280px)');
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );

  const [rows, setRows] = useState<ExecutionListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  // `q` and the status list live in the URL as well, so a reload keeps the
  // operator's view. The input text is local until the debounce settles.
  const [searchValue, setSearchValue] = useState(
    () => new URLSearchParams(search).get('q') ?? '',
  );
  const [query, setQuery] = useState(() =>
    (new URLSearchParams(search).get('q') ?? '').trim(),
  );
  const [statusFilters, setStatusFilters] = useState<ExecutionStatusKey[]>(() =>
    statusesFromSearch(search),
  );
  const [timeRange, setTimeRange] = useState<TimeRange>('all');
  const [workflowFilter, setWorkflowFilter] = useState<string>(ALL_WORKFLOWS);
  const [workflowOptions, setWorkflowOptions] = useState<{ label: string; value: string }[]>([]);
  const [workflowOptionsLoading, setWorkflowOptionsLoading] = useState(true);
  const [workflowOptionsError, setWorkflowOptionsError] = useState(false);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<ListFailure>();
  const [listAttempt, setListAttempt] = useState(0);
  const [highlightId, setHighlightId] = useState<string>();
  const [busyId, setBusyId] = useState<string>();
  const [cancelRow, setCancelRow] = useState<ExecutionListItem>();
  const [cancelling, setCancelling] = useState(false);
  const searchRef = useRef<InputRef>(null);
  useSearchShortcut(searchRef);

  // 300ms after the last keystroke the id-prefix search narrows the server
  // list once, and the URL keeps a reloadable copy of the filter. The server
  // caps `q` at 64 characters, so the request never exceeds that.
  useEffect(() => {
    const next = searchValue.trim().slice(0, 64);
    if (next === query) return undefined;
    const timer = window.setTimeout(() => {
      setQuery(next);
      setPage(1);
      history.replace(withParam(pathname, search, 'q', next || null));
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [pathname, query, search, searchValue]);

  /** Normalize the selection order and mirror it into `?status=a,b`. */
  const applyStatuses = (next: ExecutionStatusKey[]) => {
    const selected = STATUS_ORDER.filter((status) => next.includes(status));
    setStatusFilters(selected);
    setPage(1);
    history.replace(
      withParam(pathname, search, 'status', selected.length > 0 ? selected.join(',') : null),
    );
  };

  // LE-02: one page of workflows feeds the searchable filter. A failure keeps
  // the select usable ("全部工作流") — the filter row never blocks the list.
  useEffect(() => {
    let cancelled = false;
    setWorkflowOptionsLoading(true);
    api.listWorkflows({ page: 1, pageSize: WORKFLOW_OPTION_PAGE_SIZE }).then(
      (result) => {
        if (cancelled) return;
        setWorkflowOptions(
          result.list.map((workflow) => ({ label: workflow.name, value: workflow.id })),
        );
        setWorkflowOptionsError(false);
        setWorkflowOptionsLoading(false);
      },
      () => {
        if (cancelled) return;
        setWorkflowOptionsError(true);
        setWorkflowOptionsLoading(false);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api]);

  // A failed fetch keeps the loaded page (the loading state does the same);
  // 重试 refetches exactly the page and filters that failed. Every filter
  // change resets to page 1, and the three server filters stack.
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const createdAfter = createdAfterFor(timeRange);
    api.listExecutions({
      page,
      pageSize: EXECUTION_PAGE_SIZE,
      ...(statusFilters.length === 0 ? {} : { statuses: statusFilters }),
      ...(query ? { q: query } : {}),
      ...(workflowFilter === ALL_WORKFLOWS ? {} : { workflowId: workflowFilter }),
      ...(createdAfter === undefined ? {} : { createdAfter }),
    }).then(
      (result) => {
        if (cancelled) return;
        setRows(result.list);
        setTotal(result.total);
        setListError(undefined);
        setLoading(false);
      },
      (reason: unknown) => {
        if (cancelled) return;
        setListError(listFailure(reason));
        setLoading(false);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, page, statusFilters, workflowFilter, timeRange, listAttempt, query]);

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

  // The page owns its filters now (提案 C): they live in the list card's
  // toolbar, not in a portal into the shell's top bar.

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
      // a failed/success filter. Reset to all statuses before refetching,
      // otherwise "page 1 with the new row highlighted" silently does nothing.
      applyStatuses([]);
      setHighlightId(result.executionId);
      setPage(1);
      setListAttempt((attempt) => attempt + 1);
      toast.success(rerunToast(result), {
        key: `rerun:${rerunRow.executionId}`,
        action: openExecutionAction(rerunRow, result.executionId),
      });
    } catch (reason: unknown) {
      toast.error('重跑提交失败', rerunErrorMessage(reason));
    } finally {
      setSubmitting(false);
    }
  };

  /**
   * LE-03: pause/resume answer with the new detail, but the table is a server
   * page, so the row's own status is refreshed by refetching the same page
   * (the current filters and page number stay put).
   */
  const runControl = async (row: ExecutionListItem, op: 'pause' | 'resume') => {
    if (busyId) return;
    setBusyId(row.executionId);
    try {
      if (op === 'pause') {
        await api.pauseExecution(row.executionId);
      } else {
        await api.resumeExecution(row.executionId);
      }
      toast.success(`${op === 'pause' ? '已暂停' : '已继续'} ${row.executionId}`, {
        key: `${op}:${row.executionId}`,
        action: openExecutionAction(row, row.executionId),
      });
      setListAttempt((attempt) => attempt + 1);
    } catch (reason: unknown) {
      toast.error(op === 'pause' ? '暂停失败' : '继续失败', controlErrorMessage(reason, op));
    } finally {
      setBusyId(undefined);
    }
  };

  /** A cancel is irreversible, so the row opens a confirmation first. */
  const submitCancel = async () => {
    if (!cancelRow || cancelling) return;
    const target = cancelRow;
    setCancelling(true);
    try {
      await api.cancelExecution(target.executionId);
      setCancelRow(undefined);
      toast.success(`已请求取消 ${target.executionId}`, {
        key: `cancel:${target.executionId}`,
        action: openExecutionAction(target, target.executionId),
      });
      setListAttempt((attempt) => attempt + 1);
    } catch (reason: unknown) {
      toast.error('取消失败', controlErrorMessage(reason, 'cancel'));
    } finally {
      setCancelling(false);
    }
  };

  // A row records its workflow name at submit time, so it can label a filter
  // value the option page did not return (old/stale list, failed load).
  const workflowNameById = useMemo(() => {
    const names = new Map<string, string>();
    for (const option of workflowOptions) names.set(option.value, option.label);
    for (const row of rows) {
      if (row.workflowId && row.workflowName) names.set(row.workflowId, row.workflowName);
    }
    return names;
  }, [rows, workflowOptions]);

  const workflowSelectOptions = useMemo(() => {
    const options = [{ label: '全部工作流', value: ALL_WORKFLOWS }, ...workflowOptions];
    const known = new Set(options.map((option) => option.value));
    // LE-02 backfill: when the workflow list is empty or failed, the execution
    // rows still carry an id + a display name, so a workflow seen on the page
    // stays selectable instead of disappearing from the filter.
    for (const row of rows) {
      if (!row.workflowId || known.has(row.workflowId)) continue;
      known.add(row.workflowId);
      options.push({ label: row.workflowName || row.workflowId, value: row.workflowId });
    }
    if (workflowFilter !== ALL_WORKFLOWS && !known.has(workflowFilter)) {
      options.push({
        label: workflowNameById.get(workflowFilter) ?? workflowFilter,
        value: workflowFilter,
      });
    }
    return options;
  }, [rows, workflowFilter, workflowNameById, workflowOptions]);

  const pageCount = Math.max(1, Math.ceil(total / EXECUTION_PAGE_SIZE));
  // AD-06: skeleton only on the first page; later pages keep the old rows and
  // show the 2px TableProgressBar (AL-28) instead of a Spin overlay.
  const firstLoad = loading && rows.length === 0 && !listError;
  // A later read in flight: the old rows stay up, but they are being replaced.
  const refreshing = loading && !firstLoad;

  const copyId = (id: string) => {
    const clipboard = typeof navigator === 'undefined' ? undefined : navigator.clipboard;
    if (!clipboard?.writeText) {
      message.error('复制失败');
      return;
    }
    clipboard.writeText(id).then(
      () => message.success('已复制执行 ID'),
      () => message.error('复制失败'),
    );
  };

  // Compact widths below 1280 keep the eight visible columns inside a 1024
  // viewport (the card is ~750px once the 220px nav and body padding are
  // removed). The two secondary columns only exist from 1280, where the extra
  // width is available, so the table never scrolls horizontally.
  const columns: TableColumnsType<ExecutionListItem> = [
    {
      title: '执行 ID',
      dataIndex: 'executionId',
      className: 'xflow-admin-col-exec',
      width: showSecondaryColumns ? ID_COLUMN_WIDTH_WITH_COPY : ID_COLUMN_WIDTH,
      render: (id: string) => <ExecutionIdCell id={id} onCopy={copyId} />,
    },
    {
      title: '工作流',
      dataIndex: 'workflowName',
      className: 'xflow-admin-col-workflow',
      width: showSecondaryColumns ? 104 : 84,
      render: (_name: string, row) => <WorkflowNameLink row={row} />,
    },
    {
      title: '状态',
      dataIndex: 'status',
      className: 'xflow-admin-col-status',
      width: showSecondaryColumns ? 96 : 92,
      render: (status: ExecutionStatusKey) => <StatusChip status={status} />,
    },
    {
      title: '触发来源',
      key: 'trigger',
      className: 'xflow-admin-col-trigger',
      width: showSecondaryColumns ? 92 : 88,
      render: (_value: unknown, row) => <TriggerCell kind={row.triggerKind} />,
    },
    {
      title: '节点进度',
      key: 'progress',
      className: 'xflow-admin-col-progress',
      width: showSecondaryColumns ? 100 : 88,
      render: (_value: unknown, row) => <NodeProgressCell progress={row.nodeProgress} />,
    },
    {
      title: '耗时',
      dataIndex: 'durationMs',
      className: 'xflow-admin-col-duration',
      // The longest value (`59m 59s`, about 43px) leaves room in 72px; the 4px
      // saved in the wide set goes to the id column.
      width: showSecondaryColumns ? 72 : 68,
      render: (value?: number) => <DurationCell ms={value} />,
    },
    ...(showSecondaryColumns
      ? ([
          {
            title: '重试',
            dataIndex: 'retries',
            className: 'xflow-admin-col-retries',
            width: 52,
            render: (value?: number) => <RetryCell retries={value} />,
          },
        ] satisfies TableColumnsType<ExecutionListItem>)
      : []),
    {
      title: '开始时间',
      dataIndex: 'createdAt',
      className: 'xflow-admin-col-start',
      width: showSecondaryColumns ? 88 : 76,
      render: (value?: string) => <TimeCell value={value} />,
    },
    ...(showSecondaryColumns
      ? ([
          {
            title: '结束时间',
            dataIndex: 'finishedAt',
            className: 'xflow-admin-col-finish',
            width: 88,
            render: (value?: string) => <TimeCell value={value} />,
          },
        ] satisfies TableColumnsType<ExecutionListItem>)
      : []),
    {
      title: '错误',
      dataIndex: 'error',
      className: 'xflow-admin-col-error',
      // The compact error is already an ellipsis line (full text in the tooltip);
      // 4px of it pays for the id column's eight glyphs.
      width: showSecondaryColumns ? 112 : 74,
      render: (value: string) => {
        const line = firstLine(value);
        return line ? (
          <Tooltip title={value}>
            <span className="xflow-admin-err">{line}</span>
          </Tooltip>
        ) : (
          <EmDash />
        );
      },
    },
    {
      title: '',
      key: 'ops',
      className: 'xflow-admin-col-ops',
      align: 'right',
      // At most two operations per status (pause/continue + cancel). The table's
      // scoped rule trims the button padding to 4px so 92px carries both.
      width: showSecondaryColumns ? 100 : 92,
      render: (_value: unknown, row) => {
        const meta = workflowStatusMeta[row.status];
        return (
          <span className="xflow-admin-ops" data-ops={meta.ops.join(',')}>
            {meta.ops.map((op: ExecutionOp) => (
              <Button
                className="xflow-admin-op"
                size="small"
                type="text"
                key={op}
                data-op={op}
                // While a refetch is in flight the rows on screen may not match
                // the filter any more, so no operation starts from them.
                disabled={busyId === row.executionId || refreshing}
                onClick={() => {
                  if (op === 'rerun') {
                    openRerun(row);
                    return;
                  }
                  if (op === 'cancel') {
                    setCancelRow(row);
                    return;
                  }
                  void runControl(row, op);
                }}
              >
                {executionOpLabel[op]}
              </Button>
            ))}
          </span>
        );
      },
    },
  ];

  const filterBar = (
    <div className="flex w-full min-w-0 items-center gap-2" data-panel="exec-filters">
      <Input
        ref={searchRef}
        className="xflow-admin-search xflow-exec-search"
        aria-label="搜索执行"
        placeholder="搜索执行 ID / 工作流 ID"
        prefix={<SearchIcon />}
        suffix={
          <kbd className="xflow-admin-kbd" aria-hidden="true">
            /
          </kbd>
        }
        allowClear
        maxLength={64}
        value={searchValue}
        onChange={(event) => setSearchValue(event.target.value)}
      />
      <Select<ExecutionStatusKey[]>
        className="xflow-admin-filter-select xflow-exec-status-select"
        mode="multiple"
        size="small"
        aria-label="状态"
        placeholder="全部状态"
        maxTagCount={1}
        allowClear
        popupMatchSelectWidth={false}
        value={statusFilters}
        options={STATUS_OPTIONS}
        onChange={applyStatuses}
      />
      <Select<TimeRange>
        className="xflow-admin-filter-select"
        size="small"
        aria-label="时间范围"
        value={timeRange}
        options={TIME_RANGE_OPTIONS}
        popupMatchSelectWidth={false}
        onChange={(value) => {
          setTimeRange(value);
          setPage(1);
        }}
      />
      <Select<string>
        className="xflow-admin-filter-select xflow-admin-filter-select--workflow"
        size="small"
        aria-label="工作流"
        showSearch
        optionFilterProp="label"
        popupMatchSelectWidth={false}
        loading={workflowOptionsLoading}
        value={workflowFilter}
        options={workflowSelectOptions}
        notFoundContent={workflowOptionsError ? '工作流列表加载失败' : '没有匹配的工作流'}
        onChange={(value) => {
          setWorkflowFilter(value);
          setPage(1);
        }}
      />
    </div>
  );

  return (
    <div className="flex flex-col gap-4">
      <PageHeader
        title="运行记录"
        actions={
          // Same contract as the overview's 刷新: the loaded rows stay up (the
          // table's progress bar is the cue) and the button waits out the read
          // in flight, so repeated clicks never stack requests.
          <Button
            size="small"
            onClick={() => setListAttempt((attempt) => attempt + 1)}
            disabled={loading}
            data-action="refresh"
          >
            刷新
          </Button>
        }
      />

      <div
        className="xflow-admin-card xflow-admin-tablecard"
        data-table="executions"
        aria-busy={refreshing}
      >
        <TableToolbar
          label="运行记录筛选"
          className="xflow-exec-toolbar"
          actions={listError ? null : <span className="xflow-admin-panel-count">{total}</span>}
        >
          {filterBar}
        </TableToolbar>
        {listError ? (
          <div className="xflow-admin-banner">
            <Alert
              type="error"
              showIcon
              title="运行记录加载失败"
              description={
                <>
                  {/* A real space: the title and description are inline, so
                      innerText and screen readers would otherwise join them. */}{' '}
                  <span data-error-detail={listError.detail} title={listError.detail}>
                    {listError.message}
                    {listError.code ? `（${listError.code}）` : ''}
                  </span>
                </>
              }
              action={
                <Button size="small" onClick={() => setListAttempt((attempt) => attempt + 1)}>
                  重试
                </Button>
              }
            />
          </div>
        ) : null}
        <TableProgressBar loading={refreshing} label="正在刷新运行记录" />
        <Table
          className="xflow-admin-table xflow-exec-table"
          // Fixed layout keeps the declared column widths: auto layout lets the
          // 41-character execution id claim its full nowrap width as the
          // table's min-content, pushing the right-aligned ops column past the
          // card edge at narrower viewports (the card clips the overflow).
          tableLayout="fixed"
          size="small"
          rowKey="executionId"
          // The compact set fills a 1024 viewport; the wide set (with
          // 结束时间/重试) fits from 1280, so neither needs a horizontal scroll.
          scroll={{ x: showSecondaryColumns ? 1008 : 750 }}
          pagination={false}
          // AL-28: a refresh keeps the rows; TableProgressBar above is the cue.
          // AD-04: an error is not an empty result; the banner owns the state.
          locale={{
            emptyText: listError ? null : firstLoad ? (
              <TableSkeleton rows={5} density="compact" label="正在加载运行记录" />
            ) : (
              '没有匹配的记录'
            ),
          }}
          dataSource={rows}
          rowClassName={(row) =>
            [
              row.executionId === highlightId ? 'xflow-admin-row-highlight' : '',
              row.workflowId ? 'xflow-admin-row-clickable' : '',
            ]
              .filter(Boolean)
              .join(' ')
          }
          onRow={(row) => ({
            'data-exec': row.executionId,
            'data-status': row.status,
            'data-workflow': row.workflowName,
            tabIndex: row.workflowId ? 0 : undefined,
            onClick: (event) => {
              if (isRowControl(event.target)) return;
              openInRunRecord(row);
            },
            onKeyDown: (event) => {
              if (event.key !== 'Enter' || isRowControl(event.target)) return;
              event.preventDefault();
              openInRunRecord(row);
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
            {/* LE-05: the idempotency rule is the operator-visible promise, not
                the transport detail that implements it. */}
            <Alert
              type="info"
              showIcon
              title="重复提交不会重复创建执行"
              description="同一次重跑请求重复提交会返回首次创建的执行 id；关闭对话框后重新发起重跑才会创建新的执行。"
            />
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
                  <span className="xflow-admin-field-label">范围</span>
                  <Radio.Group
                    aria-label="重跑范围"
                    value={origin}
                    onChange={(event) => setOrigin(event.target.value as RerunOrigin)}
                  >
                    <Radio value="head">从头运行</Radio>
                    {successSource ? null : (
                      <Radio value="failed" disabled={!failedNode}>
                        从失败节点继续{failedNode ? ` · ${failedNode}` : '（无失败节点）'}
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

      {/* LE-03: cancel is irreversible, so it is the one row control behind a
          second confirmation. */}
      <Modal
        open={cancelRow !== undefined}
        title="取消执行"
        okText="确认取消"
        cancelText="返回"
        okButtonProps={{ danger: true }}
        confirmLoading={cancelling}
        getContainer={portalContainer}
        rootClassName="xflow-admin-modal-root"
        onOk={() => {
          void submitCancel();
        }}
        onCancel={() => {
          if (!cancelling) setCancelRow(undefined);
        }}
        destroyOnHidden
      >
        {cancelRow ? (
          <p className="xflow-admin-hint">
            取消后无法恢复，已完成的节点结果会保留。确定取消执行{' '}
            <span className="xflow-admin-mono">{cancelRow.executionId}</span>？
          </p>
        ) : null}
      </Modal>
    </div>
  );
}
