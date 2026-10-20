import { DownOutlined, HistoryOutlined } from "@ant-design/icons";
import { Button, Dropdown, Segmented, Tooltip } from "antd";
import { workflowStatusMeta, type ExecutionStatusKey } from "@xflow/core";
import * as React from "react";

import { EditorPanel } from "./shared";
import { statusGlyph } from "./statusGlyph";

/**
 * The three server-supported status narrowings of §6.8, plus 全部. The server
 * accepts a SINGLE lifecycle status (`ListExecutionsOptions.status`), so this
 * is a segmented control over one value, never a multi-select.
 */
export type XFlowExecutionStatusFilter = "all" | "failed" | "running" | "paused";

/** Time window, mapped to the exclusive `created_after` bound at request time. */
export type XFlowExecutionRange = "24h" | "7d" | "all";

/** One row of the host's accumulated GET /v1/executions pages. */
export interface XFlowExecutionListItem {
  executionId: string;
  status: ExecutionStatusKey;
  /** Failure message; `""` when the run has none. */
  error?: string;
  /** RFC3339 timestamps, as the listing sent them. */
  createdAt: string;
  updatedAt: string;
  workflowName?: string;
  /**
   * 1-based place in the workflow's queue while `queued`, supplied by the
   * server's list projection (`queue_position`) and passed through by the host
   * unchanged. Absent for every other status and when the server could not
   * read the live position; the row then shows the status alone, because
   * absence means unknown — never the head of the line.
   */
  queuePosition?: number;
}

/**
 * What the host has accumulated for the current filter. Page 1 replaces it;
 * later pages append. `loading`/`error` describe the last request only, so a
 * failed page keeps the rows already on screen (the baseline list does).
 */
export interface XFlowExecutionListState {
  list: XFlowExecutionListItem[];
  total: number;
  loading?: boolean;
  error?: string;
}

/** One page request. The host adds its own workflow identity filter. */
export interface XFlowExecutionListQuery {
  page: number;
  pageSize: number;
  status?: Exclude<XFlowExecutionStatusFilter, "all">;
  createdAfter?: string;
  createdBefore?: string;
}

/** Server page size for the execution list. */
export const EXECUTION_LIST_PAGE_SIZE = 20;

export interface XFlowExecutionListPanelProps {
  state?: XFlowExecutionListState;
  selectedId?: string;
  /**
   * Requests one page. Called on mount, on every filter change (page 1) and
   * when the scrolled list nears its end. Omit it and the list renders its
   * empty state instead of pretending it can load.
   */
  onLoad?: (query: XFlowExecutionListQuery) => void;
  onSelect?: (executionId: string) => void;
}

const rangeLabels: Record<XFlowExecutionRange, string> = {
  "24h": "最近 24 小时",
  "7d": "最近 7 天",
  all: "全部时间"
};

const statusFilters: { label: string; value: XFlowExecutionStatusFilter }[] = [
  { label: "全部", value: "all" },
  { label: "失败", value: "failed" },
  { label: "运行中", value: "running" },
  { label: "已暂停", value: "paused" }
];

/** The exclusive lower bound each window implies, as RFC3339. */
export function executionRangeStart(range: XFlowExecutionRange, now = Date.now()): string | undefined {
  if (range === "all") return undefined;
  const hours = range === "24h" ? 24 : 24 * 7;
  return new Date(now - hours * 60 * 60 * 1000).toISOString();
}

/** `HH:mm:ss` in the viewer's zone; an unparsable value is shown verbatim. */
export function executionClock(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const pad = (part: number) => String(part).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

/**
 * `updated_at − created_at`, terminal executions only (§6.8): a live run's
 * duration is still growing, so drawing it would be a number that changes on
 * every read. Unparsable or inverted stamps produce no duration rather than a
 * made-up one.
 */
export function executionDuration(item: XFlowExecutionListItem): string | undefined {
  if (!workflowStatusMeta[item.status].terminal) return undefined;
  const created = Date.parse(item.createdAt);
  const updated = Date.parse(item.updatedAt);
  if (!Number.isFinite(created) || !Number.isFinite(updated) || updated < created) return undefined;
  const ms = updated - created;
  if (ms < 1000) return `${ms} ms`;
  const tenths = Math.round(ms / 100);
  if (tenths < 600) return `${(tenths / 10).toFixed(1)} s`;
  const seconds = Math.round(ms / 1000);
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function firstLine(value: string | undefined): string | undefined {
  if (!value) return undefined;
  return value.split(/\r?\n/, 1)[0] || undefined;
}

function StatusChip({ status, label }: { status: ExecutionStatusKey; label?: string }): React.ReactElement {
  const meta = workflowStatusMeta[status];
  return (
    <span className="xflow-editor-chip" data-status={status} data-tone={meta.tone}>
      <span aria-hidden="true" className="xflow-editor-chip__icon">
        {statusGlyph[meta.icon]}
      </span>
      <span>{label ?? meta.label}</span>
    </span>
  );
}

/**
 * §6.8 execution list: the left column of the run-record mode. It is one
 * server-paginated list — never a client filter over the loaded page — because
 * a filter applied here would silently drop rows the server has not sent.
 */
export function ExecutionListPanel({
  state,
  selectedId,
  onLoad,
  onSelect
}: XFlowExecutionListPanelProps): React.ReactElement {
  const [filter, setFilter] = React.useState<XFlowExecutionStatusFilter>("all");
  const [range, setRange] = React.useState<XFlowExecutionRange>("24h");
  const [page, setPage] = React.useState(1);
  const [attempt, setAttempt] = React.useState(0);
  const requestedRef = React.useRef<string | undefined>(undefined);
  const rows = state?.list ?? [];
  const total = state?.total ?? 0;
  const loading = state?.loading === true;
  const hasMore = rows.length < total;

  // One request per (filter, range, page). The ref key makes the initial mount
  // and a filter change share the path, and keeps a re-render from re-asking
  // for a page that is already on screen or in flight.
  React.useEffect(() => {
    if (!onLoad) return;
    const createdAfter = executionRangeStart(range);
    const query: XFlowExecutionListQuery = {
      page,
      pageSize: EXECUTION_LIST_PAGE_SIZE,
      ...(filter === "all" ? {} : { status: filter }),
      ...(createdAfter === undefined ? {} : { createdAfter })
    };
    const key = JSON.stringify(query);
    if (requestedRef.current === key) return;
    requestedRef.current = key;
    onLoad(query);
  }, [attempt, filter, onLoad, page, range]);

  const changeFilter = (next: XFlowExecutionStatusFilter) => {
    requestedRef.current = undefined;
    setFilter(next);
    setPage(1);
  };

  const changeRange = (next: XFlowExecutionRange) => {
    requestedRef.current = undefined;
    setRange(next);
    setPage(1);
  };

  // A failed page keeps the loaded rows; 重试 re-asks for exactly that page by
  // bumping a nonce the request effect keys on.
  const retry = () => {
    requestedRef.current = undefined;
    setAttempt((current) => current + 1);
  };

  const onScroll = (event: React.UIEvent<HTMLDivElement>) => {
    if (!onLoad || loading || !hasMore) return;
    // A failed page leaves `page` on that page; advancing here would ask for the
    // next one and silently skip it. Only 重试 (or a filter change) may move on.
    if (state?.error) return;
    const element = event.currentTarget;
    if (element.scrollTop + element.clientHeight < element.scrollHeight - 24) return;
    setPage((current) => current + 1);
  };

  return (
    <EditorPanel
      className="xflow-editor-exec-list"
      icon={<HistoryOutlined />}
      title="执行"
      count={state === undefined ? undefined : `本工作流 · ${total}`}
    >
      <div className="xflow-editor-exec-filters">
        <Segmented
          className="xflow-editor-exec-filters__status"
          options={statusFilters}
          size="small"
          value={filter}
          onChange={(value) => changeFilter(value as XFlowExecutionStatusFilter)}
        />
        <Dropdown
          menu={{
            items: (Object.keys(rangeLabels) as XFlowExecutionRange[]).map((value) => ({
              key: value,
              label: rangeLabels[value]
            })),
            selectedKeys: [range],
            onClick: ({ key }) => changeRange(key as XFlowExecutionRange)
          }}
          trigger={["click"]}
        >
          <Button icon={<DownOutlined />} iconPlacement="end" size="small">
            {rangeLabels[range]}
          </Button>
        </Dropdown>
      </div>
      {state?.error ? (
        <div className="xflow-editor-exec-error" role="alert">
          <span>{state.error}</span>
          <Button autoInsertSpace={false} size="small" onClick={retry}>
            重试
          </Button>
        </div>
      ) : null}
      <div className="xflow-editor-exec-rows" role="list" onScroll={onScroll}>
        {rows.map((row) => {
          const duration = executionDuration(row);
          const error = firstLine(row.error);
          return (
            <div
              className="xflow-editor-exec-item"
              data-status={row.status}
              key={row.executionId}
              role="listitem"
            >
              <button
                aria-current={row.executionId === selectedId ? "true" : undefined}
                className="xflow-editor-exec-item__button"
                type="button"
                onClick={() => onSelect?.(row.executionId)}
              >
                <span className="xflow-editor-exec-item__id" title={row.executionId}>
                  {row.executionId}
                </span>
                <StatusChip
                  label={row.queuePosition ? `排队 · 第 ${row.queuePosition} 位` : undefined}
                  status={row.status}
                />
                <span className="xflow-editor-exec-item__sub">
                  {executionClock(row.createdAt)}
                  {duration ? ` · ${duration}` : ""}
                  {error ? ` · ${error}` : ""}
                </span>
              </button>
            </div>
          );
        })}
        {rows.length === 0 && !loading && (state !== undefined || !onLoad) ? (
          <p className="xflow-editor-exec-empty">
            {state?.error ? "加载失败，暂无记录。" : "没有匹配的执行。"}
          </p>
        ) : null}
        {loading || (state === undefined && onLoad) ? (
          <p className="xflow-editor-exec-empty">正在加载…</p>
        ) : null}
        {!loading && !state?.error && rows.length > 0 && hasMore ? (
          <p className="xflow-editor-exec-empty xflow-editor-exec-more">
            <Tooltip title="向下滚动加载更多">
              <span>还有 {total - rows.length} 条</span>
            </Tooltip>
          </p>
        ) : null}
      </div>
    </EditorPanel>
  );
}
