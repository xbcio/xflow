import { LocateIcon, PauseIcon, PlayIcon, SkipIcon, StopIcon, XMarkIcon } from "@xflow/icons";
import { Button, Divider, Popconfirm, Tooltip, Typography } from "antd";
import {
  executionOpLabel,
  workflowStatusMeta,
  type ExecutionOp,
  type ExecutionStatusKey,
  type NodeStatus,
  type XFlowEditorCapabilities
} from "@xflow/core";
import * as React from "react";

import { statusGlyph } from "./statusGlyph";

/**
 * A pause / resume / cancel, i.e. the status table's ops minus `rerun`. Rerun
 * is not a control of a running execution: it opens the §6.9 dialog, so it
 * travels as its own callback (`onRerun`) rather than through `onAction`.
 */
export type ExecutionControlAction = Exclude<ExecutionOp, "rerun">;

/**
 * The structural slice of `@xflow/api`'s `ExecutionDetail` the editor reads.
 * The editor must not import `@xflow/api` (package boundary), so the host's
 * value is accepted structurally; an `ExecutionDetail` satisfies this shape.
 */
export interface XFlowExecutionNode {
  name: string;
  status: NodeStatus;
  attempt?: number;
  port?: string;
  error?: string;
  errorDetails?: Record<string, unknown>;
  output?: Record<string, unknown>;
  childExecutionId?: string;
}

export interface XFlowExecution {
  executionId: string;
  status: ExecutionStatusKey;
  /** The workflow definition identity captured at submit (B1/B2a). */
  workflowId?: string;
  /** The version label captured with {@link workflowId}. */
  workflowVersion?: string;
  /** B2b stamps; absent on executions recorded before the backends wrote them. */
  createdAt?: string;
  updatedAt?: string;
  /** The execution this one derives from, on a rerun or a sub-workflow child. */
  parentExecutionId?: string;
  /** The node a rerun started at. */
  rerunFrom?: string;
  /** The parent's `xflow.subworkflow` node this child satisfies. */
  parentNode?: string;
  /** 1-based place in the workflow's concurrency queue while `queued`. */
  queuePosition?: number;
  error?: string;
  nodes?: XFlowExecutionNode[];
}

const DEFAULT_DISMISS_MS = 5000;
/**
 * The exit animation's duration (E-C): the bar fades out before it unmounts,
 * so the token-driven CSS has a frame to play. This is the CSS
 * `--xflow-motion-duration-fast` (120ms) as a number, the only place the
 * unmount delay is expressed in JS.
 */
const EXIT_MS = 120;
const CLOCK_TICK_MS = 1000;
/** WB-03: `exec-` + UUID ids are long; the bar shows a prefix, keeps the full id in tooltips/copy. */
const SHORT_ID_LENGTH = 8;

function parseTimestamp(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const time = Date.parse(value);
  return Number.isFinite(time) ? time : undefined;
}

/** WB-03 wall-clock label: `12s`, `1m 05s`, `1h 02m 05s`. */
function formatElapsed(ms: number): string {
  const total = Math.max(0, Math.floor(ms / CLOCK_TICK_MS));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  const ss = String(seconds).padStart(2, "0");
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, "0")}m ${ss}s`;
  if (minutes > 0) return `${minutes}m ${ss}s`;
  return `${seconds}s`;
}

/** C05: first 8 characters plus an ellipsis; nothing is fetched or navigated by this. */
function shortExecutionId(executionId: string): string {
  return executionId.length > SHORT_ID_LENGTH ? `${executionId.slice(0, SHORT_ID_LENGTH)}…` : executionId;
}

const opIcon: Record<ExecutionControlAction, React.ReactNode> = {
  pause: <PauseIcon />,
  resume: <PlayIcon />,
  cancel: <StopIcon />
};

/**
 * §6.3 canvas run toast: the one place a running execution's state appears,
 * floating over the canvas's top centre instead of occupying a toolbar row.
 * Operations come from `workflowStatusMeta[status].ops` — the toast restates no
 * state table — then drop whatever the host's capabilities forbid. Rerun is
 * drawn only when the host supplies `onRerun` (Q7=a); the other controls call
 * `onAction`. Cancel asks first; pause / resume run immediately; a terminal
 * toast stays for `dismissMs` (default 5s) and then hides itself.
 */
export function RunBar({
  execution,
  capabilities,
  onAction,
  onRerun,
  onLocateNode,
  onHandleApproval,
  onOpenRunHistory,
  dismissMs: dismissMsProp,
  getPopupContainer
}: {
  execution: XFlowExecution;
  capabilities?: XFlowEditorCapabilities;
  onAction?: (executionId: string, action: ExecutionControlAction) => void | Promise<void>;
  /** Opens the §6.9 rerun dialog for this execution. Omit and `rerun` is hidden. */
  onRerun?: (executionId: string) => void;
  /** Selects the first node behind a count. Required: the editor always owns a canvas. */
  onLocateNode: (nodeName: string) => void;
  /**
   * The first suspended node is an approval the editor can open (§6.12). With
   * it the 等待审批 count opens that dialog instead of only locating the card.
   */
  onHandleApproval?: (nodeName: string) => void;
  onOpenRunHistory?: () => void;
  /**
   * How long a terminal bar stays before hiding itself, in ms. Default 5000;
   * `null` keeps it until the user closes it. The host overrides the default.
   */
  dismissMs?: number | null;
  /** Popup portal target; the editor root, so popups stay inside the theme. */
  getPopupContainer?: () => HTMLElement;
}): React.ReactElement | null {
  const status = workflowStatusMeta[execution.status];
  const nodes = execution.nodes ?? [];
  const suspended = nodes.filter((node) => node.status === "suspended");
  const failed = nodes.filter((node) => node.status === "failed");
  const [dismissedId, setDismissedId] = React.useState<string>();
  /** The bar whose auto-dismiss delay elapsed; it plays the exit, then unmounts. */
  const [leavingId, setLeavingId] = React.useState<string>();
  const dismissMs = dismissMsProp === undefined ? DEFAULT_DISMISS_MS : dismissMsProp;

  React.useEffect(() => {
    if (!status.terminal || dismissMs === null) return undefined;
    const id = execution.executionId;
    const timer = window.setTimeout(() => setLeavingId(id), dismissMs);
    return () => window.clearTimeout(timer);
  }, [dismissMs, execution.executionId, status.terminal]);

  // The exit is a two-phase dismissal: the bar stays mounted for the CSS
  // animation, then leaves the tree.
  React.useEffect(() => {
    if (leavingId === undefined || leavingId !== execution.executionId) return undefined;
    const timer = window.setTimeout(() => setDismissedId(leavingId), EXIT_MS);
    return () => window.clearTimeout(timer);
  }, [leavingId, execution.executionId]);

  // WB-03: a live run counts up from created_at on the wall clock (paused runs
  // keep counting); a terminal run freezes, preferring the server's updated_at
  // and otherwise the moment this bar first saw the terminal status.
  const createdMs = parseTimestamp(execution.createdAt);
  const [clock, setClock] = React.useState<{ executionId: string; at: number }>();
  const [terminalAt, setTerminalAt] = React.useState<{ executionId: string; at: number }>();
  // The id check keeps a newly started run from reading the previous run's clock
  // for the frame before the effect below re-seeds it.
  const nowMs = clock?.executionId === execution.executionId ? clock.at : Date.now();

  React.useEffect(() => {
    if (status.terminal) {
      setTerminalAt((current) =>
        current?.executionId === execution.executionId ? current : { executionId: execution.executionId, at: Date.now() }
      );
      return undefined;
    }
    setTerminalAt(undefined);
    setClock({ executionId: execution.executionId, at: Date.now() });
    const timer = window.setInterval(
      () => setClock({ executionId: execution.executionId, at: Date.now() }),
      CLOCK_TICK_MS
    );
    return () => window.clearInterval(timer);
  }, [execution.executionId, status.terminal]);

  const endMs = status.terminal
    ? parseTimestamp(execution.updatedAt) ??
      (terminalAt?.executionId === execution.executionId ? terminalAt.at : undefined)
    : nowMs;
  const elapsed = createdMs !== undefined && endMs !== undefined ? formatElapsed(endMs - createdMs) : undefined;
  const elapsedTitle =
    elapsed === undefined
      ? undefined
      : status.terminal
        ? `总耗时 ${elapsed}（从创建时间起算）`
        : `已运行 ${elapsed}（从创建时间起算；暂停期间按墙上时间继续计时）`;

  // A new execution id is a new bar: the id comparison, not a reset effect,
  // makes the close survive re-renders but not the next run.
  if (dismissedId === execution.executionId) return null;

  const allowed = (op: ExecutionOp): boolean =>
    op === "rerun"
      ? capabilities?.executionRerun !== false && onRerun !== undefined
      : op === "cancel"
        ? capabilities?.execution !== false
        : capabilities?.executionControl !== false;
  const ops = status.ops.filter(allowed);
  // A failed terminal run is the one state the user must not miss: its live
  // region is assertive (role=alert). Every other state stays polite.
  const failedTerminal = status.terminal && execution.status === "failed";
  const firstSuspended = suspended[0];
  const firstFailed = failed[0];
  const counts: { key: string; tone: string; label: string; node: string | undefined }[] = [];
  if (firstSuspended) {
    counts.push({ key: "suspend", tone: "suspend", label: `${suspended.length} 等待审批`, node: firstSuspended.name });
  }
  if (firstFailed) {
    counts.push({ key: "danger", tone: "danger", label: `${failed.length} 失败`, node: firstFailed.name });
  }

  return (
    <div
      aria-label={`执行 ${execution.executionId} 状态`}
      className="xflow-editor-runbar"
      data-leaving={leavingId === execution.executionId ? "true" : undefined}
      role="group"
    >
      <span
        aria-live={failedTerminal ? "assertive" : "polite"}
        className="xflow-editor-chip"
        data-status={execution.status}
        data-tone={status.tone}
        role={failedTerminal ? "alert" : "status"}
      >
        <span aria-hidden="true" className="xflow-editor-chip__icon">
          {statusGlyph[status.icon]}
        </span>
        <span>
          {status.label}
          {execution.queuePosition ? ` · 第 ${execution.queuePosition} 位` : ""}
          {elapsed !== undefined ? (
            <>
              {" · "}
              {/* The chip is a live region; a per-second tick must not be
                  announced, so the timer stays out of the accessibility tree
                  while its title still explains the wall-clock basis. */}
              <span aria-hidden="true" className="xflow-editor-runbar__duration" title={elapsedTitle}>
                {elapsed}
              </span>
            </>
          ) : null}
        </span>
      </span>

      {/* §6.3 item 8: the execution id, mono and copyable; the copy control is
          AntD's own so it keeps keyboard access and its copied feedback. C05:
          the text is the 8-character prefix; hover, the accessible name and the
          copied text all carry the full id, which is what every control uses. */}
      <Typography.Text
        className="xflow-editor-runbar__id"
        copyable={{ text: execution.executionId, tooltips: ["复制执行 id", "已复制"] }}
      >
        <span
          aria-label={execution.executionId}
          className="xflow-editor-runbar__id-text"
          title={execution.executionId}
        >
          {shortExecutionId(execution.executionId)}
        </span>
      </Typography.Text>

      {counts.length > 0 ? (
        <span className="xflow-editor-runbar__counts">
          {counts.map((count) => (
            <Tooltip key={count.key} title={count.key === "suspend" && onHandleApproval ? "处理审批" : "定位到节点"}>
              <Button
                className="xflow-editor-runbar__count"
                data-tone={count.tone}
                disabled={count.node === undefined}
                icon={<LocateIcon />}
                size="small"
                type="text"
                onClick={() => {
                  if (count.node === undefined) return;
                  if (count.key === "suspend" && onHandleApproval) {
                    onHandleApproval(count.node);
                    return;
                  }
                  onLocateNode(count.node);
                }}
              >
                {count.label}
              </Button>
            </Tooltip>
          ))}
        </span>
      ) : null}

      {status.terminal || ops.length > 0 ? (
        <>
          <Divider className="xflow-editor-sep xflow-editor-runbar__sep" orientation="vertical" />
          <span className="xflow-editor-runbar__ops">
            {status.terminal ? (
              <Button
                className="xflow-editor-runbar__link"
                disabled={!onOpenRunHistory}
                size="small"
                type="link"
                onClick={() => onOpenRunHistory?.()}
              >
                查看运行记录
              </Button>
            ) : null}
            {ops.map((op) =>
              op === "rerun" ? (
                <Button
                  disabled={!onRerun}
                  icon={<SkipIcon />}
                  key={op}
                  size="small"
                  onClick={() => onRerun?.(execution.executionId)}
                >
                  {executionOpLabel[op]}
                </Button>
              ) : op === "cancel" ? (
                <Popconfirm
                  key={op}
                  cancelText="返回"
                  classNames={{ root: "xflow-editor-confirm" }}
                  description="在途节点提交后停止，已完成的结果保留。"
                  disabled={!onAction}
                  getPopupContainer={getPopupContainer}
                  okButtonProps={{ danger: true }}
                  okText="取消执行"
                  onConfirm={() => void onAction?.(execution.executionId, "cancel")}
                  title={`取消执行 ${execution.executionId}？`}
                >
                  <Button danger disabled={!onAction} icon={opIcon[op]} size="small">
                    {executionOpLabel[op]}
                  </Button>
                </Popconfirm>
              ) : (
                <Button
                  disabled={!onAction}
                  icon={opIcon[op]}
                  key={op}
                  size="small"
                  onClick={() => void onAction?.(execution.executionId, op)}
                >
                  {executionOpLabel[op]}
                </Button>
              )
            )}
            {status.terminal ? (
              <Tooltip title="关闭运行条">
                <Button
                  aria-label="关闭运行条"
                  className="xflow-editor-runbar__tool"
                  icon={<XMarkIcon />}
                  size="small"
                  type="text"
                  onClick={() => setDismissedId(execution.executionId)}
                />
              </Tooltip>
            ) : null}
          </span>
        </>
      ) : null}

      {status.terminal && dismissMs !== null ? (
        <span className="xflow-editor-runbar__hint">{Math.round(dismissMs / 1000)}s 后收起</span>
      ) : null}
    </div>
  );
}
