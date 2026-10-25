import { KeyIcon, PinIcon } from '@xflow/icons';
import { Button } from 'antd';
import { Fragment } from 'react';

import type { PreviewNode, SummaryValue } from '../data';
import { nodeStatusMeta } from '../meta';
import { cx } from './cx';
import { CategoryIcon, Port, StatusChip } from './primitives';
import { Todo } from './Todo';

function SummaryValueView({ value }: { value: SummaryValue }) {
  switch (value.kind) {
    case 'mono':
      return <span className="xflow-pv-mono">{value.text}</span>;
    case 'mix': {
      // Drop blank segments first: joining a row whose value is half missing
      // must not leave a separator hanging at either end.
      const parts = value.parts.filter((part) => part.text.trim() !== '');
      return (
        <>
          {parts.map((part, index) => (
            <Fragment key={`${index}-${part.text}`}>
              {index > 0 ? <span>{value.separator ?? ' '}</span> : null}
              {part.mono ? (
                <span className="xflow-pv-mono">{part.text}</span>
              ) : (
                <span>{part.text}</span>
              )}
            </Fragment>
          ))}
        </>
      );
    }
    case 'expr':
      return <span className="xflow-pv-expr">{value.text}</span>;
    case 'ref':
      return (
        <span className="xflow-pv-ref">
          {/* Colour on the wrapper, not on the icon: `.xflow-icon` strokes in
           * `currentColor`, so the tint cascades from the wrapper. */}
          <span className="xflow-pv-ref-icon">
            <KeyIcon />
          </span>
          {value.text}
        </span>
      );
    case 'secret':
      return <span className="xflow-pv-secret">••••••</span>;
    case 'missing':
      return <span className="xflow-pv-missing">{value.text}</span>;
    case 'more':
      return <span className="xflow-pv-more">{value.text}</span>;
    default:
      return <span>{value.text}</span>;
  }
}

/**
 * Node card (§6.1). `variant="canvas"` is absolutely positioned on the canvas;
 * `variant="static"` is the in-flow card the state wall and the card gallery use.
 */
export function NodeCard({
  node,
  selected,
  onSelect,
  variant = 'canvas',
  readOnly = false,
}: {
  node: PreviewNode;
  selected?: boolean;
  onSelect?: (id: string) => void;
  variant?: 'canvas' | 'static';
  /** 只读回放：卡片不提供任何操作（§6.8），即使节点数据上挂着动作。 */
  readOnly?: boolean;
}) {
  const status = node.status ? nodeStatusMeta[node.status] : undefined;
  const onCanvas = variant === 'canvas';
  /*
   * 等待说明与「处理」是**已挂起**这个状态的画法，不是节点自带的属性：把它们挂在
   * 节点数据上，回放里那个 status=skipped 的节点就还在等人、还带着按钮。§4 的
   * 卡片列只给 suspended 定义了「等待条」。
   */
  const waitNote = node.status === 'suspended' && !readOnly ? node.note : undefined;
  /*
   * §6.2: `on_error=output` 的 error 出口有自己的句柄，画在 port 行、与 error chip
   * 同一水平线；标题行那个出句柄属于 main 出口，此时不画（否则一个节点两个出口
   * 却只有一个句柄，而错误连线会从标题行出发）。
   */
  const hasErrorPort = (node.ports ?? []).includes('error');
  const showMainOut = onCanvas && (node.handles === 'out' || node.handles === 'both') && !hasErrorPort;
  const showErrorOut = onCanvas && (node.handles === 'out' || node.handles === 'both') && hasErrorPort;
  // 有 error 出口时标题行的出句柄让位，main 出口的句柄改挂在 main chip 的下沿（与真实 preview 一致）。
  const showMainChipOut = showErrorOut && (node.ports ?? []).includes('main');

  return (
    <article
      className={cx('xflow-pv-node', !onCanvas && 'xflow-pv-node--static')}
      data-status={node.status}
      data-tone={status?.tone}
      data-node-id={onCanvas ? node.id : undefined}
      data-invalid={node.invalid ? 'true' : undefined}
      data-selected={selected ? 'true' : undefined}
      style={onCanvas ? { left: node.x, top: node.y } : undefined}
      title={node.label ? `${node.label} · ${node.id}` : node.id}
      onClick={onSelect ? () => onSelect(node.id) : undefined}
    >
      {node.pinned ? (
        <span className="xflow-pv-pin">
          <PinIcon />
        </span>
      ) : null}
      {node.badge ? <span className="xflow-pv-badge-err">{node.badge}</span> : null}
      {onCanvas && (node.handles === 'in' || node.handles === 'both') ? (
        <span className="xflow-pv-handle xflow-pv-handle--in" />
      ) : null}
      {showMainOut ? <span className="xflow-pv-handle xflow-pv-handle--out" /> : null}

      <div className="xflow-pv-node-head">
        <CategoryIcon category={node.category} icon={node.icon} />
        <div className="xflow-pv-node-title">
          {/*
           * §6.2 低缩放只留「图标 + 名称 + 状态图标」，而名称取的是**显示名**：一张
           * 264px 的卡片在 45% 下只有 119px 宽，减去图标和间距留给名称的约 60px，
           * 节点 id（manager_approval）在这个宽度里只能截断，显示名（主管审批）不用。
           * 没有显示名的节点退回 id，和画布其它地方一致。
           */}
          <div className="xflow-pv-node-name">
            <span className="xflow-pv-node-name-id">{node.id}</span>
            {node.label ? <span className="xflow-pv-node-name-label">{node.label}</span> : null}
          </div>
          <div className="xflow-pv-node-type">{node.type}</div>
        </div>
        {status ? <StatusChip tone={status.tone} icon={status.icon} label={node.chipLabel ?? status.label} /> : null}
      </div>

      {waitNote ? (
        <div className="xflow-pv-node-note">
          <span>{waitNote.text}</span>
          {waitNote.action && !readOnly ? (
            <Todo label={waitNote.actionTodo ?? '待接入'}>
              <Button size="small">{waitNote.action}</Button>
            </Todo>
          ) : null}
        </div>
      ) : null}

      {node.summary?.length ? (
        <dl className="xflow-pv-node-props">
          {node.summary.map((row) => (
            <Fragment key={row.label}>
              <dt>{row.label}</dt>
              {/* 行的标签同时挂在值上：断言要按「哪个节点的哪一行」取那段可见文字，
                  而不是靠 :nth-of-type 数位置（H1 / S13）。 */}
              <dd data-summary-row={row.label}>
                {row.todo ? (
                  <Todo label={row.todo} placement="inline">
                    <SummaryValueView value={row.value} />
                  </Todo>
                ) : (
                  <SummaryValueView value={row.value} />
                )}
              </dd>
            </Fragment>
          ))}
        </dl>
      ) : null}

      {node.meta ? (
        <div className="xflow-pv-node-meta">
          <span title={node.meta.leftTitle}>{node.meta.left}</span>
          {node.meta.right ? (
            <span>
              {node.meta.rightTodo ? (
                <Todo label={node.meta.rightTodo} placement="inline">
                  {node.meta.right}
                </Todo>
              ) : (
                node.meta.right
              )}
            </span>
          ) : null}
        </div>
      ) : null}

      {node.error ? (
        <div className="xflow-pv-node-error" title={node.error}>
          {node.error}
        </div>
      ) : null}

      {/*
       * 端口行是卡片的最后一行（元信息、错误摘要都排在它上面）：出边从句柄出来后
       * 要经过卡片自己的内容才能离开，句柄离卡底越近，被不透明卡片盖住的那段越短。
       */}
      {node.ports?.length ? (
        <div className="xflow-pv-node-ports">
          {/* §6.2: the `error` outlet of an `on_error=output` node is a danger port,
              so it is the one port that is not neutral. */}
          {node.ports.map((port) => (
            <Port key={port} error={port === 'error'} handle={showMainChipOut && port === 'main'}>
              {port}
            </Port>
          ))}
          {/* 错误出口的句柄挂在 port 行里（见上），位置由这一行的高度决定。 */}
          {showErrorOut ? <span className="xflow-pv-handle xflow-pv-handle--err" /> : null}
        </div>
      ) : null}
    </article>
  );
}
