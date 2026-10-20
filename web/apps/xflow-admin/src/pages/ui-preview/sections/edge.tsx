import { Button } from 'antd';
import type { CSSProperties } from 'react';

import { Banner } from '../components/Banner';
import { cx } from '../components/cx';
import { NodeCard } from '../components/NodeCard';
import { CategoryIcon, SectionLabel } from '../components/primitives';
import { wbEdgeLabelRows, wbEdgeSamples, wbEdgeStateRows, wbExceptionCards, edgeKindOf, type IconKey } from '../data';
import { nodeStatusMeta, type Category, type EdgeVariant } from '../meta';

/**
 * A stand-in for a node card inside the edge gallery. The samples illustrate the
 * *stroke*, and two 264px cards would not fit a gallery cell; these boxes exist
 * to give the edge two endpoints.
 */
function SampleNode({ id, category, icon }: { id: string; category: Category; icon: IconKey }) {
  return (
    /* `data-node-id` 与真实卡片同名同义：S2 按边声明的 from/to 找盒子量端点，
       画廊里的两个「节点」也要能被找到。 */
    <span className="xflow-pv-edge-stub" data-node-id={id}>
      <CategoryIcon category={category} icon={icon} size="sm" />
      <span className="xflow-pv-edge-stub-name">{id}</span>
    </span>
  );
}

/**
 * Height of an exception canvas. The cards are absolutely positioned, so the box
 * has to be told how tall the content is: the lowest card plus its own height,
 * and at low zoom that whole layout is multiplied by the scale.
 */
function canvasHeight(card: (typeof wbExceptionCards)[number]): number {
  const nodes = card.nodes ?? [];
  const bottom = nodes.reduce((max, node) => Math.max(max, node.y), 0);
  const scale = card.lodScale ?? 1;
  return Math.round((bottom + 168) * scale) + 16;
}

/** One edge drawn between two stubs on a fixed 240×56 geometry. */
function EdgeSample({ variant, label, state }: { variant: EdgeVariant; label?: string; state?: string }) {  return (
    <span className="xflow-pv-edge-sample" data-state={state}>
      <SampleNode id="from" category="action" icon="globe" />
      {variant === 'none' ? (
        <span className="xflow-pv-edge-none">不画线</span>
      ) : (
        <svg className="xflow-pv-edges" aria-hidden="true">
          <path
            className={cx('xflow-pv-edge', `xflow-pv-edge--${variant}`)}
            data-edge={`sample-${variant}`}
            data-from="from"
            data-to="to"
            d="M93 28 C 110 28, 130 28, 147 28"
          />
        </svg>
      )}
      {label ? (
        <span className="xflow-pv-edge-label" style={{ left: 120, top: 28 }}>
          {label}
        </span>
      ) : null}
      <SampleNode id="to" category="action" icon="db" />
    </span>
  );
}

export function EdgeSection() {
  return (
    <>
      <SectionLabel
        title="边的外观（§4 入边列，唯一来源）"
        extra={<span className="xflow-pv-faint">nodeStatusMeta[...].edge</span>}
      />
      <div className="xflow-pv-edge-grid">
        {wbEdgeSamples.map((row) => (
          <div className="xflow-pv-cell" key={row.variant}>
            <span className="xflow-pv-cell-cap xflow-pv-mono">{row.statuses.join(' · ')}</span>
            <EdgeSample variant={row.variant} />
            <span className="xflow-pv-edge-meta">
              {row.appearances.map((appearance) => (
                <span className="xflow-pv-chip" data-tone="neutral" key={appearance}>
                  <span className="xflow-pv-chip-icon">{nodeStatusMeta[row.statuses[0]].icon}</span>
                  <span>{appearance}</span>
                </span>
              ))}
              {row.variant === 'none' ? <span className="xflow-pv-faint">无入边</span> : null}
            </span>
            {row.note ? <span className="xflow-pv-edge-note">{row.note}</span> : null}
          </div>
        ))}
      </div>

      <SectionLabel title="端口标签与错误出口（§6.2）" />
      <div className="xflow-pv-edge-grid">
        {wbEdgeLabelRows.map((row) => (
          <div className="xflow-pv-cell" key={row.id}>
            <span className="xflow-pv-cell-cap">{row.caption}</span>
            <EdgeSample variant={row.tone === 'danger' ? 'error' : 'default'} label={row.text} />
          </div>
        ))}
        <div className="xflow-pv-cell">
          {/* 说明放 note 行：标题是原子容器（.xflow-pv-cell-cap），长到折行就会在词中间断开。 */}
          <span className="xflow-pv-cell-cap">错误出口的完整卡片</span>
          <span className="xflow-pv-edge-note">error 端口在 port 行最后，句柄在右下（红色虚线从句柄出发）</span>
          <div className="xflow-pv-exc-canvas" style={{ height: 176 }}>
            <div className="xflow-pv-viewport">
              <svg className="xflow-pv-edges" aria-hidden="true">
                {/* 起点就是 error 句柄的中心（卡片右缘 12 + 264 = 276，y = 卡顶 12 + 66），
                    与 error 端口的中心同一水平线；对齐由 E1 现场测量。 */}
                {/* 出口示意：这条线不连任何卡片（没有 from/to），是「红虚线从 error
                    句柄出发」的插图；起点由 E1 现场量，S2 只查两端都声明了卡片的边。 */}
                <path className="xflow-pv-edge xflow-pv-edge--error" d="M276 78 C 298 78, 302 100, 324 100" />
              </svg>
              <NodeCard
                variant="canvas"
                node={{
                  id: 'charge_payment',
                  type: 'xflow.http',
                  category: 'action',
                  icon: 'globe',
                  x: 12,
                  y: 12,
                  handles: 'out',
                  ports: ['main', 'error'],
                }}
              />
            </div>
          </div>
        </div>
      </div>

      <SectionLabel title="选中与悬停" extra={<span className="xflow-pv-faint">§6.2 只定义了 hover</span>} />
      <div className="xflow-pv-edge-grid">
        {wbEdgeStateRows.map((row) => (
          <div className="xflow-pv-cell" key={row.id}>
            <span className="xflow-pv-cell-cap">{row.caption}</span>
            <EdgeSample variant={row.variant} label="true" state={row.state} />
          </div>
        ))}
      </div>

      <SectionLabel title="编辑态与异常态（§6.5.1 + §6.2）" />
      <div className="xflow-pv-exc-grid">
        {wbExceptionCards.map((card) => (
          <div className="xflow-pv-card xflow-pv-exc-card" key={card.id} data-card={card.id}>
            <header className="xflow-pv-exc-head">
              <span className="xflow-pv-exc-title">{card.title}</span>
              <span className="xflow-pv-exc-note">{card.note}</span>
            </header>

            {/* 异常态卡片保留横幅自带的两个操作（§6.5.1）；发布里的冲突对话框
                会关掉它们，因为那里页脚已经有一组同义操作（G4）。 */}
            {card.banner ? (
              <Banner
                tone={card.banner.tone}
                icon={card.banner.icon}
                strong={card.banner.strong}
                text={card.banner.text}
                actions={card.banner.actions}
              />
            ) : null}

            {card.banner?.conflict ? (
              <p className="xflow-pv-conflict" role="note">
                {card.banner.conflict}
              </p>
            ) : null}

            {card.empty ? (
              <div className="xflow-pv-exc-canvas xflow-pv-exc-canvas--empty">
                <div className="xflow-pv-empty">
                  <span className="xflow-pv-empty-icon">
                    <CategoryIcon category="trigger" icon="link" />
                  </span>
                  <h4 className="xflow-pv-empty-title">{card.empty.title}</h4>
                  <p className="xflow-pv-empty-text">{card.empty.text}</p>
                  <div className="xflow-pv-empty-actions">
                    <Button type="primary" size="small">
                      {card.empty.actions[0]}
                    </Button>
                    <Button size="small">{card.empty.actions[1]}</Button>
                  </div>
                </div>
              </div>
            ) : null}

            {card.json ? (
              <>
                <pre className="xflow-pv-code">{card.json}</pre>
                <p className="xflow-pv-hint">无表单定义时属性面板只显示 JSON 编辑器，保存前做语法校验。</p>
              </>
            ) : null}

            {card.nodes?.length ? (
              <div
                className="xflow-pv-exc-canvas"
                data-lod={card.lodScale ? 'low' : undefined}
                style={
                  {
                    height: canvasHeight(card),
                    ...(card.lodScale ? { '--xflow-pv-lod': card.lodScale } : {}),
                  } as CSSProperties
                }
              >
                <div
                  className="xflow-pv-viewport"
                  style={card.lodScale ? { transform: `scale(${card.lodScale})` } : undefined}
                >
                  {card.edges?.length ? (
                    <svg className="xflow-pv-edges" aria-hidden="true">
                      {card.edges.map((edge) => {
                        // 与工作台、回放同一个函数：线型 = 目标节点状态（§4 入边）。
                        const kind = edgeKindOf(edge, (id) => card.nodes?.find((node) => node.id === id)?.status);
                        if (kind === 'none') return null;
                        return (
                          <path
                            key={edge.id}
                            className={cx('xflow-pv-edge', `xflow-pv-edge--${kind}`)}
                            data-edge={edge.id}
                            data-from={edge.from}
                            data-from-port={edge.fromPort}
                            data-to={edge.to}
                            d={edge.d}
                          />
                        );
                      })}
                    </svg>
                  ) : null}
                  {card.nodes.map((node) => (
                    <NodeCard key={node.id} node={node} variant="canvas" />
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </>
  );
}
