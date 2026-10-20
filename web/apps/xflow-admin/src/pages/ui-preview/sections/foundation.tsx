import { cssVarName, foundationTokens, nodeCategoryMeta, paletteTokens, type Appearance } from '@xflow/core';

import { nodeStatusMeta } from '../meta';

type FoundationKey = keyof typeof foundationTokens;
type PaletteKey = keyof (typeof paletteTokens)['light'];

const typeRows: { key: FoundationKey; role: string }[] = [
  { key: 'fontXl', role: '工作流名' },
  { key: 'fontLg', role: '面板标题' },
  { key: 'fontMd', role: '正文与控件值（默认）' },
  { key: 'fontSm', role: '字段 label、说明' },
  { key: 'fontXs', role: 'meta（耗时、计数）' },
];

/** The whole palette, in reading order: surfaces → lines → text → tone. */
const colorRows: { key: PaletteKey; role: string }[] = [
  { key: 'app', role: '页面底色' },
  { key: 'panel', role: '面板' },
  { key: 'panelRaised', role: '抬升面（表格头、端口）' },
  { key: 'field', role: '控件底' },
  { key: 'canvas', role: '画布' },
  { key: 'canvasDot', role: '画布点阵' },
  { key: 'border', role: '边框' },
  { key: 'borderStrong', role: '强边框（入边）' },
  { key: 'hover', role: '悬停' },
  { key: 'text', role: '正文' },
  { key: 'textSecondary', role: '次级文本' },
  { key: 'textTertiary', role: '三级文本 / meta' },
  { key: 'primary', role: '主色' },
  { key: 'onPrimary', role: '主色上的文字' },
  { key: 'info', role: '信息' },
  { key: 'success', role: '成功' },
  { key: 'warning', role: '警告' },
  { key: 'danger', role: '危险' },
  { key: 'suspend', role: '挂起' },
  { key: 'neutral', role: '中性' },
  { key: 'accent', role: '代码' },
  { key: 'orange', role: '触发器' },
  { key: 'cyan', role: '转换' },
];

const radiusRows: FoundationKey[] = ['radiusXs', 'radiusSm', 'radiusMd', 'radiusLg', 'radiusPill'];
/** Shadows live in the palette because they differ per appearance. */
const shadowRows: PaletteKey[] = ['elev1', 'elev2', 'elev3'];
const sizeRows: { key: FoundationKey; role: string }[] = [
  { key: 'controlHeight', role: '控件高' },
  { key: 'controlHeightSm', role: '小控件高' },
  { key: 'floatHeight', role: '画布浮动条高' },
  { key: 'nodeWidth', role: '节点卡宽' },
  { key: 'ruler', role: '标尺厚' },
];

/** One status per tone, so every tone shows the chip the editor would render. */
const toneSamples = Object.values(nodeStatusMeta).filter(
  (meta, index, all) => all.findIndex((candidate) => candidate.tone === meta.tone) === index,
);

/**
 * 4 · foundation tokens. Nothing here writes a colour, a size or a radius: the
 * labels come from `tokens.ts` and the samples are painted with the very custom
 * property the token renders as (`cssVarName`), so a swatch and its hex can only
 * disagree if the token pipeline is broken.
 */
export function FoundationSection({ appearance }: { appearance: Appearance }) {
  const palette = paletteTokens[appearance];

  return (
    <div className="xflow-pv-foundation">
      {/* Wide: `--xflow-font-sans` is a five-family stack, and a token table
        * that ellipsizes the token it exists to document is worth nothing. */}
      <div className="xflow-pv-cell xflow-pv-cell--wide">
        <span className="xflow-pv-cell-cap">字体族</span>
        <div className="xflow-pv-token-row">
          <span style={{ fontFamily: `var(${cssVarName('fontSans')})` }}>Inter / PingFang SC · 界面正文</span>
          <code className="xflow-pv-token-value">
            {cssVarName('fontSans')} · {foundationTokens.fontSans}
          </code>
        </div>
        <div className="xflow-pv-token-row">
          <span className="xflow-pv-mono" style={{ fontFamily: `var(${cssVarName('fontMono')})` }}>
            {'JetBrains Mono · xflow.http · {{ $json.id }}'}
          </span>
          <code className="xflow-pv-token-value">
            {cssVarName('fontMono')} · {foundationTokens.fontMono}
          </code>
        </div>
      </div>

      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">字阶</span>
        {typeRows.map(({ key, role }) => (
          <div className="xflow-pv-token-row" key={key}>
            <span style={{ fontSize: `var(${cssVarName(key)})` }}>
              {foundationTokens[key]} / {role}
            </span>
            <code className="xflow-pv-token-value">
              {cssVarName(key)} · {foundationTokens[key]}
            </code>
          </div>
        ))}
      </div>

      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">状态 tone（色 + 图标 + 文本）</span>
        {toneSamples.map((meta) => (
          <div className="xflow-pv-token-row" key={meta.tone}>
            <span className="xflow-pv-swatch" data-tone={meta.tone} />
            <span className="xflow-pv-chip" data-tone={meta.tone}>
              <span className="xflow-pv-chip-icon">{meta.icon}</span>
              <span>{meta.label}</span>
            </span>
            <code className="xflow-pv-token-value">{cssVarName(meta.tone)}</code>
          </div>
        ))}
      </div>

      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">分类色（节点 rail 与卡片图标）</span>
        {Object.entries(nodeCategoryMeta).map(([category, meta]) => (
          <div className="xflow-pv-token-row" key={category}>
            <span className="xflow-pv-swatch" data-cat={category} />
            <span className="xflow-pv-token-name">{meta.label}</span>
            <code className="xflow-pv-token-value">{`data-cat="${category}"`}</code>
          </div>
        ))}
      </div>

      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">圆角</span>
        {radiusRows.map((key) => (
          <div className="xflow-pv-token-row" key={key}>
            <span className="xflow-pv-radius" style={{ borderRadius: `var(${cssVarName(key)})` }} />
            <code className="xflow-pv-token-value">
              {cssVarName(key)} · {foundationTokens[key]}
            </code>
          </div>
        ))}
      </div>

      {/* Second wide cell: the full palette table, next to the shadows and sizes
        * that are also raw values rather than components. */}
      <div className="xflow-pv-cell xflow-pv-cell--wide">
        <span className="xflow-pv-cell-cap">语义色（当前外观：{appearance}）</span>
        <div className="xflow-pv-swatches">
          {colorRows.map(({ key, role }) => (
            <div className="xflow-pv-token-row" key={key}>
              <span className="xflow-pv-swatch" style={{ background: `var(${cssVarName(key)})` }} />
              <span className="xflow-pv-token-name">{role}</span>
              <code className="xflow-pv-token-value">
                {cssVarName(key)} · {palette[key]}
              </code>
            </div>
          ))}
        </div>
      </div>

      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">阴影</span>
        {shadowRows.map((key) => (
          <div className="xflow-pv-token-row" key={key}>
            {/* The chip is panel-coloured, so it needs a surface of a different
              * colour under it: on the panel itself a dark-appearance shadow is
              * black on near-black. */}
            <span className="xflow-pv-elev-stage">
              <span className="xflow-pv-elev" style={{ boxShadow: `var(${cssVarName(key)})` }} />
            </span>
            <code className="xflow-pv-token-value">{cssVarName(key)}</code>
          </div>
        ))}
      </div>

      <div className="xflow-pv-cell">
        <span className="xflow-pv-cell-cap">尺寸</span>
        {sizeRows.map(({ key, role }) => (
          <div className="xflow-pv-token-row" key={key}>
            <span className="xflow-pv-token-name">{role}</span>
            <code className="xflow-pv-token-value">
              {cssVarName(key)} · {foundationTokens[key]}
            </code>
          </div>
        ))}
      </div>
    </div>
  );
}
