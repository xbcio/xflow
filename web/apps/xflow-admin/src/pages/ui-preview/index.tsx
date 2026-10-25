import '@xflow/core/tokens.css';
import type { Appearance } from '@xflow/core';
import { App, ConfigProvider, Segmented, Switch } from 'antd';
import { useLayoutEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { TodoVisibleProvider } from './components/Todo';
import { ApprovalSection } from './sections/approval';
import { CardPropsSection } from './sections/cardprops';
import { CompactSection } from './sections/compact';
import { ComposerSection } from './sections/composer';
import { EdgeSection } from './sections/edge';
import { FoundationSection } from './sections/foundation';
import { InspectorSection } from './sections/inspector';
import { ListSection } from './sections/list';
import { ListExecSection } from './sections/list-exec';
import { PublishSection } from './sections/publish';
import { RunBarSection } from './sections/runbar';
import { RunsSection } from './sections/runs';
import { StatesSection } from './sections/states';
import { WorkbenchSection } from './sections/wb';
import { themeConfig } from './tokens';
import './ui-preview.css';

interface PreviewSection {
  id: string;
  title: string;
  /** Anchor text: short enough that every label fits without wrapping mid-word. */
  nav: string;
  caption: string;
  /** The appearance is passed only to the token gallery, which prints values. */
  render: (appearance: Appearance) => ReactNode;
}

const sections: PreviewSection[] = [
  {
    id: 'wb',
    nav: '工作台',
    title: '1 · 工作台（运行中，选中「拉取客户档案」，诊断台展开）',
    caption:
      '顶栏一个主操作；执行状态只在运行条一处，按钮全部由状态表生成。左栏节点与大纲可收起，右栏属性面板收起后画布右上出现恢复入口；画布内容整体让出浮动行的高度。',
    render: () => <WorkbenchSection />,
  },
  {
    id: 'runbar',
    nav: '运行条',
    title: '1c · 运行条变体（ExecutionStatus 9 个值）',
    caption:
      '每个执行状态一条：文案、tone、图标与可用操作全部取自 workflowStatusMeta。终态保留 5s 并给"查看运行记录"，取消二次确认，排队显示位次，取消中无操作。',
    render: () => <RunBarSection />,
  },
  {
    id: 'states',
    nav: '节点状态',
    title: '2 · 节点状态全集（状态表 §4，唯一来源）',
    caption: '每个状态 = 色 + 图标 + 文本。编辑态（无运行快照）不显示胶囊；运行态下 pending 也必须可见。',
    render: () => <StatesSection />,
  },
  {
    id: 'inspector',
    nav: '属性面板',
    title: '1b · 属性面板：配置 / 连接 / 运行 + 未选中时的工作流设置',
    caption:
      '配置：基础信息 + 条件规则 + 底部操作；连接：输入来源与每个输出端口的去向（含未连接的错误出口）；运行：最近一次执行该节点的输入、输出、尝试与耗时；未选中节点时面板变成工作流设置。校验错误态用同一套消息样式区分 danger 与 warning。',
    render: () => <InspectorSection />,
  },
  {
    id: 'cardprops',
    nav: '卡片摘要',
    title: '2b · 节点卡片配置摘要',
    caption:
      '卡片头部下方最多 3 行「标签 · 值」，不点开就能读懂节点在做什么：表达式带 fx 标记，凭证只显示引用名，密钥打码，必填缺失红字，长值单行省略。哪几项由节点类型声明——这个字段后端还没有。',
    render: () => <CardPropsSection />,
  },
  {
    id: 'composer',
    nav: 'composer',
    title: '3 · composer 控件与状态',
    caption:
      'label 在上、必填红星；错误在控件下方；表达式态整框等宽，变量渲染为色块。全部是真实 AntD 控件（Input / Select / InputNumber / Switch / Slider / Segmented / Collapse），只有外观用语义类约束。',
    render: () => <ComposerSection />,
  },
  {
    id: 'foundation',
    nav: 'foundation',
    title: '4 · Foundation token',
    caption:
      '字体族、字号阶梯、语义色、状态 tone、分类色、圆角、阴影、尺寸。取值全部来自 tokens.ts：色板按当前外观打印，样例用同一批自定义属性上色，两者对不上就说明 token 管线断了。',
    render: (appearance) => <FoundationSection appearance={appearance} />,
  },
  {
    id: 'compact',
    nav: 'compact',
    title: '5 · 紧凑布局（< 1280 左栏折叠，< 1024 两栏都折叠）',
    caption:
      '断点由 xflow-core 单一导出，JS 与 CSS 共用：工作台按自身实测宽度折叠，不按窗口宽度。两个宿主分别是 1200 与 960，折叠后左栏/属性变成抽屉，rail 上的按钮是恢复入口，顶栏次要操作收进 ⋯。',
    render: () => <CompactSection />,
  },
  {
    id: 'edge',
    nav: '边与异常态',
    title: '5b · 边的外观、端口标签、选中与悬停 + 编辑态与异常态',
    caption:
      '边的线型只从状态表 §4 的「入边」列推导，不另写一套；悬停加粗到 2px 主色（§6.2），选中规范未定义。异常态含空画布、类型降级、保存冲突、只读、错误出口与低缩放（§6.5.1 / §6.2）。',
    render: () => <EdgeSection />,
  },
  {
    id: 'runs',
    nav: '运行记录',
    title: '7 · 运行记录模式（编辑 | 运行记录）',
    caption:
      '同一工作台切到只读回放：左栏换成执行列表，画布按所选执行的快照逐节点显示状态，属性面板落在运行 tab。执行 id 与节点名与工作台一致，状态栏的节点数与状态计数都从回放数组算出来。',
    render: () => <RunsSection />,
  },
  {
    id: 'approval',
    nav: '审批处理',
    title: '8 · 审批处理',
    caption:
      '节点卡的「处理」、运行条的等待数、管理端的「处理审批」都打开同一个对话框：同一个 manager_approval 与同一个 exec-8432。依次审批 2/3 与已处理/待处理人数由审批记录算出，三个决定分别写 approved / rejected / returned，timeout 是页脚的超时策略。',
    render: () => <ApprovalSection />,
  },
  {
    id: 'publish',
    nav: '发布与版本',
    title: '9 · 发布与版本',
    caption:
      '服务端不设草稿：保存 = 同一个版本号原地覆盖线上，发布 = 换新版本号、旧版本进历史。草稿相对线上只有修改——节点与连线集合不变，所以回放快照、管理端列表说的还是同一张图。差异行、非零计数 chip、工作台顶栏的未保存数都出自同一份变更数组；每条变更的「改后值」从图里读（节点显示名、卡片摘要行、审批超时），凡是卡片上画着的那一项，差异行说的必须与它是同一个值。校验警告沿用诊断台，冲突横幅与异常态那一节是同一份。',
    render: () => <PublishSection />,
  },
  {
    id: 'list',
    nav: '工作流列表',
    title: '6 · 管理端控制台（工作流中心）',
    caption:
      '首版只放有数据源的内容：工作流列表、节点库、Runner 与池。KPI、负责人、队列与存储状态没有接口，不出现。名称与版本读工作台那一份数据，计数与分页由行数算出；节点数接口不返回（裁定 20），触发与最近运行两列接口还没有，按需新字段标出。',
    render: () => <ListSection />,
  },
  {
    id: 'list-exec',
    nav: '执行列表',
    title: '6b · 管理端 · 运行记录',
    caption:
      '数据源 GET /v1/executions。行操作完全由 workflowStatusMeta[status].ops 生成，没有第二份「什么状态能做什么」；同一个 exec id 在运行条、回放与管理端列表里状态一致。没有批量接口，不做批量操作。',
    render: () => <ListExecSection />,
  },
];

/**
 * Anchor nav. It scrolls sideways rather than wrapping (a wrapped flex row
 * breaks each label mid-word), and the two ends carry a fade mask while there is
 * more to scroll: at 1024px the fourteen labels do not fit, and a row that just
 * stops at 「卡片摘」 reads as a truncated page rather than a scrollable one.
 */
function HeadNav({ items }: { items: PreviewSection[] }) {
  const navRef = useRef<HTMLElement>(null);
  const [overflow, setOverflow] = useState('');

  useLayoutEffect(() => {
    const nav = navRef.current;
    if (!nav) return;
    const update = () => {
      const end = nav.scrollWidth - nav.clientWidth - nav.scrollLeft;
      const next = `${nav.scrollLeft > 1 ? 'start' : ''}${end > 1 ? ' end' : ''}`.trim();
      setOverflow((previous) => (previous === next ? previous : next));
    };
    update();
    nav.addEventListener('scroll', update, { passive: true });
    const observer = new ResizeObserver(update);
    observer.observe(nav);
    return () => {
      nav.removeEventListener('scroll', update);
      observer.disconnect();
    };
  }, []);

  return (
    <nav
      className="xflow-pv-head-nav"
      aria-label="页内导航"
      ref={navRef}
      data-overflow={overflow || undefined}
    >
      {items.map((section) => (
        <a key={section.id} href={`#${section.id}`}>
          {section.nav}
        </a>
      ))}
    </nav>
  );
}

function readQuery(): { theme?: Appearance; section?: string } {
  if (typeof window === 'undefined') return {};
  const params = new URLSearchParams(window.location.search);
  const theme = params.get('theme');
  return {
    theme: theme === 'light' || theme === 'dark' ? theme : undefined,
    section: params.get('section') ?? undefined,
  };
}

/**
 * Dev-only UI preview route (XFLOW_UI_PREVIEW=1). It renders the static mockup
 * `.claude/mockups/xflow-ui-v1-preview.html` with the real stack — AntD 6 inside the
 * host's `StyleProvider layer` (app.tsx), Tailwind for layout, one semantic
 * stylesheet for the design-specific surfaces, and the `--xflow-*` custom
 * properties from @xflow/core's tokens.css — so the design can be reviewed
 * against real components and real tokens instead of a hand-written HTML page.
 *
 * The `__XFLOW_DEV_ONLY__` marker is deliberate: the production build must not
 * contain it, and `scripts/check-production-fixtures.mjs` fails if it does.
 */
export default function UiPreviewPage() {
  const query = useMemo(readQuery, []);
  const [appearance, setAppearance] = useState<Appearance>(query.theme ?? 'light');
  const [todoVisible, setTodoVisible] = useState(true);
  const visible = query.section ? sections.filter((section) => section.id === query.section) : sections;

  return (
    <>
      {/*
       * `autoInsertSpace: false` — AntD's default splits a two-character label
       * into `处 理` / `发 布`. The mockup draws them tight, and the space is a
       * rendering rule, not content, so it is turned off at the root rather than
       * patched per button.
       */}
      <ConfigProvider theme={themeConfig(appearance)} button={{ autoInsertSpace: false }}>
        <App>
          <TodoVisibleProvider visible={todoVisible}>
            <div
              className="xflow-pv xflow-preview-root"
              data-theme={appearance}
              data-dev-marker="__XFLOW_DEV_ONLY__"
            >
              <header className="xflow-pv-head bg-xflow-panel">
                <h1 className="xflow-pv-head-title">XFlow UI 预览</h1>
                <HeadNav items={sections} />
                <span className="flex-1" />
                <span className="xflow-pv-legend">
                  标出 <i>待接入</i> / <i>需新字段</i>
                </span>
                <Switch className="xflow-pv-switch" size="small" checked={todoVisible} onChange={setTodoVisible} />
                <Segmented
                  value={appearance}
                  onChange={(value) => setAppearance(value as Appearance)}
                  options={[
                    { label: '浅色', value: 'light' },
                    { label: '深色', value: 'dark' },
                  ]}
                />
              </header>

              <main className="xflow-pv-main">
                {visible.map((section) => (
                  <section className="xflow-pv-section" id={section.id} key={section.id}>
                    <h2 className="xflow-pv-section-title">{section.title}</h2>
                    <p className="xflow-pv-section-caption">{section.caption}</p>
                    {section.render(appearance)}
                  </section>
                ))}
                {visible.length === 0 ? (
                  <section className="xflow-pv-section">
                    <p className="xflow-pv-section-caption">
                      没有名为 {query.section} 的节。可用：{sections.map((section) => section.id).join(' / ')}
                    </p>
                  </section>
                ) : null}
              </main>
            </div>
          </TodoVisibleProvider>
        </App>
      </ConfigProvider>
    </>
  );
}
