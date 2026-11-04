import {
  CheckIcon,
  CodeIcon,
  CopyIcon,
  DownIcon,
  MoreIcon,
  OutlineIcon,
  PlusIcon,
  SearchIcon,
  SlidersIcon,
  WarnIcon,
} from '@xflow/icons';
import { Button, Collapse, Input, Tabs, Tooltip } from 'antd';
import { Fragment, type ReactNode } from 'react';

import { CategoryIcon, IconButton, PanelTitle, SectionLabel, StatusChip } from '../components/primitives';
import { SpecField } from '../components/SpecField';
import { Todo } from '../components/Todo';
import {
  inspConnectedCount,
  inspConnections,
  inspSettings,
  inspInvalid,
  inspRuns,
  inspSwitch,
  wfSettings,
  type FieldSpec,
  type IconKey,
  type PortRowSpec,
  type RuleSpec,
} from '../data';
import type { Category } from '../meta';

/**
 * Section stack, identical to the workbench panel's: the gallery only ever
 * previews what the editor docks, so it reuses the same classes.
 *
 * `defaultActive: false` opens a section collapsed — used where the point of
 * the panel is that the whole stack is visible at once.
 */
function Sections({
  items,
}: {
  items: { key: string; label: ReactNode; children: ReactNode; defaultActive?: boolean; fixed?: boolean }[];
}) {
  return (
    <Collapse
      ghost
      className="xflow-pv-secs"
      defaultActiveKey={items.filter((item) => item.defaultActive !== false).map((item) => item.key)}
      expandIconPlacement="start"
      expandIcon={({ isActive }) => <DownIcon style={{ transform: isActive ? undefined : 'rotate(-90deg)' }} />}
      classNames={{ header: 'xflow-pv-sec-head', body: 'xflow-pv-sec-body p-0' }}
      items={items.map((item) => ({
        key: item.key,
        label: item.label,
        children: <div className="xflow-pv-sec-fields">{item.children}</div>,
        /* 基础信息卡不可折叠（§2.2）：只留图标作开关再把它藏掉，卡头就没有入口了。 */
        ...(item.fixed ? { className: 'xflow-pv-sec-fixed', collapsible: 'icon' as const, showArrow: false } : null),
      }))}
    />
  );
}

/** One port and where it goes; `error` is the unconnected outlet (§6.6). */
function PortRow({
  row,
  dir,
  icon,
  action,
  align = 'end',
}: {
  row: PortRowSpec;
  dir: 'in' | 'out';
  /** Category block in front of the target name. */
  icon?: ReactNode;
  /** Trailing control (the entry row's locate button). */
  action?: ReactNode;
  /** Where the target sits: the entry row keeps it next to the port. */
  align?: 'start' | 'end';
}) {
  return (
    <div className="xflow-pv-port-row" data-tone={row.tone} data-align={align}>
      <span className="xflow-pv-port">{row.port}</span>
      <span className="xflow-pv-port-to">
        {row.tone === 'danger' ? '未连接 · ' : dir === 'in' ? '← ' : '→ '}
        {row.link ? (
          <Button type="link" size="small" className="xflow-pv-port-link">
            {row.link}
          </Button>
        ) : (
          <>
            {icon}
            {row.to}
          </>
        )}
      </span>
      {action ? <span className="xflow-pv-port-action">{action}</span> : null}
    </div>
  );
}

function Rule({ rule }: { rule: RuleSpec }) {
  return (
    <div className="xflow-pv-rule">
      <div className="xflow-pv-rule-head">
        {rule.title}
        <span className="xflow-pv-port">{rule.port}</span>
      </div>
      {rule.expr ? <span className="xflow-pv-expr-box xflow-pv-rule-expr">{rule.expr}</span> : null}
      {rule.note ? <span className="xflow-pv-hint">{rule.note}</span> : null}
    </div>
  );
}

/** The panel shell: title bar, node head, tabs, scrolling body, footer. */
function InspPanel({
  title = '属性',
  titleCount,
  head,
  activeTab,
  connected = inspConnectedCount,
  runFailed,
  body,
  foot,
}: {
  title?: string;
  titleCount?: ReactNode;
  head?: ReactNode;
  activeTab?: 'config' | 'connections' | 'runs';
  /** Linked edges in + out (user ruling 2026-10-17); 0 shows no count. */
  connected?: number;
  /** The node's latest run failed: the 运行 tab carries a danger dot. */
  runFailed?: boolean;
  body: ReactNode;
  foot?: ReactNode;
}) {
  const placeholder = (name: string) => <p className="xflow-pv-tab-note">{name} tab 见本节的另一个面板。</p>;
  const tab = (key: NonNullable<typeof activeTab>, name: string) =>
    activeTab === key ? body : placeholder(name);
  return (
    <aside className="xflow-pv-insp" aria-label={title}>
      <PanelTitle
        icon={<SlidersIcon />}
        title={title}
        count={titleCount}
        className="xflow-pv-panel-title--bar"
      />
      {head}
      {activeTab ? (
        <Tabs
          size="small"
          className="xflow-pv-insp-tabs"
          defaultActiveKey={activeTab}
          // 三个 tab（§2.1，2026-11-04 用户裁定）：设置并入配置页的执行策略卡。
          tabBarExtraContent={<IconButton small label="以 JSON 编辑" icon={<CodeIcon />} />}
          classNames={{
            header: 'xflow-pv-insp-tabs-head',
            body: 'xflow-pv-insp-tabs-body',
            content: 'xflow-pv-insp-tabs-content',
          }}
          items={[
            { key: 'config', label: '配置', children: tab('config', '配置') },
            {
              key: 'connections',
              label: (
                <span data-tab-count={connected || undefined}>
                  连接{connected ? <span className="xflow-pv-faint"> {connected}</span> : null}
                </span>
              ),
              children: tab('connections', '连接'),
            },
            {
              key: 'runs',
              label: (
                <span className="xflow-pv-tab-label" data-run-failed={runFailed ? '' : undefined}>
                  运行
                  {runFailed ? <span className="xflow-pv-tab-dot" role="img" aria-label="最近一次运行失败" /> : null}
                </span>
              ),
              children: tab('runs', '运行'),
            },
          ]}
        />
      ) : (
        <div className="xflow-pv-insp-tabs-body xflow-pv-insp-tabs-content">{body}</div>
      )}
      {foot ? <div className="xflow-pv-insp-foot">{foot}</div> : null}
    </aside>
  );
}

/**
 * Panel head. The status / error chip sits at the end of the title row, and the
 * type id takes the row below it: sharing one line put the name, the chip and
 * two icon buttons in a 360px panel and left the type — the one string a reader
 * needs in full — as 「xflow.h…」.
 */
function NodeHead({
  label,
  sub,
  category,
  icon,
  chip,
  renaming,
}: {
  label: string;
  sub: string;
  category: Category;
  icon: IconKey;
  chip?: ReactNode;
  /** Inline rename (user ruling 2026-10-17): double-click the name; Enter commits, Esc cancels. */
  renaming?: boolean;
}) {
  const copy = (
    <Tooltip title="复制节点">
      <Button type="text" size="small" className="xflow-pv-tool" icon={<CopyIcon />} aria-label="复制节点" />
    </Tooltip>
  );
  const more = (
    <Tooltip title="更多操作">
      <Button type="text" size="small" className="xflow-pv-tool" icon={<MoreIcon />} aria-label="更多操作" />
    </Tooltip>
  );
  return (
    <div className="xflow-pv-insp-head">
      <CategoryIcon category={category} icon={icon} />
      {renaming ? (
        <Input
          size="small"
          readOnly
          value={label}
          aria-label="节点显示名"
          className="xflow-pv-insp-rename"
          data-renaming=""
        />
      ) : (
        <Tooltip title="双击改名">
          <span className="xflow-pv-insp-node" title={label}>
            {label}
          </span>
        </Tooltip>
      )}
      {chip}
      {copy}
      {more}
      <div className="xflow-pv-insp-type" title={sub}>
        {sub}
      </div>
    </div>
  );
}

const fields = (specs: FieldSpec[]) => specs.map((field) => <SpecField key={field.label} field={field} />);

const switchHead = (
  <NodeHead
    label={inspSwitch.label}
    sub={`${inspSwitch.nodeName} · ${inspSwitch.type} · v1`}
    category={inspSwitch.category}
    icon={inspSwitch.icon}
  />
);

/**
 * 1b · the inspector as a whole: the three tabs, the empty state, and the
 * failing-field state.
 */
export function InspectorSection() {
  return (
    <div className="xflow-pv-gallery">
      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="config"
          head={switchHead}
          body={
            <Sections
              items={[
                {
                  key: 'base',
                  fixed: true,
                  label: <SectionLabel title="基础信息" />,
                  children: fields(inspSwitch.base),
                },
                {
                  key: 'route',
                  label: (
                    <SectionLabel title="路由">
                      <StatusChip tone="success" icon={<CheckIcon />} label="有效" />
                    </SectionLabel>
                  ),
                  children: (
                    <>
                      {fields(inspSwitch.route)}
                      {inspSwitch.rules.map((rule) => (
                        <Rule key={rule.id} rule={rule} />
                      ))}
                      <Button size="small" icon={<PlusIcon />} className="xflow-pv-add-row">
                        新增规则
                      </Button>
                    </>
                  ),
                },
                {
                  key: 'execution',
                  defaultActive: false,
                  label: <SectionLabel title="执行策略" extra="继承工作流 · 重试 2 次" />,
                  children: fields(inspSettings[0].fields),
                },
                {
                  key: 'notes',
                  defaultActive: false,
                  label: <SectionLabel title="备注" extra="无" />,
                  children: <Input.TextArea rows={2} placeholder="给同事看的说明，不参与运行" />,
                },
              ]}
            />
          }
          foot={
            <Todo label="宿主未接 onRunNode">
              <Button block icon={<CheckIcon />}>
                测试此节点
              </Button>
            </Todo>
          }
        />
        <figcaption>
          配置 tab：基础信息（不可折叠）+ 路由（端口只在路由卡里出现一次），执行策略与备注折叠；宿主没接 onRunNode 时底部整条不渲染
        </figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="config"
          head={switchHead}
          body={
            <Sections
              items={[
                ...inspSettings.map((section) => ({
                  key: section.key,
                  label: <SectionLabel title={section.title} />,
                  children: fields(section.fields),
                })),
                {
                  key: 'notes',
                  defaultActive: false,
                  label: <SectionLabel title="备注" extra="无" />,
                  children: <Input.TextArea rows={2} placeholder="给同事看的说明，不参与运行" />,
                },
              ]}
            />
          }
        />
        <figcaption>
          配置 tab 底部：执行策略展开态（错误处理 / 节点执行超时 / 重试 / Runner 选择 / 禁用），备注默认折叠
        </figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="connections"
          head={switchHead}
          body={
            <Sections
              items={[
                {
                  key: 'in',
                  label: <SectionLabel title="输入" />,
                  children: inspConnections.inputs.map((row) => (
                    <PortRow
                      key={row.id}
                      row={row}
                      dir="in"
                      icon={<CategoryIcon category="action" icon="globe" size="sm" />}
                    />
                  )),
                },
                {
                  key: 'out',
                  label: <SectionLabel title="输出端口" />,
                  children: inspConnections.outputs.map((row) => <PortRow key={row.id} row={row} dir="out" />),
                },
                {
                  key: 'policy',
                  label: <SectionLabel title="错误策略" />,
                  children: fields(inspConnections.policy),
                },
              ]}
            />
          }
        />
        <figcaption>连接 tab：输入来源 + 每个输出端口的去向，error 出口未连接时给修复入口</figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="runs"
          head={
            <NodeHead
              label="拉取客户档案"
              sub="fetch_profile · xflow.http · v1"
              category="action"
              icon="globe"
              chip={<StatusChip tone="success" icon={<CheckIcon />} label="成功" />}
            />
          }
          body={
            <>
              <dl className="xflow-pv-kvlist">
                {inspRuns.kv.map((row) => (
                  <Fragment key={row.label}>
                    <dt>{row.label}</dt>
                    <dd className={row.mono ? 'xflow-pv-mono' : undefined}>
                      {row.port ? <span className="xflow-pv-port">{row.value}</span> : row.value}
                    </dd>
                  </Fragment>
                ))}
              </dl>
              <Sections
                items={[
                  {
                    key: 'in',
                    label: <SectionLabel title="输入" />,
                    children: <pre className="xflow-pv-code">{inspRuns.input}</pre>,
                  },
                  {
                    key: 'out',
                    label: (
                      <SectionLabel title="输出">
                        <Tooltip title="复制输出">
                          <Button
                            type="text"
                            size="small"
                            className="xflow-pv-tool"
                            icon={<CopyIcon />}
                            aria-label="复制输出"
                          />
                        </Tooltip>
                      </SectionLabel>
                    ),
                    children: <pre className="xflow-pv-code">{inspRuns.output}</pre>,
                  },
                ]}
              />
            </>
          }
          foot={
            <>
              <Button block>固定为测试数据</Button>
              <Todo label="宿主未接 onRunNode">
                <Button block icon={<CheckIcon />}>
                  重新运行
                </Button>
              </Todo>
            </>
          }
        />
        <figcaption>运行 tab：该节点最近一次的输入、输出、尝试与耗时（只读回放）</figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="config"
          runFailed
          head={
            <NodeHead
              label="通知负责人"
              sub="notify_owner · xflow.notification · v1"
              category="action"
              icon="bell"
              renaming
              chip={<StatusChip tone="danger" icon={<WarnIcon />} label="失败" />}
            />
          }
          body={<Sections items={[{ key: 'base', label: <SectionLabel title="基础信息" />, children: fields(inspSwitch.base.slice(0, 1)) }]} />}
        />
        <figcaption>失败 + 改名态：最近一次运行失败时「运行」tab 带红点；双击节点名进入内联改名，Enter 提交、Esc 取消</figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="config"
          head={
            <NodeHead
              label={inspSwitch.label}
              sub={`${inspSwitch.nodeName} · ${inspSwitch.type} · v1`}
              category={inspSwitch.category}
              icon={inspSwitch.icon}
              chip={<StatusChip tone="neutral" icon={<OutlineIcon />} label="已禁用" />}
            />
          }
          body={
            <Sections
              items={[
                {
                  key: 'base',
                  fixed: true,
                  label: <SectionLabel title="基础信息" />,
                  children: fields(inspSwitch.base),
                },
              ]}
            />
          }
        />
        <figcaption>禁用态：名字旁显示 neutral「已禁用」；禁用开关在配置页的执行策略卡里</figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          title="工作流设置"
          titleCount="未选中节点"
          body={
            <Sections
              items={[
                {
                  key: 'basic',
                  label: <SectionLabel title="基本" />,
                  children: <div className="xflow-pv-field-grid">{fields(wfSettings.basic)}</div>,
                },
                {
                  key: 'entry',
                  label: <SectionLabel title="入口" extra="1" />,
                  children: (
                    <PortRow
                      row={wfSettings.entry[0]}
                      dir="in"
                      align="start"
                      icon={<CategoryIcon category="trigger" icon="link" size="sm" />}
                      action={
                        <Button size="small" icon={<SearchIcon />}>
                          定位
                        </Button>
                      }
                    />
                  ),
                },
                {
                  key: 'concurrency',
                  label: <SectionLabel title="并发" />,
                  children: <div className="xflow-pv-field-grid">{fields(wfSettings.concurrency)}</div>,
                },
                /*
                 * The last two sections open collapsed, with the number of
                 * fields they hold: the panel is 640px and five expanded
                 * sections are taller than that, which left 错误与重试 and 高级
                 * below the fold — the reader could not see that the panel has
                 * five sections at all.
                 */
                {
                  key: 'retry',
                  defaultActive: false,
                  label: (
                    <SectionLabel title="错误与重试" extra="节点默认继承">
                      <StatusChip tone="neutral" icon={<OutlineIcon />} label={`${wfSettings.retry.length} 项`} />
                    </SectionLabel>
                  ),
                  children: <div className="xflow-pv-field-grid">{fields(wfSettings.retry)}</div>,
                },
                {
                  key: 'advanced',
                  defaultActive: false,
                  label: (
                    <SectionLabel title="高级" extra="调度 · 实验">
                      <StatusChip tone="neutral" icon={<OutlineIcon />} label={`${wfSettings.advanced.length} 项`} />
                    </SectionLabel>
                  ),
                  children: <div className="xflow-pv-field-grid">{fields(wfSettings.advanced)}</div>,
                },
              ]}
            />
          }
        />
        <figcaption>
          未选中节点：面板变成工作流设置（基本 / 入口 / 并发 / 错误与重试 / 高级），五节在一屏内看全，后两节折叠并带字段数
        </figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <InspPanel
          activeTab="config"
          head={
            <NodeHead
              label={inspInvalid.label}
              sub={`${inspInvalid.nodeName} · ${inspInvalid.type} · v1`}
              category={inspInvalid.category}
              icon={inspInvalid.icon}
              chip={<StatusChip tone="danger" icon={<WarnIcon />} label={`${inspInvalid.badge} 个错误`} />}
            />
          }
          body={
            <Sections
              items={inspInvalid.sections.map((section) => ({
                key: section.key,
                label: (
                  <SectionLabel title={section.title} extra={section.extra}>
                    {section.errors ? (
                      <StatusChip tone="danger" icon={<WarnIcon />} label={`${section.errors} 个错误`} />
                    ) : null}
                  </SectionLabel>
                ),
                children: fields(section.fields),
              }))}
            />
          }
          foot={
            <Todo label="宿主未接 onRunNode">
              <Button block disabled>
                测试此节点
              </Button>
            </Todo>
          }
        />
        <figcaption>
          校验错误态：必填缺失用 danger（控件边框 + 红字），环境缺省用 warning；两种严重度都走同一条消息样式
        </figcaption>
      </figure>
    </div>
  );
}
