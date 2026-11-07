import * as React from "react";
import { AuditIcon, BranchIcon, CopyIcon, DownIcon, GearIcon, LinkIcon, PinIcon, SlidersIcon } from "@xflow/icons";
import { Button, Input, InputNumber, Segmented, Select, Switch, Tooltip } from "antd";
import type {
  ConcurrencyMode,
  ConcurrencySettings,
  ErrorPolicy,
  RunnerSelector,
  RunnerSelectorMode,
  RetryPolicy,
  RetryStrategy,
  WorkflowDef,
  WorkflowNode,
  WorkflowOptions,
  WorkflowSettings as WorkflowExecutionSettings
} from "@xflow/core";
import {
  SectionCard,
  WorkflowJsonTextArea,
  editorInputClassNames,
  editorSelectClassNames,
  editorTooltipClassNames,
  type WorkflowJsonDrafts,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./shared";
import { AddRowButton, DeleteRowButton, KeyInput, LiteralInput, SourceToggle, renameKey, withoutKey } from "./variables";
import { NodeCategoryIcon } from "./categoryIcon";
import { nodeKey, nodeLabel } from "./workflowOps";

/**
 * What the Inspector shows with no node selected: the workflow's own
 * metadata and execution settings, laid out in the §6.4 order — 基本 / 入口 /
 * 并发 / 错误与重试 / 高级. 入口 lists the start and trigger nodes and locates
 * them on the canvas through the Inspector's `onLocateNode`; without that
 * callback the rows stay readable but draw no 定位 key.
 *
 * The domains that already edited raw JSON keep their source toggle: the
 * Runner selector's moved with the whole Runner block into 高级, and the
 * execution settings' sits beside it. A draft that does not parse forces the
 * advanced section open and keeps its source on screen, so the text blocking
 * save is always visible. 高级 is collapsed by default; that open state is
 * component-local and never written into the workflow. Pin Data stays
 * source-only, because it maps arbitrary node names to arbitrary pinned
 * values.
 */

const runnerModes: { value: RunnerSelectorMode; label: string }[] = [
  { value: "default", label: "任意可用 Runner" },
  { value: "required", label: "必须匹配标签" }
];

const errorPolicies: { value: ErrorPolicy; label: string }[] = [
  { value: "stop", label: "失败即停止" },
  { value: "error_output", label: "错误输出端口" },
  { value: "main_output", label: "主输出端口" },
  { value: "continue", label: "继续执行" }
];

const pinDataModes: { value: NonNullable<WorkflowExecutionSettings["pin_data_mode"]>; label: string }[] = [
  { value: "test_only", label: "仅测试运行" },
  { value: "always", label: "总是使用" },
  { value: "disabled", label: "关闭" }
];

const retryStrategies: { value: RetryStrategy; label: string }[] = [
  { value: "fixed", label: "固定间隔" },
  { value: "exponential", label: "指数退避" }
];

const concurrencyModes: { value: ConcurrencyMode; label: string }[] = [
  { value: "queue", label: "排队" },
  { value: "reject", label: "拒绝" }
];

const concurrencyModeClassNames = {
  item: "xflow-editor-concurrency-mode__item",
  label: "xflow-editor-concurrency-mode__label",
  root: "xflow-editor-concurrency-mode__root"
};

const NANOS_PER_MINUTE = 60_000_000_000;
const NANOS_PER_SECOND = 1_000_000_000;
const NANOS_PER_HOUR = 3_600_000_000_000;

/**
 * 标识 in the 基本 head (§6.4, 2026-11-07): the full server id, shortened from
 * the middle when the head cannot hold it. The title keeps the whole value.
 */
function middleEllipsis(id: string, max = 28): string {
  if (id.length <= max) return id;
  const head = Math.ceil((max - 1) / 2);
  return `${id.slice(0, head)}…${id.slice(-(max - 1 - head))}`;
}

/** How the settings rows hold `settings.concurrency`; the wire form is rebuilt on every write. */
interface ConcurrencyDraft {
  limit?: number;
  mode: ConcurrencyMode;
  maxQueued?: number;
  queueTimeoutNs?: number;
}

/**
 * 入口: the start node and every trigger node. The DSL spells the start node
 * `xflow.start`, triggers `xflow.trigger.*`, and a compiled definition may
 * also carry the `trigger` kind; either marker makes a node an entry.
 */
function isEntryNode(node: WorkflowNode): boolean {
  return (
    node.type === "xflow.start" ||
    Boolean(node.type?.startsWith("xflow.trigger.")) ||
    node.kind === "trigger"
  );
}

interface EntryRow {
  key: string;
  label: string;
  type: string;
}

/** 0 and absent both mean "no limit" for every concurrency number. */
function positiveInteger(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) && Number.isInteger(value) && value > 0 ? value : undefined;
}

/** Accepts both wire forms: a bare int is the queue-mode shorthand. */
function readConcurrency(value: number | ConcurrencySettings | undefined): ConcurrencyDraft {
  if (typeof value === "number") return { limit: positiveInteger(value), mode: "queue" };
  return {
    limit: positiveInteger(value?.limit),
    mode: value?.mode === "reject" ? "reject" : "queue",
    maxQueued: positiveInteger(value?.max_queued),
    queueTimeoutNs: positiveInteger(value?.queue_timeout)
  };
}

/**
 * Rebuilds the wire form. The bare int stays the spelling while it can express
 * the setting — only a limit, in the default queue mode, with no queue
 * controls — so a shorthand definition keeps its hash. Once the mode is
 * reject, the queue controls are dropped: the server rejects that combination
 * at compile time.
 */
function concurrencyWireValue(draft: ConcurrencyDraft): number | ConcurrencySettings | undefined {
  const limit = positiveInteger(draft.limit);
  const queueMode = draft.mode !== "reject";
  const maxQueued = queueMode ? positiveInteger(draft.maxQueued) : undefined;
  const queueTimeout = queueMode ? positiveInteger(draft.queueTimeoutNs) : undefined;
  if (queueMode && maxQueued === undefined && queueTimeout === undefined) return limit;
  const settings: ConcurrencySettings = { mode: draft.mode };
  if (limit !== undefined) settings.limit = limit;
  if (maxQueued !== undefined) settings.max_queued = maxQueued;
  if (queueTimeout !== undefined) settings.queue_timeout = queueTimeout;
  return settings;
}

/** A numeric settings row; 0 and blank both commit "no value". */
function NumberField({
  ariaLabel,
  className,
  integer = true,
  placeholder,
  readOnly,
  unit,
  value,
  onChange
}: {
  ariaLabel: string;
  className: string;
  integer?: boolean;
  placeholder: string;
  readOnly: boolean;
  /** Unit shown to the right of the control (§6.4 D5), e.g. 个执行 / 分钟 / 次. */
  unit?: string;
  value: number | undefined;
  onChange: (next: number | undefined) => void;
}): React.ReactElement {
  return (
    <span className="xflow-editor-number-field">
      <InputNumber<number>
        aria-label={ariaLabel}
        className={`${className} xflow-editor-number-field__control`}
        min={0}
        placeholder={placeholder}
        precision={integer ? 0 : undefined}
        readOnly={readOnly}
        value={value}
        onChange={(next) => onChange(next ?? undefined)}
      />
      {unit ? <span className="xflow-editor-number-field__unit">{unit}</span> : null}
    </span>
  );
}

/** settings.timeout is Go `time.Duration`: integer nanoseconds. */
type TimeoutUnit = "s" | "m" | "h";

const timeoutUnits: { value: TimeoutUnit; label: string; nanos: number }[] = [
  { value: "s", label: "秒", nanos: NANOS_PER_SECOND },
  { value: "m", label: "分", nanos: NANOS_PER_MINUTE },
  { value: "h", label: "时", nanos: NANOS_PER_HOUR }
];

/** The largest unit that divides the stored duration exactly, default 分. */
function timeoutUnitFor(nanos: number | undefined): TimeoutUnit {
  if (nanos === undefined || nanos === 0) return "m";
  if (nanos % NANOS_PER_HOUR === 0) return "h";
  if (nanos % NANOS_PER_MINUTE === 0) return "m";
  return "s";
}

/**
 * §6.4 (2026-11-07): a number plus a 秒 / 分 / 时 unit, still stored as integer
 * nanoseconds; blank reads as 不限. Switching the unit never rewrites the
 * stored value — it only changes how it reads.
 */
function TimeoutField({
  readOnly,
  value,
  onChange
}: {
  readOnly: boolean;
  value: number | undefined;
  onChange: (next: number | undefined) => void;
}): React.ReactElement {
  const [unit, setUnit] = React.useState<TimeoutUnit>(() => timeoutUnitFor(value));
  const divisor = timeoutUnits.find((item) => item.value === unit)?.nanos ?? NANOS_PER_MINUTE;
  // The unit follows an outside write (the JSON source, a new workflow), but
  // never the user's own keystrokes: picking 秒 and typing 120 must not snap
  // back to 分 mid-edit.
  const lastCommitted = React.useRef(value);
  React.useEffect(() => {
    if (lastCommitted.current === value) return;
    lastCommitted.current = value;
    setUnit(timeoutUnitFor(value));
  }, [value]);
  const commit = (next: number | undefined) => {
    lastCommitted.current = next;
    onChange(next);
  };
  return (
    <span className="xflow-editor-number-field xflow-editor-timeout-field">
      <InputNumber<number>
        aria-label="执行超时"
        className="xflow-editor-timeout-field__control"
        min={0}
        placeholder="不限"
        readOnly={readOnly}
        value={value === undefined ? undefined : value / divisor}
        onChange={(next) => commit(next === undefined || next === null ? undefined : Math.round(next * divisor))}
      />
      <Select<TimeoutUnit>
        aria-label="执行超时单位"
        className="xflow-editor-timeout-field__unit"
        classNames={editorSelectClassNames}
        disabled={readOnly}
        options={timeoutUnits.map(({ value: unitValue, label }) => ({ value: unitValue, label }))}
        value={unit}
        onChange={(next) => setUnit(next)}
      />
    </span>
  );
}

/** Writes a top-level object field, dropping it when empty so the DSL stays clean. */
function withObjectField<K extends "runner_selector" | "settings" | "options">(
  workflow: WorkflowDef,
  field: K,
  next: NonNullable<WorkflowDef[K]>
): WorkflowDef {
  if (Object.keys(next).length > 0) return { ...workflow, [field]: next };
  const { [field]: _removed, ...rest } = workflow;
  return rest as WorkflowDef;
}

/** The same rule one level down: a match-label map with no labels is dropped. */
function withMatchLabels(runner: RunnerSelector, labels: Record<string, string>): RunnerSelector {
  if (Object.keys(labels).length > 0) return { ...runner, match_labels: labels };
  const { match_labels: _removed, ...rest } = runner;
  return rest;
}

/** One advanced option, deleted rather than written when the control is cleared. */
function withOption<K extends keyof WorkflowOptions>(
  options: WorkflowOptions,
  key: K,
  value: WorkflowOptions[K] | undefined
): WorkflowOptions {
  const next = { ...options };
  if (value === undefined) delete next[key];
  else next[key] = value;
  return next;
}

function SelectField<T extends string>({
  ariaLabel,
  allowClear = true,
  options,
  placeholder,
  readOnly,
  value,
  onChange
}: {
  ariaLabel: string;
  allowClear?: boolean;
  options: { value: T; label: string }[];
  placeholder: string;
  readOnly: boolean;
  value: T | undefined;
  onChange: (next: T | undefined) => void;
}): React.ReactElement {
  return (
    <Select<T>
      allowClear={allowClear}
      aria-label={ariaLabel}
      className="xflow-editor-field"
      classNames={editorSelectClassNames}
      disabled={readOnly}
      options={options}
      placeholder={placeholder}
      value={value}
      // ED-18: the option text is the Chinese meaning only; the wire enum stays
      // visible as secondary mono text (and as the option's title/tooltip).
      optionRender={(option) => {
        const raw = String(option.data.value ?? "");
        return (
          <span className="xflow-editor-select__option" title={raw}>
            <span>{option.data.label}</span>
            <code className="xflow-editor-select__option-value">{raw}</code>
          </span>
        );
      }}
      onChange={(next) => onChange(next ?? undefined)}
    />
  );
}

/**
 * ED-18: one-line explanation plus a "?" help key holding the long form. The
 * key is a real button, so it is reachable and its tooltip opens on focus.
 */
function Hint({ help, text }: { help: string; text: string }): React.ReactElement {
  return (
    <p className="xflow-editor-variable-hint xflow-editor-hint">
      <span>{text}</span>
      <HelpKey help={help} label={text} />
    </p>
  );
}

/** The label-side "?" (§6.4 D-3): the explanation lives in its tooltip. */
function HelpKey({ help, label }: { help: string; label: string }): React.ReactElement {
  return (
    <Tooltip classNames={editorTooltipClassNames} title={help}>
      <button aria-label={`帮助：${label}`} className="xflow-editor-hint__help" type="button">
        ?
      </button>
    </Tooltip>
  );
}

/** One labelled row of the settings cards. `top` puts the label above a
 * long control (the baseline's full-row field, §6.4 D5). */
function SettingRow({
  label,
  help,
  children,
  top = false
}: {
  label: string;
  /** A label-side "?" whose tooltip holds the explanation (D-3). */
  help?: React.ReactNode;
  children: React.ReactNode;
  top?: boolean;
}): React.ReactElement {
  return (
    <label
      className={`xflow-editor-form-row xflow-editor-form-row--workflow${top ? " xflow-editor-form-row--top" : ""}`}
    >
      <span className="xflow-editor-form-row__label">
        {label}
        {help}
      </span>
      {children}
    </label>
  );
}

/**
 * 高级 is a section card whose body is only rendered while open. SectionCard
 * has no collapsed state and shared.tsx is outside this batch, so this keeps
 * the same markup and class hooks with local state; the open state is never
 * written into the workflow.
 */
function CollapsibleCard({
  actions,
  ariaLabel,
  expanded,
  icon,
  meta,
  onToggle,
  title,
  children
}: {
  actions?: React.ReactNode;
  ariaLabel: string;
  expanded: boolean;
  icon: React.ReactNode;
  meta?: React.ReactNode;
  onToggle: () => void;
  title: React.ReactNode;
  children: React.ReactNode;
}): React.ReactElement {
  const bodyId = React.useId();
  return (
    <div aria-label={ariaLabel} className="xflow-editor-section-card" role="group">
      {/* §6.4 (2026-11-07): the whole head toggles; the chevron keeps the
       * keyboard path, and the actions stop the click from folding the card. */}
      <header className="xflow-editor-section-card__header" data-collapsible="true" onClick={onToggle}>
        <span className="xflow-editor-section-card__title">
          {icon}
          <strong>{title}</strong>
        </span>
        <span
          className="xflow-editor-section-card__trailing"
          onClick={(event) => event.stopPropagation()}
        >
          {meta !== undefined ? <span className="xflow-editor-section-card__meta">{meta}</span> : null}
          <span className="xflow-editor-section-card__actions">
            {actions}
            <Button
              aria-controls={expanded ? bodyId : undefined}
              aria-expanded={expanded}
              aria-label={expanded ? `收起${ariaLabel}` : `展开${ariaLabel}`}
              className="xflow-editor-advanced-toggle"
              icon={
                <DownIcon
                  className="xflow-editor-advanced-toggle__chevron"
                  style={{ transform: expanded ? undefined : "rotate(-90deg)" }}
                />
              }
              size="small"
              type="text"
              onClick={(event) => {
                event.stopPropagation();
                onToggle();
              }}
            />
          </span>
        </span>
      </header>
      {expanded ? (
        <div className="xflow-editor-section-card__body" id={bodyId}>
          {children}
        </div>
      ) : null}
    </div>
  );
}

export function WorkflowSettings({
  workflow,
  onChangeWith,
  workflowJsonDrafts,
  onWorkflowJsonText,
  onLocateNode,
  readOnly = false
}: {
  workflow: WorkflowDef;
  onChangeWith?: (update: WorkflowUpdater) => void;
  workflowJsonDrafts?: WorkflowJsonDrafts;
  onWorkflowJsonText?: (field: WorkflowJsonField, text: string) => void;
  /** Selects a node by its canvas key and centres it (the 入口 rows' 定位). */
  onLocateNode?: (key: string) => void;
  readOnly?: boolean;
}): React.ReactElement {
  const brokenRunner = Boolean(workflowJsonDrafts?.runner_selector?.error);
  const brokenSettings = Boolean(workflowJsonDrafts?.settings?.error);
  // A broken draft opens its source view, so the text that blocks save is on screen.
  const [runnerSource, setRunnerSource] = React.useState(brokenRunner);
  const [settingsSource, setSettingsSource] = React.useState(brokenSettings);
  const [advancedOpen, setAdvancedOpen] = React.useState(false);
  // A broken draft also forces 高级 open: both source editors live there.
  const advancedExpanded = advancedOpen || brokenRunner || brokenSettings;

  React.useEffect(() => {
    if (brokenRunner) setRunnerSource(true);
  }, [brokenRunner]);
  React.useEffect(() => {
    if (brokenSettings) setSettingsSource(true);
  }, [brokenSettings]);

  // The replay's identity fields are read-only: `readOnly` stops real keystrokes,
  // and this guard keeps a change event from writing the snapshot into the draft.
  const updateMeta = (patch: Partial<WorkflowDef>) => {
    if (readOnly) return;
    onChangeWith?.((latest) => ({ ...latest, ...patch }));
  };
  const writeRunner = (update: (current: RunnerSelector) => RunnerSelector) =>
    onChangeWith?.((latest) => withObjectField(latest, "runner_selector", update(latest.runner_selector ?? {})));
  const writeSettings = (update: (current: WorkflowExecutionSettings) => WorkflowExecutionSettings) =>
    onChangeWith?.((latest) => withObjectField(latest, "settings", update(latest.settings ?? {})));
  const writeOptions = (update: (current: WorkflowOptions) => WorkflowOptions) =>
    onChangeWith?.((latest) => withObjectField(latest, "options", update(latest.options ?? {})));

  const runner = workflow.runner_selector ?? {};
  const matchLabels = runner.match_labels ?? {};
  const labelKeys = Object.keys(matchLabels);
  const runnerReadOnly = readOnly || brokenRunner;

  const settings = workflow.settings ?? {};
  const retry = settings.retry ?? {};
  const concurrency = readConcurrency(settings.concurrency);
  const settingsReadOnly = readOnly || brokenSettings;
  const options = workflow.options ?? {};
  const writeRetry = (patch: Partial<RetryPolicy>) =>
    writeSettings((current) => {
      const nextRetry: RetryPolicy = { ...(current.retry ?? {}), ...patch };
      return { ...current, retry: nextRetry };
    });
  const writeConcurrency = (patch: Partial<ConcurrencyDraft>) =>
    writeSettings((current) => {
      const next = concurrencyWireValue({ ...readConcurrency(current.concurrency), ...patch });
      const { concurrency: _previous, ...rest } = current;
      return next === undefined ? rest : { ...rest, concurrency: next };
    });
  const toggleRunnerSource = () => {
    setRunnerSource((on) => !on);
    setAdvancedOpen(true);
  };
  const toggleSettingsSource = () => {
    setSettingsSource((on) => !on);
    setAdvancedOpen(true);
  };

  const entryNodes: EntryRow[] = (workflow.nodes ?? []).flatMap((node, index) =>
    isEntryNode(node)
      ? [{ key: nodeKey(node, index), label: nodeLabel(node, index), type: node.type ?? "" }]
      : []
  );

  return (
    <div className="xflow-editor-workflow-settings">
      <SectionCard
        actions={
          workflow.id ? (
            <Tooltip classNames={editorTooltipClassNames} title="复制工作流标识">
              <Button
                aria-label="复制工作流标识"
                className="xflow-editor-workflow-id__copy"
                icon={<CopyIcon />}
                size="small"
                type="text"
                onClick={() => {
                  void navigator.clipboard?.writeText(workflow.id ?? "").catch(() => undefined);
                }}
              />
            </Tooltip>
          ) : undefined
        }
        ariaLabel="基本"
        icon={<AuditIcon />}
        meta={
          workflow.id ? (
            <span className="xflow-editor-workflow-id" title={workflow.id}>
              {middleEllipsis(workflow.id)}
            </span>
          ) : (
            <span className="xflow-editor-workflow-id xflow-editor-workflow-id--empty">未保存</span>
          )
        }
        title="基本"
      >
        <SettingRow label="名称">
          <Input
            aria-label="工作流名称"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            readOnly={readOnly}
            value={workflow.name ?? ""}
            onChange={(event) => updateMeta({ name: event.target.value })}
          />
        </SettingRow>
        <SettingRow label="版本">
          <Input
            aria-label="工作流版本"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            readOnly={readOnly}
            value={workflow.version ?? ""}
            onChange={(event) => updateMeta({ version: event.target.value })}
          />
        </SettingRow>
        <SettingRow label="时区">
          <LiteralInput
            ariaLabel="执行时区"
            mode="string"
            placeholder="例如 Asia/Shanghai"
            readOnly={settingsReadOnly}
            value={settings.timezone}
            onCommit={(next) =>
              writeSettings((current) => {
                const nextSettings = { ...current };
                if (next === undefined || next === "") delete nextSettings.timezone;
                else nextSettings.timezone = String(next);
                return nextSettings;
              })
            }
          />
        </SettingRow>
        <SettingRow
          help={
            <HelpKey
              help="整个执行的预算，含排队与挂起等待；留空为不限。到期后执行以超时终态结束，不走错误策略。"
              label="执行超时"
            />
          }
          label="执行超时"
        >
          <TimeoutField
            readOnly={settingsReadOnly}
            value={settings.timeout}
            onChange={(next) =>
              writeSettings((current) => {
                const nextSettings = { ...current };
                if (next === undefined) delete nextSettings.timeout;
                else nextSettings.timeout = next;
                return nextSettings;
              })
            }
          />
        </SettingRow>
        <SettingRow label="描述" top>
          <Input.TextArea
            aria-label="工作流描述"
            className="xflow-editor-textarea"
            readOnly={readOnly}
            rows={2}
            value={workflow.description ?? ""}
            onChange={(event) => updateMeta({ description: event.target.value })}
          />
        </SettingRow>
      </SectionCard>
      <SectionCard
        ariaLabel="触发器"
        icon={<LinkIcon />}
        meta={entryNodes.length > 0 ? entryNodes.length : undefined}
        title="触发器"
      >
        {entryNodes.length === 0 ? (
          <p className="xflow-editor-variable-empty">该工作流没有入口节点。</p>
        ) : (
          <ul className="xflow-editor-variable-list xflow-editor-trigger-list">
            {entryNodes.map((entry) =>
              onLocateNode ? (
                <li key={entry.key}>
                  <button
                    className="xflow-editor-trigger-row"
                    title={`定位到画布：${entry.label}`}
                    type="button"
                    onClick={() => onLocateNode(entry.key)}
                  >
                    <NodeCategoryIcon size="sm" type={entry.type || undefined} />
                    <span className="xflow-editor-trigger-row__label">{entry.label}</span>
                    <code className="xflow-editor-trigger-row__type">{entry.type || "—"}</code>
                  </button>
                </li>
              ) : (
                <li className="xflow-editor-trigger-row" key={entry.key}>
                  <NodeCategoryIcon size="sm" type={entry.type || undefined} />
                  <span className="xflow-editor-trigger-row__label">{entry.label}</span>
                  <code className="xflow-editor-trigger-row__type">{entry.type || "—"}</code>
                </li>
              )
            )}
          </ul>
        )}
      </SectionCard>
      <SectionCard ariaLabel="并发" icon={<SlidersIcon />} title="并发">
        <SettingRow label="并发上限">
          <NumberField
            ariaLabel="并发上限"
            className="xflow-editor-concurrency-limit"
            placeholder="0 = 不限"
            readOnly={settingsReadOnly}
            unit="个执行"
            value={concurrency.limit}
            onChange={(next) => writeConcurrency({ limit: next })}
          />
        </SettingRow>
        {/* Not SettingRow: a radiogroup is not a labelable control, and
            wrapping it in <label> folds the row title into each radio name. */}
        <div className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span className="xflow-editor-form-row__label">超限时</span>
          <Segmented
            aria-label="并发超限时"
            block
            className="xflow-editor-concurrency-mode xflow-editor-card-segmented"
            classNames={concurrencyModeClassNames}
            disabled={settingsReadOnly}
            options={concurrencyModes}
            value={concurrency.mode}
            onChange={(next) => writeConcurrency({ mode: next as ConcurrencyMode })}
          />
        </div>
        {concurrency.mode === "queue" ? (
          <>
            <SettingRow label="队列上限">
              <NumberField
                ariaLabel="队列上限"
                className="xflow-editor-concurrency-max-queued"
                placeholder="0 = 不限"
                readOnly={settingsReadOnly}
                value={concurrency.maxQueued}
                onChange={(next) => writeConcurrency({ maxQueued: next })}
              />
            </SettingRow>
            <SettingRow label="排队超时">
              <NumberField
                ariaLabel="排队超时（分钟）"
                className="xflow-editor-concurrency-queue-timeout"
                integer={false}
                placeholder="0 = 不限"
                readOnly={settingsReadOnly}
                unit="分钟"
                value={
                  concurrency.queueTimeoutNs === undefined ? undefined : concurrency.queueTimeoutNs / NANOS_PER_MINUTE
                }
                onChange={(next) =>
                  writeConcurrency({
                    queueTimeoutNs: next === undefined ? undefined : Math.round(next * NANOS_PER_MINUTE)
                  })
                }
              />
            </SettingRow>
          </>
        ) : null}
      </SectionCard>
      <SectionCard ariaLabel="错误与重试" icon={<BranchIcon />} meta="节点默认继承" title="错误与重试">
        <Hint
          help="节点没有单独配置错误处理与重试时，使用这里的值；节点头部的「节点默认继承」标记指的就是这些默认值。"
          text="未单独配置的节点沿用这里的默认值。"
        />
        <SettingRow label="出错时">
          <SelectField
            ariaLabel="执行出错时"
            options={errorPolicies}
            placeholder="继承引擎默认"
            readOnly={settingsReadOnly}
            value={settings.on_error}
            onChange={(next) =>
              writeSettings((current) => {
                const nextSettings = { ...current };
                if (next === undefined) delete nextSettings.on_error;
                else nextSettings.on_error = next;
                return nextSettings;
              })
            }
          />
        </SettingRow>
        <SettingRow label="自动重试">
          <Switch
            aria-label="自动重试"
            checked={Boolean(retry.enabled)}
            className="xflow-editor-setting-switch"
            disabled={settingsReadOnly}
            size="small"
            onChange={(checked) => writeRetry({ enabled: checked })}
          />
        </SettingRow>
        {retry.enabled ? (
          <>
            <SettingRow label="最大尝试次数">
              <LiteralInput
                ariaLabel="重试最大尝试次数"
                mode="json"
                placeholder="例如 3"
                readOnly={settingsReadOnly}
                value={retry.max_attempts}
                onCommit={(next) => writeRetry({ max_attempts: next === undefined ? undefined : Number(next) })}
              />
            </SettingRow>
            <SettingRow label="退避策略">
              <SelectField
                ariaLabel="重试退避策略"
                options={retryStrategies}
                placeholder="未设置"
                readOnly={settingsReadOnly}
                value={retry.strategy}
                onChange={(next) => writeRetry({ strategy: next })}
              />
            </SettingRow>
            <SettingRow label="初始间隔">
              <LiteralInput
                ariaLabel="重试初始间隔"
                mode="json"
                placeholder="例如 1"
                readOnly={settingsReadOnly}
                value={retry.initial_interval}
                onCommit={(next) => writeRetry({ initial_interval: next === undefined ? undefined : Number(next) })}
              />
            </SettingRow>
            <SettingRow label="最大间隔">
              <LiteralInput
                ariaLabel="重试最大间隔"
                mode="json"
                placeholder="例如 30"
                readOnly={settingsReadOnly}
                value={retry.max_interval}
                onCommit={(next) => writeRetry({ max_interval: next === undefined ? undefined : Number(next) })}
              />
            </SettingRow>
            <SettingRow label="退避倍数">
              <LiteralInput
                ariaLabel="重试退避倍数"
                mode="json"
                placeholder="例如 2"
                readOnly={settingsReadOnly}
                value={retry.multiplier}
                onCommit={(next) => writeRetry({ multiplier: next === undefined ? undefined : Number(next) })}
              />
            </SettingRow>
          </>
        ) : null}
      </SectionCard>
      <CollapsibleCard
        actions={
          <>
            <SourceToggle
              ariaLabel="以 JSON 编辑 Runner 选择"
              pressed={runnerSource}
              onToggle={toggleRunnerSource}
            />
            <SourceToggle
              ariaLabel="以 JSON 编辑执行设置"
              pressed={settingsSource}
              onToggle={toggleSettingsSource}
            />
          </>
        }
        ariaLabel="高级"
        expanded={advancedExpanded}
        icon={<GearIcon />}
        meta="调度 · 实验"
        title="高级"
        onToggle={() => setAdvancedOpen((open) => !open)}
      >
        {runnerSource ? (
          <WorkflowJsonTextArea
            ariaLabel="工作流 Runner 选择 JSON"
            drafts={workflowJsonDrafts}
            field="runner_selector"
            readOnly={readOnly}
            workflow={workflow}
            onText={onWorkflowJsonText}
          />
        ) : (
          <>
            <p className="xflow-editor-variable-hint">工作流默认的 Runner 选择；节点可在自身「执行策略」中覆盖匹配标签。</p>
            {brokenRunner ? (
              <p className="xflow-editor-variable-notice" role="status">
                JSON 源码未通过解析，修正后才能在此编辑；保存与运行已被阻止。
              </p>
            ) : null}
            <SettingRow label="模式">
              <SelectField
                ariaLabel="Runner 模式"
                options={runnerModes}
                placeholder="任意可用 Runner"
                readOnly={runnerReadOnly}
                value={runner.mode}
                onChange={(next) => writeRunner((current) => ({ ...current, mode: next }))}
              />
            </SettingRow>
            <div className="xflow-editor-variable-block">
              <span className="xflow-editor-variable-block__title">匹配标签</span>
              {labelKeys.length === 0 ? (
                <p className="xflow-editor-variable-empty">暂无标签，模式为「必须匹配标签」时 Runner 需全部命中。</p>
              ) : (
                <ul className="xflow-editor-variable-list">
                  {Object.entries(matchLabels).map(([key, value], index) => (
                    <li className="xflow-editor-variable-row xflow-editor-variable-row--pair" key={index}>
                      <KeyInput
                        ariaLabel={`匹配标签 ${index + 1} 键`}
                        existing={labelKeys}
                        placeholder="标签"
                        readOnly={runnerReadOnly}
                        value={key}
                        onCommit={(next) =>
                          writeRunner((current) =>
                            withMatchLabels(current, renameKey(current.match_labels ?? {}, key, next))
                          )
                        }
                      />
                      <LiteralInput
                        ariaLabel={`匹配标签 ${index + 1} 值`}
                        emptyValue=""
                        mode="string"
                        placeholder="值"
                        readOnly={runnerReadOnly}
                        value={value}
                        onCommit={(next) =>
                          writeRunner((current) =>
                            withMatchLabels(current, { ...(current.match_labels ?? {}), [key]: String(next ?? "") })
                          )
                        }
                      />
                      <span className="xflow-editor-variable-row__actions">
                        <DeleteRowButton
                          ariaLabel={`删除匹配标签 ${key}`}
                          readOnly={runnerReadOnly}
                          onDelete={() =>
                            writeRunner((current) =>
                              withMatchLabels(current, withoutKey(current.match_labels ?? {}, key))
                            )
                          }
                        />
                      </span>
                    </li>
                  ))}
                </ul>
              )}
              <AddRowButton
                label="添加标签"
                readOnly={runnerReadOnly}
                onAdd={() =>
                  writeRunner((current) => {
                    const currentLabels = current.match_labels ?? {};
                    const base = "label";
                    let candidate = base;
                    for (let index = Object.keys(currentLabels).length + 1; currentLabels[candidate]; index += 1) {
                      candidate = `${base}_${index}`;
                    }
                    return withMatchLabels(current, { ...currentLabels, [candidate]: "" });
                  })
                }
              />
            </div>
          </>
        )}
        {settingsSource ? (
          <>
            {brokenSettings ? (
              <p className="xflow-editor-variable-notice" role="status">
                JSON 源码未通过解析，修正后才能在此编辑；保存与运行已被阻止。
              </p>
            ) : null}
            <WorkflowJsonTextArea
              ariaLabel="工作流执行设置 JSON"
              drafts={workflowJsonDrafts}
              field="settings"
              readOnly={readOnly}
              workflow={workflow}
              onText={onWorkflowJsonText}
            />
          </>
        ) : (
          <SettingRow label="Pin 数据">
            <SelectField
              ariaLabel="Pin 数据模式"
              options={pinDataModes}
              placeholder="未设置"
              readOnly={settingsReadOnly}
              value={settings.pin_data_mode}
              onChange={(next) =>
                writeSettings((current) => {
                  const nextSettings = { ...current };
                  if (next === undefined) delete nextSettings.pin_data_mode;
                  else nextSettings.pin_data_mode = next;
                  return nextSettings;
                })
              }
            />
          </SettingRow>
        )}
        <div className="xflow-editor-variable-block">
          <span className="xflow-editor-variable-block__title">运行选项</span>
          <SettingRow label="允许环路">
            <Switch
              aria-label="允许环路"
              checked={Boolean(options.allow_cycles)}
              className="xflow-editor-setting-switch"
              disabled={readOnly}
              size="small"
              onChange={(checked) => writeOptions((current) => withOption(current, "allow_cycles", checked))}
            />
          </SettingRow>
          <SettingRow label="最大自动深度">
            <NumberField
              ariaLabel="最大自动深度"
              className="xflow-editor-option-max-auto-depth"
              placeholder="0 = 引擎默认"
              readOnly={readOnly}
              value={options.max_auto_depth}
              onChange={(next) =>
                writeOptions((current) =>
                  withOption(current, "max_auto_depth", next === undefined || next <= 0 ? undefined : next)
                )
              }
            />
          </SettingRow>
          <SettingRow label="节点组协议（实验）">
            <Switch
              aria-label="节点组协议（实验）"
              checked={Boolean(options.experimental_node_group)}
              className="xflow-editor-setting-switch"
              disabled={readOnly}
              size="small"
              onChange={(checked) =>
                writeOptions((current) => withOption(current, "experimental_node_group", checked))
              }
            />
          </SettingRow>
          <SettingRow label="Transient 执行">
            <Switch
              aria-label="Transient 执行"
              checked={Boolean(options.transient)}
              className="xflow-editor-setting-switch"
              disabled={readOnly}
              size="small"
              onChange={(checked) => writeOptions((current) => withOption(current, "transient", checked))}
            />
          </SettingRow>
          {options.transient ? (
            <SettingRow label="Transient 保留时长（秒）">
              <NumberField
                ariaLabel="Transient 保留时长（秒）"
                className="xflow-editor-option-transient-ttl"
                integer={false}
                placeholder="0 = 引擎默认"
                readOnly={readOnly}
                value={options.transient_ttl === undefined ? undefined : options.transient_ttl / NANOS_PER_SECOND}
                onChange={(next) =>
                  writeOptions((current) =>
                    withOption(
                      current,
                      "transient_ttl",
                      next === undefined || next <= 0 ? undefined : Math.round(next * NANOS_PER_SECOND)
                    )
                  )
                }
              />
            </SettingRow>
          ) : null}
        </div>
      </CollapsibleCard>
      <SectionCard ariaLabel="Pin Data" icon={<PinIcon />} title="Pin Data">
        <Hint
          help="固定值是「节点名 → 任意 JSON」，键与值都不受表单约束，因此只能通过源码编辑。"
          text="按节点名固定输出，调试时跳过真实执行。"
        />
        <WorkflowJsonTextArea
          ariaLabel="工作流 Pin Data JSON"
          drafts={workflowJsonDrafts}
          field="pin_data"
          readOnly={readOnly}
          workflow={workflow}
          onText={onWorkflowJsonText}
        />
      </SectionCard>
    </div>
  );
}
