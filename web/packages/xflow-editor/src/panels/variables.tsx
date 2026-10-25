import * as React from "react";
import { AuditIcon, CodeIcon, CopyIcon, GearIcon, KeyIcon, PlusIcon, TrashIcon } from "@xflow/icons";
import { Button, Input, Select, Switch, Tooltip, message } from "antd";
import type { WorkflowCredential, WorkflowDef, WorkflowParam } from "@xflow/core";
import {
  EditorPanel,
  SectionCard,
  WorkflowJsonTextArea,
  editorInputClassNames,
  editorSelectClassNames,
  editorTooltipClassNames,
  serializeJson,
  type WorkflowJsonDrafts,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./shared";

/**
 * The workflow's data surface (n8n "Variables", Dify "环境变量"): input params,
 * context vars/config and credential references, each as structured rows with
 * a `</>` source view. Rows write through the editor's `onChangeWith` updater
 * (one undo entry per edit); the source views reuse the editor's workflow JSON
 * drafts, so an unparseable source still blocks save/run.
 *
 * The row primitives (KeyInput / LiteralInput / SourceToggle / AddRowButton /
 * DeleteRowButton) are exported for the Inspector's workflow settings, which
 * follows the same row-plus-source pattern.
 */

const paramTypes = ["string", "number", "boolean", "object", "array"] as const;
/** DSL-SPECIFICATION credentials.type. */
const credentialTypes = ["postgres", "mysql", "redis", "oauth2", "apiKey", "basic"] as const;
const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/;

type ContextRoot = "vars" | "config";
type LiteralMode = "string" | "json" | "auto";

/** `${{ $params.x }}`; keys that are not identifiers use bracket access, as `$nodes['name']` does. */
export function expressionReference(root: "$params" | "$vars" | "$config", key: string): string {
  const path = identifierPattern.test(key)
    ? `${root}.${key}`
    : `${root}['${key.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}']`;
  return `\${{ ${path} }}`;
}

function uniqueKey(existing: readonly string[], base: string): string {
  for (let index = existing.length + 1; ; index += 1) {
    const candidate = `${base}_${index}`;
    if (!existing.includes(candidate)) return candidate;
  }
}

/** Renames a key in place, keeping the record's order (rows are positional). */
export function renameKey<T>(record: Record<string, T>, previous: string, next: string): Record<string, T> {
  return Object.fromEntries(Object.entries(record).map(([key, value]) => [key === previous ? next : key, value]));
}

export function withoutKey<T>(record: Record<string, T>, removed: string): Record<string, T> {
  return Object.fromEntries(Object.entries(record).filter(([key]) => key !== removed));
}

/** Writes a top-level record field, dropping it when empty so the DSL stays clean. */
function withRecord<K extends "params" | "credentials">(
  workflow: WorkflowDef,
  field: K,
  next: NonNullable<WorkflowDef[K]>
): WorkflowDef {
  if (Object.keys(next).length > 0) return { ...workflow, [field]: next };
  const { [field]: _removed, ...rest } = workflow;
  return rest as WorkflowDef;
}

function withContextRecord(workflow: WorkflowDef, root: ContextRoot, next: Record<string, unknown>): WorkflowDef {
  const context = { ...(workflow.context ?? {}) };
  if (Object.keys(next).length > 0) context[root] = next;
  else delete context[root];
  if (Object.keys(context).length > 0) return { ...workflow, context };
  const { context: _removed, ...rest } = workflow;
  return rest as WorkflowDef;
}

/** Source JSON may hold `null` or a scalar where an entry object belongs; rows render it as empty. */
function entryObject<T extends object>(value: unknown): T {
  return (value && typeof value === "object" && !Array.isArray(value) ? value : {}) as T;
}

function literalModeForType(type: string | undefined): LiteralMode {
  if (type === "string") return "string";
  if (type && (paramTypes as readonly string[]).includes(type)) return "json";
  return "auto";
}

function parsesAsNonString(text: string): boolean {
  try {
    return typeof JSON.parse(text) !== "string";
  } catch {
    return false;
  }
}

function displayLiteral(value: unknown, mode: LiteralMode): string {
  if (value === undefined) return "";
  if (typeof value === "string" && (mode === "string" || (mode === "auto" && !parsesAsNonString(value)))) return value;
  return JSON.stringify(value);
}

/**
 * `json`: a JSON literal or an error. `auto`: a JSON literal, else the raw
 * text. `string`: the raw text. Blank text means "no value".
 */
function parseLiteral(text: string, mode: LiteralMode): { value?: unknown; error?: string } {
  if (!text.trim()) return {};
  if (mode === "string") return { value: text };
  try {
    return { value: JSON.parse(text) as unknown };
  } catch {
    return mode === "json" ? { error: "需要合法的 JSON 字面量，例如 1、true、[]、{}" } : { value: text };
  }
}

/** A record key: blank or duplicate names show an error and are never written. */
export function KeyInput({
  ariaLabel,
  value,
  existing,
  placeholder,
  readOnly,
  onCommit
}: {
  ariaLabel: string;
  value: string;
  existing: readonly string[];
  placeholder: string;
  readOnly: boolean;
  onCommit: (next: string) => void;
}): React.ReactElement {
  const [text, setText] = React.useState(value);
  const [error, setError] = React.useState<string>();
  const textRef = React.useRef(text);
  textRef.current = text;
  React.useEffect(() => {
    // The user's own text that just committed stays as typed (trailing space included).
    if (textRef.current.trim() === value) return;
    setText(value);
    setError(undefined);
  }, [value]);
  const change = (next: string) => {
    setText(next);
    const trimmed = next.trim();
    const problem = !trimmed
      ? "名称不能为空"
      : trimmed !== value && existing.includes(trimmed)
        ? `名称已存在: ${trimmed}`
        : undefined;
    setError(problem);
    if (!problem && trimmed !== value) onCommit(trimmed);
  };
  return (
    <span className="xflow-editor-variable-field">
      <Input
        aria-invalid={error ? true : undefined}
        aria-label={ariaLabel}
        className="xflow-editor-field xflow-editor-variable-key"
        classNames={editorInputClassNames}
        placeholder={placeholder}
        readOnly={readOnly}
        status={error ? "error" : undefined}
        value={text}
        onChange={(event) => change(event.target.value)}
      />
      {error ? <span className="xflow-editor-variable-error" role="alert">{error}</span> : null}
    </span>
  );
}

/** A literal value; keeps the user's text while it still means the committed value. */
export function LiteralInput({
  ariaLabel,
  value,
  mode,
  placeholder,
  readOnly,
  emptyValue,
  onCommit
}: {
  ariaLabel: string;
  value: unknown;
  mode: LiteralMode;
  placeholder: string;
  readOnly: boolean;
  /** What blank text commits; `undefined` removes the value. */
  emptyValue?: unknown;
  onCommit: (next: unknown) => void;
}): React.ReactElement {
  const [text, setText] = React.useState(() => displayLiteral(value, mode));
  const [error, setError] = React.useState<string>();
  const textRef = React.useRef(text);
  textRef.current = text;
  React.useEffect(() => {
    const parsed = parseLiteral(textRef.current, mode);
    const current = parsed.error ? undefined : parsed.value ?? emptyValue;
    if (!parsed.error && serializeJson(current) === serializeJson(value)) return;
    setText(displayLiteral(value, mode));
    setError(undefined);
  }, [emptyValue, mode, value]);
  const change = (next: string) => {
    setText(next);
    const parsed = parseLiteral(next, mode);
    setError(parsed.error);
    if (parsed.error) return;
    const committed = parsed.value ?? emptyValue;
    if (serializeJson(committed) !== serializeJson(value)) onCommit(committed);
  };
  return (
    <span className="xflow-editor-variable-field">
      <Input
        aria-invalid={error ? true : undefined}
        aria-label={ariaLabel}
        className="xflow-editor-field xflow-editor-variable-value"
        classNames={editorInputClassNames}
        placeholder={placeholder}
        readOnly={readOnly}
        status={error ? "error" : undefined}
        value={text}
        onChange={(event) => change(event.target.value)}
      />
      {error ? <span className="xflow-editor-variable-error" role="alert">{error}</span> : null}
    </span>
  );
}

function CopyReferenceButton({
  ariaLabel,
  reference,
  tooltip,
  onCopy
}: {
  ariaLabel: string;
  reference: string;
  tooltip: string;
  onCopy?: (text: string) => void;
}): React.ReactElement {
  return (
    <Tooltip classNames={editorTooltipClassNames} title={onCopy ? `${tooltip}：${reference}` : "当前环境不支持剪贴板"}>
      <span className="xflow-editor-variable-action-wrap">
        <Button
          aria-label={ariaLabel}
          className="xflow-editor-variable-action"
          disabled={!onCopy}
          icon={<CopyIcon />}
          size="small"
          type="text"
          onClick={() => onCopy?.(reference)}
        />
      </span>
    </Tooltip>
  );
}

export function DeleteRowButton({ ariaLabel, readOnly, onDelete }: { ariaLabel: string; readOnly: boolean; onDelete: () => void }): React.ReactElement {
  return (
    <Button
      aria-label={ariaLabel}
      className="xflow-editor-variable-action xflow-editor-variable-action--danger"
      danger
      disabled={readOnly}
      icon={<TrashIcon />}
      size="small"
      type="text"
      onClick={onDelete}
    />
  );
}

export function SourceToggle({ ariaLabel, pressed, onToggle }: { ariaLabel: string; pressed: boolean; onToggle: () => void }): React.ReactElement {
  return (
    <Tooltip classNames={editorTooltipClassNames} title={pressed ? "返回表单" : "以 JSON 源码编辑"}>
      <Button
        aria-label={ariaLabel}
        aria-pressed={pressed}
        className={`xflow-editor-variable-action xflow-editor-source-toggle ${pressed ? "is-active" : ""}`}
        icon={<CodeIcon />}
        size="small"
        type="text"
        onClick={onToggle}
      />
    </Tooltip>
  );
}

export function AddRowButton({ label, readOnly, onAdd }: { label: string; readOnly: boolean; onAdd: () => void }): React.ReactElement {
  return (
    <Button
      aria-label={label}
      className="xflow-editor-variable-add"
      disabled={readOnly}
      icon={<PlusIcon />}
      size="small"
      type="dashed"
      onClick={onAdd}
    >
      {label}
    </Button>
  );
}

function BrokenDraftNotice(): React.ReactElement {
  return (
    <p className="xflow-editor-variable-notice" role="status">
      JSON 源码未通过解析，修正后才能在此编辑；保存与运行已被阻止。
    </p>
  );
}

export interface VariablesPanelProps {
  workflow: WorkflowDef;
  readOnly?: boolean;
  onChangeWith?: (update: WorkflowUpdater) => void;
  workflowJsonDrafts?: WorkflowJsonDrafts;
  onWorkflowJsonText?: (field: WorkflowJsonField, text: string) => void;
  /** Editor-owned title-bar action (the §6.0 收起左栏 key). */
  headerAction?: React.ReactNode;
}

export function VariablesPanel({
  workflow,
  readOnly = false,
  onChangeWith,
  workflowJsonDrafts,
  onWorkflowJsonText,
  headerAction
}: VariablesPanelProps): React.ReactElement {
  const [messageApi, messageHolder] = message.useMessage();
  const brokenParams = Boolean(workflowJsonDrafts?.params?.error);
  const brokenContext = Boolean(workflowJsonDrafts?.context?.error);
  const brokenCredentials = Boolean(workflowJsonDrafts?.credentials?.error);
  // A broken draft opens its source view, so the text that blocks save is on screen.
  const [paramsSource, setParamsSource] = React.useState(brokenParams);
  const [contextSource, setContextSource] = React.useState(brokenContext);
  const [credentialsSource, setCredentialsSource] = React.useState(brokenCredentials);

  const clipboard = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
  const copy = React.useCallback((text: string) => {
    if (!clipboard) return;
    clipboard.writeText(text).then(
      () => void messageApi.success(`已复制 ${text}`),
      () => void messageApi.error("复制失败，请检查浏览器剪贴板权限")
    );
  }, [clipboard, messageApi]);
  const onCopy = clipboard ? copy : undefined;

  const params = workflow.params ?? {};
  const vars = workflow.context?.vars ?? {};
  const config = workflow.context?.config ?? {};
  const credentials = workflow.credentials ?? {};
  const paramNames = Object.keys(params);
  const credentialNames = Object.keys(credentials);
  const titleCount = paramNames.length + Object.keys(vars).length + Object.keys(config).length;

  const write = (update: WorkflowUpdater) => {
    if (!readOnly) onChangeWith?.(update);
  };
  const updateParam = (name: string, patch: (param: WorkflowParam) => WorkflowParam) =>
    write((latest) => {
      const current = latest.params ?? {};
      if (!(name in current)) return latest;
      return withRecord(latest, "params", { ...current, [name]: patch(entryObject<WorkflowParam>(current[name])) });
    });
  const updateContext = (root: ContextRoot, update: (record: Record<string, unknown>) => Record<string, unknown>) =>
    write((latest) => withContextRecord(latest, root, update(latest.context?.[root] ?? {})));
  const updateCredential = (name: string, patch: Partial<WorkflowCredential>) =>
    write((latest) => {
      const current = latest.credentials ?? {};
      if (!(name in current)) return latest;
      return withRecord(latest, "credentials", { ...current, [name]: { ...current[name], ...patch } });
    });

  const contextRows = (root: ContextRoot, record: Record<string, unknown>, label: string, broken: boolean) => {
    const keys = Object.keys(record);
    const rowsReadOnly = readOnly || broken;
    if (keys.length === 0) {
      return (
        <p className="xflow-editor-variable-empty">
          {root === "vars" ? "暂无变量。变量是只读的业务常量，例如阈值、标签。" : "暂无配置。配置随环境变化，例如服务地址、租户。"}
        </p>
      );
    }
    return (
      <ul className="xflow-editor-variable-list">
        {Object.entries(record).map(([key, value], index) => (
          <li className="xflow-editor-variable-row xflow-editor-variable-row--pair" key={index}>
            <KeyInput
              ariaLabel={`${label} ${index + 1} 键`}
              existing={keys}
              placeholder="键"
              readOnly={rowsReadOnly}
              value={key}
              onCommit={(next) => updateContext(root, (current) => renameKey(current, key, next))}
            />
            <LiteralInput
              ariaLabel={`${label} ${index + 1} 值`}
              emptyValue=""
              mode="auto"
              placeholder="值（JSON 字面量或文本）"
              readOnly={rowsReadOnly}
              value={value}
              onCommit={(next) => updateContext(root, (current) => ({ ...current, [key]: next }))}
            />
            <span className="xflow-editor-variable-row__actions">
              <CopyReferenceButton
                ariaLabel={`复制 ${key} 的引用`}
                reference={expressionReference(root === "vars" ? "$vars" : "$config", key)}
                tooltip="复制表达式引用"
                onCopy={onCopy}
              />
              <DeleteRowButton
                ariaLabel={`删除${label} ${key}`}
                readOnly={rowsReadOnly}
                onDelete={() => updateContext(root, (current) => withoutKey(current, key))}
              />
            </span>
          </li>
        ))}
      </ul>
    );
  };

  return (
    <EditorPanel
      actions={headerAction}
      bodyClassName="xflow-editor-variables__body"
      className="xflow-editor-variables"
      count={readOnly ? `${titleCount} · 只读` : titleCount}
      icon={<CodeIcon />}
      title="变量"
    >
      {messageHolder}
      <SectionCard
        ariaLabel="输入参数"
        icon={<AuditIcon />}
        meta={paramNames.length}
        title="输入参数"
        actions={<SourceToggle ariaLabel="以 JSON 编辑输入参数" pressed={paramsSource} onToggle={() => setParamsSource((on) => !on)} />}
      >
        {paramsSource ? (
          <WorkflowJsonTextArea
            ariaLabel="工作流输入参数 JSON"
            drafts={workflowJsonDrafts}
            field="params"
            readOnly={readOnly}
            workflow={workflow}
            onText={onWorkflowJsonText}
          />
        ) : (
          <>
            {brokenParams ? <BrokenDraftNotice /> : null}
            {paramNames.length === 0 ? (
              <p className="xflow-editor-variable-empty">暂无输入参数，运行时由调用方传入；表达式中用 $params 访问。</p>
            ) : (
              <ul className="xflow-editor-variable-list">
                {Object.entries(params).map(([name, rawParam], index) => {
                  const param = entryObject<WorkflowParam>(rawParam);
                  const rowReadOnly = readOnly || brokenParams;
                  const type = param.type ?? "string";
                  const typeOptions = (paramTypes as readonly string[]).includes(type) ? paramTypes : [...paramTypes, type];
                  return (
                    <li className="xflow-editor-variable-row xflow-editor-variable-row--param" key={index}>
                      <div className="xflow-editor-variable-row__line">
                        <KeyInput
                          ariaLabel={`输入参数 ${index + 1} 名称`}
                          existing={paramNames}
                          placeholder="名称"
                          readOnly={rowReadOnly}
                          value={name}
                          onCommit={(next) => write((latest) => withRecord(latest, "params", renameKey(latest.params ?? {}, name, next)))}
                        />
                        <Select
                          aria-label={`输入参数 ${index + 1} 类型`}
                          className="xflow-editor-variable-type"
                          classNames={editorSelectClassNames}
                          disabled={rowReadOnly}
                          options={typeOptions.map((option) => ({ label: option, value: option }))}
                          popupMatchSelectWidth={false}
                          value={type}
                          onChange={(next: string) => updateParam(name, (current) => ({ ...current, type: next }))}
                        />
                        <span className="xflow-editor-variable-row__actions">
                          <CopyReferenceButton
                            ariaLabel={`复制 ${name} 的引用`}
                            reference={expressionReference("$params", name)}
                            tooltip="复制表达式引用"
                            onCopy={onCopy}
                          />
                          <DeleteRowButton
                            ariaLabel={`删除输入参数 ${name}`}
                            readOnly={rowReadOnly}
                            onDelete={() => write((latest) => withRecord(latest, "params", withoutKey(latest.params ?? {}, name)))}
                          />
                        </span>
                      </div>
                      <div className="xflow-editor-variable-row__line xflow-editor-variable-row__line--details">
                        <Input
                          aria-label={`输入参数 ${index + 1} 显示名`}
                          className="xflow-editor-field xflow-editor-variable-display"
                          classNames={editorInputClassNames}
                          placeholder="显示名"
                          readOnly={rowReadOnly}
                          value={param.display_name ?? ""}
                          onChange={(event) => {
                            const displayName = event.target.value;
                            updateParam(name, ({ display_name: _previous, ...current }) =>
                              displayName ? { ...current, display_name: displayName } : current
                            );
                          }}
                        />
                        <LiteralInput
                          ariaLabel={`输入参数 ${index + 1} 默认值`}
                          mode={literalModeForType(param.type)}
                          placeholder="默认值"
                          readOnly={rowReadOnly}
                          value={param.default}
                          onCommit={(next) => updateParam(name, ({ default: _previous, ...current }) =>
                            next === undefined ? current : { ...current, default: next }
                          )}
                        />
                        <label className="xflow-editor-variable-required">
                          <Switch
                            aria-label={`输入参数 ${index + 1} 必填`}
                            checked={param.required ?? false}
                            disabled={rowReadOnly}
                            className="xflow-editor-switch xflow-editor-variable-switch"
                            size="small"
                            onChange={(checked) => updateParam(name, ({ required: _previous, ...current }) =>
                              checked ? { ...current, required: true } : current
                            )}
                          />
                          <span>必填</span>
                        </label>
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
            <AddRowButton
              label="添加输入参数"
              readOnly={readOnly || brokenParams}
              onAdd={() => write((latest) => {
                const current = latest.params ?? {};
                return withRecord(latest, "params", { ...current, [uniqueKey(Object.keys(current), "param")]: { type: "string" } });
              })}
            />
          </>
        )}
      </SectionCard>

      <SectionCard
        ariaLabel="变量"
        icon={<CodeIcon />}
        meta={Object.keys(vars).length}
        title="变量"
        actions={<SourceToggle ariaLabel="以 JSON 编辑变量" pressed={contextSource} onToggle={() => setContextSource((on) => !on)} />}
      >
        {contextSource ? (
          <>
            <p className="xflow-editor-variable-hint">变量与配置同属 context，这里一次编辑整个 context JSON（vars + config）。</p>
            <WorkflowJsonTextArea
              ariaLabel="工作流变量配置 JSON"
              drafts={workflowJsonDrafts}
              field="context"
              maxRows={14}
              readOnly={readOnly}
              workflow={workflow}
              onText={onWorkflowJsonText}
            />
          </>
        ) : (
          <>
            {brokenContext ? <BrokenDraftNotice /> : null}
            {contextRows("vars", vars, "变量", brokenContext)}
            <AddRowButton
              label="添加变量"
              readOnly={readOnly || brokenContext}
              onAdd={() => updateContext("vars", (current) => ({ ...current, [uniqueKey(Object.keys(current), "var")]: "" }))}
            />
          </>
        )}
      </SectionCard>

      <SectionCard
        ariaLabel="配置"
        icon={<GearIcon />}
        meta={Object.keys(config).length}
        title="配置"
        actions={<SourceToggle ariaLabel="以 JSON 编辑配置" pressed={contextSource} onToggle={() => setContextSource((on) => !on)} />}
      >
        {contextSource ? (
          <p className="xflow-editor-variable-hint">配置与变量共用一份 context JSON，已在上方「变量」卡片中编辑。</p>
        ) : (
          <>
            {contextRows("config", config, "配置", brokenContext)}
            <AddRowButton
              label="添加配置"
              readOnly={readOnly || brokenContext}
              onAdd={() => updateContext("config", (current) => ({ ...current, [uniqueKey(Object.keys(current), "config")]: "" }))}
            />
          </>
        )}
      </SectionCard>

      <SectionCard
        ariaLabel="密钥引用"
        icon={<KeyIcon />}
        meta={credentialNames.length}
        title="密钥引用"
        actions={<SourceToggle ariaLabel="以 JSON 编辑密钥引用" pressed={credentialsSource} onToggle={() => setCredentialsSource((on) => !on)} />}
      >
        {credentialsSource ? (
          <WorkflowJsonTextArea
            ariaLabel="工作流密钥 JSON"
            drafts={workflowJsonDrafts}
            field="credentials"
            readOnly={readOnly}
            workflow={workflow}
            onText={onWorkflowJsonText}
          />
        ) : (
          <>
            {brokenCredentials ? <BrokenDraftNotice /> : null}
            <p className="xflow-editor-variable-hint">只登记凭证的名称与类型，密文保存在密钥管理系统中，这里不会显示或保存任何值。</p>
            {credentialNames.length === 0 ? (
              <p className="xflow-editor-variable-empty">暂无密钥引用。节点通过 credential / authentication 参数按名引用。</p>
            ) : (
              <ul className="xflow-editor-variable-list">
                {Object.entries(credentials).map(([key, rawCredential], index) => {
                  const credential = entryObject<WorkflowCredential>(rawCredential);
                  const rowReadOnly = readOnly || brokenCredentials;
                  const type = credential.type ?? "";
                  const typeOptions = !type || (credentialTypes as readonly string[]).includes(type) ? credentialTypes : [...credentialTypes, type];
                  return (
                    <li className="xflow-editor-variable-row xflow-editor-variable-row--credential" key={index}>
                      <div className="xflow-editor-variable-row__line">
                        <KeyInput
                          ariaLabel={`密钥引用 ${index + 1} 键`}
                          existing={credentialNames}
                          placeholder="引用名"
                          readOnly={rowReadOnly}
                          value={key}
                          onCommit={(next) => write((latest) => withRecord(latest, "credentials", renameKey(latest.credentials ?? {}, key, next)))}
                        />
                        <Select
                          aria-label={`密钥引用 ${index + 1} 类型`}
                          className="xflow-editor-variable-type"
                          classNames={editorSelectClassNames}
                          disabled={rowReadOnly}
                          options={typeOptions.map((option) => ({ label: option, value: option }))}
                          placeholder="类型"
                          popupMatchSelectWidth={false}
                          value={type || undefined}
                          onChange={(next: string) => updateCredential(key, { type: next })}
                        />
                        <span className="xflow-editor-variable-row__actions">
                          <CopyReferenceButton
                            ariaLabel={`复制 ${key} 的引用`}
                            reference={key}
                            tooltip="复制引用名（填入节点的 credential 参数）"
                            onCopy={onCopy}
                          />
                          <DeleteRowButton
                            ariaLabel={`删除密钥引用 ${key}`}
                            readOnly={rowReadOnly}
                            onDelete={() => write((latest) => withRecord(latest, "credentials", withoutKey(latest.credentials ?? {}, key)))}
                          />
                        </span>
                      </div>
                      <Input
                        aria-label={`密钥引用 ${index + 1} 凭证名称`}
                        className="xflow-editor-field xflow-editor-variable-display"
                        classNames={editorInputClassNames}
                        placeholder="凭证名称（密钥管理系统中的凭证 ID）"
                        readOnly={rowReadOnly}
                        value={credential.name ?? ""}
                        onChange={(event) => updateCredential(key, { name: event.target.value })}
                      />
                    </li>
                  );
                })}
              </ul>
            )}
            <AddRowButton
              label="添加密钥引用"
              readOnly={readOnly || brokenCredentials}
              onAdd={() => write((latest) => {
                const current = latest.credentials ?? {};
                return withRecord(latest, "credentials", { ...current, [uniqueKey(Object.keys(current), "credential")]: { name: "", type: "apiKey" } });
              })}
            />
          </>
        )}
      </SectionCard>
    </EditorPanel>
  );
}
