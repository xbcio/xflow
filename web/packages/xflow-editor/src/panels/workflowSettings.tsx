import * as React from "react";
import { CloudServerOutlined, FileSearchOutlined, PushpinOutlined, SettingOutlined } from "@ant-design/icons";
import { Input, Select, Switch } from "antd";
import type { ErrorPolicy, RunnerSelector, RunnerSelectorMode, RetryPolicy, RetryStrategy, WorkflowDef, WorkflowSettings as WorkflowExecutionSettings } from "@xflow/core";
import {
  SectionCard,
  WorkflowJsonTextArea,
  editorInputClassNames,
  editorSelectClassNames,
  type WorkflowJsonDrafts,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./shared";
import { AddRowButton, DeleteRowButton, KeyInput, LiteralInput, SourceToggle, renameKey, withoutKey } from "./variables";

/**
 * What the Inspector shows with no node selected: the workflow's own
 * metadata, the Runner selector and the execution settings. Runner and 执行设置
 * are structured rows with a `</>` source view (the same interaction as the
 * variables panel); Pin Data stays source-only, because it maps arbitrary node
 * names to arbitrary pinned values.
 */

const runnerModes: { value: RunnerSelectorMode; label: string }[] = [
  { value: "default", label: "默认：任意可用 Runner" },
  { value: "required", label: "必须匹配标签" }
];

const errorPolicies: { value: ErrorPolicy; label: string }[] = [
  { value: "stop", label: "stop：失败即停止" },
  { value: "error_output", label: "error_output：错误输出端口" },
  { value: "main_output", label: "main_output：主输出端口" },
  { value: "continue", label: "continue：继续执行" }
];

const pinDataModes: { value: NonNullable<WorkflowExecutionSettings["pin_data_mode"]>; label: string }[] = [
  { value: "test_only", label: "test_only：仅测试运行" },
  { value: "always", label: "always：总是使用" },
  { value: "disabled", label: "disabled：关闭" }
];

const retryStrategies: { value: RetryStrategy; label: string }[] = [
  { value: "fixed", label: "fixed：固定间隔" },
  { value: "exponential", label: "exponential：指数退避" }
];

/** Writes a top-level object field, dropping it when empty so the DSL stays clean. */
function withObjectField<K extends "runner_selector" | "settings">(
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
      onChange={(next) => onChange(next ?? undefined)}
    />
  );
}

/** One labelled row of the settings cards. */
function SettingRow({ label, children }: { label: string; children: React.ReactNode }): React.ReactElement {
  return (
    <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
      <span>{label}</span>
      {children}
    </label>
  );
}

export function WorkflowSettings({
  workflow,
  onChangeWith,
  workflowJsonDrafts,
  onWorkflowJsonText,
  readOnly = false
}: {
  workflow: WorkflowDef;
  onChangeWith?: (update: WorkflowUpdater) => void;
  workflowJsonDrafts?: WorkflowJsonDrafts;
  onWorkflowJsonText?: (field: WorkflowJsonField, text: string) => void;
  readOnly?: boolean;
}): React.ReactElement {
  const brokenRunner = Boolean(workflowJsonDrafts?.runner_selector?.error);
  const brokenSettings = Boolean(workflowJsonDrafts?.settings?.error);
  // A broken draft opens its source view, so the text that blocks save is on screen.
  const [runnerSource, setRunnerSource] = React.useState(brokenRunner);
  const [settingsSource, setSettingsSource] = React.useState(brokenSettings);
  const updateMeta = (patch: Partial<WorkflowDef>) => onChangeWith?.((latest) => ({ ...latest, ...patch }));
  const writeRunner = (update: (current: RunnerSelector) => RunnerSelector) =>
    onChangeWith?.((latest) => withObjectField(latest, "runner_selector", update(latest.runner_selector ?? {})));
  const writeSettings = (update: (current: WorkflowExecutionSettings) => WorkflowExecutionSettings) =>
    onChangeWith?.((latest) => withObjectField(latest, "settings", update(latest.settings ?? {})));

  const runner = workflow.runner_selector ?? {};
  const matchLabels = runner.match_labels ?? {};
  const labelKeys = Object.keys(matchLabels);
  const runnerReadOnly = readOnly || brokenRunner;

  const settings = workflow.settings ?? {};
  const retry = settings.retry ?? {};
  const settingsReadOnly = readOnly || brokenSettings;
  const writeRetry = (patch: Partial<RetryPolicy>) =>
    writeSettings((current) => {
      const nextRetry: RetryPolicy = { ...(current.retry ?? {}), ...patch };
      return { ...current, retry: nextRetry };
    });

  return (
    <div className="xflow-editor-workflow-settings">
      <SectionCard ariaLabel="基本信息" icon={<FileSearchOutlined />} title="基本信息">
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>名称</span>
          <Input
            aria-label="工作流名称"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={workflow.name ?? ""}
            onChange={(event) => updateMeta({ name: event.target.value })}
          />
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>版本</span>
          <Input
            aria-label="工作流版本"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={workflow.version ?? ""}
            onChange={(event) => updateMeta({ version: event.target.value })}
          />
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>描述</span>
          <Input
            aria-label="工作流描述"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={workflow.description ?? ""}
            onChange={(event) => updateMeta({ description: event.target.value })}
          />
        </label>
      </SectionCard>
      <SectionCard
        actions={
          <SourceToggle
            ariaLabel="以 JSON 编辑 Runner 选择"
            pressed={runnerSource}
            onToggle={() => setRunnerSource((on) => !on)}
          />
        }
        ariaLabel="Runner"
        icon={<CloudServerOutlined />}
        title="Runner"
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
                placeholder="默认：任意可用 Runner"
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
      </SectionCard>
      <SectionCard
        actions={
          <SourceToggle
            ariaLabel="以 JSON 编辑执行设置"
            pressed={settingsSource}
            onToggle={() => setSettingsSource((on) => !on)}
          />
        }
        ariaLabel="执行设置"
        icon={<SettingOutlined />}
        title="执行设置"
      >
        {settingsSource ? (
          <WorkflowJsonTextArea
            ariaLabel="工作流执行设置 JSON"
            drafts={workflowJsonDrafts}
            field="settings"
            readOnly={readOnly}
            workflow={workflow}
            onText={onWorkflowJsonText}
          />
        ) : (
          <>
            <p className="xflow-editor-variable-hint">
              超时、时区、错误策略等执行级设置。执行超时是整个执行的预算（整数纳秒，含排队与挂起等待），0 或留空为不限；运行时到期处理尚未实现，目前只校验与保存，不会因它超时。
            </p>
            {brokenSettings ? (
              <p className="xflow-editor-variable-notice" role="status">
                JSON 源码未通过解析，修正后才能在此编辑；保存与运行已被阻止。
              </p>
            ) : null}
            <SettingRow label="超时">
              <LiteralInput
                ariaLabel="执行超时"
                mode="json"
                placeholder="纳秒，例如 1800000000000；留空不限"
                readOnly={settingsReadOnly}
                value={settings.timeout}
                onCommit={(next) =>
                  writeSettings((current) => {
                    const nextSettings = { ...current };
                    if (next === undefined) delete nextSettings.timeout;
                    else nextSettings.timeout = next as number;
                    return nextSettings;
                  })
                }
              />
            </SettingRow>
            <SettingRow label="并发">
              <LiteralInput
                ariaLabel="执行并发"
                mode="json"
                placeholder="例如 4"
                readOnly={settingsReadOnly}
                value={settings.concurrency}
                onCommit={(next) =>
                  writeSettings((current) => {
                    const nextSettings = { ...current };
                    if (next === undefined) delete nextSettings.concurrency;
                    else nextSettings.concurrency = next as number;
                    return nextSettings;
                  })
                }
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
            <SettingRow label="错误策略">
              <SelectField
                ariaLabel="执行错误策略"
                options={errorPolicies}
                placeholder="未设置"
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
            <SettingRow label="重试">
              <Switch
                aria-label="启用重试"
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
                    placeholder="fixed"
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
          </>
        )}
      </SectionCard>
      <SectionCard ariaLabel="Pin Data" icon={<PushpinOutlined />} title="Pin Data">
        <p className="xflow-editor-variable-hint">
          按节点名固定输出（节点名 → 固定值），调试时跳过真实执行。键是任意节点名、值是任意 JSON，因此只用源码编辑。
        </p>
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
