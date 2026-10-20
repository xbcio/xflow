import * as React from "react";
import { UploadOutlined } from "@ant-design/icons";
import { Button, Input, Modal } from "antd";
import type { WorkflowDef } from "@xflow/core";
import type { WorkflowChange } from "../workflowChanges";
import { bareVersion, suggestFeatureVersion, versionHashPrefix, versionLabel } from "../versioning";
import { Banner } from "./banner";

/**
 * §6.10 publish dialog: the online version + hash, the new version label
 * (default patch +1), the structured diff against the online definition, the
 * validation gate and the in-flight note. The diff array is `workflowChanges`
 * — the very one the top-bar chip counts, so the two surfaces cannot disagree.
 */

const KIND_META = {
  add: { label: "新增", sign: "+", tone: "success" },
  mod: { label: "修改", sign: "~", tone: "warning" },
  del: { label: "删除", sign: "−", tone: "danger" }
} as const;

const KIND_ORDER = ["add", "mod", "del"] as const;

export interface PublishDialogProps {
  open: boolean;
  workflow: WorkflowDef;
  /** The current online version label, from the version list when it has loaded. */
  onlineVersion?: string;
  /** The current online definition hash; absent until the version list answers. */
  onlineHash?: string;
  /** The version list failed to load: the online state and `usedVersions` are unconfirmed. */
  versionsError?: string;
  /** Retry the version list from inside the dialog. */
  onReloadVersions?: () => void;
  /** Labels already published; publishing over one is refused before the request. */
  usedVersions: string[];
  changes: WorkflowChange[];
  /** Diagnostics errors block the publish (§6.10: errors block, warnings do not). */
  errorCount: number;
  warningCount: number;
  suggestedVersion: string;
  version: string;
  /** The mapped failure of the last publish attempt, if any. */
  error?: string;
  publishing?: boolean;
  onVersionChange: (version: string) => void;
  onPublish: () => void;
  onCancel: () => void;
  getContainer?: () => HTMLElement;
}

export function PublishDialog({
  open,
  workflow,
  onlineVersion,
  onlineHash,
  versionsError,
  usedVersions,
  changes,
  errorCount,
  warningCount,
  suggestedVersion,
  version,
  error,
  publishing = false,
  onVersionChange,
  onPublish,
  onCancel,
  onReloadVersions,
  getContainer
}: PublishDialogProps): React.ReactElement {
  const inputId = React.useId();
  const [fullDiffOpen, setFullDiffOpen] = React.useState(false);
  // Publishing closes the dialog while the full-diff modal may still be open;
  // the nested surface must not outlive the surface it belongs to.
  React.useEffect(() => {
    if (!open) setFullDiffOpen(false);
  }, [open]);
  const trimmed = version.trim();
  const taken = trimmed !== "" && usedVersions.includes(bareVersion(trimmed));
  const sameAsOnline = trimmed !== "" && onlineVersion !== undefined
    && bareVersion(trimmed) === bareVersion(onlineVersion);
  const blocked = publishing || trimmed === "" || taken || sameAsOnline || errorCount > 0;

  const featureSuggestion = suggestFeatureVersion(onlineVersion);
  // The same label twice is the same label: when it equals the online version
  // the hint must say 保存, even though the online label is also in `usedVersions`.
  const hint = trimmed === ""
    ? "填写新版本号后才能发布。已发布过的版本号不能再用于不同内容。"
    : sameAsOnline
      ? "与线上版本相同：原地更新请用 保存，发布必须换一个新版本号。"
      : taken
        ? "该版本号已发布过；同一版本号不能承载不同内容，请换一个。"
        : suggestedVersion
          ? `建议 ${suggestedVersion}（修订）${featureSuggestion ? ` / ${featureSuggestion}（功能）` : ""}。已发布过的版本号不能再用于不同内容。`
          : "版本号按发布标签填写；已发布过的版本号不能再用于不同内容。";

  // One row renderer for both surfaces: the dialog's capped list and the
  // 查看完整 diff modal show the same array, so they cannot disagree.
  const renderChange = (change: WorkflowChange): React.ReactElement => (
    <div
      className="xflow-editor-publish-dialog__diff-row"
      data-change-field={change.fields[0]?.field}
      data-change-kind={change.kind}
      data-change-target={change.target}
      key={change.id}
    >
      <span aria-hidden="true" className="xflow-editor-publish-dialog__diff-sign">
        {KIND_META[change.kind].sign}
      </span>
      <span className="xflow-editor-publish-dialog__diff-target">{change.target}</span>
      <span className="xflow-editor-publish-dialog__diff-fields">
        {change.fields.map((field, index) => (
          <span
            className="xflow-editor-publish-dialog__diff-field"
            data-change-field={field.field}
            key={`${field.field}:${index}`}
          >
            <span className="xflow-editor-publish-dialog__diff-field-name">{field.field}</span>
            {field.before !== undefined ? (
              <span className="xflow-editor-publish-dialog__diff-before" title={field.before}>
                {field.before}
              </span>
            ) : null}
            {field.before !== undefined && field.after !== undefined ? (
              <span aria-hidden="true" className="xflow-editor-publish-dialog__diff-arrow">
                →
              </span>
            ) : null}
            {field.after !== undefined ? (
              <span className="xflow-editor-publish-dialog__diff-after" title={field.after}>
                {field.after}
              </span>
            ) : null}
          </span>
        ))}
      </span>
    </div>
  );

  return (
    <Modal
      centered={false}
      className="xflow-editor-publish-dialog"
      classNames={{
        body: "xflow-editor-publish-dialog__body",
        container: "xflow-editor-publish-dialog__content",
        footer: "xflow-editor-publish-dialog__footer",
        header: "xflow-editor-publish-dialog__header"
      }}
      getContainer={getContainer ?? (() => document.body)}
      open={open}
      style={{ top: 64 }}
      title={
        <span className="xflow-editor-publish-dialog__title">
          <span aria-hidden="true" className="xflow-editor-publish-dialog__icon">
            <UploadOutlined />
          </span>
          <span className="xflow-editor-publish-dialog__heading-group">
            <span className="xflow-editor-publish-dialog__heading">发布新版本</span>
            <span className="xflow-editor-publish-dialog__sub">
              {workflow.id ?? "未保存"} · 线上 {onlineVersion
                ? versionLabel(onlineVersion)
                : versionsError ? "版本未知" : "尚未发布"}
            </span>
          </span>
        </span>
      }
      width={560}
      footer={[
        <Button
          className="xflow-editor-publish-dialog__full-diff"
          key="full-diff"
          size="small"
          onClick={() => setFullDiffOpen(true)}
        >
          查看完整 diff
        </Button>,
        <Button key="cancel" size="small" onClick={onCancel}>
          取消
        </Button>,
        <Button
          data-publish-version={trimmed}
          disabled={blocked}
          key="publish"
          loading={publishing}
          size="small"
          type="primary"
          onClick={onPublish}
        >
          {trimmed ? `发布 ${versionLabel(trimmed)}` : "发布"}
        </Button>
      ]}
      onCancel={onCancel}
    >
      <dl className="xflow-editor-publish-dialog__kv">
        <dt>线上版本</dt>
        <dd data-versions-error={versionsError ? "" : undefined}>
          {onlineVersion ? (
            <>
              <span>{versionLabel(onlineVersion)}</span>
              <span aria-hidden="true"> · </span>
              <span className="xflow-editor-publish-dialog__mono">{versionHashPrefix(onlineHash)}</span>
            </>
          ) : versionsError ? (
            "无法确认（版本历史加载失败）"
          ) : (
            "尚未发布"
          )}
          {versionsError && onReloadVersions ? (
            <Button className="xflow-editor-publish-dialog__retry" size="small" type="link" onClick={onReloadVersions}>
              重试
            </Button>
          ) : null}
        </dd>
      </dl>
      {versionsError ? (
        <p className="xflow-editor-publish-dialog__hint" data-versions-error-note="">
          版本历史加载失败，线上 hash 与已用版本号无法确认。
        </p>
      ) : null}

      <div className="xflow-editor-publish-dialog__field">
        <label className="xflow-editor-publish-dialog__label" htmlFor={inputId}>
          新版本号 <span aria-hidden="true">*</span>
        </label>
        <Input
          aria-label="新版本号"
          id={inputId}
          size="small"
          value={version}
          onChange={(event) => onVersionChange(event.target.value)}
        />
        <p className="xflow-editor-publish-dialog__hint">{hint}</p>
      </div>

      <div className="xflow-editor-publish-dialog__field">
        {/* The counts belong to the change list's heading, not under it: they
            summarize the list rather than continuing it. */}
        <div className="xflow-editor-publish-dialog__field-head">
          <label className="xflow-editor-publish-dialog__label">变更</label>
          <span className="xflow-editor-publish-dialog__counts" data-diff-counts="">
            {KIND_ORDER.map((kind) => {
              const count = changes.filter((change) => change.kind === kind).length;
              if (count === 0) return null;
              return (
                <span
                  className="xflow-editor-chip"
                  data-count={count}
                  data-diff-count={kind}
                  data-tone={KIND_META[kind].tone}
                  key={kind}
                >
                  <span aria-hidden="true">{KIND_META[kind].sign}</span>
                  {count} {KIND_META[kind].label}
                </span>
              );
            })}
          </span>
        </div>
        <div className="xflow-editor-publish-dialog__diff" data-diff="">
          {changes.length === 0 ? (
            <p className="xflow-editor-publish-dialog__empty">没有相对线上的变更。</p>
          ) : (
            changes.map(renderChange)
          )}
        </div>
      </div>

      {errorCount > 0 ? (
        <Banner
          strong="校验未通过"
          text={`校验有 ${errorCount} 个错误，修正后才能发布。发布后新触发的执行使用新版本，在途执行不受影响。`}
          tone="danger"
        />
      ) : warningCount > 0 ? (
        <Banner
          strong="可以发布"
          text={`校验有 ${warningCount} 个警告。发布后新触发的执行使用新版本，在途执行不受影响。`}
          tone="warning"
        />
      ) : (
        <Banner
          strong="校验通过"
          text="发布后新触发的执行使用新版本，在途执行不受影响。"
          tone="neutral"
        />
      )}
      {error ? <Banner strong="发布失败" text={error} tone="danger" /> : null}
      <Modal
        centered={false}
        className="xflow-editor-publish-dialog xflow-editor-publish-dialog--full"
        footer={null}
        getContainer={getContainer ?? (() => document.body)}
        open={fullDiffOpen}
        style={{ top: 64 }}
        title="完整变更"
        width={720}
        onCancel={() => setFullDiffOpen(false)}
      >
        <div
          className="xflow-editor-publish-dialog__diff xflow-editor-publish-dialog__diff--full"
          data-diff-full=""
        >
          {changes.length === 0 ? (
            <p className="xflow-editor-publish-dialog__empty">没有相对线上的变更。</p>
          ) : (
            changes.map(renderChange)
          )}
        </div>
      </Modal>
    </Modal>
  );
}
