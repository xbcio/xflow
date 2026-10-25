import { UploadIcon } from '@xflow/icons';
import { XFlowApiError, type RegisterWorkflowResult, type XFlowApiClient } from '@xflow/api';
import type { ParamIssue, WorkflowDef } from '@xflow/core';
import { Alert, Input, Modal, Segmented, Upload } from 'antd';
import { load as loadYaml, type YAMLException } from 'js-yaml';
import { useState } from 'react';

/** Frontend read limit for one import source; larger files never produce a request. */
export const MAX_IMPORT_BYTES = 1024 * 1024;

type ImportMode = 'file' | 'paste';
type ImportFormat = 'json' | 'yaml';

/** One local or server-side failure shown above the dialog footer. */
interface ImportFailure {
  title: string;
  detail?: string;
  /** ParamSpec findings carried by a 400 `workflow_param_invalid`. */
  issues?: ParamIssue[];
  /** Server errors put their message behind a disclosure, never in the toast. */
  expandable?: boolean;
  /** A local parse error points the editor at the textarea (AD-15). */
  fieldError?: boolean;
}

interface SelectedFile {
  name: string;
  size: number;
  text: string;
}

export interface ImportWorkflowDialogProps {
  open: boolean;
  api: Pick<XFlowApiClient, 'createWorkflow'>;
  onClose: () => void;
  onImported: (result: RegisterWorkflowResult) => void;
}

function portalContainer(): HTMLElement {
  return document.querySelector<HTMLElement>('.xflow-admin') ?? document.body;
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).length;
}

/** FileReader, not `File.text()`: jsdom (and older hosts) lack Blob.text(). */
function readFileText(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error ?? new Error('file read failed'));
    reader.onload = () => {
      if (typeof reader.result === 'string') {
        resolve(reader.result);
        return;
      }
      reject(new Error('unexpected file read result'));
    };
    reader.readAsText(file);
  });
}

function formatBytes(size: number): string {
  if (size >= MAX_IMPORT_BYTES) return `${(size / MAX_IMPORT_BYTES).toFixed(1)} MiB`;
  if (size >= 1024) return `${Math.ceil(size / 1024)} KiB`;
  return `${size} B`;
}

/** 1-based line/column for a JSON.parse `position` (a 0-based UTF-16 offset). */
function lineColumn(text: string, position: number): { line: number; column: number } {
  const end = Math.min(Math.max(position, 0), text.length);
  let line = 1;
  let lineStart = 0;
  for (let index = 0; index < end; index += 1) {
    if (text.charCodeAt(index) === 10) {
      line += 1;
      lineStart = index + 1;
    }
  }
  return { line, column: end - lineStart + 1 };
}

/**
 * `JSON.parse` plus the location V8 sometimes omits: when the engine reports a
 * byte offset (or only "unexpected end"), turn it into a line/column the user
 * can jump to. The exception text is kept verbatim; the file is only ever data.
 */
function parseJson(text: string): { ok: true; value: unknown } | { ok: false; failure: ImportFailure } {
  try {
    return { ok: true, value: JSON.parse(text) as unknown };
  } catch (reason) {
    const raw = reason instanceof Error && reason.message ? reason.message : '无法解析 JSON。';
    const at = /position\s+(\d+)/i.exec(raw);
    if (at) {
      const { line, column } = lineColumn(text, Number(at[1]));
      return { ok: false, failure: { title: 'JSON 解析失败', detail: `第 ${line} 行第 ${column} 列：${raw}` } };
    }
    if (/unexpected end of json input/i.test(raw)) {
      const { line, column } = lineColumn(text, text.length);
      return { ok: false, failure: { title: 'JSON 解析失败', detail: `第 ${line} 行第 ${column} 列（文本末尾）：${raw}` } };
    }
    return { ok: false, failure: { title: 'JSON 解析失败', detail: raw } };
  }
}

/**
 * YAML via js-yaml 4 `load`: the default (safe) schema has no constructor or
 * function tags, so a DSL document can only ever produce plain data. The
 * exception's `mark` is 0-based; it is shown 1-based.
 */
function parseYaml(text: string): { ok: true; value: unknown } | { ok: false; failure: ImportFailure } {
  try {
    return { ok: true, value: loadYaml(text) };
  } catch (reason) {
    const exception = reason as Partial<YAMLException> | undefined;
    if (exception && exception.name === 'YAMLException') {
      const raw = exception.reason || exception.message || '无法解析 YAML。';
      const mark = exception.mark;
      if (mark && typeof mark.line === 'number' && typeof mark.column === 'number') {
        return {
          ok: false,
          failure: { title: 'YAML 解析失败', detail: `第 ${mark.line + 1} 行第 ${mark.column + 1} 列：${raw}` },
        };
      }
      return { ok: false, failure: { title: 'YAML 解析失败', detail: raw } };
    }
    const raw = reason instanceof Error && reason.message ? reason.message : '无法解析 YAML。';
    return { ok: false, failure: { title: 'YAML 解析失败', detail: raw } };
  }
}

/** File extension is the only format hint available before the bytes are read. */
function formatHint(fileName: string | undefined): ImportFormat | undefined {
  if (!fileName) return undefined;
  const lower = fileName.toLowerCase();
  if (lower.endsWith('.json')) return 'json';
  if (lower.endsWith('.yaml') || lower.endsWith('.yml')) return 'yaml';
  return undefined;
}

/**
 * Files start from their extension; pasted text starts from its first
 * significant character (`{` is JSON, anything else is tried as YAML first).
 * Whichever format is tried first, the other one is the fallback, so JSON
 * saved as `.yaml` and flow-style YAML saved as `.json` both still import.
 * Only when both parsers reject the text is the first failure reported.
 */
function parseSource(
  text: string,
  hint: ImportFormat | undefined,
): { ok: true; value: unknown } | { ok: false; failure: ImportFailure } {
  const primary: ImportFormat = hint ?? (text.trimStart().startsWith('{') ? 'json' : 'yaml');
  const first = primary === 'json' ? parseJson(text) : parseYaml(text);
  if (first.ok) return first;
  const second = primary === 'json' ? parseYaml(text) : parseJson(text);
  return second.ok ? second : first;
}

/**
 * Structural budget applied to the parsed document before it can reach the
 * API client's `JSON.stringify`. YAML aliases are resolved by reference, so a
 * sub-kilobyte document of anchors can expand to a huge value on
 * serialization; the walk re-visits every shared reference (which is exactly
 * the expansion we must count) and aborts the moment a limit is crossed.
 * A self-referential value (which safe YAML loaders do not produce, but plain
 * objects can) is bounded by the depth limit, so no visited-set is needed.
 */
const MAX_STRUCTURE_NODES = 200_000;
const MAX_STRUCTURE_DEPTH = 200;

/** Internal signal for the short-circuiting walk; never leaves this module. */
class StructureBudgetError extends Error {}

function walkWithinStructureBudget(root: unknown): void {
  const stack: Array<{ value: unknown; depth: number }> = [{ value: root, depth: 0 }];
  let visited = 0;
  while (stack.length > 0) {
    const frame = stack.pop() as { value: unknown; depth: number };
    visited += 1;
    if (visited > MAX_STRUCTURE_NODES) {
      throw new StructureBudgetError(
        `展开后的节点数超过上限 ${MAX_STRUCTURE_NODES.toLocaleString('en-US')}。`,
      );
    }
    if (frame.depth > MAX_STRUCTURE_DEPTH) {
      throw new StructureBudgetError(`嵌套深度超过上限 ${MAX_STRUCTURE_DEPTH} 层。`);
    }
    if (Array.isArray(frame.value)) {
      for (let index = 0; index < frame.value.length; index += 1) {
        stack.push({ value: frame.value[index], depth: frame.depth + 1 });
      }
    } else if (frame.value !== null && typeof frame.value === 'object') {
      const record = frame.value as Record<string, unknown>;
      for (const key of Object.keys(record)) {
        stack.push({ value: record[key], depth: frame.depth + 1 });
      }
    }
  }
}

/** `undefined` when the document fits, otherwise the failure to show. */
function structureBudgetFailure(root: unknown): ImportFailure | undefined {
  try {
    walkWithinStructureBudget(root);
    return undefined;
  } catch (reason) {
    if (reason instanceof StructureBudgetError) {
      return { title: '定义结构过深或展开后过大', detail: reason.message };
    }
    throw reason;
  }
}

/**
 * The local pre-flight only covers what would make registration meaningless
 * (wrong root, wrong `nodes`, missing `name`). Everything else — node schemas,
 * connections, compilation — stays server-side: POST /v1/workflows compiles and
 * returns `warnings`/`param_issues`, so re-implementing those rules here would
 * drift from the compiler.
 *
 * Identity is server-issued: a client-supplied `id` is rejected by the server,
 * and `namespace` is resolved from the authenticated principal. Both are
 * dropped instead of being forwarded. Every other field is passed through
 * unchanged.
 */
function registrationPayload(
  value: unknown,
): { ok: true; workflow: WorkflowDef } | { ok: false; failure: ImportFailure } {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return { ok: false, failure: { title: '顶层必须是对象', detail: 'DSL 顶层应为 { ... } 对象（JSON 对象或 YAML 映射）。' } };
  }
  const record = value as Record<string, unknown>;
  if (!Array.isArray(record.nodes)) {
    return { ok: false, failure: { title: 'nodes 必须是数组', detail: 'DSL 顶层的 nodes 字段必须是节点数组。' } };
  }
  if (typeof record.name !== 'string' || record.name.trim() === '') {
    return { ok: false, failure: { title: 'name 必须是非空字符串', detail: 'DSL 顶层需要一个非空的 name。' } };
  }
  const payload: Record<string, unknown> = { ...record };
  delete payload.id;
  delete payload.namespace;
  return { ok: true, workflow: payload as WorkflowDef };
}

/** OpenAPI error codes for POST /v1/workflows, each pointing at a different fix. */
function importFailure(reason: unknown): ImportFailure {
  if (reason instanceof XFlowApiError) {
    const failure = (title: string): ImportFailure => ({
      title,
      detail: reason.message,
      expandable: true,
      ...(reason.paramIssues && reason.paramIssues.length > 0 ? { issues: reason.paramIssues } : {}),
    });
    switch (reason.code) {
      case 'workflow_name_conflict':
        return failure('名称冲突：已有同名工作流。');
      case 'workflow_conflict':
        return failure('定义冲突：同名同版本已存在不同定义，请修改 version 或打开现有工作流。');
      case 'workflow_version_immutable':
        return failure('版本不可变：该版本标签已发布过不同定义，请换一个新的 version。');
      case 'workflow_invalid':
        return failure('定义无效：服务端拒绝了该工作流定义。');
      case 'workflow_compile_failed':
        return failure('编译失败：服务端无法编译该工作流定义。');
      case 'workflow_param_invalid':
        return failure('参数校验失败：服务端以严格模式拒绝了节点参数。');
      default:
        break;
    }
    if (reason.status === 401) return failure('登录已过期，请重新登录后重试。');
    if (reason.status === 403) return failure('当前凭据缺少工作流注册权限。');
    if (reason.status === 429) return failure('请求过于频繁，请稍后重试。');
    return failure('导入失败：服务端拒绝了该定义。');
  }
  return {
    title: '导入失败，请稍后重试。',
    ...(reason instanceof Error && reason.message ? { detail: reason.message } : {}),
  };
}

/**
 * ED-01 导入 DSL. Two sources — a local `.json` / `.yaml` / `.yml` file or
 * pasted text — feed one pre-flight and the existing `POST /v1/workflows`
 * wrapper. JSON is parsed first when the text starts with `{`; every other
 * document is tried as YAML first. A failure falls back to the other parser
 * before it is reported, so the format only has to be *plausible*, not exact.
 */
export function ImportWorkflowDialog({ open, api, onClose, onImported }: ImportWorkflowDialogProps) {
  const [mode, setMode] = useState<ImportMode>('file');
  const [pasted, setPasted] = useState('');
  const [file, setFile] = useState<SelectedFile>();
  const [failure, setFailure] = useState<ImportFailure>();
  const [submitting, setSubmitting] = useState(false);

  // AD-15: 导入 stays disabled until a source carries bytes; parse errors mark
  // the textarea and point it at the failure alert.
  const hasContent = mode === 'file' ? file !== undefined : pasted.trim() !== '';

  // Closing always discards the selected bytes and the last error, so a reopen
  // starts from a clean dialog even when the previous import failed.
  const reset = () => {
    setMode('file');
    setPasted('');
    setFile(undefined);
    setFailure(undefined);
    setSubmitting(false);
  };

  const close = () => {
    if (submitting) return;
    reset();
    onClose();
  };

  /**
   * Upload.Dragger only carries the file here (beforeUpload returns false), so
   * the text never leaves the page: the size and structure budgets below are
   * the same pre-flight the native input used.
   */
  const selectFile = async (selected: File) => {
    setFile(undefined);
    setFailure(undefined);

    const hint = formatHint(selected.name);
    if (!hint) {
      setFailure({
        title: '仅支持 .json / .yaml / .yml 文件',
        detail: `「${selected.name}」不是受支持的 DSL 文件。`,
      });
      return;
    }
    if (selected.size > MAX_IMPORT_BYTES) {
      setFailure({
        title: '文件超过 1 MiB 上限',
        detail: `「${selected.name}」为 ${formatBytes(selected.size)}，请裁剪后再导入。`,
      });
      return;
    }
    try {
      const text = await readFileText(selected);
      if (byteLength(text) > MAX_IMPORT_BYTES) {
        setFailure({
          title: '文件超过 1 MiB 上限',
          detail: `「${selected.name}」的文本内容超过 1 MiB，请裁剪后再导入。`,
        });
        return;
      }
      setFile({ name: selected.name, size: selected.size, text });
    } catch {
      setFailure({ title: '文件读取失败', detail: '无法读取所选文件，请重新选择。' });
    }
  };

  const submit = async () => {
    if (submitting) return;
    setFailure(undefined);

    const text = mode === 'file' ? file?.text : pasted;
    if (text === undefined || text.trim() === '') {
      setFailure({ title: '请先选择 DSL 文件或粘贴 DSL 内容' });
      return;
    }
    if (byteLength(text) > MAX_IMPORT_BYTES) {
      setFailure({
        title: '内容超过 1 MiB 上限',
        detail: '导入内容超过 1 MiB，请裁剪后再试。',
      });
      return;
    }

    const parsed = parseSource(text, mode === 'file' ? formatHint(file?.name) : undefined);
    if (!parsed.ok) {
      setFailure({ ...parsed.failure, fieldError: true });
      return;
    }
    // Aliases expand when the payload is serialized; check the parsed graph
    // before anything can send it to `JSON.stringify`.
    const oversized = structureBudgetFailure(parsed.value);
    if (oversized) {
      setFailure(oversized);
      return;
    }
    const payload = registrationPayload(parsed.value);
    if (!payload.ok) {
      setFailure(payload.failure);
      return;
    }

    setSubmitting(true);
    try {
      const result = await api.createWorkflow(payload.workflow);
      if (!result.workflowId) {
        setSubmitting(false);
        setFailure({ title: '导入失败：服务端未返回工作流 id' });
        return;
      }
      reset();
      onImported(result);
    } catch (reason) {
      setSubmitting(false);
      setFailure(importFailure(reason));
    }
  };

  return (
    <Modal
      open={open}
      title="导入 DSL"
      okText="导入"
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ disabled: submitting || !hasContent }}
      cancelButtonProps={{ disabled: submitting }}
      closable={!submitting}
      keyboard={!submitting}
      mask={{ closable: !submitting }}
      getContainer={portalContainer}
      rootClassName="xflow-admin-modal-root"
      onOk={() => {
        void submit();
      }}
      onCancel={close}
      destroyOnHidden
    >
      <div className="flex flex-col gap-3">
        <p className="xflow-admin-hint">支持 JSON（.json）与 YAML（.yaml / .yml）定义。</p>

        <Segmented<ImportMode>
          block
          aria-label="导入方式"
          value={mode}
          disabled={submitting}
          onChange={(value) => {
            setMode(value);
            setFailure(undefined);
          }}
          options={[
            { label: '选择文件', value: 'file' },
            { label: '粘贴内容', value: 'paste' },
          ]}
        />

        {mode === 'file' ? (
          <div className="flex flex-col gap-2">
            <Upload.Dragger
              accept=".json,.yaml,.yml,application/json,application/yaml,text/yaml"
              aria-label="选择 DSL 文件"
              multiple={false}
              showUploadList={false}
              disabled={submitting}
              // Read the bytes locally; nothing is uploaded to the server by
              // the control itself (registration is the dialog's own POST).
              beforeUpload={(selected) => {
                void selectFile(selected);
                return false;
              }}
            >
              <p className="ant-upload-drag-icon">
                <UploadIcon />
              </p>
              <p className="ant-upload-text">点击或拖拽 DSL 文件到此处</p>
              <p className="ant-upload-hint">支持 .json / .yaml / .yml，最大 1 MiB。</p>
            </Upload.Dragger>
            {file ? (
              <p className="xflow-admin-hint" data-file={file.name}>
                {file.name} · {formatBytes(file.size)}
              </p>
            ) : (
              <p className="xflow-admin-hint">读取后的文件只会作为文本解析，不会被执行。</p>
            )}
          </div>
        ) : (
          <Input.TextArea
            aria-label="粘贴 DSL 定义"
            aria-invalid={failure?.fieldError || undefined}
            aria-describedby={failure ? 'xflow-import-error' : undefined}
            status={failure?.fieldError ? 'error' : undefined}
            rows={10}
            value={pasted}
            disabled={submitting}
            placeholder={'name: 工作流名称\nnodes: []'}
            onChange={(event) => {
              setPasted(event.target.value);
              setFailure(undefined);
            }}
          />
        )}

        {failure ? (
          <Alert
            id="xflow-import-error"
            className="xflow-admin-import-error"
            type="error"
            showIcon
            title={failure.title}
            description={
              <div className="flex flex-col gap-1">
                {failure.issues && failure.issues.length > 0 ? (
                  <ul className="m-0 pl-4">
                    {failure.issues.map((issue, index) => (
                      <li key={`${issue.node}-${issue.path}-${index}`}>
                        {issue.node}
                        {issue.path}：{issue.message}
                      </li>
                    ))}
                  </ul>
                ) : null}
                {failure.detail ? (
                  failure.expandable ? (
                    <details>
                      <summary>服务端详情</summary>
                      <pre className="m-0 max-h-40 overflow-auto whitespace-pre-wrap break-all text-xs">
                        {failure.detail}
                      </pre>
                    </details>
                  ) : (
                    <span>{failure.detail}</span>
                  )
                ) : null}
              </div>
            }
          />
        ) : null}
      </div>
    </Modal>
  );
}
