/**
 * Version labels and the 409/404 codes of the publish / rollback routes
 * (§6.10). The editor does not depend on @xflow/api, so every failure is read
 * structurally (`status` + `code`, the shape `XFlowApiError` carries) exactly
 * like `isWorkflowConflictError` reads the save conflict.
 */

function errorRecord(error: unknown): Record<string, unknown> | undefined {
  return typeof error === "object" && error !== null ? (error as Record<string, unknown>) : undefined;
}

function errorCode(error: unknown): string | undefined {
  const code = errorRecord(error)?.code;
  return typeof code === "string" ? code : undefined;
}

function errorMessageText(error: unknown): string | undefined {
  const message = errorRecord(error)?.message;
  return typeof message === "string" && message.trim() ? message : undefined;
}

/**
 * The next version label when none is chosen: the current label's patch +1
 * (§6.10). A label that is not `major.minor.patch` has no patch to bump, so the
 * dialog asks for one instead of inventing a scheme; an unpublished workflow
 * starts at 0.1.0. The leading `v` the chip draws is not part of the label.
 */
export function suggestNextVersion(version?: string): string {
  const label = (version ?? "").trim().replace(/^v/, "");
  if (!label) return "0.1.0";
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(label);
  if (!match) return "";
  return `${match[1]}.${match[2]}.${Number(match[3]) + 1}`;
}

/** `v` + the label, unless the label already carries it. */
export function versionLabel(version: string): string {
  const label = version.trim();
  return label.startsWith("v") ? label : `v${label}`;
}

/**
 * The feature (minor) bump the publish hint offers next to the patch one
 * (`1.2.0` → `1.3.0`). Empty when the label has no semver to bump, so the hint
 * omits it instead of inventing a scheme.
 */
export function suggestFeatureVersion(version?: string): string {
  const label = (version ?? "").trim().replace(/^v/, "");
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(label);
  if (!match) return "";
  return `${match[1]}.${Number(match[2]) + 1}.0`;
}

/** A label without its `v` prefix, for comparing two spellings of one version. */
export function bareVersion(version: string): string {
  return version.trim().replace(/^v/, "");
}

/**
 * The version hash shortened for a list row: its leading bytes plus an
 * ellipsis. Short hashes are drawn whole so a test fixture cannot be mistaken
 * for a truncated real hash.
 */
export function versionHashPrefix(hash?: string): string {
  const value = (hash ?? "").trim();
  if (!value) return "hash 不可用";
  return value.length <= 10 ? value : `${value.slice(0, 8)}…`;
}

const PUBLISH_ERROR_TEXT: Record<string, string> = {
  workflow_version_immutable: "该版本号已被发布过不同内容，请换一个新版本号再发布。",
  workflow_name_conflict: "同一命名空间已有同名工作流，请先改名再发布。",
  workflow_conflict: "线上版本已被更新，请重新加载最新定义后再发布。"
};

/** What the publish dialog says for a rejected publish (§6.10, §6.5.1). */
export function publishErrorText(error: unknown): string {
  const code = errorCode(error);
  if (code !== undefined && PUBLISH_ERROR_TEXT[code]) return PUBLISH_ERROR_TEXT[code];
  return errorMessageText(error) ?? "发布失败";
}

/**
 * Why a rollback row must stay disabled: the server refused this version with
 * `workflow_version_unavailable` (the current compiler cannot reproduce the
 * hash it was published with). Undefined for every other failure, which is
 * reported once rather than pinned to the row.
 */
export function versionUnavailableReason(error: unknown): string | undefined {
  return errorCode(error) === "workflow_version_unavailable"
    ? "当前编译器重算哈希不一致，服务端会拒绝回滚。"
    : undefined;
}

/** What the version list reports for a rollback that failed for another reason. */
export function rollbackErrorText(error: unknown): string {
  if (errorCode(error) === "workflow_version_not_found") {
    return "该版本已超出保留期，无法回滚。";
  }
  return errorMessageText(error) ?? "回滚失败";
}
