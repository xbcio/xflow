import * as React from "react";
import { ShieldIcon, WarnIcon, XMarkIcon } from "@xflow/icons";
import { Button } from "antd";

/**
 * §6.5.1 / §6.10 banners. One shell (tone icon, strong lead, text, actions) for
 * the three states the editor can be in besides "all good": node types failed
 * to load (warning), the save lost a version race (danger) and the viewer may
 * not edit (neutral). The stack floats at the top of the canvas and never
 * pushes the canvas around, so the preview is not resized or refitted.
 */

export type BannerTone = "warning" | "danger" | "neutral";

export interface BannerAction {
  key: string;
  label: string;
  onClick: () => void;
}

export interface BannerSpec {
  key: string;
  tone: BannerTone;
  strong: string;
  text: string;
  /** Native tooltip on the text, for detail that does not belong in the sentence. */
  textTitle?: string;
  actions?: BannerAction[];
}

const toneIcon: Record<BannerTone, React.ReactNode> = {
  warning: <WarnIcon />,
  danger: <XMarkIcon />,
  neutral: <ShieldIcon />
};

export function Banner({
  tone,
  strong,
  text,
  textTitle,
  actions = [],
  onFocusLost
}: Omit<BannerSpec, "key"> & {
  /** Called as the banner goes away while focus is inside it (an action was just pressed). */
  onFocusLost?: () => void;
}): React.ReactElement {
  const rootRef = React.useRef<HTMLDivElement | null>(null);
  const focusLostRef = React.useRef(onFocusLost);
  focusLostRef.current = onFocusLost;
  // Layout-effect cleanup runs before the node leaves the document, while
  // `activeElement` is still the pressed button: hand focus on instead of
  // letting it fall to <body>.
  React.useLayoutEffect(
    () => () => {
      if (rootRef.current?.contains(document.activeElement)) focusLostRef.current?.();
    },
    []
  );
  return (
    // alert for the states that need attention now, status for the quiet one.
    <div className="xflow-editor-banner" data-tone={tone} ref={rootRef} role={tone === "neutral" ? "status" : "alert"}>
      {/* The wrapper carries the tone colour; the sprite glyph strokes in
          currentColor, so the icon itself inherits it. */}
      <span aria-hidden="true" className="xflow-editor-banner__icon">
        {toneIcon[tone]}
      </span>
      <strong className="xflow-editor-banner__strong">{strong}</strong>
      <span className="xflow-editor-banner__text" title={textTitle}>
        {text}
      </span>
      {actions.length > 0 ? (
        <span className="xflow-editor-banner__actions">
          {actions.map((action) => (
            // autoInsertSpace off: AntD would render the two-character 重试 as "重 试".
            <Button
              autoInsertSpace={false}
              className="xflow-editor-banner__action"
              key={action.key}
              size="small"
              onClick={action.onClick}
            >
              {action.label}
            </Button>
          ))}
        </span>
      ) : null}
    </div>
  );
}

/** Banners stacked top-down, most urgent first; renders nothing when there are none. */
export function BannerStack({
  banners,
  onFocusLost
}: {
  banners: BannerSpec[];
  onFocusLost?: () => void;
}): React.ReactElement | null {
  if (banners.length === 0) return null;
  return (
    <div className="xflow-editor-banners">
      {banners.map(({ key, ...banner }) => (
        <Banner key={key} {...banner} onFocusLost={onFocusLost} />
      ))}
    </div>
  );
}

export interface EditorBannerInput {
  nodeTypesError?: { message?: string };
  onRetryNodeTypes?: () => void;
  saveConflict: boolean;
  onReloadLatest?: () => void;
  onSaveAsCopy?: () => void;
  readOnlyReason?: string;
  /** The archived version the host loaded read-only; draws the return entry (§6.10). */
  historicalVersion?: string;
  onExitHistory?: () => void;
}

/** The banners the current state calls for, in stacking order. A button exists only when its handler does. */
export function editorBanners(input: EditorBannerInput): BannerSpec[] {
  const banners: BannerSpec[] = [];
  if (input.saveConflict) {
    const actions: BannerAction[] = [];
    if (input.onReloadLatest) actions.push({ key: "reload", label: "重新加载", onClick: input.onReloadLatest });
    if (input.onSaveAsCopy) actions.push({ key: "copy", label: "另存为副本", onClick: input.onSaveAsCopy });
    banners.push({
      key: "conflict",
      tone: "danger",
      strong: "保存失败：工作流已被更新",
      text: "你基于的版本已不是最新。本地修改仍保留。",
      actions
    });
  }
  if (input.nodeTypesError) {
    banners.push({
      key: "node-types",
      tone: "warning",
      strong: "节点类型加载失败",
      text: "参数仅可按 JSON 编辑；画布与连线不受影响。",
      textTitle: input.nodeTypesError.message || undefined,
      actions: input.onRetryNodeTypes ? [{ key: "retry", label: "重试", onClick: input.onRetryNodeTypes }] : []
    });
  }
  if (input.historicalVersion) {
    // The archived view owns the read-only explanation: its reason is the
    // version itself, and the way out is the host's, not a scope being missing.
    const actions: BannerAction[] = input.onExitHistory
      ? [{ key: "exit-history", label: "退出历史", onClick: input.onExitHistory }]
      : [];
    banners.push({
      key: "history",
      tone: "neutral",
      strong: `历史版本 v${input.historicalVersion.replace(/^v/, "")}`,
      text: "正在查看归档版本，编辑器只读。回滚或退出历史后可继续编辑。",
      actions
    });
  } else if (input.readOnlyReason) {
    banners.push({ key: "read-only", tone: "neutral", strong: "只读", text: input.readOnlyReason });
  }
  return banners;
}

/**
 * The 409 `workflow_conflict` of a save: another writer replaced the version
 * this draft was based on. Read structurally (`status` + `code`, what an
 * `XFlowApiError` carries) so the editor does not depend on @xflow/api. The
 * sibling 409 `workflow_name_conflict` is a different problem (a name clash)
 * and must not claim the draft is stale.
 */
export function isWorkflowConflictError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const { status, code } = error as { status?: unknown; code?: unknown };
  return status === 409 && code === "workflow_conflict";
}
