import * as React from "react";
import { CloseOutlined } from "@ant-design/icons";
import { Button, Input, Modal, Segmented } from "antd";
import {
  APPROVAL_MAX_CHAIN,
  APPROVAL_NODE_TYPE,
  approvalActionLabel,
  approvalActions,
  approvalCanDecide,
  approvalCanForce,
  approvalChain,
  approvalClosesSlot,
  approvalDecisions,
  approvalHasDecision,
  approvalIgnored,
  approvalIgnoredCount,
  approvalIgnoredReasonLabel,
  approvalParamsFrom,
  approvalProgress,
  approvalProgressText,
  approvalSignalName,
  approvalWaitText,
  type ApprovalDecisionAction,
  type ApprovalMode,
  type XFlowEditorCapabilities,
  type WorkflowNode
} from "@xflow/core";

import { NodeCategoryIcon } from "./categoryIcon";
import { nodeLabel, nodeName } from "./workflowOps";

/**
 * The structural execution-node slice the dialog reads. The editor must not
 * import `@xflow/api` (package boundary); the host's `ExecutionNodeDetail`
 * satisfies this shape, and the public `output` is where the node republishes
 * `decisions` / `ignored` (its private state slot is stripped by inspect).
 */
export interface XFlowApprovalExecutionNode {
  name: string;
  status: string;
  output?: Record<string, unknown>;
}

/** One delivered signal: the wire body of POST /v1/executions/{id}/signals. */
export interface XFlowApprovalSignal {
  name: string;
  data?: Record<string, unknown>;
}

export interface ApprovalDialogProps {
  open: boolean;
  executionId: string;
  /** Execution status; a paused execution is the one that can hold a queued signal. */
  executionStatus: string;
  /** The suspended approval node's execution record, when the host reports it. */
  executionNode?: XFlowApprovalExecutionNode;
  /** The workflow node definition (parameters and display label). */
  node?: WorkflowNode;
  currentUserSubject?: string;
  capabilities?: XFlowEditorCapabilities;
  /**
   * Delivers the decision. Omit and the dialog is read-only: the editor never
   * fabricates a successful submission.
   */
  onSignal?: (executionId: string, signal: XFlowApprovalSignal) => Promise<void> | void;
  /** DELETE /v1/executions/{id}/signals/{name}, for a not-yet-consumed signal. */
  onRevokeSignal?: (executionId: string, name: string) => Promise<void> | void;
  onClose: () => void;
  getContainer?: () => HTMLElement;
}

type SecondaryKind = "delegate" | "add_signer";

interface PendingSignal {
  name: string;
  action: string;
  /** Ledger lengths before the submit: growth is how acceptance is read back. */
  decisionsBefore: number;
  /** `ignored_count` before the submit; the trail is capped, the count is not. */
  ignoredCountBefore: number;
  /** Same-name refusals already on the (capped) trail before the submit. */
  ignoredNameBefore: number;
}

function messageOf(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

/** `HH:mm` in the viewer's zone; an unparseable timestamp is shown as written. */
function timeText(at: string | undefined): string {
  if (!at) return "";
  const date = new Date(at);
  if (Number.isNaN(date.getTime())) return at;
  return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function reasonText(reason: string): string {
  return approvalIgnoredReasonLabel[reason] ?? reason;
}

/** `timeout` + `timeout_action` in the words the node actually acts on. */
function timeoutPolicy(timeout: string | undefined, action: "route" | "reject"): string {
  if (!timeout) return "未配置超时";
  return action === "reject"
    ? `超时 ${timeout} 后自动驳回（rejected 出口）`
    : `超时 ${timeout} 后走 timeout 出口`;
}

/** The port's tone (baseline `wbApproval.ports`): approved green, rejected red, returned amber. */
const approvalChoiceTone: Record<ApprovalDecisionAction, "success" | "danger" | "warning"> = {
  approve: "success",
  reject: "danger",
  return: "warning"
};

function errorText(error: unknown): string {
  return messageOf(error, "提交失败");
}

/**
 * §6.12 approval dialog. Entry points live in the editor (canvas card 处理,
 * run bar's 等待审批 count, Inspector 运行 tab); this component owns only the
 * dialog and the decisions, and reads the ledger from the node's public output.
 *
 * The signal name is computed from the node name, mode and the *current user*
 * — never from a client-supplied identity — and a successful POST only means
 * "delivered". Acceptance is read back from the published ledger; a refusal is
 * read from `ignored` and shown as 未计入 rather than as success.
 */
export function ApprovalDialog({
  open,
  executionId,
  executionStatus,
  executionNode,
  node,
  currentUserSubject,
  capabilities,
  onSignal,
  onRevokeSignal,
  onClose,
  getContainer
}: ApprovalDialogProps): React.ReactElement {
  const [choice, setChoice] = React.useState<ApprovalDecisionAction>("approve");
  const [comment, setComment] = React.useState("");
  const [submitting, setSubmitting] = React.useState<string>();
  const [error, setError] = React.useState<string>();
  const [pending, setPending] = React.useState<PendingSignal>();
  const [revoked, setRevoked] = React.useState(false);
  const [secondary, setSecondary] = React.useState<{
    kind: SecondaryKind;
    assignee: string;
    position: "before" | "after";
  }>();

  const params = approvalParamsFrom(node?.parameters);
  const output = executionNode?.output;
  const decisions = approvalDecisions(output);
  const ignored = approvalIgnored(output);
  const ignoredCount = approvalIgnoredCount(output);
  const mode: ApprovalMode | undefined = params.mode;
  const chain = params.approvers ? approvalChain(params.approvers, decisions) : [];
  const progress = approvalProgress(params.approvers ?? [], decisions);
  const subject = currentUserSubject?.trim() || undefined;
  const nodeKey = node ? nodeName(node) : executionNode?.name ?? "";
  const suspended = executionNode?.status === "suspended";
  const canSignal = capabilities?.execution !== false && Boolean(onSignal);

  const canDecide = Boolean(
    suspended &&
      canSignal &&
      params.approvers &&
      mode &&
      subject &&
      approvalCanDecide(mode, chain, decisions, subject)
  );
  const canForce = Boolean(
    suspended && canSignal && node && subject && approvalCanForce(params.forceApprovers, subject)
  );
  const canAmend = canDecide;

  // Why the decision controls are absent. The dialog hides rather than disables
  // them (§6.12): a visible button that cannot act reads as a broken permission.
  let readOnlyReason: string | undefined;
  let outsider = false;
  if (capabilities?.execution === false) {
    readOnlyReason = "没有 execution 权限，无法提交决定";
  } else if (!onSignal) {
    readOnlyReason = "宿主未提供 onSignal，无法提交决定";
  } else if (!executionNode) {
    readOnlyReason = "找不到该审批节点的运行记录";
  } else if (!suspended) {
    readOnlyReason = "节点已不处于挂起状态";
  } else if (!node || !params.approvers) {
    readOnlyReason = "审批配置由表达式决定，无法在界面计算审批链";
  } else if (!mode) {
    readOnlyReason = "审批模式无法在界面判断，请以服务端记录为准";
  } else if (!subject) {
    readOnlyReason = "无法确认当前用户身份";
  } else if (!canDecide) {
    if (!chain.includes(subject)) {
      outsider = true;
      // A force-only user is not "read-only": the force button below is theirs.
      readOnlyReason = canForce ? "你不是审批人，只能强制通过" : "你不是审批人，本次审批只读";
    } else if (approvalHasDecision(decisions, subject)) {
      readOnlyReason = "你已在此节点作出决定";
    } else {
      readOnlyReason = `未到你，等待 ${progress.current ?? "其他审批人"}`;
    }
  }

  // Acceptance / refusal of the last submission, read back from the ledger the
  // host polled in. A delivery that has not shown up anywhere yet is reported
  // as pending, never as success.
  const newDecisions = pending ? decisions.slice(pending.decisionsBefore) : [];
  const recorded = Boolean(
    pending && subject && newDecisions.some((decision) => decision.approver === subject && decision.action === pending.action)
  );
  // The server's ignored trail is capped at 20 and drops its oldest entry, so
  // its length can stay flat after a refusal. `ignored_count` always grows;
  // the trail only supplies the reason, matched to the signal we submitted so
  // another approver's refusal is not reported as ours.
  const newIgnored = pending
    ? ignored.filter((item) => item.signal === pending.name).slice(pending.ignoredNameBefore)
    : [];
  const refusedTotal = pending ? Math.max(0, ignoredCount - pending.ignoredCountBefore) : 0;
  const refused = newIgnored.length > 0;
  const refusedElsewhere = !refused && refusedTotal > 0;
  const title = node ? nodeLabel(node, 0) : nodeKey || "审批节点";
  const progressHead = approvalProgressText(mode, progress);
  // Only a computable, actionable name is shown: an outsider's own-subject
  // spelling is never armed by the node, and force travels under its own name.
  const signalPreview = canDecide
    ? approvalSignalName(nodeKey, mode, subject)
    : canForce
      ? approvalSignalName(nodeKey, mode, subject, true)
      : undefined;

  const submit = async (action: string, extra?: Record<string, unknown>): Promise<void> => {
    if (!subject || !onSignal || !node) return;
    const force = action === approvalActions.force;
    // `force` is answered before the mode checks server-side (approval.go) and
    // its name does not depend on the mode, so an expression mode must not
    // leave the force button silently dead.
    const name = approvalSignalName(nodeKey, mode, subject, force);
    if (!name) return;
    setSubmitting(action);
    setError(undefined);
    setRevoked(false);
    const decisionsBefore = decisions.length;
    const ignoredCountBefore = ignoredCount;
    const ignoredNameBefore = ignored.filter((item) => item.signal === name).length;
    try {
      const data: Record<string, unknown> = { action };
      const commentText = comment.trim();
      if (commentText) data.comment = commentText;
      if (extra) Object.assign(data, extra);
      await onSignal(executionId, { name, data });
      setPending({ name, action, decisionsBefore, ignoredCountBefore, ignoredNameBefore });
      setSecondary(undefined);
    } catch (submitError) {
      setError(errorText(submitError));
    } finally {
      setSubmitting(undefined);
    }
  };

  const revoke = async (): Promise<void> => {
    if (!pending || !onRevokeSignal) return;
    setSubmitting("revoke");
    setError(undefined);
    try {
      await onRevokeSignal(executionId, pending.name);
      setPending(undefined);
      setRevoked(true);
    } catch (revokeError) {
      setError(messageOf(revokeError, "撤回失败"));
    } finally {
      setSubmitting(undefined);
    }
  };

  const submitSecondary = (): void => {
    if (!secondary) return;
    const assignee = secondary.assignee.trim();
    if (!assignee) return;
    const extra: Record<string, unknown> = { assignee };
    if (secondary.kind === "add_signer") extra.position = secondary.position;
    void submit(secondary.kind, extra);
  };

  return (
    <Modal
      centered={false}
      className="xflow-editor-approval"
      classNames={{
        body: "xflow-editor-approval__body",
        container: "xflow-editor-approval__content",
        footer: "xflow-editor-approval__footer",
        header: "xflow-editor-approval__header"
      }}
      // The title draws the design's one close control; AntD's default would
      // add a second one in the corner (the baseline DialogHead has a single).
      closable={false}
      destroyOnHidden
      getContainer={getContainer ?? (() => document.body)}
      open={open}
      style={{ top: 64 }}
      title={
        <span className="xflow-editor-approval__title">
          <NodeCategoryIcon type={node?.type ?? APPROVAL_NODE_TYPE} />
          <span className="xflow-editor-approval__heading-group">
            <span className="xflow-editor-approval__heading">{title}</span>
            <span className="xflow-editor-approval__sub">
              <span className="xflow-editor-approval__mono">{nodeKey}</span>
              <span aria-hidden="true"> · </span>
              <span className="xflow-editor-approval__mono">{executionId}</span>
              <span aria-hidden="true"> · </span>
              <span>{progressHead}</span>
            </span>
          </span>
          <Button
            aria-label="关闭审批对话框"
            className="xflow-editor-approval__close"
            icon={<CloseOutlined />}
            size="small"
            type="text"
            onClick={onClose}
          />
        </span>
      }
      width={560}
      footer={
        <div className="xflow-editor-approval__foot">
          <span className="xflow-editor-approval__policy">
            {timeoutPolicy(params.timeout, params.timeoutAction)}
          </span>
          <span className="xflow-editor-approval__foot-gap" />
          <Button size="small" onClick={onClose}>
            关闭
          </Button>
          {canDecide ? (
            <Button
              disabled={Boolean(submitting)}
              loading={submitting === choice}
              size="small"
              type="primary"
              onClick={() => void submit(choice)}
            >
              提交{approvalActionLabel[choice]}
            </Button>
          ) : null}
        </div>
      }
      onCancel={onClose}
    >
      {node && params.approvers ? (
        <p className="xflow-editor-approval__wait">{approvalWaitText(mode, progress)}</p>
      ) : null}

      {readOnlyReason ? (
        <div className="xflow-editor-approval__banner" data-tone={outsider ? "neutral" : "warning"} role="note">
          <strong>{readOnlyReason}</strong>
          {outsider ? (
            <span>
              当前登录用户是 {subject ?? "未知"}，不在本节点的审批链里；记录与意见仍可查看。
            </span>
          ) : null}
        </div>
      ) : null}

      <div aria-label="审批记录" className="xflow-editor-approval__ledger">
        {decisions.map((decision, index) => {
          // An amendment (add_signer / force) is a ledger event but leaves the
          // actor's own slot open (`closesSlot`), so it must not read as ✓.
          const closes = approvalClosesSlot(decision.action);
          return (
            <div
              className="xflow-editor-approval__row"
              data-state={closes ? "decided" : "amended"}
              key={`decision:${index}`}
            >
              <span aria-hidden="true" className="xflow-editor-approval__row-icon" data-tone={closes ? "success" : "neutral"}>
                {closes ? "✓" : "↳"}
              </span>
              <span className="xflow-editor-approval__actor">{decision.approver}</span>
              <span className="xflow-editor-approval__say">
                <strong>{approvalActionLabel[decision.action] ?? decision.action}</strong>
                {decision.assignee ? <span> · {decision.assignee}</span> : null}
                {decision.comment ? (
                  <span className="xflow-editor-approval__muted"> “{decision.comment}”</span>
                ) : null}
              </span>
              <span className="xflow-editor-approval__when">{timeText(decision.at)}</span>
            </div>
          );
        })}
        {chain
          .filter((approver) => !approvalHasDecision(decisions, approver))
          .map((approver) => {
            const current = mode === "sequential" && progress.current === approver;
            return (
              <div
                className="xflow-editor-approval__row"
                data-state={current ? "current" : "waiting"}
                key={`pending:${approver}`}
              >
                <span aria-hidden="true" className="xflow-editor-approval__row-icon" data-tone={current ? "info" : "neutral"}>
                  {current ? "◔" : "…"}
                </span>
                <span className="xflow-editor-approval__actor">{approver}</span>
                <span className="xflow-editor-approval__say">
                  {current ? (approver === subject ? "当前待你处理" : "当前待处理") : mode === "sequential" ? "未到" : "待处理"}
                </span>
                <span className="xflow-editor-approval__when" />
              </div>
            );
          })}
        {chain.length === 0 && decisions.length === 0 ? (
          <p className="xflow-editor-approval__empty">暂无审批记录。</p>
        ) : null}
      </div>

      <p className="xflow-editor-approval__meta">
        已处理 {progress.decided} 人 · 待处理 {Math.max(0, progress.total - progress.decided)} 人
        {signalPreview ? (
          <>
            <span aria-hidden="true"> · </span>
            信号 <span className="xflow-editor-approval__mono">{signalPreview}</span>
          </>
        ) : null}
      </p>

      {canDecide ? (
        <>
          <div aria-label="决定" className="xflow-editor-approval__choices" role="group">
            {(["approve", "reject", "return"] as const).map((action) => (
              <button
                aria-pressed={choice === action}
                className="xflow-editor-approval__choice"
                data-action={action}
                data-tone={approvalChoiceTone[action]}
                key={action}
                type="button"
                onClick={() => setChoice(action)}
              >
                <span>{approvalActionLabel[action]}</span>
                <small>
                  {action === "approve" ? "走 approved 出口" : action === "reject" ? "走 rejected 出口" : "走 returned 出口"}
                </small>
              </button>
            ))}
          </div>

          <div className="xflow-editor-approval__field">
            <label className="xflow-editor-approval__label" htmlFor="xflow-editor-approval-comment">
              意见
            </label>
            <Input.TextArea
              id="xflow-editor-approval-comment"
              aria-label="审批意见"
              placeholder="选填，会写入审批记录"
              rows={2}
              value={comment}
              onChange={(event) => setComment(event.target.value)}
            />
          </div>
        </>
      ) : null}

      {canAmend || canForce ? (
        <div className="xflow-editor-approval__actions">
          {canAmend ? (
            <>
              <Button
                disabled={Boolean(submitting)}
                size="small"
                onClick={() => setSecondary({ kind: "delegate", assignee: "", position: "after" })}
              >
                转交…
              </Button>
              <Button
                disabled={Boolean(submitting)}
                size="small"
                onClick={() => setSecondary({ kind: "add_signer", assignee: "", position: "after" })}
              >
                加签…
              </Button>
            </>
          ) : null}
          {canForce ? (
            <Button
              className="xflow-editor-approval__force"
              danger
              disabled={Boolean(submitting)}
              loading={submitting === approvalActions.force}
              size="small"
              onClick={() => void submit(approvalActions.force)}
            >
              强制通过
            </Button>
          ) : null}
        </div>
      ) : null}

      {secondary && canAmend ? (
        <div className="xflow-editor-approval__secondary" role="group" aria-label={secondary.kind === "delegate" ? "转交" : "加签"}>
          <div className="xflow-editor-approval__field">
            <label className="xflow-editor-approval__label" htmlFor="xflow-editor-approval-assignee">
              {secondary.kind === "delegate" ? "转交人" : "加签人"} <span aria-hidden="true">*</span>
            </label>
            <Input
              aria-label={secondary.kind === "delegate" ? "转交人" : "加签人"}
              id="xflow-editor-approval-assignee"
              placeholder="输入审批人标识"
              size="small"
              value={secondary.assignee}
              onChange={(event) => setSecondary((current) => (current ? { ...current, assignee: event.target.value } : current))}
            />
          </div>
          {secondary.kind === "add_signer" ? (
            <div className="xflow-editor-approval__field">
              <span className="xflow-editor-approval__label">位置</span>
              <Segmented
                className="xflow-editor-approval__positions"
                options={[
                  { label: "在我之前", value: "before" },
                  { label: "在我之后", value: "after" }
                ]}
                size="small"
                value={secondary.position}
                onChange={(value) =>
                  setSecondary((current) =>
                    current ? { ...current, position: value === "before" ? "before" : "after" } : current
                  )
                }
              />
              <p className="xflow-editor-approval__hint">
                只在「依次审批」模式下影响顺序；审批链最多 {APPROVAL_MAX_CHAIN} 人。
              </p>
            </div>
          ) : null}
          <div className="xflow-editor-approval__secondary-actions">
            <Button size="small" onClick={() => setSecondary(undefined)}>
              取消
            </Button>
            <Button
              disabled={!secondary.assignee.trim() || Boolean(submitting)}
              loading={submitting === secondary.kind}
              size="small"
              type="primary"
              onClick={submitSecondary}
            >
              {secondary.kind === "delegate" ? "转交" : "加签"}
            </Button>
          </div>
        </div>
      ) : null}

      {pending ? (
        <div
          className="xflow-editor-approval__status"
          data-tone={recorded ? "success" : refused ? "danger" : refusedElsewhere ? "warning" : "neutral"}
          role="status"
        >
          <strong>
            {recorded
              ? "决定已记入审批记录"
              : refused
                ? "信号未被采纳（未计入）"
                : refusedElsewhere
                  ? "有信号未被计入"
                  : "已提交，等待节点处理…"}
          </strong>
          {refused ? (
            <span>
              {newIgnored.map((item) => `${item.signal}：${reasonText(item.reason)}`).join("；")}
            </span>
          ) : null}
          {refusedElsewhere ? (
            <span>未计入的尾迹里出现了其他投递；请以审批记录为准。</span>
          ) : null}
          {!recorded && !refused && !refusedElsewhere && executionStatus === "paused" && onRevokeSignal ? (
            <Button
              disabled={Boolean(submitting)}
              loading={submitting === "revoke"}
              size="small"
              onClick={() => void revoke()}
            >
              撤回信号
            </Button>
          ) : null}
        </div>
      ) : null}

      {revoked ? (
        <div className="xflow-editor-approval__status" data-tone="neutral" role="status">
          <strong>已撤回投递的信号</strong>
          <span>节点不会再消费它；如需决定请重新提交。</span>
        </div>
      ) : null}

      {error ? (
        <div className="xflow-editor-approval__status" data-tone="danger" role="alert">
          <strong>操作失败</strong>
          <span>{error}</span>
        </div>
      ) : null}

      {ignored.length > 0 ? (
        <div className="xflow-editor-approval__trail" aria-label="未计入的信号">
          <span className="xflow-editor-approval__trail-title">
            未计入的信号（{ignoredCount}）
          </span>
          {ignored.map((item, index) => (
            <div className="xflow-editor-approval__trail-row" key={`ignored:${index}`}>
              <span className="xflow-editor-approval__mono">{item.signal}</span>
              <span>{reasonText(item.reason)}</span>
            </div>
          ))}
          {ignoredCount > ignored.length ? (
            <span className="xflow-editor-approval__hint">
              仅显示最近 {ignored.length} 条，共 {ignoredCount} 条未被计入。
            </span>
          ) : null}
        </div>
      ) : null}
    </Modal>
  );
}
