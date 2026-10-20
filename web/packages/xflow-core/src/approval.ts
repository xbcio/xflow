/**
 * The `xflow.approval` node's wire contract, mirrored from
 * `node/internal/group/approval.go` so the editor can name signals and read the
 * public ledger without importing the node package.
 *
 * Two vocabularies that must never be swapped: a decision *action* is
 * `approve` / `reject` / `return` / `delegate` / `add_signer` / `force`, while
 * the *output port* it leaves by is `approved` / `rejected` / `returned`. The
 * signal name is derived from the node name, the mode and the deciding subject
 * (`approvalSignal`, `approverSignal`, `forceSignal` in the Go node).
 */

export const APPROVAL_NODE_TYPE = "xflow.approval";

/** Actions carried in a signal payload under `action` (approval.go:17-25). */
export const approvalActions = {
  approve: "approve",
  reject: "reject",
  return: "return",
  delegate: "delegate",
  addSigner: "add_signer",
  force: "force"
} as const;

export type ApprovalAction = (typeof approvalActions)[keyof typeof approvalActions];

/**
 * The three decisions a person makes about the request. The amend-the-chain
 * actions (`delegate` / `add_signer`) and `force` are secondary operations, not
 * choices between ports.
 */
export const approvalDecisionActions = [
  approvalActions.approve,
  approvalActions.reject,
  approvalActions.return
] as const;

export type ApprovalDecisionAction = (typeof approvalDecisionActions)[number];

/**
 * Action → the output port the node leaves by. `approve` never leaves by
 * `approve`, and a signal named after the port is ignored (`signal-name-mismatch`).
 */
export const approvalActionPort: Record<ApprovalDecisionAction, "approved" | "rejected" | "returned"> = {
  approve: "approved",
  reject: "rejected",
  return: "returned"
};

export type ApprovalMode = "any" | "all" | "sequential";

export const approvalModes: readonly ApprovalMode[] = ["any", "all", "sequential"];

/** `maxApproverChain` in the Go node: a chain cannot grow past 32 slots. */
export const APPROVAL_MAX_CHAIN = 32;

/** Mode labels shared by the canvas wait note and the dialog header (§6.12). */
export const approvalModeLabel: Record<ApprovalMode, string> = {
  any: "任一审批",
  all: "全部审批",
  sequential: "依次审批"
};

/** Decision labels for the ledger rows. Unknown actions read as their wire value. */
export const approvalActionLabel: Record<string, string> = {
  approve: "同意",
  reject: "驳回",
  return: "退回",
  delegate: "转交",
  add_signer: "加签",
  force: "强制通过"
};

/**
 * Why the node refused a delivered signal, in the words of the public `ignored`
 * trail (`ignoreApprovalSignal` callers in approval.go). Unknown reasons are
 * shown verbatim so a newer server is not silently misdescribed.
 */
export const approvalIgnoredReasonLabel: Record<string, string> = {
  "unverified-actor": "无法确认审批人身份",
  "malformed-action": "动作无法识别",
  "unauthorized-approver": "不在审批链",
  "signal-name-mismatch": "信号名与节点不匹配",
  "not-current-approver": "尚未轮到该审批人",
  "unauthorized-force-approver": "不在强制通过名单",
  "malformed-assignee": "转交 / 加签人无效",
  "malformed-position": "加签位置无效",
  "chain-limit-reached": "审批链已达上限",
  "unknown-action": "未知动作"
};

export interface ApprovalParams {
  /**
   * The configured approvers. `undefined` when the param is absent or is not a
   * literal string list (e.g. an expression), because the client cannot know
   * the chain the node will evaluate.
   */
  approvers?: string[];
  /** `undefined` when the mode is not a recognized literal (an expression). */
  mode?: ApprovalMode;
  /** Raw duration text (`72h`); absent when unset. */
  timeout?: string;
  /** `reject` routes to the rejected port, anything else to `timeout`. */
  timeoutAction: "route" | "reject";
  forceApprovers: string[];
}

function literalStringList(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  return value.filter((item): item is string => typeof item === "string" && item.trim().length > 0);
}

function durationText(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return undefined;
}

/**
 * Reads the approval node's parameters. Everything the dialog cannot know from
 * a literal value is left `undefined` (and the dialog goes read-only) rather
 * than guessed: a wrong mode yields a wrong signal name, which the node records
 * as `signal-name-mismatch` instead of counting the decision.
 */
export function approvalParamsFrom(parameters: Record<string, unknown> | undefined): ApprovalParams {
  const params = parameters ?? {};
  const mode = typeof params.mode === "string" && (approvalModes as readonly string[]).includes(params.mode)
    ? (params.mode as ApprovalMode)
    : undefined;
  return {
    // `mode` defaults to "any" server-side when the param is absent (a JSON
    // null counts as absent there too); an unrecognized literal is not that
    // default and stays unknown.
    mode: params.mode === undefined || params.mode === null ? "any" : mode,
    approvers: literalStringList(params.approvers),
    timeout: durationText(params.timeout),
    timeoutAction: params.timeout_action === "reject" ? "reject" : "route",
    forceApprovers: literalStringList(params.force_approvers) ?? []
  };
}

export interface ApprovalDecision {
  approver: string;
  action: string;
  comment?: string;
  /** RFC3339 timestamp the node recorded (`at`). */
  at?: string;
  /** `delegate` / `add_signer`: who the decision was handed to. */
  assignee?: string;
  /** `add_signer`: where the signer was inserted. */
  position?: "before" | "after";
}

export interface ApprovalIgnoredSignal {
  signal: string;
  reason: string;
}

/** The node's public output key carrying its decision ledger. */
export const APPROVAL_DECISIONS_KEY = "decisions";
export const APPROVAL_IGNORED_KEY = "ignored";
export const APPROVAL_IGNORED_COUNT_KEY = "ignored_count";

/** Reads `output.decisions`, dropping entries that are not a decision record. */
export function approvalDecisions(output: Record<string, unknown> | undefined): ApprovalDecision[] {
  const list = output?.[APPROVAL_DECISIONS_KEY];
  if (!Array.isArray(list)) return [];
  const decisions: ApprovalDecision[] = [];
  for (const item of list) {
    if (typeof item !== "object" || item === null) continue;
    const record = item as Record<string, unknown>;
    const approver = record.approver;
    const action = record.action;
    if (typeof approver !== "string" || !approver || typeof action !== "string" || !action) continue;
    const decision: ApprovalDecision = { approver, action };
    if (typeof record.comment === "string") decision.comment = record.comment;
    if (typeof record.at === "string") decision.at = record.at;
    if (typeof record.assignee === "string" && record.assignee) decision.assignee = record.assignee;
    if (record.position === "before" || record.position === "after") decision.position = record.position;
    decisions.push(decision);
  }
  return decisions;
}

/** Reads `output.ignored`: the bounded trail of signals the node refused. */
export function approvalIgnored(output: Record<string, unknown> | undefined): ApprovalIgnoredSignal[] {
  const list = output?.[APPROVAL_IGNORED_KEY];
  if (!Array.isArray(list)) return [];
  const ignored: ApprovalIgnoredSignal[] = [];
  for (const item of list) {
    if (typeof item !== "object" || item === null) continue;
    const record = item as Record<string, unknown>;
    if (typeof record.signal === "string" && typeof record.reason === "string") {
      ignored.push({ signal: record.signal, reason: record.reason });
    }
  }
  return ignored;
}

/** `output.ignored_count`: every refusal, including those past the trail limit. */
export function approvalIgnoredCount(output: Record<string, unknown> | undefined): number {
  const value = output?.[APPROVAL_IGNORED_COUNT_KEY];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

/** Whether an action closes the actor's own slot (`closesSlot` in approval.go). */
export function approvalClosesSlot(action: string): boolean {
  return (
    action === approvalActions.approve ||
    action === approvalActions.reject ||
    action === approvalActions.return ||
    action === approvalActions.delegate
  );
}

/** Whether this approver's own slot is resolved by a recorded decision. */
export function approvalHasDecision(decisions: readonly ApprovalDecision[], approver: string): boolean {
  return decisions.some((decision) => decision.approver === approver && approvalClosesSlot(decision.action));
}

function replaceApprover(chain: readonly string[], from: string, to: string): string[] {
  const next = [...chain];
  const index = next.indexOf(from);
  if (index >= 0) next[index] = to;
  return next;
}

function insertApprover(chain: readonly string[], actor: string, assignee: string, position?: string): string[] {
  const next: string[] = [];
  for (const candidate of chain) {
    if (candidate === actor && position === "before") next.push(assignee);
    next.push(candidate);
    if (candidate === actor && position !== "before") next.push(assignee);
  }
  return next;
}

/**
 * The chain the gate is actually waiting on: configured approvers as amended by
 * delegations and added signers on the ledger (`approvedChain` in approval.go).
 * Recomputing it from the ledger keeps the wait and the record from drifting.
 */
export function approvalChain(
  approvers: readonly string[],
  decisions: readonly ApprovalDecision[]
): string[] {
  let chain = [...approvers];
  for (const decision of decisions) {
    if (!decision.assignee) continue;
    if (decision.action === approvalActions.delegate) {
      chain = replaceApprover(chain, decision.approver, decision.assignee);
    } else if (decision.action === approvalActions.addSigner) {
      chain = insertApprover(chain, decision.approver, decision.assignee, decision.position);
    }
  }
  return chain;
}

export interface ApprovalProgress {
  chain: string[];
  /** Chain members whose slot has been closed. */
  decided: number;
  /** Configured + amended chain length. */
  total: number;
  /** First undecided member: the only operable name in sequential mode. */
  current?: string;
  /** 1-based place of {@link current} in the chain; `total` once everyone decided. */
  position: number;
}

/** Progress read off the chain and ledger, mirroring `currentSequentialApprover`. */
export function approvalProgress(
  approvers: readonly string[],
  decisions: readonly ApprovalDecision[]
): ApprovalProgress {
  const chain = approvalChain(approvers, decisions);
  const decided = chain.filter((approver) => approvalHasDecision(decisions, approver)).length;
  const current = chain.find((approver) => !approvalHasDecision(decisions, approver));
  return {
    chain,
    decided,
    total: chain.length,
    current,
    position: current ? chain.indexOf(current) + 1 : chain.length
  };
}

/**
 * Whether `subject` may act on the gate now: a chain member whose slot is open,
 * and — in sequential mode — the one whose turn it is.
 */
export function approvalCanDecide(
  mode: ApprovalMode | undefined,
  chain: readonly string[],
  decisions: readonly ApprovalDecision[],
  subject: string | undefined
): boolean {
  if (!mode || !subject || !chain.includes(subject)) return false;
  if (approvalHasDecision(decisions, subject)) return false;
  if (mode === "sequential") {
    const current = chain.find((approver) => !approvalHasDecision(decisions, approver));
    return current === subject;
  }
  return true;
}

/** `force` is authorized by the workflow's own force list, not by a chain slot. */
export function approvalCanForce(
  forceApprovers: readonly string[],
  subject: string | undefined
): boolean {
  return Boolean(subject) && forceApprovers.includes(subject as string);
}

/**
 * The signal name a decision travels under, mirroring `approvalSignal`,
 * `approverSignal` and `forceSignal` in approval.go:813-847:
 *
 * - any                      → `<node>/approval`
 * - all / sequential         → `<node>/approval/<subject>`
 * - force                    → `<node>/approval/force`
 *
 * `undefined` means no name can be computed (an unknown mode, or a subject the
 * caller could not establish); sending another spelling would be ignored by the
 * node as `signal-name-mismatch`.
 */
export function approvalSignalName(
  nodeName: string,
  mode: ApprovalMode | undefined,
  subject?: string,
  force = false
): string | undefined {
  const name = nodeName.trim();
  if (!name) return undefined;
  if (force) return `${name}/approval/force`;
  if (!mode) return undefined;
  if (mode === "any") return `${name}/approval`;
  const actor = subject?.trim();
  return actor ? `${name}/approval/${actor}` : undefined;
}

/**
 * The dialog header's mode-and-progress segment (§6.12): `任一` carries no
 * fraction because one vote resolves it, `全部` counts the decided slots, and
 * `依次` counts the current turn. A `position` in all mode would read as
 * "全部审批 2/2 · 等待 b" whenever the second person is the one still open.
 */
export function approvalProgressText(
  mode: ApprovalMode | undefined,
  progress: ApprovalProgress
): string {
  if (!mode) return "模式未知";
  if (mode === "any") return approvalModeLabel[mode];
  const count = mode === "all" ? progress.decided : progress.position;
  return `${approvalModeLabel[mode]} ${count}/${progress.total}`;
}

/**
 * The canvas card's wait line (§6.12): mode, progress and the person being
 * waited on. Reads the same progress the dialog header does. `any` names no
 * count: one approval closes the gate, so "等待 2 人" would promise a quorum
 * the node never waits for.
 */
export function approvalWaitText(
  mode: ApprovalMode | undefined,
  progress: ApprovalProgress
): string {
  if (!mode) return `${progress.decided}/${progress.total} · 等待审批`;
  const label = approvalModeLabel[mode];
  if (mode === "any") {
    return progress.current ? `${label} · 等待任一审批人` : label;
  }
  const count = mode === "all" ? progress.decided : progress.position;
  const head = `${label} ${count}/${progress.total}`;
  return progress.current ? `${head} · 等待 ${progress.current}` : head;
}
