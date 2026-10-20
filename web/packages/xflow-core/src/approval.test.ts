import { describe, expect, it } from "vitest";
import {
  APPROVAL_MAX_CHAIN,
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
  approvalActionPort,
  type ApprovalDecision
} from "./approval";

const decision = (approver: string, action: string, extra: Partial<ApprovalDecision> = {}): ApprovalDecision => ({
  approver,
  action,
  ...extra
});

describe("approval signal names (§6.12 / approval.go:813-847)", () => {
  it("uses one name in any mode and a per-approver name in the other modes", () => {
    expect(approvalSignalName("manager_approval", "any", "finance-mgr")).toBe("manager_approval/approval");
    expect(approvalSignalName("manager_approval", "all", "finance-mgr")).toBe(
      "manager_approval/approval/finance-mgr"
    );
    expect(approvalSignalName("manager_approval", "sequential", "finance-mgr")).toBe(
      "manager_approval/approval/finance-mgr"
    );
  });

  it("uses the fixed force name regardless of who forces", () => {
    expect(approvalSignalName("manager_approval", "sequential", "cfo", true)).toBe(
      "manager_approval/approval/force"
    );
    expect(approvalSignalName("manager_approval", "any", undefined, true)).toBe(
      "manager_approval/approval/force"
    );
  });

  it("refuses to guess a name when the mode or the subject is unknown", () => {
    expect(approvalSignalName("manager_approval", undefined, "finance-mgr")).toBeUndefined();
    expect(approvalSignalName("manager_approval", "all")).toBeUndefined();
    expect(approvalSignalName("manager_approval", "sequential", "  ")).toBeUndefined();
    expect(approvalSignalName("  ", "any")).toBeUndefined();
  });

  it("keeps actions and ports distinct", () => {
    // A signal named after the port is ignored by the node (signal-name-mismatch).
    expect(approvalActionPort).toEqual({ approve: "approved", reject: "rejected", return: "returned" });
    expect(Object.values(approvalActions)).toEqual([
      "approve",
      "reject",
      "return",
      "delegate",
      "add_signer",
      "force"
    ]);
  });
});

describe("approval params", () => {
  it("defaults the mode to any and keeps literal values", () => {
    expect(approvalParamsFrom(undefined)).toEqual({
      mode: "any",
      approvers: undefined,
      timeout: undefined,
      timeoutAction: "route",
      forceApprovers: []
    });
    expect(approvalParamsFrom({ mode: "sequential", approvers: ["a", "b"], timeout: "72h", timeout_action: "reject", force_approvers: ["cfo"] })).toEqual({
      mode: "sequential",
      approvers: ["a", "b"],
      timeout: "72h",
      timeoutAction: "reject",
      forceApprovers: ["cfo"]
    });
  });

  it("leaves a derived mode or approver list unknown instead of guessing", () => {
    const derived = approvalParamsFrom({ mode: "$params.strategy", approvers: "$params.people" });
    // A computed mode cannot be named locally; the dialog must not send a guess.
    expect(derived.mode).toBeUndefined();
    expect(derived.approvers).toBeUndefined();
    // A JSON null mode is the server's default, not an expression.
    expect(approvalParamsFrom({ mode: null }).mode).toBe("any");
  });

  it("drops non-string approver entries and reads numeric durations", () => {
    const params = approvalParamsFrom({
      approvers: ["a", 7, "", " b "],
      force_approvers: "nobody",
      timeout: 300
    });
    expect(params.approvers).toEqual(["a", " b "]);
    expect(params.forceApprovers).toEqual([]);
    expect(params.timeout).toBe("300");
  });
});

describe("approval chain and progress (approvedChain / currentSequentialApprover)", () => {
  it("replaces a delegator's slot and inserts an added signer where asked", () => {
    expect(approvalChain(["a", "b", "c"], [decision("a", "delegate", { assignee: "d" })])).toEqual([
      "d",
      "b",
      "c"
    ]);
    expect(
      approvalChain(
        ["a", "b", "c"],
        [
          decision("b", "add_signer", { assignee: "before-b", position: "before" }),
          decision("b", "add_signer", { assignee: "after-b", position: "after" })
        ]
      )
    ).toEqual(["a", "before-b", "b", "after-b", "c"]);
    // The Go node defaults a missing position to "after".
    expect(approvalChain(["a"], [decision("a", "add_signer", { assignee: "z" })])).toEqual(["a", "z"]);
  });

  it("counts a delegate as closing the delegator's slot and reports the sequential turn", () => {
    const decisions = [
      decision("ops-lead", "approve"),
      decision("finance-mgr", "delegate", { assignee: "d" })
    ];
    const progress = approvalProgress(["ops-lead", "finance-mgr", "cfo"], decisions);
    expect(progress.chain).toEqual(["ops-lead", "d", "cfo"]);
    // The delegated slot belongs to "d" now: the delegate closes the
    // delegator's old slot, but "d" has not decided yet.
    expect(progress.decided).toBe(1);
    expect(progress.current).toBe("d");
    expect(progress.position).toBe(2);
  });

  it("closes slots on reject / return but not on add_signer or force", () => {
    const decisions = [decision("a", "add_signer", { assignee: "b" }), decision("a", "force")];
    expect(approvalProgress(["a"], decisions)).toMatchObject({ decided: 0, current: "a", position: 1 });
    expect(approvalProgress(["a", "b"], [decision("a", "return")])).toMatchObject({
      decided: 1,
      current: "b"
    });
    expect(approvalProgress(["a", "b"], [decision("b", "reject")])).toMatchObject({ decided: 1, current: "a" });
  });

  it("exposes the slot-closing rule the ledger needs", () => {
    // Mirrors `closesSlot` in approval.go: an amended slot is still open, so a
    // reader that filters on "has any decision" hides a pending row.
    expect(approvalClosesSlot("approve")).toBe(true);
    expect(approvalClosesSlot("reject")).toBe(true);
    expect(approvalClosesSlot("return")).toBe(true);
    expect(approvalClosesSlot("delegate")).toBe(true);
    expect(approvalClosesSlot("add_signer")).toBe(false);
    expect(approvalClosesSlot("force")).toBe(false);
    const amended = [decision("a", "add_signer", { assignee: "b" })];
    expect(approvalHasDecision(amended, "a")).toBe(false);
    expect(approvalHasDecision([...amended, decision("a", "delegate", { assignee: "c" })], "a")).toBe(true);
  });
});

describe("who may act", () => {
  it("lets any undecided chain member decide in any / all mode", () => {
    expect(approvalCanDecide("any", ["a", "b"], [], "b")).toBe(true);
    expect(approvalCanDecide("all", ["a", "b"], [decision("a", "approve")], "b")).toBe(true);
    expect(approvalCanDecide("all", ["a", "b"], [decision("a", "approve")], "a")).toBe(false);
    expect(approvalCanDecide("any", ["a"], [], "outsider")).toBe(false);
    expect(approvalCanDecide(undefined, ["a"], [], "a")).toBe(false);
    expect(approvalCanDecide("any", ["a"], [], undefined)).toBe(false);
  });

  it("allows only the current approver in sequential mode", () => {
    const decisions = [decision("a", "approve")];
    expect(approvalCanDecide("sequential", ["a", "b", "c"], decisions, "b")).toBe(true);
    expect(approvalCanDecide("sequential", ["a", "b", "c"], decisions, "c")).toBe(false);
    expect(approvalCanDecide("sequential", ["a", "b", "c"], [], "b")).toBe(false);
  });

  it("authorizes force by the workflow's force list, not by the chain", () => {
    expect(approvalCanForce(["cfo"], "cfo")).toBe(true);
    expect(approvalCanForce(["cfo"], "a")).toBe(false);
    expect(approvalCanForce([], "cfo")).toBe(false);
    expect(approvalCanForce(["cfo"], undefined)).toBe(false);
  });
});

describe("public ledger", () => {
  it("reads decisions and ignores malformed entries", () => {
    expect(
      approvalDecisions({
        decisions: [
          { approver: "a", action: "approve", comment: "ok", at: "2026-10-20T10:00:00Z" },
          { approver: "b", action: "delegate", assignee: "c", position: "before" },
          { approver: 7, action: "approve" },
          { approver: "c" },
          "nope"
        ]
      })
    ).toEqual([
      { approver: "a", action: "approve", comment: "ok", at: "2026-10-20T10:00:00Z" },
      { approver: "b", action: "delegate", assignee: "c", position: "before" }
    ]);
    expect(approvalDecisions(undefined)).toEqual([]);
    expect(approvalDecisions({ decisions: "nope" })).toEqual([]);
  });

  it("reads the ignored trail and its total count", () => {
    const output = {
      ignored: [
        { signal: "manager_approval/approval/qa", reason: "unauthorized-approver" },
        { signal: "x", reason: 1 }
      ],
      ignored_count: 5
    };
    expect(approvalIgnored(output)).toEqual([
      { signal: "manager_approval/approval/qa", reason: "unauthorized-approver" }
    ]);
    expect(approvalIgnoredCount(output)).toBe(5);
    expect(approvalIgnoredCount(undefined)).toBe(0);
    expect(approvalIgnoredReasonLabel["unauthorized-approver"]).toBe("不在审批链");
    // The chain limit is the server's number, quoted rather than re-invented.
    expect(APPROVAL_MAX_CHAIN).toBe(32);
  });
});

describe("canvas wait note", () => {
  it("names the mode, progress and who is being waited on", () => {
    const sequential = approvalProgress(["ops-lead", "finance-mgr", "cfo"], [decision("ops-lead", "approve")]);
    expect(approvalWaitText("sequential", sequential)).toBe("依次审批 2/3 · 等待 finance-mgr");
    expect(approvalWaitText("all", approvalProgress(["a", "b"], []))).toBe("全部审批 0/2 · 等待 a");
    // `all` counts decided slots, not the open person's index: one approval is
    // 1/2, never the 2/2 that reads as "everyone is done, yet b is still waited".
    expect(approvalWaitText("all", approvalProgress(["a", "b"], [decision("a", "approve")]))).toBe(
      "全部审批 1/2 · 等待 b"
    );
    // `any` waits for one vote, so it promises no count.
    expect(approvalWaitText("any", approvalProgress(["a", "b"], []))).toBe("任一审批 · 等待任一审批人");
    expect(approvalWaitText(undefined, approvalProgress(["a"], []))).toBe("0/1 · 等待审批");
    // Everyone decided: no dangling "等待".
    expect(approvalWaitText("sequential", approvalProgress(["a"], [decision("a", "approve")]))).toBe("依次审批 1/1");
    expect(approvalWaitText("any", approvalProgress(["a"], [decision("a", "approve")]))).toBe("任一审批");
  });

  it("heads the dialog with 任一 alone, 全部 by decided count and 依次 by turn", () => {
    expect(approvalProgressText("any", approvalProgress(["a", "b"], []))).toBe("任一审批");
    expect(approvalProgressText("all", approvalProgress(["a", "b"], [decision("a", "approve")]))).toBe(
      "全部审批 1/2"
    );
    expect(
      approvalProgressText("sequential", approvalProgress(["ops-lead", "finance-mgr", "cfo"], [decision("ops-lead", "approve")]))
    ).toBe("依次审批 2/3");
    expect(approvalProgressText(undefined, approvalProgress(["a"], []))).toBe("模式未知");
  });
});
