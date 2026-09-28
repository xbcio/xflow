import { describe, expect, it } from "vitest";
import { deriveExpressionMode, deriveExpressionModeForPointer } from "./expressionMode";

describe("deriveExpressionMode (temporary stub of graph.ExpressionMode, Doc A §2.3)", () => {
  it.each([
    // trigger params are rendered at activation ($config/$vars) → template
    ["trigger", "xflow.trigger.kafka", ["topic"], "template"],
    ["trigger", "xflow.trigger.redis", ["tuning", "dial_timeout"], "template"],
    ["trigger", "xflow.trigger.cron", ["expression"], "template"],
    // ② sub-graph body → none
    ["action", "xflow.map", ["body"], "none"],
    // ③ host source → literal (even though script.code is also evaluable)
    ["action", "xflow.script", ["code"], "literal"],
    // ④ evaluable params / sub-fields → pure
    ["action", "xflow.if", ["condition"], "pure"],
    ["action", "xflow.map", ["items"], "pure"],
    ["action", "xflow.map", ["expression"], "pure"],
    ["action", "xflow.function", ["code"], "pure"],
    ["action", "xflow.transform.set", ["expressions"], "pure"],
    ["action", "xflow.transform.set", ["expressions", "x"], "pure"],
    ["action", "xflow.switch", ["rules", "*", "condition"], "pure"],
    ["action", "xflow.switch", ["rules", "condition"], "pure"],
    // ⑤ otherwise → template
    ["action", "xflow.switch", ["rules", "*", "output"], "template"],
    ["action", "xflow.switch", ["rules"], "template"],
    ["action", "xflow.http", ["body"], "template"],
    ["action", "xflow.http", ["headers"], "template"],
    ["action", "xflow.script", ["language"], "template"],
    ["action", "xflow.unknown", ["x"], "template"],
    ["action", "xflow.wait", [], "template"]
  ] as const)("%s %s %j → %s", (kind, type, segments, mode) => {
    expect(deriveExpressionMode(kind, type, segments)).toBe(mode);
  });

  it("classifies pointers; fields outside /parameters are none", () => {
    expect(deriveExpressionModeForPointer("action", "xflow.map", "/parameters/body")).toBe("none");
    expect(deriveExpressionModeForPointer("action", "xflow.http", "/parameters/url")).toBe("template");
    expect(deriveExpressionModeForPointer("action", "xflow.http", "/timeout")).toBe("none");
    expect(deriveExpressionModeForPointer("action", "xflow.switch", "/parameters/rules/0/condition")).toBe("pure");
  });
});
