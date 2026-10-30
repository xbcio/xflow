import { describe, expect, it } from "vitest";

import { declaredRunParams, initialRunParamValues, parseRunParams } from "./runParams";

const schema = {
  amount: { type: "number", required: true, display_name: "金额", validation: { min: 0, max: 1000 } },
  applicant: { type: "string", required: true, display_name: "申请人" },
  department: { type: "string", display_name: "部门" },
  urgent: { type: "boolean", default: false },
  extra: { type: "object" },
  items: { type: "array", required: true, default: [1] },
  any: {}
};

describe("declaredRunParams", () => {
  it("lists required params first and labels by display_name", () => {
    const fields = declaredRunParams(schema);
    expect(fields.map((field) => field.name)).toEqual([
      "amount",
      "applicant",
      "items",
      "department",
      "urgent",
      "extra",
      "any"
    ]);
    expect(fields[0]).toMatchObject({ label: "金额", kind: "number", required: true, min: 0, max: 1000 });
    expect(fields.find((field) => field.name === "any")).toMatchObject({ label: "any", kind: "json" });
  });

  it("returns nothing for a workflow without params", () => {
    expect(declaredRunParams(undefined)).toEqual([]);
    expect(declaredRunParams({})).toEqual([]);
  });
});

describe("initialRunParamValues", () => {
  it("pre-fills declared defaults as editable text", () => {
    const values = initialRunParamValues(declaredRunParams(schema));
    expect(values.urgent).toBe("false");
    expect(values.items).toBe("[\n  1\n]");
    expect(values.amount).toBe("");
  });
});

describe("parseRunParams", () => {
  const fields = declaredRunParams(schema);
  const valid = { amount: "12.5", applicant: "alice", items: "[1, 2]" };

  it("builds typed params and leaves blank optional fields out", () => {
    const { params, errors } = parseRunParams(fields, { ...valid, department: "", urgent: "true" });
    expect(errors).toEqual({});
    expect(params).toEqual({ amount: 12.5, applicant: "alice", items: [1, 2], urgent: true });
  });

  it("keeps a string exactly as typed, spaces included", () => {
    const { params } = parseRunParams(fields, { ...valid, applicant: "  alice " });
    expect(params.applicant).toBe("  alice ");
  });

  it("requires a required param that has no default", () => {
    const { errors } = parseRunParams(fields, { amount: "", applicant: "   ", items: "" });
    expect(errors).toEqual({ amount: "必填", applicant: "必填" });
  });

  it("leaves a blank required param with a default to the server", () => {
    const { params, errors } = parseRunParams(fields, { ...valid, items: "" });
    expect(errors).toEqual({});
    expect(params).not.toHaveProperty("items");
  });

  it.each(["-", "1e", "abc", "1,5", "0x10", "Infinity", "NaN", "1.2.3"])("rejects the number text %j", (text) => {
    expect(parseRunParams(fields, { ...valid, amount: text }).errors.amount).toBe("请输入有效数字");
  });

  it.each([["-0.5", "不能小于 0"], ["1000.1", "不能大于 1000"]])("applies min and max to %j", (text, message) => {
    expect(parseRunParams(fields, { ...valid, amount: text }).errors.amount).toBe(message);
  });

  it("accepts the numeric edges", () => {
    expect(parseRunParams(fields, { ...valid, amount: "0" }).params.amount).toBe(0);
    expect(parseRunParams(fields, { ...valid, amount: "1000" }).params.amount).toBe(1000);
    expect(parseRunParams(fields, { ...valid, amount: ".5" }).params.amount).toBe(0.5);
    expect(parseRunParams(fields, { ...valid, amount: "1e2" }).params.amount).toBe(100);
  });

  it("rejects malformed and wrongly shaped JSON", () => {
    expect(parseRunParams(fields, { ...valid, items: "[1," }).errors.items).toBe("JSON 格式错误");
    expect(parseRunParams(fields, { ...valid, items: "{}" }).errors.items).toBe("需要 JSON 数组，如 []");
    expect(parseRunParams(fields, { ...valid, extra: "[]" }).errors.extra).toBe("需要 JSON 对象，如 {}");
    expect(parseRunParams(fields, { ...valid, extra: '{"a":1}' }).params.extra).toEqual({ a: 1 });
  });

  it("accepts any JSON for an undeclared type", () => {
    expect(parseRunParams(fields, { ...valid, any: '"x"' }).params.any).toBe("x");
    expect(parseRunParams(fields, { ...valid, any: "7" }).params.any).toBe(7);
  });

  it("treats JSON null as blank, as the server does", () => {
    const { params, errors } = parseRunParams(fields, { ...valid, extra: "null", items: "null" });
    expect(errors).toEqual({});
    expect(params).not.toHaveProperty("extra");
    expect(params).not.toHaveProperty("items");
  });

  it("requires a boolean to be chosen when it is required", () => {
    const required = declaredRunParams({ ok: { type: "boolean", required: true } });
    expect(parseRunParams(required, { ok: "" }).errors.ok).toBe("请选择是或否");
    expect(parseRunParams(required, { ok: "false" }).params.ok).toBe(false);
  });
});
