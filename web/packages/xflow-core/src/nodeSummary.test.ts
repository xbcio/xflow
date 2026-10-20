import { describe, expect, it } from "vitest";
import type { NodeFormField } from "./nodeForm";
import { cardSummaryRows } from "./nodeSummary";

const field = (name: string, extra: Partial<NodeFormField> = {}): NodeFormField => ({
  name,
  path: `/parameters/${name}`,
  type: "string",
  ...extra
});

describe("cardSummaryRows (spec §6.1.1)", () => {
  it("falls back to the first three required scalar values without a declaration, and honours an explicit empty list", () => {
    const fields = [
      field("url", { label: "URL", required: true }),
      field("method", { label: "Method" }),
      field("body", { type: "object", required: true }),
      field("pattern", { label: "Pattern", required: true }),
      field("retry", { label: "Retry", type: "number", required: true }),
      field("extra", { label: "Extra", required: true })
    ];
    expect(cardSummaryRows({ fields }, { url: "https://a", pattern: "ab", retry: 3, extra: "x" })).toEqual([
      { label: "URL", value: "https://a", kind: "plain", path: "/parameters/url" },
      { label: "Pattern", value: "ab", kind: "plain", path: "/parameters/pattern" },
      { label: "Retry", value: "3", kind: "plain", path: "/parameters/retry" }
    ]);
    // A required field with no value does not enter the fallback; an explicit []
    // means the type draws no summary at all.
    expect(cardSummaryRows({ fields }, { retry: 3 })).toEqual([
      { label: "Retry", value: "3", kind: "plain", path: "/parameters/retry" }
    ]);
    expect(cardSummaryRows({ fields: [field("url", { required: true })], card_summary: [] }, { url: "https://a" })).toEqual([]);
  });

  it("prints one row per declared parameter, in declaration order, labelled by the field", () => {
    const rows = cardSummaryRows(
      {
        fields: [field("url", { label: "URL" }), field("method", { label: "Method" })],
        card_summary: ["url", "method"]
      },
      { url: "https://a", method: "GET" }
    );
    expect(rows).toEqual([
      { label: "URL", value: "https://a", kind: "plain", path: "/parameters/url" },
      { label: "Method", value: "GET", kind: "plain", path: "/parameters/method" }
    ]);
  });

  it("joins a+b into one row and drops a blank half without a dangling separator", () => {
    const schema = {
      fields: [field("method", { label: "Method" }), field("url", { label: "URL" })],
      card_summary: ["method+url"]
    };
    expect(cardSummaryRows(schema, { method: "GET", url: "https://a" })).toEqual([
      {
        label: "Method + URL",
        value: "GET https://a",
        kind: "plain",
        path: "/parameters/method",
        parts: [
          { text: "GET", kind: "plain" },
          { text: "https://a", kind: "plain" }
        ]
      }
    ]);
    expect(cardSummaryRows(schema, { method: "POST" })).toEqual([
      { label: "Method", value: "POST", kind: "plain", path: "/parameters/method" }
    ]);
    expect(cardSummaryRows(schema, { url: "  " })).toEqual([]);
  });

  it("keeps each half's own kind on a joined row", () => {
    const schema = {
      fields: [field("method", { label: "Method" }), field("url", { label: "URL" })],
      card_summary: ["method+url"]
    };
    const rows = cardSummaryRows(schema, { method: "GET", url: "/hooks/{{id}}" });
    expect(rows[0].value).toBe("GET /hooks/{{id}}");
    expect(rows[0].parts).toEqual([
      { text: "GET", kind: "plain" },
      { text: "/hooks/{{id}}", kind: "mono" }
    ]);
  });

  it("skips every empty value instead of drawing a label with nothing behind it", () => {
    const schema = {
      fields: [field("a", { label: "A" }), field("b", { label: "B" }), field("c", { label: "C" })],
      card_summary: ["a", "b", "c"]
    };
    expect(cardSummaryRows(schema, { a: "", b: [], c: {} })).toEqual([]);
    expect(cardSummaryRows(schema, undefined)).toEqual([]);
  });

  it("masks secrets and shows only the reference name of a credential", () => {
    const rows = cardSummaryRows(
      {
        fields: [
          field("token", { label: "Token", secret: true }),
          field("auth", { label: "Auth", widget: "credential-select" })
        ],
        card_summary: ["token", "auth"]
      },
      { token: "s3cr3t", auth: "crm_token" }
    );
    expect(rows).toEqual([
      { label: "Token", value: "••••••", kind: "secret", path: "/parameters/token" },
      { label: "Auth", value: "crm_token", kind: "ref", path: "/parameters/auth" }
    ]);
    expect(rows.map((row) => row.value)).not.toContain("s3cr3t");
  });

  it("reads a credential stored as a reference object by its name", () => {
    const rows = cardSummaryRows(
      { fields: [field("auth", { label: "Auth", widget: "credential-select" })], card_summary: ["auth"] },
      { auth: { ref: "db_conn" } }
    );
    expect(rows).toEqual([{ label: "Auth", value: "db_conn", kind: "ref", path: "/parameters/auth" }]);
  });

  it("joins a credential array by reference name and never prints undefined", () => {
    const schema = {
      fields: [field("credentials", { label: "Credentials", type: "array", widget: "credential-select" })],
      card_summary: ["credentials"]
    };
    expect(cardSummaryRows(schema, { credentials: [{ ref: "a" }, { name: "b" }] })).toEqual([
      { label: "Credentials", value: "a → b", kind: "ref", path: "/parameters/credentials" }
    ]);
    expect(cardSummaryRows(schema, { credentials: [{ value: "s3cr3t" }] })).toEqual([
      { label: "Credentials", value: "1 个凭证", kind: "ref", path: "/parameters/credentials" }
    ]);
    expect(cardSummaryRows(schema, { credentials: {} })).toEqual([]);
  });

  it("formats enums by their display name, booleans as 是 / 否 and arrays as a chain or count", () => {
    const rows = cardSummaryRows(
      {
        fields: [
          field("mode", { label: "Mode", options: [{ value: "all", label: "全部" }] }),
          field("enabled", { label: "Enabled", type: "boolean" }),
          field("approvers", { label: "Approvers", type: "array" }),
          field("rules", { label: "Rules", type: "array" })
        ],
        card_summary: ["mode", "enabled", "approvers", "rules"]
      },
      { mode: "all", enabled: false, approvers: ["a", "b"], rules: [{}, {}] }
    );
    expect(rows).toEqual([
      { label: "Mode", value: "全部", kind: "plain", path: "/parameters/mode" },
      { label: "Enabled", value: "否", kind: "plain", path: "/parameters/enabled" },
      { label: "Approvers", value: "a → b", kind: "plain", path: "/parameters/approvers" },
      { label: "Rules", value: "2 条", kind: "plain", path: "/parameters/rules" }
    ]);
  });

  it("marks pure expressions, prints code as its language and line count, and interpolations as mono", () => {
    const rows = cardSummaryRows(
      {
        fields: [
          field("condition", { label: "Condition", widget: "expression", expression: { mode: "pure" } }),
          field("code", { label: "Code", widget: "expression", expression: { mode: "pure" } }),
          field("body", { label: "Body", widget: "code" }),
          field("url", { label: "URL" })
        ],
        card_summary: ["condition", "code", "body", "url"]
      },
      { condition: "$json.tier == \"vip\"", code: "a();\nb();", body: "x()\ny()", url: "/hooks/{{id}}" }
    );
    expect(rows).toEqual([
      { label: "Condition", value: "$json.tier == \"vip\"", kind: "expr", path: "/parameters/condition" },
      // A `code` parameter whose widget is an expression keeps the expression
      // (xflow.function's inline body), never a line count.
      { label: "Code", value: "a();\nb();", kind: "expr", path: "/parameters/code" },
      { label: "Body", value: "2 行", kind: "plain", path: "/parameters/body" },
      { label: "URL", value: "/hooks/{{id}}", kind: "mono", path: "/parameters/url" }
    ]);
  });

  it("prints a code payload's language when the schema names one", () => {
    const schema = {
      node_type: "xflow.script",
      fields: [
        field("language", { label: "Language", options: [{ value: "JavaScript", label: "JavaScript" }] }),
        field("code", { label: "Code", widget: "code" })
      ],
      card_summary: ["code"]
    };
    expect(cardSummaryRows(schema, { language: "JavaScript", code: "x()\ny()" })).toEqual([
      { label: "Code", value: "JavaScript · 2 行", kind: "plain", path: "/parameters/code" }
    ]);
    expect(cardSummaryRows({ node_type: "xflow.function", fields: [field("body", { label: "Body", widget: "code" })], card_summary: ["body"] }, { body: "x()" })).toEqual([
      { label: "Body", value: "JavaScript · 1 行", kind: "plain", path: "/parameters/body" }
    ]);
  });

  it("formats duration parameters as 5 s / 72h and keeps an unparsable value", () => {
    const rows = cardSummaryRows(
      { fields: [field("timeout", { label: "Timeout", widget: "duration" })], card_summary: ["timeout"] },
      { timeout: "5s" }
    );
    expect(rows[0].value).toBe("5 s");
    expect(cardSummaryRows({ fields: [field("ttl", { label: "TTL", widget: "duration" })], card_summary: ["ttl"] }, { ttl: "72h" })[0].value).toBe("72h");
    expect(cardSummaryRows({ fields: [field("ttl", { label: "TTL", widget: "duration" })], card_summary: ["ttl"] }, { ttl: "{{ttl}}" })[0].value).toBe("{{ttl}}");
  });

  it("draws a required missing value as the spec's danger row", () => {
    const schema = {
      fields: [field("url", { label: "URL", required: true }), field("note", { label: "Note" })],
      card_summary: ["url", "note"]
    };
    expect(cardSummaryRows(schema, { note: "hi" })).toEqual([
      { label: "URL", value: "URL 未填写", kind: "missing", path: "/parameters/url" },
      { label: "Note", value: "hi", kind: "plain", path: "/parameters/note" }
    ]);
    // required_when that does not hold leaves the field optional: no row.
    const timed = {
      fields: [
        field("mode", { label: "Mode" }),
        field("duration", { label: "Duration", required_when: { param: "mode", eq: "timer" } })
      ],
      card_summary: ["mode", "duration"]
    };
    expect(cardSummaryRows(timed, { mode: "signal" })).toEqual([
      { label: "Mode", value: "signal", kind: "plain", path: "/parameters/mode" }
    ]);
    expect(cardSummaryRows(timed, { mode: "timer" })).toEqual([
      { label: "Mode", value: "timer", kind: "plain", path: "/parameters/mode" },
      { label: "Duration", value: "Duration 未填写", kind: "missing", path: "/parameters/duration" }
    ]);
  });

  it("shows a declared parameter the schema has no field for under its wire name", () => {
    const rows = cardSummaryRows({ fields: [], card_summary: ["tenant"] }, { tenant: "acme" });
    expect(rows).toEqual([{ label: "tenant", value: "acme", kind: "plain", path: "/parameters/tenant" }]);
  });

  it("clips a long value to one card line", () => {
    const long = "x".repeat(200);
    const rows = cardSummaryRows({ fields: [field("where", { label: "Where" })], card_summary: ["where"] }, { where: long });
    expect(rows[0].value).toHaveLength(80);
    expect(rows[0].value.endsWith("…")).toBe(true);
  });
});
