import { describe, expect, it } from "vitest";

import { localizeNodeFormSchema, localizeNodeTypes, translateNodeFormText, type NodeFormCatalog } from "./i18n";
import { zhCN } from "./locales/zh-CN";
import type { NodeFormField, NodeFormItem, NodeFormSchema, NodeTypesResponse } from "./schema";
import generated from "./testdata/node-types.generated.json";

const response = generated as unknown as NodeTypesResponse;

/** Catalog keys that come from the offline node library in index.tsx, not the fixture. */
const OFFLINE_LIBRARY_ONLY = ["If", "Timer", "External Supply", "Static Supply", "Webhook", "Kafka", "Cron", "HTTP", "gRPC"];

/** Every translatable source string of one schema, with where it came from. */
function sourceStrings(schema: NodeFormSchema): Array<{ text: string; where: string }> {
  const out: Array<{ text: string; where: string }> = [];
  const add = (text: string | null | undefined, where: string) => {
    if (typeof text === "string" && text !== "") out.push({ text, where: `${schema.node_type} ${where}` });
  };
  const walk = (fields: ReadonlyArray<NodeFormField | NodeFormItem> | null | undefined, prefix: string) => {
    for (const field of fields ?? []) {
      const at = `${prefix}${field.name ?? "[]"}`;
      add(field.label, `${at} label`);
      add(field.help, `${at} help`);
      add(typeof field.deprecated === "string" ? field.deprecated : undefined, `${at} deprecated`);
      for (const rule of field.rules ?? []) add(rule.message, `${at} rule`);
      const options = [...(field.options ?? []), ...(field.options_when ?? []).flatMap((entry) => entry.options)];
      for (const option of options) {
        add(option.label, `${at} option ${String(option.value)}`);
        add(option.description, `${at} option ${String(option.value)} description`);
      }
      walk(field.fields, `${at}.`);
      if (field.item) walk(field.item.fields, `${at}[].`);
    }
  };
  add(schema.display_name, "display_name");
  add(schema.description, "description");
  for (const group of schema.groups ?? []) {
    add(group.display_name, `group ${group.key}`);
    add(group.description, `group ${group.key} description`);
  }
  for (const port of [...(schema.ports?.inputs ?? []), ...(schema.ports?.outputs ?? [])]) add(port.display_name, `port ${port.name}`);
  walk(schema.fields, "");
  return out;
}

describe("zh-CN node-form catalog", () => {
  const sources = response.node_types.flatMap(sourceStrings);
  const keep = new Set(zhCN.keep);

  it("translates or deliberately keeps every builtin source string", () => {
    const missing = sources.filter(({ text, where }) => {
      const nodeType = where.split(" ")[0];
      return translateNodeFormText(zhCN, text, nodeType) === text && !keep.has(text);
    });
    // A failure here means a Go Descriptor gained or reworded text: add the
    // new English string to locales/zh-CN.ts (or to `keep`).
    expect(missing.map(({ text, where }) => `${where}: ${JSON.stringify(text)}`)).toEqual([]);
  });

  it("has no entry that matches no source string", () => {
    const known = new Set([...sources.map(({ text }) => text), ...OFFLINE_LIBRARY_ONLY]);
    const byTypeKeys = Object.values(zhCN.byType ?? {}).flatMap((entries) => Object.keys(entries));
    const stale = [...Object.keys(zhCN.messages), ...byTypeKeys, ...(zhCN.keep ?? [])].filter((key) => !known.has(key));
    expect(stale).toEqual([]);
  });

  it("keeps translations free of stray whitespace and empty values", () => {
    const bad = Object.entries(zhCN.messages).filter(([, value]) => value.trim() !== value || value === "");
    expect(bad).toEqual([]);
  });
});

describe("localizeNodeFormSchema", () => {
  const catalog: NodeFormCatalog = {
    locale: "x-test",
    messages: { Mode: "模式", Timer: "定时器", "Pick one": "选一个", Fast: "快", Slow: "慢", Row: "行", Main: "主端口", Basics: "基础" },
    byType: { "t.scoped": { Timer: "定时" } }
  };
  const schema: NodeFormSchema = {
    spec: "node-form/v1",
    node_type: "t.plain",
    node_version: 1,
    kind: "action",
    display_name: "Timer",
    ports: { inputs: [{ name: "main", display_name: "Main" }], outputs: [{ name: "out" }] },
    groups: [{ key: "basics", display_name: "Basics" }],
    fields: [
      {
        name: "mode",
        path: "/parameters/mode",
        type: "string",
        label: "Mode",
        help: "Pick one",
        options: [{ value: "fast", label: "Fast", description: "Untranslated" }],
        options_when: [{ when: { param: "x", eq: 1 }, options: [{ value: "slow", label: "Slow" }] }]
      },
      { name: "rows", path: "/parameters/rows", type: "array", item: { type: "object", fields: [{ name: "a", path: "a", type: "string", label: "Row" }] } }
    ]
  };

  it("translates text and leaves identity, values, and unknown text alone", () => {
    const out = localizeNodeFormSchema(schema, catalog);
    expect(out.display_name).toBe("定时器");
    expect(out.ports?.inputs?.[0]).toEqual({ name: "main", display_name: "主端口" });
    expect(out.ports?.outputs?.[0]).toEqual({ name: "out" });
    expect(out.groups?.[0]).toEqual({ key: "basics", display_name: "基础" });
    const [mode, rows] = out.fields;
    expect(mode).toMatchObject({ name: "mode", path: "/parameters/mode", label: "模式", help: "选一个" });
    expect(mode?.options).toEqual([{ value: "fast", label: "快", description: "Untranslated" }]);
    expect(mode?.options_when?.[0]?.options).toEqual([{ value: "slow", label: "慢" }]);
    expect(rows?.item?.fields?.[0]).toMatchObject({ name: "a", label: "行" });
    expect(rows).not.toHaveProperty("label");
    // The input is not mutated.
    expect(schema.fields[0]?.label).toBe("Mode");
  });

  it("prefers a byType entry for its node type only", () => {
    expect(localizeNodeFormSchema({ ...schema, node_type: "t.scoped" }, catalog).display_name).toBe("定时");
    expect(localizeNodeFormSchema(schema, catalog).display_name).toBe("定时器");
  });
});

describe("localizeNodeTypes", () => {
  it("memoises per response so consumers keyed by identity stay stable", () => {
    const first = localizeNodeTypes(response, zhCN);
    expect(localizeNodeTypes(response, zhCN)).toBe(first);
    expect(first).not.toBe(response);
    const approval = first.node_types.find((schema) => schema.node_type === "xflow.approval");
    expect(approval?.display_name).toBe("审批");
    expect(approval?.fields.find((field) => field.name === "mode")?.options?.map((option) => option.label)).toEqual(["任一", "全部", "依次"]);
  });

  it("passes an absent or malformed response through", () => {
    expect(localizeNodeTypes(undefined, zhCN)).toBeUndefined();
    const malformed = { param_validation_mode: "warn" } as unknown as NodeTypesResponse;
    expect(localizeNodeTypes(malformed, zhCN)).toBe(malformed);
  });
});
