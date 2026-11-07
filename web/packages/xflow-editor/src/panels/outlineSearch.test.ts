import { describe, expect, it } from "vitest";
import type { OutlineRow } from "./outline";
import { filterOutlineRows } from "./outlineSearch";

const section = (label: string, count: number): OutlineRow => ({
  kind: "section",
  id: `section:${label}`,
  section: "entry",
  label,
  count,
  category: "unknown"
});
const item = (name: string, label = name): OutlineRow => ({
  kind: "item",
  id: `item:${name}`,
  key: name,
  name,
  label,
  type: "xflow.http",
  category: "action",
  indent: 0,
  disabled: false
});
const port = (name: string): OutlineRow => ({ kind: "port", id: `port:${name}`, port: name, indent: 0 });

/** Compact form: sections as `# label`, ports as `@port`, items by name. */
const shape = (rows: OutlineRow[]): string[] =>
  rows.map((row) => (row.kind === "section" ? `# ${row.label}` : row.kind === "port" ? `@${row.port}` : row.name));

const rows: OutlineRow[] = [
  section("入口", 1),
  item("start"),
  section("流程", 2),
  item("fetch", "拉取档案"),
  item("check"),
  section("结束", 1),
  item("finish")
];

describe("filterOutlineRows", () => {
  it("keeps every row for an empty query", () => {
    expect(filterOutlineRows(rows, "  ").matched).toBe(true);
    expect(shape(filterOutlineRows(rows, "  ").rows)).toEqual(shape(rows));
  });

  it("keeps the whole section when its own name matches", () => {
    const result = filterOutlineRows(rows, "流程");
    expect(result.matched).toBe(true);
    expect(shape(result.rows)).toEqual(["# 流程", "fetch", "check"]);
  });

  it("keeps only matching members, and is case-insensitive", () => {
    expect(shape(filterOutlineRows(rows, "FETCH").rows)).toEqual(["# 流程", "fetch"]);
    // The display label is searchable too.
    expect(shape(filterOutlineRows(rows, "拉取").rows)).toEqual(["# 流程", "fetch"]);
    // A section with no matching member disappears.
    expect(shape(filterOutlineRows(rows, "start").rows)).toEqual(["# 入口", "start"]);
  });

  it("keeps a port heading only while a matching node still follows it", () => {
    const branched: OutlineRow[] = [
      section("流程", 3),
      port("true"),
      item("vip"),
      item("vip2"),
      port("false"),
      item("normal")
    ];
    expect(shape(filterOutlineRows(branched, "vip2").rows)).toEqual(["# 流程", "@true", "vip2"]);
    expect(shape(filterOutlineRows(branched, "normal").rows)).toEqual(["# 流程", "@false", "normal"]);
  });

  it("reports no match when nothing survives", () => {
    const result = filterOutlineRows(rows, "missing");
    expect(result.matched).toBe(false);
    expect(result.rows).toEqual([]);
  });
});
