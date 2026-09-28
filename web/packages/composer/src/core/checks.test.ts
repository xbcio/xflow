import { describe, expect, it } from "vitest";
import { createCheckRegistry, defineCheck, type CheckContext } from "./checks";
import { FORMATS, isCron, isGoDuration, isHostPort, isUrl } from "./formats";
import { getPointer } from "./pointer";

const registry = createCheckRegistry();

function run(type: string, value: unknown, args: Record<string, unknown> = {}, state: unknown = {}): boolean {
  const ctx: CheckContext = { get: (p) => getPointer(state, p), cond: () => ({ value: true }) };
  return registry.get(type)!.fn(value, args, ctx).length === 0;
}

describe("built-in checks", () => {
  it("required uses the 'set' semantics of Doc A", () => {
    expect(run("required", undefined)).toBe(false);
    expect(run("required", null)).toBe(false);
    expect(run("required", "")).toBe(false);
    expect(run("required", 0)).toBe(true);
    expect(run("required", false)).toBe(true);
    expect(run("required", [])).toBe(true);
  });

  it("non-required checks pass unset values", () => {
    for (const type of ["min", "max", "minLength", "maxLength", "pattern", "format", "minItems", "maxItems", "uniqueItems", "json"]) {
      expect(run(type, undefined, { value: 1, pattern: "x", format: "duration" })).toBe(true);
      expect(run(type, "", { value: 1, pattern: "x", format: "duration" })).toBe(true);
    }
  });

  it("min / max", () => {
    expect(run("min", 1, { value: 2 })).toBe(false);
    expect(run("min", 2, { value: 2 })).toBe(true);
    expect(run("max", 3, { value: 2 })).toBe(false);
    expect(run("max", 2.0, { value: 2 })).toBe(true);
  });

  it("minLength / maxLength count code points", () => {
    expect(run("minLength", "ab", { value: 3 })).toBe(false);
    expect(run("maxLength", "😀😀", { value: 2 })).toBe(true);
    expect(run("maxLength", "abc", { value: 2 })).toBe(false);
  });

  it("pattern", () => {
    expect(run("pattern", "abc", { pattern: "^[a-z]+$" })).toBe(true);
    expect(run("pattern", "ab1", { pattern: "^[a-z]+$" })).toBe(false);
  });

  it("minItems / maxItems / uniqueItems", () => {
    expect(run("minItems", [1], { value: 2 })).toBe(false);
    expect(run("maxItems", [1, 2, 3], { value: 2 })).toBe(false);
    expect(run("uniqueItems", [1, 2, 1.0])).toBe(false);
    expect(run("uniqueItems", [{ a: 1 }, { a: 1 }])).toBe(false);
    expect(run("uniqueItems", [1, "1"])).toBe(true);
  });

  it("json", () => {
    expect(run("json", '{"a":1}')).toBe(true);
    expect(run("json", "{a:1}")).toBe(false);
  });

  it("oneOf counts set paths per mode", () => {
    const paths = ["/code", "/digest"];
    expect(run("oneOf", undefined, { paths }, { code: "x" })).toBe(true);
    expect(run("oneOf", undefined, { paths }, { code: "x", digest: "y" })).toBe(false);
    expect(run("oneOf", undefined, { paths }, { code: "" })).toBe(false);
    expect(run("oneOf", undefined, { paths, mode: "at_most" }, {})).toBe(true);
    expect(run("oneOf", undefined, { paths, mode: "at_most" }, { code: 1, digest: 2 })).toBe(false);
    expect(run("oneOf", undefined, { paths, mode: "at_least" }, {})).toBe(false);
    expect(run("oneOf", undefined, { paths, mode: "at_least" }, { code: 1, digest: 2 })).toBe(true);
    expect(registry.get("oneOf")!.evaluateWhenHidden).toBe(true);
  });

  it("format allowExpression lets template values through", () => {
    expect(run("format", "{{ $config.t }}", { format: "duration" })).toBe(false);
    expect(run("format", "{{ $config.t }}", { format: "duration", allowExpression: true })).toBe(true);
  });

  it("defineCheck registers host checks and rejects duplicates", () => {
    const custom = defineCheck("even", (value) => (typeof value === "number" && value % 2 ? [{}] : []));
    const reg = createCheckRegistry([custom]);
    expect(reg.get("even")).toBe(custom);
    expect(reg.get("required")).toBeDefined();
    expect(() => createCheckRegistry([custom, custom])).toThrow(/duplicate/);
  });
});

describe("formats", () => {
  it("duration follows Go time.ParseDuration syntax", () => {
    for (const ok of ["0", "1s", "1.5h", "300ms", "-1m30s", "+2h45m", "1us", "1µs", "1μs", "1ns", ".5s", "1.s"]) {
      expect(isGoDuration(ok), ok).toBe(true);
    }
    for (const bad of ["", "1", "s", "1d", "1 s", "1h-30m", "--1s", "1.5", "one"]) {
      expect(isGoDuration(bad), bad).toBe(false);
    }
  });

  it("cron accepts basic 5/6-field expressions and descriptors", () => {
    for (const ok of ["* * * * *", "*/5 0-6 1,15 JAN-MAR MON-FRI", "0 0 * * 7", "0 30 9 * * ?", "@daily", "@every 1h30m"]) {
      expect(isCron(ok), ok).toBe(true);
    }
    for (const bad of ["* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "@often", "@every 5", "*/0 * * * *", "? * * * *"]) {
      expect(isCron(bad), bad).toBe(false);
    }
  });

  it("expression means 'contains a template', recursively", () => {
    expect(FORMATS.expression("{{ $config.x }}/api")).toBe(true);
    expect(FORMATS.expression("${{ secrets.a }}")).toBe(true);
    expect(FORMATS.expression({ a: ["x", "{{ y }}"] })).toBe(true);
    expect(FORMATS.expression("plain")).toBe(false);
    expect(FORMATS.expression("{ single }")).toBe(false);
  });

  it("sha256-digest", () => {
    expect(FORMATS["sha256-digest"]("sha256:" + "a".repeat(64))).toBe(true);
    expect(FORMATS["sha256-digest"]("sha256:" + "A".repeat(64))).toBe(false);
    expect(FORMATS["sha256-digest"]("sha256:abc")).toBe(false);
  });

  it("url", () => {
    expect(isUrl("https://example.com/a?b=1")).toBe(true);
    expect(isUrl("redis://localhost:6379")).toBe(true);
    expect(isUrl("example.com")).toBe(false);
    expect(isUrl("http://")).toBe(false);
  });

  it("host-port", () => {
    for (const ok of ["localhost:80", "a.b-c.example:65535", "10.0.0.1:6379", "[::1]:8080"]) expect(isHostPort(ok), ok).toBe(true);
    for (const bad of ["localhost", ":80", "host:0", "host:65536", "host:x", "::1:80", "-bad:80"]) expect(isHostPort(bad), bad).toBe(false);
  });

  it("json format", () => {
    expect(FORMATS.json("[1,2]")).toBe(true);
    expect(FORMATS.json("[1,")).toBe(false);
  });
});
