import { createCheckRegistry } from "@xflow/composer/core";
import { describe, expect, it } from "vitest";
import {
  expressionOnlyCheck,
  literalOnlyCheck,
  looksLikeSecret,
  nodeFormChecks,
  noTemplateInCodeCheck,
  notEvaluatedCheck,
  secretLikeCheck
} from "./checks";

const ctx = { get: () => undefined, cond: () => ({ value: true }) };
const pattern = { check: "pattern", args: { pattern: "^[a-z]+$" } };

describe("node-form checks", () => {
  it("registers alongside the composer builtins", () => {
    const registry = createCheckRegistry(nodeFormChecks);
    for (const type of ["literalOnly", "expressionOnly", "noTemplateInCode", "notEvaluated", "secretLike", "required"]) {
      expect(registry.has(type)).toBe(true);
    }
  });

  it("literalOnly runs the wrapped rule on literals only; expressionOnly on expressions only", () => {
    expect(literalOnlyCheck.fn("ABC", pattern, ctx)).toHaveLength(1);
    expect(literalOnlyCheck.fn("abc", pattern, ctx)).toHaveLength(0);
    expect(literalOnlyCheck.fn("${{ $vars.x }}", pattern, ctx)).toHaveLength(0);
    expect(expressionOnlyCheck.fn("${{ $vars.x }}", pattern, ctx)).toHaveLength(1);
    expect(expressionOnlyCheck.fn("ABC", pattern, ctx)).toHaveLength(0);
  });

  it("validates wrapped args", () => {
    expect(literalOnlyCheck.validateArgs?.(pattern)).toEqual([]);
    expect(literalOnlyCheck.validateArgs?.({ check: "nope" })).toHaveLength(1);
    expect(literalOnlyCheck.validateArgs?.({ check: "required" })).toHaveLength(1);
    expect(literalOnlyCheck.validateArgs?.({ check: "pattern", args: { pattern: "(" } })).toHaveLength(1);
    expect(literalOnlyCheck.validateArgs?.({ check: "min", args: { value: 1 }, extra: true })).toHaveLength(1);
  });

  it("template detectors flag any ${{ }} / {{ }}", () => {
    for (const check of [noTemplateInCodeCheck, notEvaluatedCheck]) {
      expect(check.fn("const s = `${{ a }}`", {}, ctx)).toHaveLength(1);
      expect(check.fn("{{ $x }}/api", {}, ctx)).toHaveLength(1);
      expect(check.fn("plain", {}, ctx)).toHaveLength(0);
      expect(check.fn(undefined, {}, ctx)).toHaveLength(0);
    }
  });

  it("secretLike flags credential-looking literals in free maps", () => {
    expect(looksLikeSecret("Bearer abcdefghijklmnop")).toBe(true);
    expect(looksLikeSecret("AKIAABCDEFGHIJKLMNOP")).toBe(true);
    expect(looksLikeSecret("Bearer ${{ $credentials.token }}")).toBe(false);
    expect(looksLikeSecret("application/json")).toBe(false);
    expect(secretLikeCheck.fn({ Authorization: "Bearer abcdefghijklmnop", Accept: "json" }, {}, ctx)).toEqual([{ detail: "Authorization" }]);
    expect(secretLikeCheck.fn({ Accept: "json" }, {}, ctx)).toEqual([]);
    expect(secretLikeCheck.fn("not a map", {}, ctx)).toEqual([]);
  });
});
