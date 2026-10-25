import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// Radius values live in exactly one place: the `--xflow-radius-*` scale in
// @xflow/core (mirrored into AntD tokens through `foundationTokens`). A literal
// pixel radius in CSS or a style prop silently forks that scale, so this guard
// scans every host package and fails with the offending file and line.
//
// Allowed by design:
//   - `0` (a flat edge, e.g. a joined split button's inner corner)
//   - `50%` (a circle drawn from its own box)
//   - `var(...)` fallbacks that name a token (`var(--xflow-radius-sm, 6px)`)
//   - the token declarations themselves (packages/xflow-core/src/tokens.css)
//   - the documented whitelist below.

const WEB_ROOT = join(import.meta.dirname, "../../..");
const ROOTS = [
  "packages/xflow-editor/src",
  "packages/xflow-preview/src",
  "packages/composer/src",
  "packages/xflow-core/src",
  "apps/xflow-admin/src"
];
const SKIP = /(?:\.test\.|\.spec\.|\.snap$|__snapshots__|\/testdata\/|\/dist\/|\/node_modules\/)/;

const ALLOW = [
  {
    file: "apps/xflow-admin/src/pages/ui-preview/ui-preview.css",
    value: "2px",
    reason:
      "The minimap's node silhouettes are 2px in the design mockup (.mm-node); the radius scale has no tier between 0 and xs."
  }
];

interface Violation {
  file: string;
  line: number;
  value: string;
  declaration: string;
}

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (SKIP.test(full)) continue;
    if (statSync(full).isDirectory()) walk(full, out);
    else if (/\.(?:css|ts|tsx)$/.test(full)) out.push(full);
  }
  return out;
}

/** Strip `var(...)` (including nested fallbacks) so only literal values remain. */
function stripVars(value: string): string {
  let next = value;
  let previous = "";
  while (next !== previous) {
    previous = next;
    next = next.replace(/var\([^()]*\)/g, "");
  }
  return next;
}

function lineOf(text: string, index: number): number {
  return text.slice(0, index).split("\n").length;
}

function isAllowed(file: string, value: string): boolean {
  return ALLOW.some((entry) => entry.file === file && entry.value === value);
}

describe("radius scale guard", () => {
  it("has no literal pixel border radius outside the token scale", () => {
    const violations: Violation[] = [];
    for (const root of ROOTS) {
      for (const absolute of walk(join(WEB_ROOT, root))) {
        const file = relative(WEB_ROOT, absolute).replaceAll("\\", "/");
        if (file === "packages/xflow-core/src/tokens.css") continue;
        const text = readFileSync(absolute, "utf8");

        for (const match of text.matchAll(/border(?:[a-z-]+)?-radius\s*:\s*([^;]+);/g)) {
          const literal = stripVars(match[1]);
          for (const px of literal.matchAll(/(-?\d+(?:\.\d+)?)px/g)) {
            const value = `${px[1]}px`;
            if (Number.parseFloat(px[1]) === 0 || isAllowed(file, value)) continue;
            violations.push({ file, line: lineOf(text, match.index ?? 0), value, declaration: match[0].trim() });
          }
        }

        for (const match of text.matchAll(/borderRadius\s*:\s*(\d+(?:\.\d+)?)\b/g)) {
          const value = `${match[1]}px`;
          if (Number.parseFloat(match[1]) === 0 || isAllowed(file, value)) continue;
          violations.push({ file, line: lineOf(text, match.index ?? 0), value, declaration: match[0].trim() });
        }

        for (const match of text.matchAll(/borderRadius\s*:\s*["'](\d+(?:\.\d+)?)px["']/g)) {
          const value = `${match[1]}px`;
          if (Number.parseFloat(match[1]) === 0 || isAllowed(file, value)) continue;
          violations.push({ file, line: lineOf(text, match.index ?? 0), value, declaration: match[0].trim() });
        }
      }
    }

    const detail = violations.map((v) => `${v.file}:${v.line}  ${v.declaration}  -> use a --xflow-radius-* token`).join("\n");
    expect(violations, `Hard-coded pixel radii found:\n${detail}`).toEqual([]);
  });
});
