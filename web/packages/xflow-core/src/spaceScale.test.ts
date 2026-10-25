import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

/**
 * Stage 1d spacing guard. Every padding/margin/gap value in the three shipped
 * sheets must come from the `--xflow-space-*` scale; the only literals allowed
 * are `0` and the `1px` half of a border compensation, written as
 * `calc(var(--xflow-space-N) - 1px)`. Values are matched by property, so
 * layout sizes (`width`, `height`, `top`, …) and `inset` positioning stay out
 * of scope. `web/packages/composer/src/form/styles.css` (node form) is not
 * listed: its spacing was excluded from this stage.
 */
const sheets = [
  ["editor", "../../xflow-editor/src/styles.css"],
  ["preview", "../../xflow-preview/src/styles.css"],
  ["admin layout", "../../../apps/xflow-admin/src/layouts/admin.css"],
  ["admin executions", "../../../apps/xflow-admin/src/pages/executions/execution-row.css"],
  ["admin node types", "../../../apps/xflow-admin/src/pages/node-types/node-types.css"],
  ["admin overview", "../../../apps/xflow-admin/src/pages/overview/overview.css"],
  ["admin ui preview", "../../../apps/xflow-admin/src/pages/ui-preview/ui-preview.css"],
  ["admin workflows", "../../../apps/xflow-admin/src/pages/workflows/workflow-row.css"],
] as const;

const SPACING = /^(?:padding|margin|gap|row-gap|column-gap)(?:-[a-z-]+)?$/;

interface Declaration {
  property: string;
  value: string;
  line: number;
}

/** Blank out comments, keeping newlines so reported line numbers stay true. */
function withoutComments(css: string): string {
  return css.replace(/\/\*[\s\S]*?\*\//g, (comment) => comment.replace(/[^\n]/g, " "));
}

/** `var()` contents never carry a spacing literal; ignore them while scanning. */
function withoutVarCalls(value: string): string {
  let out = "";
  for (let i = 0; i < value.length; ) {
    if (value.startsWith("var(", i)) {
      let depth = 0;
      let j = i;
      for (; j < value.length; j++) {
        if (value[j] === "(") depth++;
        else if (value[j] === ")" && --depth === 0) {
          j++;
          break;
        }
      }
      out += " ".repeat(j - i);
      i = j;
    } else {
      out += value[i++];
    }
  }
  return out;
}

function declarationsOf(css: string): Declaration[] {
  const source = withoutComments(css);
  const out: Declaration[] = [];
  // The closing delimiter stays out of the match so adjacent declarations on
  // one line (`padding: 4px; gap: 8px;`) are both seen. A match only counts
  // when it starts a declaration, which rules out selectors and at-rules.
  const pattern = /([a-zA-Z-]+)\s*:\s*([^;{}]*)(?=[;}])/g;
  for (const match of source.matchAll(pattern)) {
    const property = match[1];
    if (property.startsWith("--") || !SPACING.test(property)) continue;
    let before = match.index - 1;
    while (before >= 0 && /\s/.test(source[before])) before--;
    if (before >= 0 && !"{;}".includes(source[before])) continue;
    const line = source.slice(0, match.index).split("\n").length;
    out.push({ property, value: match[2].trim(), line });
  }
  return out;
}

describe("spacing scale guard (stage 1d)", () => {
  for (const [name, path] of sheets) {
    it(`${name}: every padding/margin/gap literal is 0, 1px or a space token`, () => {
      const css = readFileSync(new URL(path, import.meta.url), "utf8");
      const declarations = declarationsOf(css);
      expect(declarations.length, `${name} has spacing declarations`).toBeGreaterThan(0);
      const raw = declarations.flatMap(({ property, value, line }) => {
        const numbers = [...withoutVarCalls(value).matchAll(/(\d*\.?\d+)px/g)].map((m) => Number(m[1]));
        const bare = numbers.filter((number) => number !== 0 && number !== 1);
        return bare.length > 0 ? [`${name}:${line} ${property}: ${value}`] : [];
      });
      expect(raw).toEqual([]);
    });
  }

  it("scans the whole migration, not a handful of declarations", () => {
    // A broken parser must not turn the guard into a no-op: the eight sheets
    // carried 529 direct spacing declarations before the migration.
    const total = sheets.reduce(
      (sum, [, path]) => sum + declarationsOf(readFileSync(new URL(path, import.meta.url), "utf8")).length,
      0
    );
    expect(total).toBeGreaterThan(500);
  });
});
