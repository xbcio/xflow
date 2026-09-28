// Doc B §7.2 / B2 acceptance: only kernel-json-render and react/defaultKernel
// may import @json-render/*; components (composer/form, testing components)
// and kernel-native never import a kernel. composer/form reaches the engine
// only through the component contract (react) and core.

import { describe, expect, it } from "vitest";

type Glob = (pattern: string, options: { query: string; import: string; eager: true }) => Record<string, string>;
const sources = (import.meta as unknown as { glob: Glob }).glob("./**/*.{ts,tsx}", {
  query: "?raw",
  import: "default",
  eager: true
});

const importsOf = (text: string) =>
  [...text.matchAll(/(?:from|import)\s*\(?\s*["']([^"']+)["']/g)].map((match) => match[1]);

const JSON_RENDER_ALLOWED = new Set(["./kernel-json-render/index.tsx", "./react/defaultKernel.ts"]);

describe("composer import boundaries", () => {
  const files = Object.entries(sources).filter(
    ([file]) => !file.endsWith(".test.ts") && !file.endsWith(".test.tsx") && !file.endsWith(".testkit.tsx")
  );

  it("scans every subpath", () => {
    for (const dir of ["core", "react", "form", "kernel-native", "kernel-json-render", "testing"]) {
      expect(files.some(([file]) => file.startsWith(`./${dir}/`)), dir).toBe(true);
    }
  });

  it("only kernel-json-render and react/defaultKernel import @json-render/*", () => {
    const offenders = files
      .filter(([file, text]) => !JSON_RENDER_ALLOWED.has(file) && importsOf(text).some((spec) => spec.startsWith("@json-render/")))
      .map(([file]) => file);
    expect(offenders).toEqual([]);
    expect(importsOf(sources["./kernel-json-render/index.tsx"])).toContain("@json-render/react");
  });

  it("kernel-native and components do not import kernels; react imports kernels only via defaultKernel", () => {
    const violations: string[] = [];
    for (const [file, text] of files) {
      for (const spec of importsOf(text)) {
        const kernelImport = /kernel-(native|json-render)/.test(spec) || spec.startsWith("@json-render/");
        if (!kernelImport) continue;
        if (file.startsWith("./kernel-native/") && spec.includes("json-render")) violations.push(`${file}: ${spec}`);
        if (file === "./testing/components.tsx") violations.push(`${file}: ${spec}`);
        if (file.startsWith("./form/")) violations.push(`${file}: ${spec}`);
        if (file.startsWith("./react/") && file !== "./react/defaultKernel.ts") violations.push(`${file}: ${spec}`);
        if (file.startsWith("./core/")) violations.push(`${file}: ${spec}`);
      }
    }
    expect(violations).toEqual([]);
  });

  it("composer/form imports only react, antd, icons, zod and the core/react subpaths", () => {
    const allowed = /^(react|antd|@ant-design\/icons|zod|\.\.\/core|\.\.\/react|\.\/.+)$/;
    const violations = files
      .filter(([file]) => file.startsWith("./form/"))
      .flatMap(([file, text]) => importsOf(text).filter((spec) => !allowed.test(spec)).map((spec) => `${file}: ${spec}`));
    expect(violations).toEqual([]);
  });
});
