#!/usr/bin/env node
import { readdir, readFile, writeFile, rm, mkdtemp } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const packagesDir = path.resolve(__dirname, "../packages");

// Forbidden dependency patterns. A dependency is rejected when any pattern
// matches its full name. Patterns cover exact names, scopes, or prefixes.
// ADR D9: ahooks and zustand are app-layer only; public packages must not
// declare them (not even as optional peers).
// ADR D9 (2026-07-19): @tanstack/react-query is forbidden across the whole
// web workspace; the boundary script rejects it in public packages too so
// CI catches accidental reintroduction.
const forbiddenPatterns = [
  { source: "@umijs/max", regex: /^@umijs\// },
  { source: "@ant-design/pro-layout", regex: /^@ant-design\/pro-/ },
  { source: "ahooks", regex: /^ahooks$/ },
  { source: "zustand", regex: /^zustand$/ },
  { source: "@tanstack/react-query", regex: /^@tanstack\/react-query$/ },
];

const dependencyFields = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
];

function isForbidden(dep) {
  for (const { source, regex } of forbiddenPatterns) {
    if (regex.test(dep)) {
      return source;
    }
  }
  return null;
}

async function checkPackages() {
  const entries = await readdir(packagesDir, { withFileTypes: true });
  const packages = entries.filter((e) => e.isDirectory()).map((e) => e.name);

  let failed = false;

  for (const pkg of packages) {
    const pkgJsonPath = path.join(packagesDir, pkg, "package.json");
    let pkgJson;
    try {
      pkgJson = JSON.parse(await readFile(pkgJsonPath, "utf-8"));
    } catch (err) {
      console.error(`Boundary check: cannot read ${pkgJsonPath}: ${err.message}`);
      failed = true;
      continue;
    }

    const deps = {};
    for (const field of dependencyFields) {
      if (pkgJson[field] && typeof pkgJson[field] === "object") {
        Object.assign(deps, pkgJson[field]);
      }
    }

    for (const dep of Object.keys(deps)) {
      const source = isForbidden(dep);
      if (source) {
        console.error(
          `BOUNDARY VIOLATION: public package ${pkgJson.name} declares forbidden dependency "${dep}" (matched by "${source}")`
        );
        failed = true;
      }
    }
  }

  return failed;
}

async function runNegativeTest() {
  // Each fixture maps a forbidden dependency to the pattern that should catch
  // it. Covers scopes, prefixes, and exact app-layer names (ahooks/zustand).
  const fixtures = [
    { dep: "@umijs/max", version: "4.0.0", source: "@umijs/max" },
    { dep: "@ant-design/pro-layout", version: "2.8.8", source: "@ant-design/pro-layout" },
    { dep: "ahooks", version: "3.8.4", source: "ahooks" },
    { dep: "zustand", version: "5.0.5", source: "zustand" },
    { dep: "@tanstack/react-query", version: "5.62.0", source: "@tanstack/react-query" },
  ];

  let failed = false;

  for (const { dep, version, source } of fixtures) {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "xflow-boundary-test-"));
    const pkgJsonPath = path.join(tmpDir, "package.json");

    try {
      await writeFile(
        pkgJsonPath,
        JSON.stringify({
          name: "@xflow/boundary-negative-fixture",
          optionalDependencies: { [dep]: version },
        })
      );

      const pkgJson = JSON.parse(await readFile(pkgJsonPath, "utf-8"));
      const deps = {};
      for (const field of dependencyFields) {
        if (pkgJson[field] && typeof pkgJson[field] === "object") {
          Object.assign(deps, pkgJson[field]);
        }
      }

      const matched = Object.keys(deps).some((d) => isForbidden(d) === source);
      if (matched) {
        console.log(`Negative test passed: fixture with ${dep} was rejected.`);
      } else {
        console.error(`Negative test failed: fixture with ${dep} was not rejected.`);
        failed = true;
      }
    } finally {
      await rm(tmpDir, { recursive: true, force: true });
    }
  }

  return failed;
}

async function main() {
  const [mode] = process.argv.slice(2);

  if (mode === "--self-test") {
    const failed = await runNegativeTest();
    process.exit(failed ? 1 : 0);
  }

  const packageFailed = await checkPackages();
  const negativeFailed = await runNegativeTest();

  if (packageFailed || negativeFailed) {
    process.exit(1);
  }

  console.log("Boundary check passed: no public package depends on Umi/ProComponents/TanStack Query.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
