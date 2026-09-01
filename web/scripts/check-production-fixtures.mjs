#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { readdir, readFile, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const adminSource = path.join(webRoot, "apps/xflow-admin/src");
const packagesDirectory = path.join(webRoot, "packages");
const adminDist = path.join(webRoot, "apps/xflow-admin/dist");
const sourceExtensions = new Set([".js", ".jsx", ".mjs", ".ts", ".tsx"]);
const forbiddenBundleMarkers = [
  "__XFLOW_DEV_FIXTURE__",
  "__XFLOW_DEV_ONLY__",
  "__XFLOW_MOCK_RESPONSE__",
  "__XFLOW_MOCK_API__"
];

async function listFiles(directory, predicate) {
  const files = [];
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const absolutePath = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await listFiles(absolutePath, predicate)));
    else if (entry.isFile() && predicate(absolutePath)) files.push(absolutePath);
  }
  return files;
}

function isTestSource(sourceDirectory, filePath) {
  const relative = path.relative(sourceDirectory, filePath);
  return (
    /(?:^|[/\\])__tests__(?:[/\\]|$)/.test(relative) ||
    /\.(?:test|spec)\.[cm]?[jt]sx?$/.test(relative)
  );
}

function importSpecifiers(source) {
  const specifiers = [];
  const staticImport = /\b(?:import|export)\s+(?:type\s+)?(?:[^"'`;]*?\s+from\s*)?["']([^"']+)["']/g;
  const dynamicImport = /\bimport\s*\(\s*["']([^"']+)["']/g;
  for (const pattern of [staticImport, dynamicImport]) {
    for (const match of source.matchAll(pattern)) specifiers.push(match[1]);
  }
  return specifiers;
}

function isFixtureImport(specifier) {
  return specifier.split(/[\\/]/).some((segment) => /^(?:mocks?|fixtures?)$/i.test(segment));
}

function runSelfTest() {
  const fixtureSource = `
    import workflow from "@/fixtures/workflow";
    const mock = import("../mocks/api");
  `;
  const detected = importSpecifiers(fixtureSource).filter(isFixtureImport);
  if (detected.length !== 2 || isFixtureImport("@/features/mockery")) {
    throw new Error("production-fixtures self-test failed");
  }
}

async function productionSourceDirectories() {
  const packageEntries = await readdir(packagesDirectory, { withFileTypes: true });
  return [
    adminSource,
    ...packageEntries
      .filter((entry) => entry.isDirectory())
      .map((entry) => path.join(packagesDirectory, entry.name, "src"))
  ];
}

async function scanSourceImports() {
  const violations = [];

  for (const sourceDirectory of await productionSourceDirectories()) {
    const files = await listFiles(
      sourceDirectory,
      (filePath) =>
        sourceExtensions.has(path.extname(filePath)) && !isTestSource(sourceDirectory, filePath)
    );
    for (const filePath of files) {
      const source = await readFile(filePath, "utf8");
      for (const specifier of importSpecifiers(source)) {
        if (isFixtureImport(specifier)) violations.push({ filePath, specifier });
      }
    }
  }
  return violations;
}

async function buildProductionBundle() {
  if (process.env.PRODUCTION_FIXTURES_SKIP_BUILD === "1") {
    console.log("production-fixtures: skipping build by explicit environment request");
    return;
  }

  // Remove the prior output first so a Turbo cache restore cannot leave stale
  // development chunks beside the current production artifacts.
  await rm(adminDist, { recursive: true, force: true });
  console.log("production-fixtures: building current production bundle...");
  const executable = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
  const result = spawnSync(executable, ["build"], {
    cwd: webRoot,
    env: process.env,
    stdio: "inherit"
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`pnpm build exited with status ${result.status ?? "unknown"}`);
  }
}

async function scanBundle() {
  const chunks = await listFiles(adminDist, (filePath) => filePath.endsWith(".js"));
  if (chunks.length === 0) {
    throw new Error(
      `no JavaScript chunks found under ${path.relative(webRoot, adminDist)} after production build`
    );
  }

  const violations = [];
  for (const chunk of chunks) {
    const source = await readFile(chunk, "utf8");
    for (const marker of forbiddenBundleMarkers) {
      if (source.includes(marker)) violations.push({ chunk, marker });
    }
  }
  return { chunks, violations };
}

async function main() {
  runSelfTest();

  const importViolations = await scanSourceImports();
  for (const { filePath, specifier } of importViolations) {
    console.error(
      `PRODUCTION FIXTURE VIOLATION: ${path.relative(webRoot, filePath)} imports ${JSON.stringify(specifier)}`
    );
  }
  if (importViolations.length > 0) {
    throw new Error("production source imports mock or fixture modules");
  }

  await buildProductionBundle();
  const { chunks, violations } = await scanBundle();
  for (const { chunk, marker } of violations) {
    console.error(
      `PRODUCTION FIXTURE VIOLATION: ${path.relative(webRoot, chunk)} contains ${marker}`
    );
  }
  if (violations.length > 0) throw new Error("dev/mock markers leaked into the production bundle");

  console.log(
    `production-fixtures: ${chunks.length} JavaScript chunks scanned; no fixture leakage found.`
  );
}

main().catch((error) => {
  console.error(`production-fixtures: ${error.message}`);
  process.exitCode = 1;
});
