#!/usr/bin/env node
// check-production-fixtures.mjs
//
// Purpose: Negative scan over production build artifacts. F0.2 (commit 8bc0d5f)
// introduced `import.meta.env.DEV` guards plus `config.mockEnabled` so that
// dev-only mock/fixture data is tree-shaken out of the prod bundle and the
// runtime flag is hard-coded to `false` in production. This script asserts
// that guard holds — i.e. the built prod bundles of workflow-editor and
// workflow-viewer do NOT contain dev-only fixture data.
//
// What it checks (markers chosen because they are fixture-only data that
// has no legitimate reason to appear in production code):
//   1. `wf-health-check` — fixture workflow ID exported from
//      apps/{workflow-editor,workflow-viewer}/src/mocks/fixtures.ts.
//   2. `workflowFixture` — exported symbol name from the same file.
//   3. "Static workflow fixture used for the health page" — fixture
//      description string. This is the strongest signal: a literal string
//      that exists only inside the fixture data, never in production code.
//
// Why these markers: under F0.2 the mock module is only imported inside an
// `import.meta.env.DEV && config.mockEnabled` branch. Vite/Rollup tree-shakes
// the dynamic `import("./fixtures")` in production, so none of these strings
// should survive. If any appear in the built bundle, the dev guard has
// regressed (e.g. someone removed the DEV guard or imported fixtures at
// module top-level). Note: `mockEnabled` itself is NOT a valid marker —
// it's a property of the runtime config object that legitimately exists in
// production (with value `false`). Similarly, `data-testid="fixture-nodes"`
// strings remain in JSX in production by design; they are not dev-only.
//
// The script runs `pnpm build` to produce fresh artifacts, then greps the
// emitted .js chunks. Exits 1 on any match.

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(__dirname, "..");

const apps = ["workflow-editor", "workflow-viewer"];

// Markers that legitimately indicate dev-only fixture data leaked into the
// prod bundle. See file header comment for rationale.
const forbiddenMarkers = [
  "wf-health-check",
  "workflowFixture",
  "Static workflow fixture used for the health page",
];

function listJsChunks(distDir) {
  const assetsDir = path.join(distDir, "assets");
  return readdir(assetsDir)
    .then((files) => files.filter((f) => f.endsWith(".js")).map((f) => path.join(assetsDir, f)))
    .catch((err) => {
      if (err.code === "ENOENT") return [];
      throw err;
    });
}

async function scanApp(app) {
  const distDir = path.join(webRoot, "apps", app, "dist");
  const chunks = await listJsChunks(distDir);
  if (chunks.length === 0) {
    console.error(`production-fixtures: no built .js chunks found for ${app} (dist/assets missing). Run \`pnpm build\` first.`);
    return [{ app, marker: "<no-chunks>", chunk: "<none>" }];
  }

  const hits = [];
  for (const chunk of chunks) {
    const content = await readFile(chunk, "utf-8");
    for (const marker of forbiddenMarkers) {
      if (content.includes(marker)) {
        hits.push({ app, marker, chunk: path.relative(webRoot, chunk) });
      }
    }
  }
  return hits;
}

function buildAll() {
  // Use the workspace build so all apps' dist/ are fresh. pnpm build runs
  // turbo which is idempotent and respects the package graph.
  console.log("production-fixtures: building production bundles (pnpm build)...");
  execSync("pnpm build", { cwd: webRoot, stdio: "inherit" });
}

async function main() {
  if (process.env.PRODUCTION_FIXTURES_SKIP_BUILD === "1") {
    console.log("production-fixtures: PRODUCTION_FIXTURES_SKIP_BUILD=1, skipping build step.");
  } else {
    buildAll();
  }

  let failed = false;
  for (const app of apps) {
    const hits = await scanApp(app);
    if (hits.length === 0) {
      console.log(`production-fixtures: ${app} OK (no dev-only fixture markers in prod bundle).`);
      continue;
    }
    for (const { marker, chunk } of hits) {
      console.error(`production-fixtures: REGRESSION in ${app}: marker "${marker}" found in ${chunk}`);
    }
    failed = true;
  }

  if (failed) {
    console.error("production-fixtures: dev-only fixture data leaked into production bundle. Check import.meta.env.DEV guards in apps/*/src/pages/* and apps/*/src/mocks/index.ts.");
    process.exit(1);
  }
  console.log("production-fixtures: all prod bundles clean of dev-only fixture markers.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
