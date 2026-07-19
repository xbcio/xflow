#!/usr/bin/env node
import { readdir, readFile, writeFile, rm, mkdtemp } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const packagesDir = path.resolve(__dirname, "../packages");

// Forbidden dependency patterns. A dependency is rejected when any pattern
// matches its full name. Patterns cover exact names, scopes, or prefixes.
const forbiddenPatterns = [
  { source: "@umijs/max", regex: /^@umijs\// },
  { source: "@ant-design/pro-layout", regex: /^@ant-design\/pro-/ },
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
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "xflow-boundary-test-"));
  const pkgJsonPath = path.join(tmpDir, "package.json");

  try {
    await writeFile(
      pkgJsonPath,
      JSON.stringify({
        name: "@xflow/boundary-negative-fixture",
        optionalDependencies: {
          "@umijs/max": "4.0.0",
        },
      })
    );

    // Read the fixture through the same logic used for real packages.
    const pkgJson = JSON.parse(await readFile(pkgJsonPath, "utf-8"));
    const deps = {};
    for (const field of dependencyFields) {
      if (pkgJson[field] && typeof pkgJson[field] === "object") {
        Object.assign(deps, pkgJson[field]);
      }
    }

    for (const dep of Object.keys(deps)) {
      if (isForbidden(dep)) {
        console.log("Negative test passed: fixture with @umijs/max was rejected.");
        return false;
      }
    }

    console.error("Negative test failed: fixture with @umijs/max was not rejected.");
    return true;
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
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

  console.log("Boundary check passed: no public package depends on Umi/ProComponents.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
