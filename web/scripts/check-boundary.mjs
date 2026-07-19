#!/usr/bin/env node
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const packagesDir = path.resolve(__dirname, "../packages");

const forbidden = ["@umijs/max", "@ant-design/pro-layout"];

async function main() {
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

    const deps = {
      ...pkgJson.dependencies,
      ...pkgJson.devDependencies,
      ...pkgJson.peerDependencies,
    };

    for (const dep of Object.keys(deps)) {
      if (forbidden.includes(dep)) {
        console.error(
          `BOUNDARY VIOLATION: public package ${pkgJson.name} declares forbidden dependency "${dep}"`
        );
        failed = true;
      }
    }
  }

  if (failed) {
    process.exit(1);
  }

  console.log("Boundary check passed: no public package depends on Umi/ProLayout.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
