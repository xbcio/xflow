#!/usr/bin/env node
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dependencyFields = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies"
];

const publicPackagePolicy = new Map([
  ["@xflow/core", new Set(["@xflow/typescript-config"])],
  ["@xflow/api", new Set(["@xflow/core", "@xflow/typescript-config"])],
  ["@xflow/preview", new Set(["@xflow/core", "@xflow/typescript-config"])],
  [
    "@xflow/editor",
    new Set(["@xflow/core", "@xflow/preview", "@xflow/typescript-config"])
  ]
]);

const publicForbiddenPatterns = [
  { pattern: /^@umijs(?:\/|$)/, reason: "Umi belongs to the application layer" },
  {
    pattern: /^@ant-design\/pro-(?:[^/]+)(?:\/|$)/,
    reason: "Ant Design Pro belongs to the application layer"
  }
];

const coreForbiddenPatterns = [
  { pattern: /^react(?:\/|$)/, reason: "@xflow/core must remain framework-free" },
  { pattern: /^react-dom(?:\/|$)/, reason: "@xflow/core must remain framework-free" },
  { pattern: /^@types\/react(?:-dom)?$/, reason: "@xflow/core must remain framework-free" },
  { pattern: /^antd(?:\/|$)/, reason: "@xflow/core must remain framework-free" },
  { pattern: /^@xyflow\/react(?:\/|$)/, reason: "@xflow/core must remain framework-free" }
];

async function readProjects(directory, layer) {
  const absoluteDirectory = path.join(webRoot, directory);
  const entries = await readdir(absoluteDirectory, { withFileTypes: true });
  const projects = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const manifestPath = path.join(absoluteDirectory, entry.name, "package.json");
    try {
      const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
      projects.push({ layer, manifestPath, manifest });
    } catch (error) {
      if (error?.code === "ENOENT") continue;
      throw new Error(`Cannot read ${path.relative(webRoot, manifestPath)}: ${error.message}`);
    }
  }
  return projects;
}

function declaredDependencies(manifest) {
  const dependencies = [];
  for (const field of dependencyFields) {
    for (const name of Object.keys(manifest[field] ?? {})) {
      dependencies.push({ field, name });
    }
  }
  return dependencies;
}

function validateProject(project, workspaceLayers) {
  const { layer, manifest } = project;
  const violations = [];
  const dependencies = declaredDependencies(manifest);

  if (!manifest.name) {
    return ["workspace project has no package name"];
  }

  if (layer === "package" && !publicPackagePolicy.has(manifest.name)) {
    violations.push(`public package ${manifest.name} has no reviewed dependency policy`);
  }

  for (const { field, name } of dependencies) {
    const dependencyLayer = workspaceLayers.get(name);

    if (layer === "package" && dependencyLayer === "app") {
      violations.push(`${field} ${name}: public packages cannot depend on applications`);
    }
    if (layer === "app" && dependencyLayer === "app") {
      violations.push(`${field} ${name}: applications cannot depend on another application`);
    }

    if (layer === "package" && dependencyLayer) {
      const allowed = publicPackagePolicy.get(manifest.name);
      if (allowed && !allowed.has(name)) {
        violations.push(`${field} ${name}: not allowed by the ${manifest.name} workspace policy`);
      }
    }

    if (layer === "package") {
      for (const { pattern, reason } of publicForbiddenPatterns) {
        if (pattern.test(name)) violations.push(`${field} ${name}: ${reason}`);
      }
    }

    if (manifest.name === "@xflow/core") {
      for (const { pattern, reason } of coreForbiddenPatterns) {
        if (pattern.test(name)) violations.push(`${field} ${name}: ${reason}`);
      }
    }
  }

  return violations;
}

function runNegativeSelfTest() {
  const workspaceLayers = new Map([
    ["@xflow/core", "package"],
    ["@xflow/api", "package"],
    ["@xflow/editor", "package"],
    ["@xflow/admin", "app"],
    ["@xflow/typescript-config", "tooling"]
  ]);
  const fixtures = [
    {
      description: "framework dependency in core",
      project: {
        layer: "package",
        manifest: { name: "@xflow/core", dependencies: { react: "19.1.0" } }
      }
    },
    {
      description: "unapproved public workspace dependency",
      project: {
        layer: "package",
        manifest: { name: "@xflow/api", dependencies: { "@xflow/editor": "workspace:*" } }
      }
    },
    {
      description: "Umi dependency in a public package",
      project: {
        layer: "package",
        manifest: { name: "@xflow/api", optionalDependencies: { "@umijs/max": "4.0.0" } }
      }
    },
    {
      description: "application dependency in a public package",
      project: {
        layer: "package",
        manifest: { name: "@xflow/api", devDependencies: { "@xflow/admin": "workspace:*" } }
      }
    }
  ];

  for (const fixture of fixtures) {
    if (validateProject(fixture.project, workspaceLayers).length === 0) {
      throw new Error(`Boundary self-test failed to reject ${fixture.description}`);
    }
  }

  const validApi = {
    layer: "package",
    manifest: {
      name: "@xflow/api",
      dependencies: { "@xflow/core": "workspace:*" },
      devDependencies: { "@xflow/typescript-config": "workspace:*" }
    }
  };
  const falsePositives = validateProject(validApi, workspaceLayers);
  if (falsePositives.length > 0) {
    throw new Error(`Boundary self-test rejected a valid package: ${falsePositives.join("; ")}`);
  }
}

async function main() {
  runNegativeSelfTest();

  const projects = [
    ...(await readProjects("packages", "package")),
    ...(await readProjects("apps", "app")),
    ...(await readProjects("tooling", "tooling"))
  ];
  const workspaceLayers = new Map(projects.map(({ layer, manifest }) => [manifest.name, layer]));
  let failed = false;

  for (const project of projects) {
    const violations = validateProject(project, workspaceLayers);
    for (const violation of violations) {
      failed = true;
      console.error(
        `BOUNDARY VIOLATION: ${project.manifest.name ?? "<unnamed>"}: ${violation}`
      );
    }
  }

  if (failed) process.exitCode = 1;
  else {
    console.log(
      `Boundary check passed for ${projects.length} workspace projects (negative self-test included).`
    );
  }
}

main().catch((error) => {
  console.error(`Boundary check failed: ${error.stack ?? error.message}`);
  process.exitCode = 1;
});
